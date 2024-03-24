/*
 * Created on Tue Sep 05 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use {
    super::{
        MARKER_ACTUAL_BATCH_EVENT, MARKER_BATCH_CLOSED, MARKER_BATCH_REOPEN, MARKER_END_OF_BATCH,
        MARKER_RECOVERY_EVENT,
    },
    crate::{
        engine::{
            core::{
                index::{DcFieldIndex, PrimaryIndexKey, Row},
                model::{delta::DeltaVersion, ModelData},
            },
            data::{cell::Datacell, tag::TagUnique},
            error::{RuntimeResult, StorageError},
            idx::{MTIndex, STIndex, STIndexSeq},
            storage::{
                common::interface::fs::File,
                common_encoding::r1::{
                    obj::cell::{self, StorageCellTypeID},
                    DataSource,
                },
                v1::raw::rw::{SDSSFileIO, TrackedReader},
            },
        },
        util::compiler::TaggedEnum,
    },
    std::{
        collections::{hash_map::Entry as HMEntry, HashMap},
        mem::ManuallyDrop,
    },
};

#[derive(Debug, PartialEq)]
pub(in crate::engine::storage::v1) struct DecodedBatchEvent {
    txn_id: DeltaVersion,
    pk: PrimaryIndexKey,
    kind: DecodedBatchEventKind,
}

impl DecodedBatchEvent {
    pub(in crate::engine::storage::v1) const fn new(
        txn_id: u64,
        pk: PrimaryIndexKey,
        kind: DecodedBatchEventKind,
    ) -> Self {
        Self {
            txn_id: DeltaVersion::__new(txn_id),
            pk,
            kind,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(in crate::engine::storage::v1) enum DecodedBatchEventKind {
    Delete,
    Insert(Vec<Datacell>),
    Update(Vec<Datacell>),
}

#[derive(Debug, PartialEq)]
pub(in crate::engine::storage::v1) struct NormalBatch {
    events: Vec<DecodedBatchEvent>,
    schema_version: u64,
}

impl NormalBatch {
    pub(in crate::engine::storage::v1) fn new(
        events: Vec<DecodedBatchEvent>,
        schema_version: u64,
    ) -> Self {
        Self {
            events,
            schema_version,
        }
    }
}

enum Batch {
    RecoveredFromerror,
    Normal(NormalBatch),
    FinishedEarly(NormalBatch),
    BatchClosed,
}

pub struct DataBatchRestoreDriver {
    f: TrackedReader,
}

impl DataBatchRestoreDriver {
    pub fn new(f: SDSSFileIO<File>) -> RuntimeResult<Self> {
        Ok(Self {
            f: TrackedReader::new(f.into_buffered_reader())?,
        })
    }
    pub fn into_file(self) -> SDSSFileIO<File> {
        self.f.into_inner_file()
    }
    pub(in crate::engine::storage::v1) fn read_data_batch_into_model(
        &mut self,
        model: &ModelData,
    ) -> RuntimeResult<()> {
        self.read_all_batches_and_for_each(|batch| {
            // apply the batch
            Self::apply_batch(model, batch)
        })
    }
}

impl DataBatchRestoreDriver {
    fn read_all_batches_and_for_each(
        &mut self,
        mut f: impl FnMut(NormalBatch) -> RuntimeResult<()>,
    ) -> RuntimeResult<()> {
        // begin
        let mut closed = false;
        while !self.f.is_eof() && !closed {
            self.f.__reset_checksum();
            // try to decode this batch
            let Ok(batch) = self.read_batch() else {
                self.attempt_recover_data_batch()?;
                continue;
            };
            // see what happened when decoding it
            let finished_early = matches!(batch, Batch::FinishedEarly { .. });
            let batch = match batch {
                Batch::RecoveredFromerror => {
                    // there was an error, but it was safely "handled" because of a recovery byte mark
                    continue;
                }
                Batch::FinishedEarly(batch) | Batch::Normal(batch) => batch,
                Batch::BatchClosed => {
                    // the batch was closed; this means that we probably are done with this round; but was it re-opened?
                    closed = self.handle_reopen_is_actual_close()?;
                    continue;
                }
            };
            // now we need to read the batch summary
            let Ok(actual_commit) = self.read_batch_summary(finished_early) else {
                self.attempt_recover_data_batch()?;
                continue;
            };
            // check if we have the expected batch size
            if batch.events.len() as u64 != actual_commit {
                // corrupted
                self.attempt_recover_data_batch()?;
                continue;
            }
            f(batch)?;
            // apply the batch
        }
        if closed {
            if self.f.is_eof() {
                // that was the last batch
                return Ok(());
            }
        }
        // nope, this is a corrupted file
        Err(StorageError::V1DataBatchDecodeCorruptedBatchFile.into())
    }
    fn handle_reopen_is_actual_close(&mut self) -> RuntimeResult<bool> {
        if self.f.is_eof() {
            // yup, it was closed
            Ok(true)
        } else {
            // maybe not
            if self.f.read_byte()? == MARKER_BATCH_REOPEN {
                // driver was closed, but reopened
                Ok(false)
            } else {
                // that's just a nice bug
                Err(StorageError::V1DataBatchDecodeCorruptedBatchFile.into())
            }
        }
    }
}

impl DataBatchRestoreDriver {
    fn apply_batch(
        m: &ModelData,
        NormalBatch {
            events,
            schema_version,
        }: NormalBatch,
    ) -> RuntimeResult<()> {
        // NOTE(@ohsayan): current complexity is O(n) which is good enough (in the future I might revise this to a fancier impl)
        // pin model
        let g = unsafe { crossbeam_epoch::unprotected() };
        let mut pending_delete = HashMap::new();
        let p_index = m.primary_index().__raw_index();
        // scan rows
        for DecodedBatchEvent { txn_id, pk, kind } in events {
            match kind {
                DecodedBatchEventKind::Insert(new_row) | DecodedBatchEventKind::Update(new_row) => {
                    // this is more like a "newrow"
                    match p_index.mt_get_element(&pk, &g) {
                        Some(row) if row.d_data().read().get_txn_revised() > txn_id => {
                            // skewed
                            // resolve deltas if any
                            let _ = row.resolve_schema_deltas_and_freeze(m.delta_state());
                            continue;
                        }
                        Some(_) | None => {
                            // new row (logically)
                            let _ = p_index.mt_delete(&pk, &g);
                            let mut data = DcFieldIndex::default();
                            for (field_name, new_data) in m
                                .fields()
                                .stseq_ord_key()
                                .filter(|key| key.as_str() != m.p_key())
                                .zip(new_row)
                            {
                                data.st_insert(
                                    unsafe {
                                        // UNSAFE(@ohsayan): model in scope, we're good
                                        field_name.clone()
                                    },
                                    new_data,
                                );
                            }
                            let row = Row::new_restored(
                                pk,
                                data,
                                DeltaVersion::__new(schema_version),
                                txn_id,
                            );
                            // resolve any deltas
                            let _ = row.resolve_schema_deltas_and_freeze(m.delta_state());
                            // put it back in (lol); blame @ohsayan for this joke
                            p_index.mt_insert(row, &g);
                        }
                    }
                }
                DecodedBatchEventKind::Delete => {
                    match pending_delete.entry(pk) {
                        HMEntry::Occupied(mut existing_delete) => {
                            if *existing_delete.get() > txn_id {
                                // the existing delete "happened after" our delete, so it takes precedence
                                continue;
                            }
                            // the existing delete happened before our delete, so our delete takes precedence
                            // we have a newer delete for the same key
                            *existing_delete.get_mut() = txn_id;
                        }
                        HMEntry::Vacant(new) => {
                            // we never deleted this
                            new.insert(txn_id);
                        }
                    }
                }
            }
        }
        for (pk, txn_id) in pending_delete {
            match p_index.mt_get(&pk, &g) {
                Some(row) => {
                    if row.read().get_txn_revised() > txn_id {
                        // our delete "happened before" this row was inserted
                        continue;
                    }
                    // yup, go ahead and chuck it
                    let _ = p_index.mt_delete(&pk, &g);
                }
                None => {
                    // since we never delete rows until here, this is quite impossible
                    unreachable!()
                }
            }
        }
        Ok(())
    }
}

impl DataBatchRestoreDriver {
    fn read_batch_summary(&mut self, finished_early: bool) -> RuntimeResult<u64> {
        if !finished_early {
            // we must read the batch termination signature
            let b = self.f.read_byte()?;
            if b != MARKER_END_OF_BATCH {
                return Err(StorageError::V1DataBatchDecodeCorruptedBatch.into());
            }
        }
        // read actual commit
        let actual_commit = self.f.read_u64_le()?;
        // find actual checksum
        let actual_checksum = self.f.__reset_checksum();
        // find hardcoded checksum
        let mut hardcoded_checksum = [0; sizeof!(u64)];
        self.f.untracked_read(&mut hardcoded_checksum)?;
        if actual_checksum == u64::from_le_bytes(hardcoded_checksum) {
            Ok(actual_commit)
        } else {
            Err(StorageError::V1DataBatchDecodeCorruptedBatch.into())
        }
    }
    fn read_batch(&mut self) -> RuntimeResult<Batch> {
        let mut this_batch = vec![];
        // check batch type
        let batch_type = self.f.read_byte()?;
        match batch_type {
            MARKER_ACTUAL_BATCH_EVENT => {}
            MARKER_RECOVERY_EVENT => {
                // while attempting to write this batch, some sort of an error occurred but we got a nice recovery byte
                // so proceed that way
                return Ok(Batch::RecoveredFromerror);
            }
            MARKER_BATCH_CLOSED => {
                // this isn't a batch; it has been closed
                return Ok(Batch::BatchClosed);
            }
            _ => {
                // this is the only singular byte that is expected to be intact. If this isn't intact either, I'm sorry
                return Err(StorageError::V1DataBatchDecodeCorruptedBatch.into());
            }
        }
        // decode batch start block
        let batch_start_block = self.read_start_batch_block()?;

        let mut processed_in_this_batch = 0;
        while (processed_in_this_batch != batch_start_block.expected_commit()) & !self.f.is_eof() {
            // decode common row data
            let change_type = self.f.read_byte()?;
            // now decode event
            match change_type {
                MARKER_END_OF_BATCH => {
                    // the file tells us that we've reached the end of this batch; hmmm
                    return Ok(Batch::FinishedEarly(NormalBatch::new(
                        this_batch,
                        batch_start_block.schema_version(),
                    )));
                }
                normal_event => {
                    let txnid = self.f.read_u64_le()?;
                    match normal_event {
                        0 => {
                            // delete
                            let pk = self.decode_primary_key(batch_start_block.pk_tag())?;
                            this_batch.push(DecodedBatchEvent::new(
                                txnid,
                                pk,
                                DecodedBatchEventKind::Delete,
                            ));
                            processed_in_this_batch += 1;
                        }
                        1 | 2 => {
                            // insert or update
                            // get pk
                            let pk = self.decode_primary_key(batch_start_block.pk_tag())?;
                            // prepare row
                            let mut row = vec![];
                            let mut this_col_cnt = batch_start_block.column_cnt();
                            while this_col_cnt != 0 && !self.f.is_eof() {
                                row.push(self.decode_cell()?);
                                this_col_cnt -= 1;
                            }
                            if this_col_cnt != 0 {
                                return Err(StorageError::V1DataBatchDecodeCorruptedEntry.into());
                            }
                            if change_type == 1 {
                                this_batch.push(DecodedBatchEvent::new(
                                    txnid,
                                    pk,
                                    DecodedBatchEventKind::Insert(row),
                                ));
                            } else {
                                this_batch.push(DecodedBatchEvent::new(
                                    txnid,
                                    pk,
                                    DecodedBatchEventKind::Update(row),
                                ));
                            }
                            processed_in_this_batch += 1;
                        }
                        _ => {
                            return Err(StorageError::V1DataBatchDecodeCorruptedBatch.into());
                        }
                    }
                }
            }
        }
        Ok(Batch::Normal(NormalBatch::new(
            this_batch,
            batch_start_block.schema_version(),
        )))
    }
    fn attempt_recover_data_batch(&mut self) -> RuntimeResult<()> {
        let mut buf = [0u8; 1];
        self.f.untracked_read(&mut buf)?;
        if let [MARKER_RECOVERY_EVENT] = buf {
            return Ok(());
        }
        Err(StorageError::V1DataBatchDecodeCorruptedBatch.into())
    }
    fn read_start_batch_block(&mut self) -> RuntimeResult<BatchStartBlock> {
        let pk_tag = self.f.read_byte()?;
        let expected_commit = self.f.read_u64_le()?;
        let schema_version = self.f.read_u64_le()?;
        let column_cnt = self.f.read_u64_le()?;
        Ok(BatchStartBlock::new(
            pk_tag,
            expected_commit,
            schema_version,
            column_cnt,
        ))
    }
}

#[derive(Debug, PartialEq)]
struct BatchStartBlock {
    pk_tag: u8,
    expected_commit: u64,
    schema_version: u64,
    column_cnt: u64,
}

impl BatchStartBlock {
    const fn new(pk_tag: u8, expected_commit: u64, schema_version: u64, column_cnt: u64) -> Self {
        Self {
            pk_tag,
            expected_commit,
            schema_version,
            column_cnt,
        }
    }
    fn pk_tag(&self) -> u8 {
        self.pk_tag
    }
    fn expected_commit(&self) -> u64 {
        self.expected_commit
    }
    fn schema_version(&self) -> u64 {
        self.schema_version
    }
    fn column_cnt(&self) -> u64 {
        self.column_cnt
    }
}

impl DataBatchRestoreDriver {
    fn decode_primary_key(&mut self, pk_type: u8) -> RuntimeResult<PrimaryIndexKey> {
        let Some(pk_type) = TagUnique::try_from_raw(pk_type) else {
            return Err(StorageError::V1DataBatchDecodeCorruptedEntry.into());
        };
        Ok(match pk_type {
            TagUnique::SignedInt | TagUnique::UnsignedInt => {
                let qw = self.f.read_u64_le()?;
                unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    PrimaryIndexKey::new_from_qw(pk_type, qw)
                }
            }
            TagUnique::Str | TagUnique::Bin => {
                let len = self.f.read_u64_le()?;
                let mut data = vec![0; len as usize];
                self.f.tracked_read(&mut data)?;
                if pk_type == TagUnique::Str {
                    if core::str::from_utf8(&data).is_err() {
                        return Err(StorageError::V1DataBatchDecodeCorruptedEntry.into());
                    }
                }
                unsafe {
                    // UNSAFE(@ohsayan): +tagck +verityck
                    let mut md = ManuallyDrop::new(data);
                    PrimaryIndexKey::new_from_dual(pk_type, len, md.as_mut_ptr() as usize)
                }
            }
            _ => unsafe {
                // UNSAFE(@ohsayan): TagUnique::try_from_raw rejects an construction with Invalid as the dscr
                impossible!()
            },
        })
    }
    fn decode_cell(&mut self) -> RuntimeResult<Datacell> {
        let Some(dscr) = StorageCellTypeID::try_from_raw(self.f.read_byte()?) else {
            return Err(StorageError::V1DataBatchDecodeCorruptedEntry.into());
        };
        unsafe { cell::decode_element::<Datacell, TrackedReader>(&mut self.f, dscr) }
            .map_err(|e| e.0)
    }
}

pub struct ErrorHack(crate::engine::fractal::error::Error);
impl From<crate::engine::fractal::error::Error> for ErrorHack {
    fn from(value: crate::engine::fractal::error::Error) -> Self {
        Self(value)
    }
}
impl From<std::io::Error> for ErrorHack {
    fn from(value: std::io::Error) -> Self {
        Self(value.into())
    }
}
impl From<()> for ErrorHack {
    fn from(_: ()) -> Self {
        Self(StorageError::V1DataBatchDecodeCorruptedEntry.into())
    }
}

impl DataSource for TrackedReader {
    const RELIABLE_SOURCE: bool = false;
    type Error = ErrorHack;
    fn has_remaining(&self, cnt: usize) -> bool {
        self.remaining() >= cnt as u64
    }
    unsafe fn read_next_byte(&mut self) -> Result<u8, Self::Error> {
        Ok(self.read_byte()?)
    }
    unsafe fn read_next_block<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
        Ok(self.read_block()?)
    }
    unsafe fn read_next_u64_le(&mut self) -> Result<u64, Self::Error> {
        Ok(self.read_u64_le()?)
    }
    unsafe fn read_next_variable_block(&mut self, size: usize) -> Result<Vec<u8>, Self::Error> {
        let mut buf = vec![0; size];
        self.tracked_read(&mut buf)?;
        Ok(buf)
    }
}
