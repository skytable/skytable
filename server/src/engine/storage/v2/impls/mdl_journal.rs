/*
 * Created on Sun Feb 18 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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
    crate::{
        engine::{
            core::{
                index::{DcFieldIndex, PrimaryIndexKey, Row, RowData},
                model::{
                    delta::{DataDelta, DataDeltaKind, DeltaVersion},
                    Model,
                },
            },
            data::{
                cell::Datacell,
                tag::{DataTag, TagUnique},
            },
            error::StorageError,
            idx::{MTIndex, STIndex, STIndexSeq},
            storage::{
                common::{
                    interface::fs_traits::{FSInterface, FileInterface},
                    sdss::sdss_r1::rw::{TrackedReaderContext, TrackedWriter},
                },
                common_encoding::r1,
                v2::raw::{
                    journal::{
                        self, BatchAdapter, BatchAdapterSpec, BatchDriver, JournalAdapterEvent,
                        RawJournalAdapter,
                    },
                    spec::ModelDataBatchAofV1,
                },
            },
            RuntimeResult,
        },
        util::{compiler::TaggedEnum, EndianQW},
    },
    crossbeam_epoch::{pin, Guard},
    sky_macros::TaggedEnum,
    std::collections::{hash_map::Entry as HMEntry, HashMap},
};

pub type ModelDriver<Fs> = BatchDriver<ModelDataAdapter, Fs>;
impl<Fs: FSInterface> ModelDriver<Fs> {
    pub fn open_model_driver(mdl: &Model, model_data_file_path: &str) -> RuntimeResult<Self> {
        journal::open_journal::<_, Fs>(model_data_file_path, mdl)
    }
    /// Create a new event log
    pub fn create_model_driver(model_data_file_path: &str) -> RuntimeResult<Self> {
        journal::create_journal::<_, Fs>(model_data_file_path)
    }
}

/// The model data adapter (abstract journal adapter impl)
pub struct ModelDataAdapter;

#[derive(Debug, PartialEq, Clone, Copy, TaggedEnum)]
#[repr(u8)]
/// The kind of batch
pub enum BatchType {
    /// a standard batch (with n <= m events; n = Δdata, m = cardinality)
    Standard = 0,
}

#[derive(Debug, PartialEq, Clone, Copy, TaggedEnum)]
#[repr(u8)]
/// The type of event *inside* a batch
#[allow(unused)] // TODO(@ohsayan): somehow merge this into delta kind?
pub enum EventType {
    Delete = 0,
    Insert = 1,
    Update = 2,
    /// owing to inconsistent reads, we exited early
    EarlyExit = 3,
}

/*
    persist implementation
    ---
    this section implements persistence for a model data batch. now, there are several special
    cases to handle, for example inconsistent views of the database and such so this might look
    a little messy.
*/

struct RowWriter<'b, Fs: FSInterface> {
    f: &'b mut TrackedWriter<Fs::File, <BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec>,
}

impl<'b, Fs: FSInterface> RowWriter<'b, Fs> {
    fn write_row_metadata(&mut self, delta: &DataDelta) -> RuntimeResult<()> {
        if cfg!(debug) {
            let event_kind = EventType::try_from_raw(delta.change().value_u8()).unwrap();
            match (event_kind, delta.change()) {
                (EventType::Delete, DataDeltaKind::Delete)
                | (EventType::Insert, DataDeltaKind::Insert)
                | (EventType::Update, DataDeltaKind::Update) => {}
                (EventType::EarlyExit, _) => unreachable!(),
                _ => panic!(),
            }
        }
        // write [change type][txn id]
        let change_type = [delta.change().value_u8()];
        self.f.dtrack_write(&change_type)?;
        let txn_id = delta.data_version().value_u64().to_le_bytes();
        self.f.dtrack_write(&txn_id)?;
        Ok(())
    }
    /// encode the primary key only. this means NO TAG is encoded.
    fn write_row_pk(&mut self, pk: &PrimaryIndexKey) -> RuntimeResult<()> {
        match pk.tag() {
            TagUnique::UnsignedInt | TagUnique::SignedInt => {
                let data = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    pk.read_uint()
                }
                .to_le_bytes();
                self.f.dtrack_write(&data)?;
            }
            TagUnique::Str | TagUnique::Bin => {
                let slice = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    pk.read_bin()
                };
                let slice_l = slice.len().u64_bytes_le();
                self.f.dtrack_write(&slice_l)?;
                self.f.dtrack_write(slice)?;
            }
            TagUnique::Illegal => unsafe {
                // UNSAFE(@ohsayan): a pk can't be constructed with illegal
                impossible!()
            },
        }
        Ok(())
    }
    /// Encode a single cell
    fn write_cell(&mut self, value: &Datacell) -> RuntimeResult<()> {
        let mut buf = vec![];
        r1::obj::cell::encode(&mut buf, value);
        self.f.dtrack_write(&buf)?;
        Ok(())
    }
    /// Encode row data
    fn write_row_data(&mut self, model: &Model, row_data: &RowData) -> RuntimeResult<()> {
        for field_name in model.fields().stseq_ord_key() {
            match row_data.fields().get(field_name) {
                Some(cell) => {
                    self.write_cell(cell)?;
                }
                None if field_name.as_str() == model.p_key() => {}
                None => self.f.dtrack_write(&[0])?,
            }
        }
        Ok(())
    }
}

struct BatchWriter<'a, 'b, Fs: FSInterface> {
    model: &'a Model,
    row_writer: RowWriter<'b, Fs>,
    g: &'a Guard,
    sync_count: usize,
}

impl<'a, 'b, Fs: FSInterface> BatchWriter<'a, 'b, Fs> {
    fn write_batch(
        model: &'a Model,
        g: &'a Guard,
        count: usize,
        f: &'b mut TrackedWriter<
            Fs::File,
            <BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec,
        >,
    ) -> RuntimeResult<usize> {
        /*
            go over each delta, check if inconsistent and apply if not. we currently keep a track
            of applied deltas in a vec which is a TERRIBLY INEFFICENT WAY to do so. Instead we should
            be able to "iterate" on the concurrent queue. Since that demands a proof of correctness,
            once I do finish implementing it I'll swap it in here. This is the primary source of huge
            memory blowup during a batch sync.

            -- @ohsayan
        */
        let mut me = Self::new(model, g, f)?;
        let mut applied_deltas = vec![];
        let mut i = 0;
        while i < count {
            let delta = me.model.delta_state().__data_delta_dequeue(me.g).unwrap();
            match me.step(&delta) {
                Ok(()) => {
                    applied_deltas.push(delta);
                    i += 1;
                }
                Err(e) => {
                    // errored, so push everything back in
                    me.model.delta_state().append_new_data_delta(delta, me.g);
                    for applied_delta in applied_deltas {
                        me.model
                            .delta_state()
                            .append_new_data_delta(applied_delta, g);
                    }
                    return Err(e);
                }
            }
        }
        Ok(me.sync_count)
    }
    fn new(
        model: &'a Model,
        g: &'a Guard,
        f: &'b mut TrackedWriter<
            Fs::File,
            <BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec,
        >,
    ) -> RuntimeResult<Self> {
        // write batch start information: [pk tag:1B][schema version][column count]
        f.dtrack_write(&[model.p_tag().tag_unique().value_u8()])?;
        f.dtrack_write(
            &model
                .delta_state()
                .schema_current_version()
                .value_u64()
                .to_le_bytes(),
        )?;
        f.dtrack_write(&(model.fields().st_len() as u64).to_le_bytes())?;
        Ok(Self {
            model,
            row_writer: RowWriter { f },
            g,
            sync_count: 0,
        })
    }
    fn step(&mut self, delta: &DataDelta) -> RuntimeResult<()> {
        match delta.change() {
            DataDeltaKind::Delete => {
                self.row_writer.write_row_metadata(&delta)?;
                self.row_writer.write_row_pk(delta.row().d_key())?;
            }
            DataDeltaKind::Insert | DataDeltaKind::Update => {
                // resolve deltas (this is yet another opportunity for us to reclaim memory from deleted items)
                let row_data = delta
                    .row()
                    .resolve_schema_deltas_and_freeze_if(self.model.delta_state(), |row| {
                        row.get_txn_revised() <= delta.data_version()
                    });
                if row_data.get_txn_revised() > delta.data_version() {
                    // inconsistent read. there should already be another revised delta somewhere
                    return Ok(());
                }
                self.row_writer.write_row_metadata(&delta)?;
                // encode data
                self.row_writer.write_row_pk(delta.row().d_key())?;
                self.row_writer.write_row_data(self.model, &row_data)?;
            }
        }
        self.sync_count += 1;
        Ok(())
    }
}

/// A standard model batch where atmost the given number of keys are flushed
pub struct StdModelBatch<'a>(&'a Model, usize);

impl<'a> StdModelBatch<'a> {
    pub fn new(model: &'a Model, observed_len: usize) -> Self {
        Self(model, observed_len)
    }
}

impl<'a> JournalAdapterEvent<BatchAdapter<ModelDataAdapter>> for StdModelBatch<'a> {
    fn md(&self) -> u64 {
        BatchType::Standard.dscr_u64()
    }
    fn write_direct<Fs: FSInterface>(
        self,
        writer: &mut TrackedWriter<
            Fs::File,
            <BatchAdapter<ModelDataAdapter> as RawJournalAdapter>::Spec,
        >,
    ) -> RuntimeResult<()> {
        // [expected commit]
        writer.dtrack_write(&(self.1 as u64).to_le_bytes())?;
        let g = pin();
        let actual_commit = BatchWriter::<Fs>::write_batch(self.0, &g, self.1, writer)?;
        if actual_commit != self.1 {
            // early exit
            writer.dtrack_write(&[EventType::EarlyExit.dscr()])?;
        }
        writer.dtrack_write(&(actual_commit as u64).to_le_bytes())
    }
}

/*
    restore implementation
    ---
    the section below implements data restore from a single batch. like the persist impl,
    this is also a fairly complex implementation because some changes, for example deletes
    may need to be applied later due to out-of-order persistence; it is important to postpone
    operations that we're unsure about since a change can appear out of order and we want to
    restore the database to its exact state
*/

/// Per-batch metadata
pub struct BatchMetadata {
    pk_tag: TagUnique,
    schema_version: u64,
    column_count: u64,
}

enum DecodedBatchEventKind {
    Delete,
    Insert(Vec<Datacell>),
    Update(Vec<Datacell>),
}

/// State handling for any pending queries
pub struct BatchRestoreState {
    events: Vec<DecodedBatchEvent>,
}

struct DecodedBatchEvent {
    txn_id: DeltaVersion,
    pk: PrimaryIndexKey,
    kind: DecodedBatchEventKind,
}

impl DecodedBatchEvent {
    fn new(txn_id: u64, pk: PrimaryIndexKey, kind: DecodedBatchEventKind) -> Self {
        Self {
            txn_id: DeltaVersion::__new(txn_id),
            pk,
            kind,
        }
    }
}

impl BatchAdapterSpec for ModelDataAdapter {
    type Spec = ModelDataBatchAofV1;
    type GlobalState = Model;
    type BatchType = BatchType;
    type EventType = EventType;
    type BatchMetadata = BatchMetadata;
    type BatchState = BatchRestoreState;
    fn is_early_exit(event_type: &Self::EventType) -> bool {
        EventType::EarlyExit.eq(event_type)
    }
    fn initialize_batch_state(_: &Self::GlobalState) -> Self::BatchState {
        BatchRestoreState { events: Vec::new() }
    }
    fn decode_batch_metadata<Fs: FSInterface>(
        _: &Self::GlobalState,
        f: &mut TrackedReaderContext<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
        batch_type: Self::BatchType,
    ) -> RuntimeResult<Self::BatchMetadata> {
        // [pk tag][schema version][column cnt]
        match batch_type {
            BatchType::Standard => {}
        }
        let pk_tag = f.read_block().and_then(|[b]| {
            TagUnique::try_from_raw(b).ok_or(StorageError::RawJournalCorrupted.into())
        })?;
        let schema_version = u64::from_le_bytes(f.read_block()?);
        let column_count = u64::from_le_bytes(f.read_block()?);
        Ok(BatchMetadata {
            pk_tag,
            schema_version,
            column_count,
        })
    }
    fn update_state_for_new_event<Fs: FSInterface>(
        _: &Self::GlobalState,
        bs: &mut Self::BatchState,
        f: &mut TrackedReaderContext<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            Self::Spec,
        >,
        batch_info: &Self::BatchMetadata,
        event_type: Self::EventType,
    ) -> RuntimeResult<()> {
        // get txn id
        let txn_id = u64::from_le_bytes(f.read_block()?);
        // get pk
        let pk = restore_impls::decode_primary_key::<Fs, Self::Spec>(f, batch_info.pk_tag)?;
        match event_type {
            EventType::Delete => {
                bs.events.push(DecodedBatchEvent::new(
                    txn_id,
                    pk,
                    DecodedBatchEventKind::Delete,
                ));
            }
            EventType::Insert | EventType::Update => {
                // insert or update
                // prepare row
                let row = restore_impls::decode_row_data::<Fs>(batch_info, f)?;
                if event_type == EventType::Insert {
                    bs.events.push(DecodedBatchEvent::new(
                        txn_id,
                        pk,
                        DecodedBatchEventKind::Insert(row),
                    ));
                } else {
                    bs.events.push(DecodedBatchEvent::new(
                        txn_id,
                        pk,
                        DecodedBatchEventKind::Update(row),
                    ));
                }
            }
            EventType::EarlyExit => unreachable!(),
        }
        Ok(())
    }
    fn finish(
        batch_state: Self::BatchState,
        batch_md: Self::BatchMetadata,
        gs: &Self::GlobalState,
    ) -> RuntimeResult<()> {
        /*
            go over each change in this batch, resolve conflicts and then apply to global state
        */
        let g = unsafe { crossbeam_epoch::unprotected() };
        let mut pending_delete = HashMap::new();
        let p_index = gs.primary_index().__raw_index();
        let m = gs;
        for DecodedBatchEvent { txn_id, pk, kind } in batch_state.events {
            match kind {
                DecodedBatchEventKind::Insert(new_row) | DecodedBatchEventKind::Update(new_row) => {
                    // this is more like a "newrow"
                    match p_index.mt_get_element(&pk, &g) {
                        Some(row) if row.d_data().read().get_restored_txn_revised() > txn_id => {
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
                                DeltaVersion::__new(batch_md.schema_version),
                                DeltaVersion::__new(0),
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
        // apply pending deletes; are our conflicts would have been resolved by now
        for (pk, txn_id) in pending_delete {
            match p_index.mt_get(&pk, &g) {
                Some(row) => {
                    if row.read().get_restored_txn_revised() > txn_id {
                        // our delete "happened before" this row was inserted
                        continue;
                    }
                    // yup, go ahead and chuck it
                    let _ = p_index.mt_delete(&pk, &g);
                }
                None => {
                    // since we never delete rows until here, this is impossible
                    unreachable!()
                }
            }
        }
        Ok(())
    }
}

mod restore_impls {
    use {
        super::BatchMetadata,
        crate::engine::{
            core::index::PrimaryIndexKey,
            data::{cell::Datacell, tag::TagUnique},
            error::StorageError,
            storage::{
                common::{
                    interface::fs_traits::{FSInterface, FileInterface, FileInterfaceRead},
                    sdss::sdss_r1::{rw::TrackedReaderContext, FileSpecV1},
                },
                common_encoding::r1::{
                    obj::cell::{self, StorageCellTypeID},
                    DataSource,
                },
                v2::raw::spec::ModelDataBatchAofV1,
            },
            RuntimeResult,
        },
        std::mem::ManuallyDrop,
    };
    /// Primary key decode impl
    ///
    /// NB: We really need to make this generic, but for now we can settle for this
    pub fn decode_primary_key<Fs: FSInterface, S: FileSpecV1>(
        f: &mut TrackedReaderContext<<<Fs as FSInterface>::File as FileInterface>::BufReader, S>,
        pk_type: TagUnique,
    ) -> RuntimeResult<PrimaryIndexKey> {
        Ok(match pk_type {
            TagUnique::SignedInt | TagUnique::UnsignedInt => {
                let qw = u64::from_le_bytes(f.read_block()?);
                unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    PrimaryIndexKey::new_from_qw(pk_type, qw)
                }
            }
            TagUnique::Str | TagUnique::Bin => {
                let len = u64::from_le_bytes(f.read_block()?);
                let mut data = vec![0; len as usize];
                f.read(&mut data)?;
                if pk_type == TagUnique::Str {
                    if core::str::from_utf8(&data).is_err() {
                        return Err(StorageError::DataBatchRestoreCorruptedEntry.into());
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
    pub fn decode_row_data<Fs: FSInterface>(
        batch_info: &BatchMetadata,
        f: &mut TrackedReaderContext<
            <<Fs as FSInterface>::File as FileInterface>::BufReader,
            ModelDataBatchAofV1,
        >,
    ) -> Result<Vec<Datacell>, crate::engine::fractal::error::Error> {
        let mut row = vec![];
        let mut this_col_cnt = batch_info.column_count;
        while this_col_cnt != 0 {
            let Some(dscr) = StorageCellTypeID::try_from_raw(f.read_block().map(|[b]| b)?) else {
                return Err(StorageError::DataBatchRestoreCorruptedEntry.into());
            };
            let cell = unsafe { cell::decode_element::<Datacell, _>(f, dscr) }.map_err(|e| e.0)?;
            row.push(cell);
            this_col_cnt -= 1;
        }
        Ok(row)
    }

    /*
        this is some silly ridiculous hackery because of some of our legacy code. basically an attempt is made to directly coerce error types.
        we'll make this super generic so that no more of this madness is needed
    */
    pub struct ErrorHack(crate::engine::fractal::error::Error);
    impl From<crate::engine::fractal::error::Error> for ErrorHack {
        fn from(value: crate::engine::fractal::error::Error) -> Self {
            Self(value)
        }
    }
    impl From<()> for ErrorHack {
        fn from(_: ()) -> Self {
            Self(StorageError::DataBatchRestoreCorruptedEntry.into())
        }
    }
    impl<'a, F: FileInterfaceRead> DataSource for TrackedReaderContext<'a, F, ModelDataBatchAofV1> {
        const RELIABLE_SOURCE: bool = false;
        type Error = ErrorHack;
        fn has_remaining(&self, cnt: usize) -> bool {
            self.remaining() >= cnt as u64
        }
        unsafe fn read_next_byte(&mut self) -> Result<u8, Self::Error> {
            Ok(self.read_next_block::<1>()?[0])
        }
        unsafe fn read_next_block<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
            Ok(self.read_block()?)
        }
        unsafe fn read_next_u64_le(&mut self) -> Result<u64, Self::Error> {
            self.read_next_block().map(u64::from_le_bytes)
        }
        unsafe fn read_next_variable_block(&mut self, size: usize) -> Result<Vec<u8>, Self::Error> {
            let mut buf = vec![0; size];
            self.read(&mut buf)?;
            Ok(buf)
        }
    }
}
