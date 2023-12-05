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

use crate::engine::storage::v1::inf::obj::cell;

use {
    super::{
        MARKER_ACTUAL_BATCH_EVENT, MARKER_BATCH_CLOSED, MARKER_BATCH_REOPEN, MARKER_END_OF_BATCH,
        MARKER_RECOVERY_EVENT,
    },
    crate::{
        engine::{
            core::{
                index::{PrimaryIndexKey, RowData},
                model::{
                    delta::{DataDelta, DataDeltaKind, DeltaVersion},
                    Model,
                },
            },
            data::{
                cell::Datacell,
                tag::{DataTag, TagUnique},
            },
            error::{RuntimeResult, StorageError},
            idx::STIndexSeq,
            storage::v1::rw::{RawFSInterface, SDSSFileIO, SDSSFileTrackedWriter},
        },
        util::EndianQW,
    },
    crossbeam_epoch::pin,
};

pub struct DataBatchPersistDriver<Fs: RawFSInterface> {
    f: SDSSFileTrackedWriter<Fs>,
}

impl<Fs: RawFSInterface> DataBatchPersistDriver<Fs> {
    pub fn new(mut file: SDSSFileIO<Fs>, is_new: bool) -> RuntimeResult<Self> {
        if !is_new {
            file.fsynced_write(&[MARKER_BATCH_REOPEN])?;
        }
        Ok(Self {
            f: SDSSFileTrackedWriter::new(file)?,
        })
    }
    pub fn close(self) -> RuntimeResult<()> {
        let mut slf = self.f.into_inner_file()?;
        if slf.fsynced_write(&[MARKER_BATCH_CLOSED]).is_ok() {
            return Ok(());
        } else {
            return Err(StorageError::DataBatchCloseError.into());
        }
    }
    pub fn write_new_batch(&mut self, model: &Model, observed_len: usize) -> RuntimeResult<()> {
        // pin model
        let schema_version = model.delta_state().schema_current_version();
        let g = pin();
        // init restore list
        let mut restore_list = Vec::new();
        // prepare computations
        let mut i = 0;
        let mut inconsistent_reads = 0;
        let mut exec = || -> RuntimeResult<()> {
            // write batch start
            self.write_batch_start(
                observed_len,
                schema_version,
                model.p_tag().tag_unique(),
                model.fields().len() - 1,
            )?;
            while i < observed_len {
                let delta = model.delta_state().__data_delta_dequeue(&g).unwrap();
                restore_list.push(delta.clone()); // TODO(@ohsayan): avoid this
                match delta.change() {
                    DataDeltaKind::Delete => {
                        self.write_batch_item_common_row_data(&delta)?;
                        self.encode_pk_only(delta.row().d_key())?;
                    }
                    DataDeltaKind::Insert | DataDeltaKind::Update => {
                        // resolve deltas (this is yet another opportunity for us to reclaim memory from deleted items)
                        let row_data = delta
                            .row()
                            .resolve_schema_deltas_and_freeze_if(&model.delta_state(), |row| {
                                row.get_txn_revised() <= delta.data_version()
                            });
                        if row_data.get_txn_revised() > delta.data_version() {
                            // we made an inconsistent (stale) read; someone updated the state after our snapshot
                            inconsistent_reads += 1;
                            i += 1;
                            continue;
                        }
                        self.write_batch_item_common_row_data(&delta)?;
                        // encode data
                        self.encode_pk_only(delta.row().d_key())?;
                        self.encode_row_data(model, &row_data)?;
                    }
                }
                i += 1;
            }
            return self.append_batch_summary_and_sync(observed_len, inconsistent_reads);
        };
        match exec() {
            Ok(()) => Ok(()),
            Err(e) => {
                // republish changes since we failed to commit
                restore_list.into_iter().for_each(|delta| {
                    model.delta_state().append_new_data_delta(delta, &g);
                });
                // now attempt to fix the file
                self.attempt_fix_data_batchfile()?;
                // IMPORTANT: return an error because even though we recovered the journal we still didn't succeed in
                // writing the batch
                return Err(e);
            }
        }
    }
    /// Write the batch start block:
    /// - Batch start magic
    /// - Primary key type
    /// - Expected commit
    /// - Schema version
    /// - Column count
    fn write_batch_start(
        &mut self,
        observed_len: usize,
        schema_version: DeltaVersion,
        pk_tag: TagUnique,
        col_cnt: usize,
    ) -> RuntimeResult<()> {
        self.f
            .tracked_write_unfsynced(&[MARKER_ACTUAL_BATCH_EVENT, pk_tag.value_u8()])?;
        let observed_len_bytes = observed_len.u64_bytes_le();
        self.f.tracked_write_unfsynced(&observed_len_bytes)?;
        self.f
            .tracked_write_unfsynced(&schema_version.value_u64().to_le_bytes())?;
        self.f.tracked_write_unfsynced(&col_cnt.u64_bytes_le())?;
        Ok(())
    }
    /// Append a summary of this batch and most importantly, **sync everything to disk**
    fn append_batch_summary_and_sync(
        &mut self,
        observed_len: usize,
        inconsistent_reads: usize,
    ) -> RuntimeResult<()> {
        // [0xFD][actual_commit][checksum]
        self.f.tracked_write_unfsynced(&[MARKER_END_OF_BATCH])?;
        let actual_commit = (observed_len - inconsistent_reads).u64_bytes_le();
        self.f.tracked_write_unfsynced(&actual_commit)?;
        let cs = self.f.reset_and_finish_checksum().to_le_bytes();
        self.f.untracked_write(&cs)?;
        // IMPORTANT: now that all data has been written, we need to actually ensure that the writes pass through the cache
        self.f.sync_writes()?;
        Ok(())
    }
    /// Attempt to fix the batch journal
    // TODO(@ohsayan): declare an "international system disaster" when this happens
    fn attempt_fix_data_batchfile(&mut self) -> RuntimeResult<()> {
        /*
            attempt to append 0xFF to the part of the file where a corruption likely occurred, marking
            it recoverable
        */
        if self.f.untracked_write(&[MARKER_RECOVERY_EVENT]).is_ok() {
            return Ok(());
        }
        Err(StorageError::DataBatchRecoveryFailStageOne.into())
    }
}

impl<Fs: RawFSInterface> DataBatchPersistDriver<Fs> {
    /// encode the primary key only. this means NO TAG is encoded.
    fn encode_pk_only(&mut self, pk: &PrimaryIndexKey) -> RuntimeResult<()> {
        let buf = &mut self.f;
        match pk.tag() {
            TagUnique::UnsignedInt | TagUnique::SignedInt => {
                let data = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    pk.read_uint()
                }
                .to_le_bytes();
                buf.tracked_write_unfsynced(&data)?;
            }
            TagUnique::Str | TagUnique::Bin => {
                let slice = unsafe {
                    // UNSAFE(@ohsayan): +tagck
                    pk.read_bin()
                };
                let slice_l = slice.len().u64_bytes_le();
                buf.tracked_write_unfsynced(&slice_l)?;
                buf.tracked_write_unfsynced(slice)?;
            }
            TagUnique::Illegal => unsafe {
                // UNSAFE(@ohsayan): a pk can't be constructed with illegal
                impossible!()
            },
        }
        Ok(())
    }
    /// Encode a single cell
    fn encode_cell(&mut self, value: &Datacell) -> RuntimeResult<()> {
        let mut buf = vec![];
        cell::encode(&mut buf, value);
        self.f.tracked_write_unfsynced(&buf)?;
        Ok(())
    }
    /// Encode row data
    fn encode_row_data(&mut self, model: &Model, row_data: &RowData) -> RuntimeResult<()> {
        for field_name in model.fields().stseq_ord_key() {
            match row_data.fields().get(field_name) {
                Some(cell) => {
                    self.encode_cell(cell)?;
                }
                None if field_name.as_str() == model.p_key() => {}
                None => self.f.tracked_write_unfsynced(&[0])?,
            }
        }
        Ok(())
    }
    /// Write the change type and txnid
    fn write_batch_item_common_row_data(&mut self, delta: &DataDelta) -> RuntimeResult<()> {
        let change_type = [delta.change().value_u8()];
        self.f.tracked_write_unfsynced(&change_type)?;
        let txn_id = delta.data_version().value_u64().to_le_bytes();
        self.f.tracked_write_unfsynced(&txn_id)?;
        Ok(())
    }
}
