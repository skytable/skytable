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
    super::{MARKER_BATCH_CLOSED, MARKER_BATCH_REOPEN},
    crate::engine::{
        error::{RuntimeResult, StorageError},
        storage::{
            common::interface::fs::File,
            v1::raw::rw::{SDSSFileIO, TrackedWriter},
        },
    },
};

pub struct DataBatchPersistDriver {
    f: TrackedWriter,
}

impl DataBatchPersistDriver {
    pub fn new(mut file: SDSSFileIO<File>, is_new: bool) -> RuntimeResult<Self> {
        if !is_new {
            file.fsynced_write(&[MARKER_BATCH_REOPEN])?;
        }
        Ok(Self {
            f: TrackedWriter::new(file)?,
        })
    }
    pub fn close(self) -> RuntimeResult<()> {
        let mut slf = self.f.sync_into_inner()?;
        if slf.fsynced_write(&[MARKER_BATCH_CLOSED]).is_ok() {
            return Ok(());
        } else {
            return Err(StorageError::V1DataBatchRuntimeCloseError.into());
        }
    }
}
