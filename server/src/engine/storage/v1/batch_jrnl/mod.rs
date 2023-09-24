/*
 * Created on Sun Sep 03 2023
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

mod persist;
mod restore;

/// the data batch file was reopened
const MARKER_BATCH_REOPEN: u8 = 0xFB;
/// the data batch file was closed
const MARKER_BATCH_CLOSED: u8 = 0xFC;
/// end of batch marker
const MARKER_END_OF_BATCH: u8 = 0xFD;
/// "real" batch event marker
const MARKER_ACTUAL_BATCH_EVENT: u8 = 0xFE;
/// recovery batch event marker
const MARKER_RECOVERY_EVENT: u8 = 0xFF;
/// recovery threshold
const RECOVERY_THRESHOLD: usize = 10;

#[cfg(test)]
pub(super) use restore::{DecodedBatchEvent, DecodedBatchEventKind, NormalBatch};
pub use {persist::DataBatchPersistDriver, restore::DataBatchRestoreDriver};

use {
    super::{header_meta, rw::SDSSFileIO, RawFSInterface, SDSSResult},
    crate::engine::core::model::Model,
};

const LOG_SPECIFIER_VERSION: header_meta::FileSpecifierVersion =
    header_meta::FileSpecifierVersion::__new(0);

/// Re-initialize an existing batch journal and read all its data into model
pub fn reinit<Fs: RawFSInterface>(
    name: &str,
    model: &Model,
) -> SDSSResult<DataBatchPersistDriver<Fs>> {
    let (_header, f) = SDSSFileIO::<Fs>::open::<false>(
        name,
        header_meta::FileScope::Journal,
        header_meta::FileSpecifier::TableDataBatch,
        LOG_SPECIFIER_VERSION,
    )?;
    // restore
    let mut restore_driver = DataBatchRestoreDriver::new(f)?;
    restore_driver.read_data_batch_into_model(model)?;
    DataBatchPersistDriver::new(restore_driver.into_file(), false)
}

/// Create a new batch journal
pub fn create<Fs: RawFSInterface>(
    path: &str,
    host_setting_version: u32,
    host_run_mode: header_meta::HostRunMode,
    host_startup_counter: u64,
) -> SDSSResult<DataBatchPersistDriver<Fs>> {
    let f = SDSSFileIO::<Fs>::create(
        path,
        header_meta::FileScope::Journal,
        header_meta::FileSpecifier::TableDataBatch,
        LOG_SPECIFIER_VERSION,
        host_setting_version,
        host_run_mode,
        host_startup_counter,
    )?;
    DataBatchPersistDriver::new(f, true)
}
