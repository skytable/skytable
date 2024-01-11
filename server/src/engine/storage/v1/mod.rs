/*
 * Created on Mon May 15 2023
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

//! SDSS based storage engine driver v1 ([`versions::v1`])
//!
//! Target tags: `0.8.0-beta`, `0.8.0-beta.2`, `0.8.0-beta.3`

// impls
mod batch_jrnl;
mod journal;
pub(in crate::engine) mod loader;
mod rw;
pub mod spec;
pub mod sysdb;
// hl
pub mod inf;
#[cfg(test)]
mod tests;

// re-exports
pub(self) use spec::Header;
pub use {
    journal::{JournalAdapter, JournalWriter},
    rw::SDSSFileIO,
};
pub mod data_batch {
    pub use super::batch_jrnl::{create, DataBatchPersistDriver};
}
