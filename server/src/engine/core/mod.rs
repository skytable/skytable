/*
 * Created on Wed Oct 12 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

mod model;
mod space;
#[cfg(test)]
mod tests;
// imports
use {
    crate::engine::{core::space::Space, idx::IndexST},
    parking_lot::RwLock,
};
// re-exports
pub use model::cell::Datacell;

/// Use this for now since it substitutes for a file lock (and those syscalls are expensive),
/// but something better is in the offing
type RWLIdx<K, V> = RwLock<IndexST<K, V>>;

// FIXME(@ohsayan): Make sure we update what all structures we're making use of here

pub struct GlobalNS {
    index_space: RWLIdx<Box<str>, Space>,
}

impl GlobalNS {
    pub(self) fn spaces(&self) -> &RWLIdx<Box<str>, Space> {
        &self.index_space
    }
    pub fn empty() -> Self {
        Self {
            index_space: RWLIdx::default(),
        }
    }
    #[cfg(test)]
    pub(self) fn test_new_empty_space(&self, space_id: &str) -> bool {
        use super::idx::STIndex;
        self.index_space
            .write()
            .st_insert(space_id.into(), Space::empty())
    }
}
