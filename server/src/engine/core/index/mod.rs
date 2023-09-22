/*
 * Created on Sat Apr 08 2023
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

mod key;
mod row;

use crate::engine::{
    data::lit::Lit,
    idx::{IndexBaseSpec, IndexMTRaw, MTIndex},
    sync::atm::Guard,
};

pub use {
    key::PrimaryIndexKey,
    row::{DcFieldIndex, Row, RowData},
};

#[derive(Debug)]
pub struct PrimaryIndex {
    data: IndexMTRaw<row::Row>,
}

impl PrimaryIndex {
    pub fn new_empty() -> Self {
        Self {
            data: IndexMTRaw::idx_init(),
        }
    }
    pub fn remove<'a>(&self, key: Lit<'a>, g: &Guard) -> bool {
        self.data.mt_delete(&key, g)
    }
    pub fn select<'a, 'v, 't: 'v, 'g: 't>(&'t self, key: Lit<'a>, g: &'g Guard) -> Option<&'v Row> {
        self.data.mt_get_element(&key, g)
    }
    pub fn __raw_index(&self) -> &IndexMTRaw<row::Row> {
        &self.data
    }
    pub fn count(&self) -> usize {
        self.data.mt_len()
    }
}
