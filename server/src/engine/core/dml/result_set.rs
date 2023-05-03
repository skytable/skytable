/*
 * Created on Tue May 02 2023
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

/*
    ☢ ISOLATION WARNING ☢
    ----------------
    I went with a rather suboptimal solution for v1. Once we have CC
    we can do much better (at the cost of more complexity, ofcourse).

    We'll roll that out in 0.8.1, I think.

    FIXME(@ohsayan): Fix this
*/

use {
    crate::engine::core::index::{DcFieldIndex, PrimaryIndexKey, Row},
    parking_lot::RwLockReadGuard,
};

pub struct RowSnapshot<'a> {
    key: &'a PrimaryIndexKey,
    data: RwLockReadGuard<'a, DcFieldIndex>,
}

impl<'a> RowSnapshot<'a> {
    /// The moment you take a snapshot, you essentially "freeze" the row and prevent any changes from happening.
    ///
    /// HOWEVER: This is very inefficient subject to isolation level scrutiny
    #[inline(always)]
    pub fn snapshot(row: &'a Row) -> RowSnapshot<'a> {
        Self {
            key: row.d_key(),
            data: row.d_data().read(),
        }
    }
    #[inline(always)]
    pub fn row_key(&self) -> &'a PrimaryIndexKey {
        self.key
    }
    #[inline(always)]
    pub fn row_data(&self) -> &DcFieldIndex {
        &self.data
    }
}
