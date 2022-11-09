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

// TODO(@ohsayan): Change the underlying structures, there are just rudimentary ones used during integration with the QL

/// A [`DataType`] represents the underlying data-type, although this enumeration when used in a collection will always
/// be of one type.
#[derive(Debug, PartialEq)]
#[cfg_attr(debug_assertions, derive(Clone))]
pub enum DataType {
    /// An UTF-8 string
    String(String),
    /// Bytes
    Binary(Vec<u8>),
    /// An integer
    Number(u64),
    /// A boolean
    Boolean(bool),
    /// A single-type list. Note, you **need** to keep up the invariant that the [`DataType`] disc. remains the same for all
    /// elements to ensure correctness in this specific context
    /// FIXME(@ohsayan): Try enforcing this somehow
    List(Vec<Self>),
}

enum_impls! {
    DataType => {
        String as String,
        Vec<u8> as Binary,
        u64 as Number,
        bool as Boolean,
        Vec<Self> as List,
        &'static str as String,
    }
}

impl<const N: usize> From<[DataType; N]> for DataType {
    fn from(f: [DataType; N]) -> Self {
        Self::List(f.into())
    }
}