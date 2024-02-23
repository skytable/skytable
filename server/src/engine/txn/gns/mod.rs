/*
 * Created on Sat Feb 10 2024
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

macro_rules! impl_gns_event {
    ($($item:ty = $variant:ident),* $(,)?) => {
        $(impl crate::engine::txn::gns::GNSTransaction for $item { const CODE: crate::engine::txn::gns::GNSTransactionCode = crate::engine::txn::gns::GNSTransactionCode::$variant;})*
    }
}

pub mod model;
pub mod space;
pub mod sysctl;

#[derive(Debug, PartialEq, Clone, Copy, sky_macros::TaggedEnum)]
#[repr(u8)]
pub enum GNSTransactionCode {
    CreateSpace = 0,
    AlterSpace = 1,
    DropSpace = 2,
    CreateModel = 3,
    AlterModelAdd = 4,
    AlterModelRemove = 5,
    AlterModelUpdate = 6,
    DropModel = 7,
    CreateUser = 8,
    AlterUser = 9,
    DropUser = 10,
}

pub trait GNSTransaction {
    const CODE: GNSTransactionCode;
}
