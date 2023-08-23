/*
 * Created on Sun Aug 20 2023
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

use crate::engine::storage::v1::BufferedScanner;

use {
    super::{TransactionError, TransactionResult},
    crate::engine::{
        core::GlobalNS,
        storage::v1::{
            inf::{self, PersistObject},
            JournalAdapter,
        },
    },
};

mod space;

/*
    journal implementor
*/

/// the journal adapter for DDL queries on the GNS
pub struct GNSAdapter;

impl JournalAdapter for GNSAdapter {
    const RECOVERY_PLUGIN: bool = true;
    type JournalEvent = GNSSuperEvent;
    type GlobalState = GlobalNS;
    type Error = TransactionError;
    fn encode(GNSSuperEvent(b): Self::JournalEvent) -> Box<[u8]> {
        b
    }
    fn decode_and_update_state(_: &[u8], _: &Self::GlobalState) -> TransactionResult<()> {
        todo!()
    }
}

/*
    Events
    ---
    FIXME(@ohsayan): In the current impl, we unnecessarily use an intermediary buffer which we clearly don't need to (and also makes
    pointless allocations). We need to fix this, but with a consistent API (and preferably not something like commit_*(...) unless
    we have absolutely no other choice)
    ---
    [OPC:2B][PAYLOAD]
*/

pub struct GNSSuperEvent(Box<[u8]>);

pub trait GNSEvent
where
    Self: PersistObject<InputType = Self::CommitType, OutputType = Self::RestoreType> + Sized,
{
    const OPC: u16;
    type CommitType;
    type RestoreType;
    fn encode_super_event(commit: Self::CommitType) -> GNSSuperEvent {
        GNSSuperEvent(inf::enc::enc_full::<Self>(commit).into_boxed_slice())
    }
    fn decode_from_super_event(
        scanner: &mut BufferedScanner,
    ) -> TransactionResult<Self::RestoreType> {
        inf::dec::dec_full_from_scanner::<Self>(scanner).map_err(|e| e.into())
    }
    fn update_global_state(restore: Self::RestoreType, gns: &GlobalNS) -> TransactionResult<()>;
}
