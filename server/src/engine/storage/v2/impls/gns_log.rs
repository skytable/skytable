/*
 * Created on Sun Feb 18 2024
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

use {
    super::super::raw::{
        journal::{EventLogAdapter, EventLogSpec},
        spec::SystemDatabaseV1,
    },
    crate::{
        engine::{
            core::GNSData,
            storage::{
                common_encoding::r1::impls::gns::GNSEvent,
                v2::raw::journal::{self, EventLogDriver, JournalAdapterEvent, JournalSettings},
            },
            txn::gns::{
                model::{
                    AlterModelAddTxn, AlterModelRemoveTxn, AlterModelUpdateTxn, CreateModelTxn,
                    DropModelTxn,
                },
                space::{AlterSpaceTxn, CreateSpaceTxn, DropSpaceTxn},
                sysctl::{AlterUserTxn, CreateUserTxn, DropUserTxn},
                GNSTransaction, GNSTransactionCode,
            },
            RuntimeResult,
        },
        util::compiler::TaggedEnum,
    },
};

/*
    GNS event log impl
*/

pub type GNSDriver = EventLogDriver<GNSEventLog>;
#[derive(Debug)]
pub struct GNSEventLog;

impl GNSDriver {
    const FILE_PATH: &'static str = "gns.db-tlog";
    pub fn open_gns_with_name(
        name: &str,
        gs: &GNSData,
        settings: JournalSettings,
    ) -> RuntimeResult<Self> {
        journal::open_journal(name, gs, settings)
    }
    pub fn open_gns(gs: &GNSData, settings: JournalSettings) -> RuntimeResult<Self> {
        Self::open_gns_with_name(Self::FILE_PATH, gs, settings)
    }
    pub fn create_gns_with_name(name: &str) -> RuntimeResult<Self> {
        journal::create_journal(name)
    }
    /// Create a new event log
    pub fn create_gns() -> RuntimeResult<Self> {
        Self::create_gns_with_name(Self::FILE_PATH)
    }
}

macro_rules! make_dispatch {
    ($($obj:ty),* $(,)?) => {
        [$(<$obj as crate::engine::storage::common_encoding::r1::impls::gns::GNSEvent>::decode_apply),*]
    }
}

impl EventLogSpec for GNSEventLog {
    type Spec = SystemDatabaseV1;
    type GlobalState = GNSData;
    type EventMeta = GNSTransactionCode;
    type DecodeDispatch =
        [fn(&GNSData, Vec<u8>) -> RuntimeResult<()>; GNSTransactionCode::VARIANT_COUNT];
    const DECODE_DISPATCH: Self::DecodeDispatch = make_dispatch![
        CreateSpaceTxn,
        AlterSpaceTxn,
        DropSpaceTxn,
        CreateModelTxn,
        AlterModelAddTxn,
        AlterModelRemoveTxn,
        AlterModelUpdateTxn,
        DropModelTxn,
        CreateUserTxn,
        AlterUserTxn,
        DropUserTxn,
    ];
}

impl<T: GNSEvent> JournalAdapterEvent<EventLogAdapter<GNSEventLog>> for T {
    fn md(&self) -> u64 {
        <T as GNSTransaction>::CODE.dscr_u64()
    }
    fn write_buffered(self, b: &mut Vec<u8>, _: ()) {
        T::encode_event(self, b)
    }
}
