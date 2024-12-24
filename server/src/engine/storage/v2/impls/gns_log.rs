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
    super::super::raw::journal::{EventLogAdapter, EventLogSpec},
    crate::{
        engine::{
            control_flow::context,
            core::{model::Model, EntityID, GNSData},
            storage::{
                common::{
                    interface::fs::FileSystem, paths_v1, sdss, versions::FileSpecifierVersion,
                },
                common_encoding::r1::impls::gns::GNSEvent,
                v1,
                v2::raw::{
                    journal::{
                        self, EventLogDriver, JournalAdapterEvent, JournalHeuristics,
                        JournalSettings, JournalStats,
                    },
                    spec::{FileClass, FileSpecifier, HeaderImplV2},
                },
            },
            txn::{
                gns::{
                    model::{
                        AlterModelAddTxn, AlterModelRemoveTxn, AlterModelUpdateTxn, CreateModelTxn,
                        DropModelTxn,
                    },
                    space::{AlterSpaceTxn, CreateSpaceTxn, DropSpaceTxn},
                    sysctl::{AlterUserTxn, CreateUserTxn, DropUserTxn},
                    GNSTransaction, GNSTransactionCode,
                },
                SpaceIDRef,
            },
            RuntimeResult,
        },
        util::compiler::TaggedEnum,
    },
};

/*
    GNS event log impl
*/

pub type GNSAdapter = EventLogAdapter<GNSEventLog>;

/*
    metadata spec
*/

pub struct FSpecSystemDatabaseV1;
impl sdss::sdss_r1::SimpleFileSpecV1 for FSpecSystemDatabaseV1 {
    type HeaderSpec = HeaderImplV2;
    const FILE_CLASS: FileClass = FileClass::EventLog;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::GlobalNS;
    const FILE_SPECIFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}

#[cfg(test)]
local! {
    static EVENT_TRACING: ReadEventTracing = ReadEventTracing { total: 0, repeat: 0 };
    static EXECUTED_EVENTS: usize = 0;
}

#[cfg(test)]
pub fn get_executed_event_count() -> usize {
    local_mut!(EXECUTED_EVENTS, |ev| core::mem::take(ev))
}

#[cfg(test)]
pub fn get_tracing() -> ReadEventTracing {
    local_mut!(EVENT_TRACING, |tracing| core::mem::take(tracing))
}

#[cfg(test)]
#[derive(Debug, Default, PartialEq)]
pub struct ReadEventTracing {
    pub total: usize,
    pub repeat: usize,
}

pub type GNSDriver = EventLogDriver<GNSEventLog>;
#[derive(Debug)]
pub struct GNSEventLog;

impl GNSDriver {
    pub fn open_gns_with_name(
        name: &str,
        gs: &GNSData,
        settings: JournalSettings,
    ) -> RuntimeResult<(Self, JournalStats)> {
        journal::open_journal(name, gs, settings)
    }
    pub fn open_gns(
        gs: &GNSData,
        settings: JournalSettings,
    ) -> RuntimeResult<(Self, JournalStats)> {
        Self::open_gns_with_name(v1::GNS_PATH, gs, settings)
    }
    pub fn create_gns_with_name(name: &str) -> RuntimeResult<Self> {
        journal::create_journal(name)
    }
    /// Create a new event log
    pub fn create_gns() -> RuntimeResult<Self> {
        Self::create_gns_with_name(v1::GNS_PATH)
    }
}

macro_rules! make_dispatch {
    ($($obj:ty => $f:expr),* $(,)?) => {
        [$(|gs, heuristics, payload| {
            fn _c<F: Fn(&mut JournalHeuristics)>(f: F, heuristics: &mut JournalHeuristics) { f(heuristics) }
            <$obj as crate::engine::storage::common_encoding::r1::impls::gns::GNSEvent>::decode_apply(gs, payload)?; _c($f, heuristics);
            #[cfg(test)] { local_mut!(EVENT_TRACING, |tracing| {tracing.total += 1; tracing.repeat = heuristics.get_current_redundant()}) } Ok(())
        }),*]
    }
}

pub fn reinit_full<const INIT_DIRS: bool>(
    gns_driver: &mut GNSDriver,
    gns: &GNSData,
    for_each_model: impl Fn(&EntityID, &Model) -> RuntimeResult<()>,
) -> RuntimeResult<()> {
    // create all spaces
    context::set_dmsg("creating all spaces");
    for (space_name, space) in gns.idx().read().iter() {
        if INIT_DIRS {
            FileSystem::create_dir_all(&paths_v1::space_dir(space_name, space.get_uuid()))?;
        }
        gns_driver.commit_event(CreateSpaceTxn::new(space.props(), &space_name, space))?;
    }
    // create all users
    context::set_dmsg("creating all users");
    for (user_name, user) in gns.sys_db().users().read().iter() {
        gns_driver.commit_event(CreateUserTxn::new(&user_name, user.hash()))?;
    }
    // create all models
    context::set_dmsg("creating all models");
    for (model_id, model) in gns.idx_models().read().iter() {
        let model_data = model.data();
        let space_uuid = gns.idx().read().get(model_id.space()).unwrap().get_uuid();
        for_each_model(model_id, model)?;
        gns_driver.commit_event(CreateModelTxn::new(
            SpaceIDRef::with_uuid(model_id.space(), space_uuid),
            model_id.entity(),
            model_data,
        ))?;
    }
    Ok(())
}

impl EventLogSpec for GNSEventLog {
    type Spec = FSpecSystemDatabaseV1;
    type GlobalState = GNSData;
    type EventMeta = GNSTransactionCode;
    type DecodeDispatch = [fn(&GNSData, &mut JournalHeuristics, Vec<u8>) -> RuntimeResult<()>;
        GNSTransactionCode::VARIANT_COUNT];
    type FullSyncCtx<'a> = &'a GNSData;
    const DECODE_DISPATCH: Self::DecodeDispatch = make_dispatch![
        CreateSpaceTxn => |_| {},
        AlterSpaceTxn => |h| h.report_new_redundant_record(),
        DropSpaceTxn => |h| h.report_new_redundant_record(),
        CreateModelTxn => |_| {},
        AlterModelAddTxn => |h| h.report_new_redundant_record(),
        AlterModelRemoveTxn => |h| h.report_new_redundant_record(),
        AlterModelUpdateTxn => |h| h.report_new_redundant_record(),
        DropModelTxn => |h| h.report_new_redundant_record(),
        CreateUserTxn => |_| {},
        AlterUserTxn => |h| h.report_new_redundant_record(),
        DropUserTxn => |h| h.report_new_redundant_record(),
    ];
    fn rewrite_log<'a>(writer: &mut GNSDriver, ctx: Self::FullSyncCtx<'a>) -> RuntimeResult<()> {
        reinit_full::<false>(writer, ctx, |_, _| Ok(()))
    }
}

impl<T: GNSEvent> JournalAdapterEvent<EventLogAdapter<GNSEventLog>> for T {
    fn md(&self) -> GNSTransactionCode {
        <T as GNSTransaction>::CODE
    }
    fn write_buffered(self, b: &mut Vec<u8>, _: ()) {
        #[cfg(test)]
        {
            local_mut!(EXECUTED_EVENTS, |ev| *ev += 1);
        }
        T::encode_event(self, b)
    }
}
