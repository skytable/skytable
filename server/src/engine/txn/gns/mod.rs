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

#[cfg(test)]
use crate::engine::storage::v1::memfs::VirtualFS;
use {
    super::{TransactionError, TransactionResult},
    crate::{
        engine::{
            core::{space::Space, GlobalNS},
            data::uuid::Uuid,
            mem::BufferedScanner,
            storage::v1::{
                inf::{self, PersistObject},
                JournalAdapter, JournalWriter, LocalFS, RawFSInterface, SDSSResult,
            },
        },
        util::EndianQW,
    },
    std::marker::PhantomData,
};

mod model;
mod space;
// test
#[cfg(test)]
mod tests;

// re-exports
pub use {
    model::{
        AlterModelAddTxn, AlterModelRemoveTxn, AlterModelUpdateTxn, CreateModelTxn, DropModelTxn,
        ModelIDRef,
    },
    space::{AlterSpaceTxn, CreateSpaceTxn, DropSpaceTxn},
};

pub type GNSTransactionDriverNullZero =
    GNSTransactionDriverAnyFS<crate::engine::storage::v1::NullFS>;
#[cfg(test)]
pub type GNSTransactionDriverVFS = GNSTransactionDriverAnyFS<VirtualFS>;

/// The GNS transaction driver is used to handle DDL transactions
pub struct GNSTransactionDriverAnyFS<Fs: RawFSInterface = LocalFS> {
    journal: JournalWriter<Fs, GNSAdapter>,
}

impl<Fs: RawFSInterface> GNSTransactionDriverAnyFS<Fs> {
    pub fn new(journal: JournalWriter<Fs, GNSAdapter>) -> Self {
        Self { journal }
    }
    pub fn __journal_mut(&mut self) -> &mut JournalWriter<Fs, GNSAdapter> {
        &mut self.journal
    }
    /// Attempts to commit the given event into the journal, handling any possible recovery triggers and returning
    /// errors (if any)
    pub fn try_commit<GE: GNSEvent>(&mut self, gns_event: GE) -> TransactionResult<()> {
        let mut buf = vec![];
        buf.extend(GE::OPC.to_le_bytes());
        GE::encode_super_event(gns_event, &mut buf);
        self.journal
            .append_event_with_recovery_plugin(GNSSuperEvent(buf.into_boxed_slice()))?;
        Ok(())
    }
}

/*
    journal implementor
*/

/// the journal adapter for DDL queries on the GNS
#[derive(Debug)]
pub struct GNSAdapter;

impl JournalAdapter for GNSAdapter {
    const RECOVERY_PLUGIN: bool = true;
    type JournalEvent = GNSSuperEvent;
    type GlobalState = GlobalNS;
    type Error = TransactionError;
    fn encode(GNSSuperEvent(b): Self::JournalEvent) -> Box<[u8]> {
        b
    }
    fn decode_and_update_state(payload: &[u8], gs: &Self::GlobalState) -> TransactionResult<()> {
        if payload.len() < 2 {
            return Err(TransactionError::DecodedUnexpectedEof);
        }
        macro_rules! dispatch {
            ($($item:ty),* $(,)?) => {
                [$(<$item as GNSEvent>::decode_and_update_global_state),*, |_, _| Err(TransactionError::DecodeUnknownTxnOp)]
            };
        }
        static DISPATCH: [fn(&mut BufferedScanner, &GlobalNS) -> TransactionResult<()>; 9] = dispatch!(
            CreateSpaceTxn,
            AlterSpaceTxn,
            DropSpaceTxn,
            CreateModelTxn,
            AlterModelAddTxn,
            AlterModelRemoveTxn,
            AlterModelUpdateTxn,
            DropModelTxn
        );
        let mut scanner = BufferedScanner::new(&payload);
        let opc = unsafe {
            // UNSAFE(@ohsayan):
            u16::from_le_bytes(scanner.next_chunk())
        };
        match DISPATCH[(opc as usize).min(DISPATCH.len())](&mut scanner, gs) {
            Ok(()) if scanner.eof() => return Ok(()),
            Ok(_) => Err(TransactionError::DecodeCorruptedPayloadMoreBytes),
            Err(e) => Err(e),
        }
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

/// Definition for an event in the GNS (DDL queries)
pub trait GNSEvent
where
    Self: PersistObject<InputType = Self, OutputType = Self::RestoreType> + Sized,
{
    /// OPC for the event (unique)
    const OPC: u16;
    /// Expected type for a commit
    type CommitType;
    /// Expected type for a restore
    type RestoreType;
    /// Encodes the event into the given buffer
    fn encode_super_event(commit: Self, buf: &mut Vec<u8>) {
        inf::enc::enc_full_into_buffer::<Self>(buf, commit)
    }
    fn decode_and_update_global_state(
        scanner: &mut BufferedScanner,
        gns: &GlobalNS,
    ) -> TransactionResult<()> {
        Self::update_global_state(Self::decode(scanner)?, gns)
    }
    /// Attempts to decode the event using the given scanner
    fn decode(scanner: &mut BufferedScanner) -> TransactionResult<Self::RestoreType> {
        inf::dec::dec_full_from_scanner::<Self>(scanner).map_err(|e| e.into())
    }
    /// Update the global state from the restored event
    fn update_global_state(restore: Self::RestoreType, gns: &GlobalNS) -> TransactionResult<()>;
}

#[derive(Debug, Clone, Copy)]
pub struct SpaceIDRef<'a> {
    uuid: Uuid,
    name: &'a str,
}

impl<'a> SpaceIDRef<'a> {
    pub fn new(name: &'a str, space: &Space) -> Self {
        Self {
            uuid: space.get_uuid(),
            name,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SpaceIDRes {
    uuid: Uuid,
    name: Box<str>,
}

impl SpaceIDRes {
    pub fn new(uuid: Uuid, name: Box<str>) -> Self {
        Self { uuid, name }
    }
}
struct SpaceID<'a>(PhantomData<SpaceIDRef<'a>>);
pub struct SpaceIDMD {
    uuid: Uuid,
    space_name_l: u64,
}

impl<'a> PersistObject for SpaceID<'a> {
    const METADATA_SIZE: usize = sizeof!(u128) + sizeof!(u64);
    type InputType = SpaceIDRef<'a>;
    type OutputType = SpaceIDRes;
    type Metadata = SpaceIDMD;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left(md.space_name_l as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.uuid.to_le_bytes());
        buf.extend(data.name.len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata> {
        Ok(SpaceIDMD {
            uuid: Uuid::from_bytes(scanner.next_chunk()),
            space_name_l: scanner.next_u64_le(),
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.name.as_bytes());
    }
    unsafe fn obj_dec(s: &mut BufferedScanner, md: Self::Metadata) -> SDSSResult<Self::OutputType> {
        let str = inf::dec::utils::decode_string(s, md.space_name_l as usize)?;
        Ok(SpaceIDRes {
            uuid: md.uuid,
            name: str.into_boxed_str(),
        })
    }
}
