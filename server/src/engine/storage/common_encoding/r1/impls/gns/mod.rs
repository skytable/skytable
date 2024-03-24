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

use {
    crate::{
        engine::{
            core::GNSData,
            data::uuid::Uuid,
            error::{RuntimeResult, StorageError},
            mem::BufferedScanner,
            storage::common_encoding::r1::{self, PersistObject},
            txn::{gns::GNSTransaction, SpaceIDRef},
        },
        util::EndianQW,
    },
    std::marker::PhantomData,
};

pub mod model;
pub mod space;
// test
#[cfg(test)]
mod tests;

/*
    Events
    ---
    FIXME(@ohsayan): In the current impl, we unnecessarily use an intermediary buffer which we clearly don't need to (and also makes
    pointless allocations). We need to fix this, but with a consistent API (and preferably not something like commit_*(...) unless
    we have absolutely no other choice)
    ---
    [OPC:2B][PAYLOAD]
*/

/// Definition for an event in the GNS (DDL queries)
pub trait GNSEvent
where
    Self: PersistObject<InputType = Self, OutputType = Self::RestoreType> + Sized + GNSTransaction,
{
    /// Expected type for a commit
    type CommitType;
    /// Expected type for a restore
    type RestoreType;
    /// Encodes the event into the given buffer
    fn encode_event(commit: Self, buf: &mut Vec<u8>) {
        r1::enc::full_into_buffer::<Self>(buf, commit)
    }
    fn decode_apply(gns: &GNSData, data: Vec<u8>) -> RuntimeResult<()> {
        let mut scanner = BufferedScanner::new(&data);
        Self::decode_and_update_global_state(&mut scanner, gns)?;
        if scanner.eof() {
            Ok(())
        } else {
            Err(StorageError::V1JournalDecodeLogEntryCorrupted.into())
        }
    }
    fn decode_and_update_global_state(
        scanner: &mut BufferedScanner,
        gns: &GNSData,
    ) -> RuntimeResult<()> {
        Self::update_global_state(Self::decode(scanner)?, gns)
    }
    /// Attempts to decode the event using the given scanner
    fn decode(scanner: &mut BufferedScanner) -> RuntimeResult<Self::RestoreType> {
        r1::dec::full_from_scanner::<Self>(scanner).map_err(|e| e.into())
    }
    /// Update the global state from the restored event
    fn update_global_state(restore: Self::RestoreType, gns: &GNSData) -> RuntimeResult<()>;
}

#[derive(Debug, PartialEq)]
pub struct SpaceIDRes {
    uuid: Uuid,
    name: Box<str>,
}

impl SpaceIDRes {
    #[cfg(test)]
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
        buf.extend(data.uuid().to_le_bytes());
        buf.extend(data.name().len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(SpaceIDMD {
            uuid: Uuid::from_bytes(scanner.next_chunk()),
            space_name_l: scanner.next_u64_le(),
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.name().as_bytes());
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let str = r1::dec::utils::decode_string(s, md.space_name_l as usize)?;
        Ok(SpaceIDRes {
            uuid: md.uuid,
            name: str.into_boxed_str(),
        })
    }
}
