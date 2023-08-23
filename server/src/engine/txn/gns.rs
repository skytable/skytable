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
    super::{TransactionError, TransactionResult},
    crate::{
        engine::{
            core::{space::Space, GlobalNS},
            data::DictGeneric,
            storage::v1::{
                inf::{obj, PersistObject},
                JournalAdapter, SDSSError,
            },
        },
        util::EndianQW,
    },
    std::marker::PhantomData,
};

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

pub trait GNSEvent: PersistObject {
    const OPC: u16;
    type InputItem;
}

/*
    create space
*/

pub struct CreateSpaceTxn<'a>(PhantomData<&'a ()>);
#[derive(Clone, Copy)]
pub struct CreateSpaceTxnCommitPL<'a> {
    space_meta: &'a DictGeneric,
    space_name: &'a str,
    space: &'a Space,
}
pub struct CreateSpaceTxnRestorePL {
    space_name: Box<str>,
    space: Space,
}
pub struct CreateSpaceTxnMD {
    space_name_l: u64,
    space_meta: <obj::SpaceLayoutRef<'static> as PersistObject>::Metadata,
}

impl<'a> GNSEvent for CreateSpaceTxn<'a> {
    const OPC: u16 = 0;
    type InputItem = CreateSpaceTxnCommitPL<'a>;
}

impl<'a> PersistObject for CreateSpaceTxn<'a> {
    const METADATA_SIZE: usize =
        <obj::SpaceLayoutRef<'static> as PersistObject>::METADATA_SIZE + sizeof!(u64);
    type InputType = CreateSpaceTxnCommitPL<'a>;
    type OutputType = CreateSpaceTxnRestorePL;
    type Metadata = CreateSpaceTxnMD;
    fn pretest_can_dec_object(
        scanner: &crate::engine::storage::v1::BufferedScanner,
        md: &Self::Metadata,
    ) -> bool {
        scanner.has_left(md.space_name_l as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name.len().u64_bytes_le());
        <obj::SpaceLayoutRef<'a> as PersistObject>::meta_enc(
            buf,
            obj::SpaceLayoutRef::from((data.space, data.space_meta)),
        );
    }
    unsafe fn meta_dec(
        scanner: &mut crate::engine::storage::v1::BufferedScanner,
    ) -> crate::engine::storage::v1::SDSSResult<Self::Metadata> {
        let space_name_l = u64::from_le_bytes(scanner.next_chunk());
        let space_meta = <obj::SpaceLayoutRef as PersistObject>::meta_dec(scanner)?;
        Ok(CreateSpaceTxnMD {
            space_name_l,
            space_meta,
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name.as_bytes());
        <obj::SpaceLayoutRef as PersistObject>::meta_enc(buf, (data.space, data.space_meta).into());
    }
    unsafe fn obj_dec(
        s: &mut crate::engine::storage::v1::BufferedScanner,
        md: Self::Metadata,
    ) -> crate::engine::storage::v1::SDSSResult<Self::OutputType> {
        let space_name =
            String::from_utf8(s.next_chunk_variable(md.space_name_l as usize).to_owned())
                .map_err(|_| SDSSError::InternalDecodeStructureCorruptedPayload)?
                .into_boxed_str();
        let space = <obj::SpaceLayoutRef as PersistObject>::obj_dec(s, md.space_meta)?;
        Ok(CreateSpaceTxnRestorePL { space_name, space })
    }
}
