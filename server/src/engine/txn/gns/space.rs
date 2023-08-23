/*
 * Created on Wed Aug 23 2023
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

use crate::engine::{idx::STIndex, txn::TransactionError};

use {
    super::GNSEvent,
    crate::{
        engine::{
            core::space::Space,
            data::{uuid::Uuid, DictGeneric},
            storage::v1::{
                inf::{map, obj, PersistObject},
                SDSSError,
            },
        },
        util::EndianQW,
    },
    std::marker::PhantomData,
};

/*
    create space
*/

pub struct CreateSpaceTxn<'a>(PhantomData<&'a ()>);

#[derive(Clone, Copy)]
pub struct CreateSpaceTxnCommitPL<'a> {
    pub(crate) space_meta: &'a DictGeneric,
    pub(crate) space_name: &'a str,
    pub(crate) space: &'a Space,
}

pub struct CreateSpaceTxnRestorePL {
    pub(crate) space_name: Box<str>,
    pub(crate) space: Space,
}

pub struct CreateSpaceTxnMD {
    pub(crate) space_name_l: u64,
    pub(crate) space_meta: <obj::SpaceLayoutRef<'static> as PersistObject>::Metadata,
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

impl<'a> GNSEvent for CreateSpaceTxn<'a> {
    const OPC: u16 = 0;
    type CommitType = CreateSpaceTxnCommitPL<'a>;
    type RestoreType = CreateSpaceTxnRestorePL;
    fn update_global_state(
        CreateSpaceTxnRestorePL { space_name, space }: CreateSpaceTxnRestorePL,
        gns: &crate::engine::core::GlobalNS,
    ) -> crate::engine::txn::TransactionResult<()> {
        let mut wgns = gns.spaces().write();
        if wgns.st_insert(space_name, space) {
            Ok(())
        } else {
            Err(TransactionError::OnRestoreDataConflictAlreadyExists)
        }
    }
}

/*
    alter space
    ---
    for now dump the entire meta
*/

pub struct AlterSpaceTxn<'a>(PhantomData<&'a ()>);
pub struct AlterSpaceTxnMD {
    uuid: Uuid,
    space_name_l: u64,
    dict_len: u64,
}
#[derive(Clone, Copy)]
pub struct AlterSpaceTxnCommitPL<'a> {
    space_uuid: Uuid,
    space_name: &'a str,
    space_meta: &'a DictGeneric,
}
pub struct AlterSpaceTxnRestorePL {
    space_name: Box<str>,
    space_meta: DictGeneric,
}

impl<'a> PersistObject for AlterSpaceTxn<'a> {
    const METADATA_SIZE: usize = sizeof!(u64, 2) + sizeof!(u128);
    type InputType = AlterSpaceTxnCommitPL<'a>;
    type OutputType = AlterSpaceTxnRestorePL;
    type Metadata = AlterSpaceTxnMD;
    fn pretest_can_dec_object(
        scanner: &crate::engine::storage::v1::BufferedScanner,
        md: &Self::Metadata,
    ) -> bool {
        scanner.has_left(md.space_name_l as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_uuid.to_le_bytes());
        buf.extend(data.space_name.len().u64_bytes_le());
        buf.extend(data.space_meta.len().u64_bytes_le());
    }
    unsafe fn meta_dec(
        scanner: &mut crate::engine::storage::v1::BufferedScanner,
    ) -> crate::engine::storage::v1::SDSSResult<Self::Metadata> {
        Ok(AlterSpaceTxnMD {
            uuid: Uuid::from_bytes(scanner.next_chunk()),
            space_name_l: u64::from_le_bytes(scanner.next_chunk()),
            dict_len: u64::from_le_bytes(scanner.next_chunk()),
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name.as_bytes());
        <map::PersistMapImpl<map::GenericDictSpec> as PersistObject>::obj_enc(buf, data.space_meta);
    }
    unsafe fn obj_dec(
        s: &mut crate::engine::storage::v1::BufferedScanner,
        md: Self::Metadata,
    ) -> crate::engine::storage::v1::SDSSResult<Self::OutputType> {
        let space_name =
            String::from_utf8(s.next_chunk_variable(md.space_name_l as usize).to_owned())
                .map_err(|_| SDSSError::InternalDecodeStructureCorruptedPayload)?
                .into_boxed_str();
        let space_meta = <map::PersistMapImpl<map::GenericDictSpec> as PersistObject>::obj_dec(
            s,
            map::MapIndexSizeMD(md.dict_len as usize),
        )?;
        Ok(AlterSpaceTxnRestorePL {
            space_name,
            space_meta,
        })
    }
}

/*
    drop space
*/

pub struct DropSpace<'a>(PhantomData<&'a ()>);
pub struct DropSpaceTxnMD {
    space_name_l: u64,
    uuid: Uuid,
}
#[derive(Clone, Copy)]
pub struct DropSpaceTxnCommitPL<'a> {
    space_name: &'a str,
    uuid: Uuid,
}
pub struct DropSpaceTxnRestorePL {
    uuid: Uuid,
    space_name: Box<str>,
}

impl<'a> PersistObject for DropSpace<'a> {
    const METADATA_SIZE: usize = sizeof!(u128) + sizeof!(u64);
    type InputType = DropSpaceTxnCommitPL<'a>;
    type OutputType = DropSpaceTxnRestorePL;
    type Metadata = DropSpaceTxnMD;
    fn pretest_can_dec_object(
        scanner: &crate::engine::storage::v1::BufferedScanner,
        md: &Self::Metadata,
    ) -> bool {
        scanner.has_left(md.space_name_l as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name.len().u64_bytes_le());
        buf.extend(data.uuid.to_le_bytes());
    }
    unsafe fn meta_dec(
        scanner: &mut crate::engine::storage::v1::BufferedScanner,
    ) -> crate::engine::storage::v1::SDSSResult<Self::Metadata> {
        Ok(DropSpaceTxnMD {
            space_name_l: u64::from_le_bytes(scanner.next_chunk()),
            uuid: Uuid::from_bytes(scanner.next_chunk()),
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name.as_bytes());
    }
    unsafe fn obj_dec(
        s: &mut crate::engine::storage::v1::BufferedScanner,
        md: Self::Metadata,
    ) -> crate::engine::storage::v1::SDSSResult<Self::OutputType> {
        let space_name =
            String::from_utf8(s.next_chunk_variable(md.space_name_l as usize).to_owned())
                .map_err(|_| SDSSError::InternalDecodeStructureCorruptedPayload)?
                .into_boxed_str();
        Ok(DropSpaceTxnRestorePL {
            uuid: md.uuid,
            space_name,
        })
    }
}
