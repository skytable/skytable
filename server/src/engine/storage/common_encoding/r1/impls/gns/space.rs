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

use {
    super::GNSEvent,
    crate::{
        engine::{
            core::{space::Space, EntityIDRef, GNSData},
            data::DictGeneric,
            error::{RuntimeResult, TransactionError},
            idx::STIndex,
            mem::{unsafe_apis::BoxStr, BufferedScanner},
            storage::common_encoding::r1::{dec, map, obj, PersistObject},
            txn::gns::space::{AlterSpaceTxn, CreateSpaceTxn, DropSpaceTxn},
        },
        util::EndianQW,
    },
};

/*
    create space
*/

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct CreateSpaceTxnRestorePL {
    pub(super) space_name: BoxStr,
    pub(super) space: Space,
}

pub struct CreateSpaceTxnMD {
    pub(super) space_name_l: u64,
    pub(super) space_meta: <obj::SpaceLayoutRef<'static> as PersistObject>::Metadata,
}

impl<'a> PersistObject for CreateSpaceTxn<'a> {
    const METADATA_SIZE: usize =
        <obj::SpaceLayoutRef<'static> as PersistObject>::METADATA_SIZE + sizeof!(u64);
    type InputType = CreateSpaceTxn<'a>;
    type OutputType = CreateSpaceTxnRestorePL;
    type Metadata = CreateSpaceTxnMD;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left(md.space_name_l as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name().len().u64_bytes_le());
        <obj::SpaceLayoutRef as PersistObject>::meta_enc(
            buf,
            obj::SpaceLayoutRef::from((data.space(), data.space_meta())),
        );
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        let space_name_l = scanner.next_u64_le();
        let space_meta = <obj::SpaceLayoutRef as PersistObject>::meta_dec(scanner)?;
        Ok(CreateSpaceTxnMD {
            space_name_l,
            space_meta,
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name().as_bytes());
        <obj::SpaceLayoutRef as PersistObject>::obj_enc(
            buf,
            (data.space(), data.space_meta()).into(),
        );
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let space_name = dec::utils::decode_box_str(s, md.space_name_l as usize)?;
        let space = <obj::SpaceLayoutRef as PersistObject>::obj_dec(s, md.space_meta)?;
        Ok(CreateSpaceTxnRestorePL { space_name, space })
    }
}

impl<'a> GNSEvent for CreateSpaceTxn<'a> {
    type CommitType = CreateSpaceTxn<'a>;
    type RestoreType = CreateSpaceTxnRestorePL;
    fn update_global_state(
        CreateSpaceTxnRestorePL { space_name, space }: CreateSpaceTxnRestorePL,
        gns: &crate::engine::core::GNSData,
    ) -> RuntimeResult<()> {
        let mut spaces = gns.idx().write();
        if spaces.st_insert(space_name, space.into()) {
            Ok(())
        } else {
            Err(TransactionError::OnRestoreDataConflictAlreadyExists.into())
        }
    }
}

/*
    alter space
    ---
    for now dump the entire meta
*/

pub struct AlterSpaceTxnMD {
    space_id_meta: super::SpaceIDMD,
    dict_len: u64,
}

#[derive(Debug, PartialEq)]
pub struct AlterSpaceTxnRestorePL {
    pub(super) space_id: super::SpaceIDRes,
    pub(super) space_meta: DictGeneric,
}

impl<'a> PersistObject for AlterSpaceTxn<'a> {
    const METADATA_SIZE: usize = sizeof!(u64, 2) + sizeof!(u128);
    type InputType = AlterSpaceTxn<'a>;
    type OutputType = AlterSpaceTxnRestorePL;
    type Metadata = AlterSpaceTxnMD;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left(md.space_id_meta.space_name_l as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        <super::SpaceID as PersistObject>::meta_enc(buf, data.space_id());
        buf.extend(data.updated_props().len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(AlterSpaceTxnMD {
            space_id_meta: <super::SpaceID as PersistObject>::meta_dec(scanner)?,
            dict_len: scanner.next_u64_le(),
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        <super::SpaceID as PersistObject>::obj_enc(buf, data.space_id());
        <map::PersistMapImpl<map::GenericDictSpec> as PersistObject>::obj_enc(
            buf,
            data.updated_props(),
        );
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let space_id = <super::SpaceID as PersistObject>::obj_dec(s, md.space_id_meta)?;
        let space_meta = <map::PersistMapImpl<map::GenericDictSpec> as PersistObject>::obj_dec(
            s,
            map::MapIndexSizeMD(md.dict_len as usize),
        )?;
        Ok(AlterSpaceTxnRestorePL {
            space_id,
            space_meta,
        })
    }
}

impl<'a> GNSEvent for AlterSpaceTxn<'a> {
    type CommitType = AlterSpaceTxn<'a>;
    type RestoreType = AlterSpaceTxnRestorePL;

    fn update_global_state(
        AlterSpaceTxnRestorePL {
            space_id,
            space_meta,
        }: Self::RestoreType,
        gns: &crate::engine::core::GNSData,
    ) -> RuntimeResult<()> {
        let mut gns = gns.idx().write();
        match gns.st_get_mut(&space_id.name) {
            Some(space) => {
                if !crate::engine::data::dict::rmerge_metadata(space.props_mut(), space_meta) {
                    return Err(TransactionError::OnRestoreDataConflictMismatch.into());
                }
            }
            None => return Err(TransactionError::OnRestoreDataMissing.into()),
        }
        Ok(())
    }
}

/*
    drop space
*/

impl<'a> PersistObject for DropSpaceTxn<'a> {
    const METADATA_SIZE: usize = sizeof!(u128) + sizeof!(u64);
    type InputType = DropSpaceTxn<'a>;
    type OutputType = super::SpaceIDRes;
    type Metadata = super::SpaceIDMD;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left(md.space_name_l as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        <super::SpaceID as PersistObject>::meta_enc(buf, data.space_id());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        <super::SpaceID as PersistObject>::meta_dec(scanner)
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        <super::SpaceID as PersistObject>::obj_enc(buf, data.space_id())
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        <super::SpaceID as PersistObject>::obj_dec(s, md)
    }
}

impl<'a> GNSEvent for DropSpaceTxn<'a> {
    type CommitType = DropSpaceTxn<'a>;
    type RestoreType = super::SpaceIDRes;
    fn update_global_state(
        super::SpaceIDRes { uuid, name }: Self::RestoreType,
        gns: &GNSData,
    ) -> RuntimeResult<()> {
        let mut wgns = gns.idx().write();
        let mut wmodel = gns.idx_models().write();
        match wgns.entry(name) {
            std::collections::hash_map::Entry::Occupied(oe) => {
                if oe.get().get_uuid() == uuid {
                    for model in oe.get().models() {
                        let id: EntityIDRef<'static> = unsafe {
                            // UNSAFE(@ohsayan): I really need a pack of what the borrow checker has been reveling on
                            core::mem::transmute(EntityIDRef::new(oe.key(), &model))
                        };
                        let _ = wmodel.st_delete(&id);
                    }
                    oe.remove_entry();
                    Ok(())
                } else {
                    return Err(TransactionError::OnRestoreDataConflictMismatch.into());
                }
            }
            std::collections::hash_map::Entry::Vacant(_) => {
                return Err(TransactionError::OnRestoreDataMissing.into())
            }
        }
    }
}
