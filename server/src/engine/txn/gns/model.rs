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
            core::model::{delta::IRModel, Model},
            data::uuid::Uuid,
            idx::STIndex,
            storage::v1::{
                inf::{self, obj, PersistObject},
                BufferedScanner, SDSSResult,
            },
            txn::TransactionError,
        },
        util::EndianQW,
    },
    std::marker::PhantomData,
};

/*
    create model
*/

/// Transaction for running a `create model ... (...) with {..}` query
pub struct CreateModelTxn<'a>(PhantomData<&'a ()>);

impl<'a> CreateModelTxn<'a> {
    pub const fn new_commit(
        space_name: &'a str,
        space_uuid: Uuid,
        model_name: &'a str,
        model: &'a Model,
        model_read: &'a IRModel<'a>,
    ) -> CreateModelTxnCommitPL<'a> {
        CreateModelTxnCommitPL {
            space_name,
            space_uuid,
            model_name,
            model,
            model_read,
        }
    }
}

#[derive(Clone, Copy)]
pub struct CreateModelTxnCommitPL<'a> {
    space_name: &'a str,
    space_uuid: Uuid,
    model_name: &'a str,
    model: &'a Model,
    model_read: &'a IRModel<'a>,
}

pub struct CreateModelTxnRestorePL {
    space_name: Box<str>,
    space_uuid: Uuid,
    model_name: Box<str>,
    model: Model,
}

pub struct CreateModelTxnMD {
    space_name_l: u64,
    space_uuid: Uuid,
    model_name_l: u64,
    model_meta: <obj::ModelLayoutRef<'static> as PersistObject>::Metadata,
}

impl<'a> PersistObject for CreateModelTxn<'a> {
    const METADATA_SIZE: usize =
        sizeof!(u64, 2) + sizeof!(u128) + <obj::ModelLayoutRef<'a> as PersistObject>::METADATA_SIZE;
    type InputType = CreateModelTxnCommitPL<'a>;
    type OutputType = CreateModelTxnRestorePL;
    type Metadata = CreateModelTxnMD;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left((md.model_meta.p_key_len() + md.model_name_l) as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.space_name.len().u64_bytes_le());
        buf.extend(data.space_uuid.to_le_bytes());
        buf.extend(data.model_name.len().u64_bytes_le());
        <obj::ModelLayoutRef as PersistObject>::meta_enc(
            buf,
            obj::ModelLayoutRef::from((data.model, data.model_read)),
        )
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata> {
        let space_name_l = u64::from_le_bytes(scanner.next_chunk());
        let space_uuid = Uuid::from_bytes(scanner.next_chunk());
        let model_name_l = u64::from_le_bytes(scanner.next_chunk());
        let model_meta = <obj::ModelLayoutRef as PersistObject>::meta_dec(scanner)?;
        Ok(CreateModelTxnMD {
            space_name_l,
            space_uuid,
            model_name_l,
            model_meta,
        })
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.model_name.as_bytes());
        <obj::ModelLayoutRef as PersistObject>::obj_enc(
            buf,
            obj::ModelLayoutRef::from((data.model, data.model_read)),
        )
    }
    unsafe fn obj_dec(s: &mut BufferedScanner, md: Self::Metadata) -> SDSSResult<Self::OutputType> {
        let space_name =
            inf::dec::utils::decode_string(s, md.space_name_l as usize)?.into_boxed_str();
        let model_name =
            inf::dec::utils::decode_string(s, md.model_name_l as usize)?.into_boxed_str();
        let model = <obj::ModelLayoutRef as PersistObject>::obj_dec(s, md.model_meta)?;
        Ok(CreateModelTxnRestorePL {
            space_name,
            space_uuid: md.space_uuid,
            model_name,
            model,
        })
    }
}

impl<'a> GNSEvent for CreateModelTxn<'a> {
    const OPC: u16 = 3;
    type CommitType = CreateModelTxnCommitPL<'a>;
    type RestoreType = CreateModelTxnRestorePL;
    fn update_global_state(
        CreateModelTxnRestorePL {
            space_name,
            space_uuid,
            model_name,
            model,
        }: Self::RestoreType,
        gns: &crate::engine::core::GlobalNS,
    ) -> crate::engine::txn::TransactionResult<()> {
        let rgns = gns.spaces().read();
        /*
            NOTE(@ohsayan):
            do note that this is a little interesting situation especially because we need to be able to handle
            changes in the schema *and* be able to "sync" that (for consistency) with the model's primary index.

            There is no evident way about how this is going to be handled, but the ideal way would be to keep
            versioned index of schemas.
        */
        match rgns.st_get(&space_name) {
            Some(space) if space.get_uuid() == space_uuid => {
                if space._create_model(&model_name, model).is_ok() {
                    Ok(())
                } else {
                    Err(TransactionError::OnRestoreDataConflictAlreadyExists)
                }
            }
            Some(_) => return Err(TransactionError::OnRestoreDataConflictMismatch),
            None => return Err(TransactionError::OnRestoreDataMissing),
        }
    }
}
