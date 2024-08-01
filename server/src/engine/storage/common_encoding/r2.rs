/*
 * Created on Wed Feb 21 2024
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

/*
    gns txn impls
*/

use {
    super::r1::{dec, impls::gns::GNSEvent, PersistObject},
    crate::{
        engine::{
            core::GNSData,
            error::{StorageError, TransactionError},
            mem::{unsafe_apis::BoxStr, BufferedScanner},
            txn::gns::sysctl::{AlterUserTxn, CreateUserTxn, DropUserTxn},
            RuntimeResult,
        },
        util::EndianQW,
    },
};

/*
    create user txn
*/

impl<'a> GNSEvent for CreateUserTxn<'a> {
    type CommitType = Self;
    type RestoreType = FullUserDefinition;
    fn update_global_state(
        FullUserDefinition { username, password }: Self::RestoreType,
        gns: &GNSData,
    ) -> RuntimeResult<()> {
        if gns.sys_db().__raw_create_user(username, password) {
            Ok(())
        } else {
            Err(TransactionError::OnRestoreDataConflictAlreadyExists.into())
        }
    }
}

pub struct FullUserDefinition {
    username: BoxStr,
    password: Box<[u8]>,
}

impl FullUserDefinition {
    fn new(username: BoxStr, password: Box<[u8]>) -> Self {
        Self { username, password }
    }
}

pub struct CreateUserMetadata {
    uname_l: u64,
    pwd_l: u64,
    props_l: u64,
}

impl CreateUserMetadata {
    pub fn new(uname_l: u64, pwd_l: u64, props_l: u64) -> Self {
        Self {
            uname_l,
            pwd_l,
            props_l,
        }
    }
}

impl<'a> PersistObject for CreateUserTxn<'a> {
    const METADATA_SIZE: usize = sizeof!(u64, 3);
    type InputType = Self;
    type OutputType = FullUserDefinition;
    type Metadata = CreateUserMetadata;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left((md.uname_l + md.pwd_l) as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        // [username length: 8B][password length: 8B][properties length: 8B]
        buf.extend(data.username().len().u64_bytes_le());
        buf.extend(data.password_hash().len().u64_bytes_le());
        buf.extend(0u64.u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        let uname_l = scanner.next_u64_le();
        let pwd_l = scanner.next_u64_le();
        let props_l = scanner.next_u64_le();
        Ok(CreateUserMetadata::new(uname_l, pwd_l, props_l))
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.username().as_bytes());
        buf.extend(data.password_hash());
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let username = dec::utils::decode_box_str(s, md.uname_l as _)?;
        let password = s.next_chunk_variable(md.pwd_l as _);
        if md.props_l == 0 {
            Ok(FullUserDefinition::new(
                username,
                password.to_vec().into_boxed_slice(),
            ))
        } else {
            Err(StorageError::InternalDecodeStructureIllegalData.into())
        }
    }
}

/*
    alter user txn
*/

impl<'a> GNSEvent for AlterUserTxn<'a> {
    type CommitType = Self;
    type RestoreType = FullUserDefinition;
    fn update_global_state(
        FullUserDefinition { username, password }: Self::RestoreType,
        gns: &GNSData,
    ) -> RuntimeResult<()> {
        if gns.sys_db().__raw_alter_user(&username, password) {
            Ok(())
        } else {
            Err(TransactionError::OnRestoreDataConflictMismatch.into())
        }
    }
}

impl<'a> PersistObject for AlterUserTxn<'a> {
    const METADATA_SIZE: usize = sizeof!(u64, 3);
    type InputType = Self;
    type OutputType = FullUserDefinition;
    type Metadata = CreateUserMetadata;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left((md.uname_l + md.pwd_l) as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        // [username length: 8B][password length: 8B][properties length: 8B]
        buf.extend(data.username().len().u64_bytes_le());
        buf.extend(data.password_hash().len().u64_bytes_le());
        buf.extend(0u64.u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        let uname_l = scanner.next_u64_le();
        let pwd_l = scanner.next_u64_le();
        let props_l = scanner.next_u64_le();
        Ok(CreateUserMetadata::new(uname_l, pwd_l, props_l))
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.username().as_bytes());
        buf.extend(data.password_hash());
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let username = dec::utils::decode_box_str(s, md.uname_l as _)?;
        let password = s.next_chunk_variable(md.pwd_l as _);
        if md.props_l == 0 {
            Ok(FullUserDefinition::new(
                username,
                password.to_vec().into_boxed_slice(),
            ))
        } else {
            Err(StorageError::InternalDecodeStructureIllegalData.into())
        }
    }
}

/*
    drop user txn
*/

pub struct DropUserPayload(BoxStr);

impl<'a> GNSEvent for DropUserTxn<'a> {
    type CommitType = Self;
    type RestoreType = DropUserPayload;
    fn update_global_state(
        DropUserPayload(username): Self::RestoreType,
        gns: &GNSData,
    ) -> RuntimeResult<()> {
        if gns.sys_db().__raw_delete_user(&username) {
            Ok(())
        } else {
            Err(TransactionError::OnRestoreDataConflictMismatch.into())
        }
    }
}

impl<'a> PersistObject for DropUserTxn<'a> {
    const METADATA_SIZE: usize = sizeof!(u64);
    type InputType = Self;
    type OutputType = DropUserPayload;
    type Metadata = u64;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left(*md as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.username().len().u64_bytes_le())
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(scanner.next_u64_le())
    }
    fn obj_enc(buf: &mut Vec<u8>, data: Self::InputType) {
        buf.extend(data.username().as_bytes());
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let username = dec::utils::decode_box_str(s, md as usize)?;
        Ok(DropUserPayload(username))
    }
}
