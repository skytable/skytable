/*
 * Created on Fri Sep 22 2023
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
    super::spec::SysDBV1,
    crate::engine::{
        core::system_db::SystemDatabase,
        data::{cell::Datacell, DictEntryGeneric, DictGeneric},
        error::{RuntimeResult, StorageError},
        mem::unsafe_apis::BoxStr,
        storage::{common_encoding::r1, v1::raw::rw::SDSSFileIO},
    },
    std::collections::HashMap,
};

fn rkey<T>(
    d: &mut DictGeneric,
    key: &str,
    transform: impl Fn(DictEntryGeneric) -> Option<T>,
) -> RuntimeResult<T> {
    match d.remove(key).map(transform) {
        Some(Some(k)) => Ok(k),
        _ => Err(StorageError::V1SysDBDecodeCorrupted.into()),
    }
}

pub struct RestoredSystemDatabase {
    pub users: HashMap<BoxStr, Box<[u8]>>,
    pub _startup_counter: u64,
    pub _settings_version: u64,
}

impl RestoredSystemDatabase {
    const SYS_KEY_AUTH: &'static str = "auth";
    const SYS_KEY_AUTH_USERS: &'static str = "users";
    const SYS_KEY_SYS: &'static str = "sys";
    const SYS_KEY_SYS_STARTUP_COUNTER: &'static str = "sc";
    const SYS_KEY_SYS_SETTINGS_VERSION: &'static str = "sv";
    pub fn new(
        users: HashMap<BoxStr, Box<[u8]>>,
        startup_counter: u64,
        settings_version: u64,
    ) -> Self {
        Self {
            users,
            _startup_counter: startup_counter,
            _settings_version: settings_version,
        }
    }
    pub fn restore(name: &str) -> RuntimeResult<Self> {
        let (mut f, _) = SDSSFileIO::open::<SysDBV1>(name)?;
        let mut sysdb_data = r1::dec::dict_full::<r1::map::GenericDictSpec>(&f.read_full()?)?;
        // get our auth and sys stores
        let mut auth_store = rkey(
            &mut sysdb_data,
            Self::SYS_KEY_AUTH,
            DictEntryGeneric::into_dict,
        )?;
        let mut sys_store = rkey(
            &mut sysdb_data,
            Self::SYS_KEY_SYS,
            DictEntryGeneric::into_dict,
        )?;
        // load auth store
        let users = rkey(
            &mut auth_store,
            Self::SYS_KEY_AUTH_USERS,
            DictEntryGeneric::into_dict,
        )?;
        // load users
        let mut loaded_users = HashMap::new();
        for (username, userdata) in users {
            let mut userdata = userdata
                .into_data()
                .and_then(Datacell::into_list)
                .ok_or(StorageError::V1SysDBDecodeCorrupted)?;
            if userdata.len() != 1 {
                return Err(StorageError::V1SysDBDecodeCorrupted.into());
            }
            let user_password = userdata
                .remove(0)
                .into_bin()
                .ok_or(StorageError::V1SysDBDecodeCorrupted)?;
            loaded_users.insert(username, user_password.into_boxed_slice());
        }
        // load sys data
        let sc = rkey(&mut sys_store, Self::SYS_KEY_SYS_STARTUP_COUNTER, |d| {
            d.into_data()?.into_uint()
        })?;
        let sv = rkey(&mut sys_store, Self::SYS_KEY_SYS_SETTINGS_VERSION, |d| {
            d.into_data()?.into_uint()
        })?;
        if !(sysdb_data.is_empty()
            & auth_store.is_empty()
            & sys_store.is_empty()
            & loaded_users.contains_key(SystemDatabase::ROOT_ACCOUNT))
        {
            return Err(StorageError::V1SysDBDecodeCorrupted.into());
        }
        Ok(Self::new(loaded_users, sc, sv))
    }
}
