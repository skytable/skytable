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
    super::rw::FileOpen,
    crate::engine::{
        config::ConfigAuth,
        data::{cell::Datacell, DictEntryGeneric, DictGeneric},
        error::{RuntimeResult, StorageError},
        fractal::config::{SysAuth, SysAuthUser, SysConfig, SysHostData},
        storage::v1::{inf, spec, RawFSInterface, SDSSFileIO},
    },
    parking_lot::RwLock,
    std::collections::HashMap,
};

const SYSDB_PATH: &str = "sys.db";
const SYSDB_COW_PATH: &str = "sys.db.cow";
const SYS_KEY_AUTH: &str = "auth";
const SYS_KEY_AUTH_ROOT: &str = "root";
const SYS_KEY_AUTH_USERS: &str = "users";
const SYS_KEY_SYS: &str = "sys";
const SYS_KEY_SYS_STARTUP_COUNTER: &str = "sc";
const SYS_KEY_SYS_SETTINGS_VERSION: &str = "sv";

#[derive(Debug, PartialEq)]
/// The system store init state
pub enum SystemStoreInitState {
    /// No system store was present. it was created
    Created,
    /// The system store was present, but no new changes were applied
    Unchanged,
    /// The system store was present, root settings were updated
    UpdatedRoot,
}

#[derive(Debug, PartialEq)]
/// Result of initializing the system store (sysdb)
pub struct SystemStoreInit {
    pub store: SysConfig,
    pub state: SystemStoreInitState,
}

impl SystemStoreInit {
    pub fn new(store: SysConfig, state: SystemStoreInitState) -> Self {
        Self { store, state }
    }
}

/// Open the system database
///
/// - If it doesn't exist, create it
/// - If it exists, look for config changes and sync them
pub fn open_system_database<Fs: RawFSInterface>(
    auth: ConfigAuth,
) -> RuntimeResult<SystemStoreInit> {
    open_or_reinit_system_database::<Fs>(auth, SYSDB_PATH, SYSDB_COW_PATH)
}

/// Open or re-initialize the system database
pub fn open_or_reinit_system_database<Fs: RawFSInterface>(
    auth: ConfigAuth,
    sysdb_path: &str,
    sysdb_path_cow: &str,
) -> RuntimeResult<SystemStoreInit> {
    let sysdb_file = match SDSSFileIO::<Fs>::open_or_create_perm_rw::<spec::SysDBV1>(sysdb_path)? {
        FileOpen::Created(new) => {
            // init new syscfg
            let new_syscfg = SysConfig::new_auth(auth);
            sync_system_database_to(&new_syscfg, new)?;
            return Ok(SystemStoreInit::new(
                new_syscfg,
                SystemStoreInitState::Created,
            ));
        }
        FileOpen::Existing((ex, _)) => ex,
    };
    let prev_sysdb = decode_system_database(sysdb_file)?;
    let state;
    // see if settings have changed
    if prev_sysdb
        .auth_data()
        .read()
        .verify_user("root", &auth.root_key)
        .is_ok()
    {
        state = SystemStoreInitState::Unchanged;
    } else {
        state = SystemStoreInitState::UpdatedRoot;
    }
    // create new config
    let new_syscfg = SysConfig::new_full(
        auth,
        SysHostData::new(
            prev_sysdb.host_data().startup_counter() + 1,
            prev_sysdb.host_data().settings_version()
                + !matches!(state, SystemStoreInitState::Unchanged) as u32,
        ),
    );
    // sync
    sync_system_database_to(
        &new_syscfg,
        SDSSFileIO::<Fs>::create::<spec::SysDBV1>(sysdb_path_cow)?,
    )?;
    Fs::fs_rename_file(sysdb_path_cow, sysdb_path)?;
    Ok(SystemStoreInit::new(new_syscfg, state))
}

/// Sync the system database to the given file
pub fn sync_system_database_to<Fs: RawFSInterface>(
    cfg: &SysConfig,
    mut f: SDSSFileIO<Fs>,
) -> RuntimeResult<()> {
    // prepare our flat file
    let mut map: DictGeneric = into_dict!(
        SYS_KEY_SYS => DictEntryGeneric::Map(into_dict!(
            SYS_KEY_SYS_SETTINGS_VERSION => Datacell::new_uint(cfg.host_data().settings_version() as _),
            SYS_KEY_SYS_STARTUP_COUNTER => Datacell::new_uint(cfg.host_data().startup_counter() as _),
        )),
        SYS_KEY_AUTH => DictGeneric::new(),
    );
    let auth_key = map.get_mut(SYS_KEY_AUTH).unwrap();
    let auth = cfg.auth_data().read();
    let auth_key = auth_key.as_dict_mut().unwrap();
    auth_key.insert(
        SYS_KEY_AUTH_ROOT.into(),
        DictEntryGeneric::Data(Datacell::new_bin(auth.root_key().into())),
    );
    auth_key.insert(
        SYS_KEY_AUTH_USERS.into(),
        DictEntryGeneric::Map(
            // username -> [..settings]
            auth.users()
                .iter()
                .map(|(username, user)| {
                    (
                        username.to_owned(),
                        DictEntryGeneric::Data(Datacell::new_list(vec![Datacell::new_bin(
                            user.key().into(),
                        )])),
                    )
                })
                .collect(),
        ),
    );
    // write
    let buf = super::inf::enc::enc_dict_full::<super::inf::map::GenericDictSpec>(&map);
    f.fsynced_write(&buf)
}

fn rkey<T>(
    d: &mut DictGeneric,
    key: &str,
    transform: impl Fn(DictEntryGeneric) -> Option<T>,
) -> RuntimeResult<T> {
    match d.remove(key).map(transform) {
        Some(Some(k)) => Ok(k),
        _ => Err(StorageError::SysDBCorrupted.into()),
    }
}

/// Decode the system database
pub fn decode_system_database<Fs: RawFSInterface>(
    mut f: SDSSFileIO<Fs>,
) -> RuntimeResult<SysConfig> {
    let mut sysdb_data =
        inf::dec::dec_dict_full::<inf::map::GenericDictSpec>(&f.load_remaining_into_buffer()?)?;
    // get our auth and sys stores
    let mut auth_store = rkey(&mut sysdb_data, SYS_KEY_AUTH, DictEntryGeneric::into_dict)?;
    let mut sys_store = rkey(&mut sysdb_data, SYS_KEY_SYS, DictEntryGeneric::into_dict)?;
    // load auth store
    let root_key = rkey(&mut auth_store, SYS_KEY_AUTH_ROOT, |d| {
        d.into_data()?.into_bin()
    })?;
    let users = rkey(
        &mut auth_store,
        SYS_KEY_AUTH_USERS,
        DictEntryGeneric::into_dict,
    )?;
    // load users
    let mut loaded_users = HashMap::new();
    for (username, userdata) in users {
        let mut userdata = userdata
            .into_data()
            .and_then(Datacell::into_list)
            .ok_or(StorageError::SysDBCorrupted)?;
        if userdata.len() != 1 {
            return Err(StorageError::SysDBCorrupted.into());
        }
        let user_password = userdata
            .remove(0)
            .into_bin()
            .ok_or(StorageError::SysDBCorrupted)?;
        loaded_users.insert(username, SysAuthUser::new(user_password.into_boxed_slice()));
    }
    let sys_auth = SysAuth::new(root_key.into_boxed_slice(), loaded_users);
    // load sys data
    let sc = rkey(&mut sys_store, SYS_KEY_SYS_STARTUP_COUNTER, |d| {
        d.into_data()?.into_uint()
    })?;
    let sv = rkey(&mut sys_store, SYS_KEY_SYS_SETTINGS_VERSION, |d| {
        d.into_data()?.into_uint()
    })?;
    if !(sysdb_data.is_empty() & auth_store.is_empty() & sys_store.is_empty()) {
        return Err(StorageError::SysDBCorrupted.into());
    }
    Ok(SysConfig::new(
        RwLock::new(sys_auth),
        SysHostData::new(sc, sv as u32),
    ))
}
