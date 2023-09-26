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
    super::{rw::FileOpen, SDSSError},
    crate::engine::{
        config::ConfigAuth,
        data::{cell::Datacell, DictEntryGeneric, DictGeneric},
        fractal::config::{SysAuth, SysAuthUser, SysConfig, SysHostData},
        storage::v1::{inf, spec, RawFSInterface, SDSSFileIO, SDSSResult},
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
    /// the system store was present, auth was previously enabled but is now disabled
    UpdatedAuthDisabled,
    /// the system store was present, auth was previously disabled but is now enabled
    UpdatedAuthEnabled,
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
    auth: Option<ConfigAuth>,
) -> SDSSResult<SystemStoreInit> {
    open_or_reinit_system_database::<Fs>(auth, SYSDB_PATH, SYSDB_COW_PATH)
}

/// Open or re-initialize the system database
pub fn open_or_reinit_system_database<Fs: RawFSInterface>(
    auth: Option<ConfigAuth>,
    sysdb_path: &str,
    sysdb_path_cow: &str,
) -> SDSSResult<SystemStoreInit> {
    let (ex, _) = match SDSSFileIO::<Fs>::open_or_create_perm_rw::<spec::SysDBV1>(sysdb_path)? {
        FileOpen::Created(new_sysdb) => {
            let syscfg = SysConfig::new_auth(auth, SysHostData::new(0, 0));
            sync_system_database_to(&syscfg, new_sysdb)?;
            return Ok(SystemStoreInit::new(syscfg, SystemStoreInitState::Created));
        }
        FileOpen::Existing(ex) => ex,
    };
    let last_syscfg = decode_system_database(ex)?;
    let mut state = SystemStoreInitState::Unchanged;
    match (last_syscfg.auth_data(), &auth) {
        (Some(last_auth), Some(new_auth)) => {
            let last_auth = last_auth.read();
            if last_auth.verify_user("root", &new_auth.root_key).is_err() {
                // the root password was changed
                state = SystemStoreInitState::UpdatedRoot;
            }
        }
        (Some(_), None) => {
            state = SystemStoreInitState::UpdatedAuthDisabled;
        }
        (None, Some(_)) => {
            state = SystemStoreInitState::UpdatedAuthEnabled;
        }
        (None, None) => {}
    }
    let new_syscfg = SysConfig::new_auth(
        auth,
        SysHostData::new(
            last_syscfg.host_data().startup_counter() + 1,
            last_syscfg.host_data().settings_version()
                + !matches!(state, SystemStoreInitState::Unchanged) as u32,
        ),
    );
    // sync
    let cow_file = SDSSFileIO::<Fs>::create::<spec::SysDBV1>(sysdb_path_cow)?;
    sync_system_database_to(&new_syscfg, cow_file)?;
    // replace
    Fs::fs_rename_file(sysdb_path_cow, sysdb_path)?;
    Ok(SystemStoreInit::new(new_syscfg, state))
}

/// Sync the system database to the given file
pub fn sync_system_database_to<Fs: RawFSInterface>(
    cfg: &SysConfig,
    mut f: SDSSFileIO<Fs>,
) -> SDSSResult<()> {
    // prepare our flat file
    let mut map: DictGeneric = into_dict!(
        SYS_KEY_SYS => DictEntryGeneric::Map(into_dict!(
            SYS_KEY_SYS_SETTINGS_VERSION => Datacell::new_uint(cfg.host_data().settings_version() as _),
            SYS_KEY_SYS_STARTUP_COUNTER => Datacell::new_uint(cfg.host_data().startup_counter() as _),
        )),
        SYS_KEY_AUTH => DictGeneric::new(),
    );
    let auth_key = map.get_mut(SYS_KEY_AUTH).unwrap();
    match cfg.auth_data() {
        None => {
            *auth_key = DictEntryGeneric::Map(
                into_dict!(SYS_KEY_AUTH_ROOT => Datacell::null(), SYS_KEY_AUTH_USERS => Datacell::null()),
            )
        }
        Some(auth) => {
            let auth = auth.read();
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
                                DictEntryGeneric::Data(Datacell::new_list(vec![
                                    Datacell::new_bin(user.key().into()),
                                ])),
                            )
                        })
                        .collect(),
                ),
            );
        }
    }
    // write
    let buf = super::inf::enc::enc_dict_full::<super::inf::map::GenericDictSpec>(&map);
    f.fsynced_write(&buf)
}

fn rkey<T>(
    d: &mut DictGeneric,
    key: &str,
    transform: impl Fn(DictEntryGeneric) -> Option<T>,
) -> SDSSResult<T> {
    match d.remove(key).map(transform) {
        Some(Some(k)) => Ok(k),
        _ => Err(SDSSError::SysDBCorrupted),
    }
}

/// Decode the system database
pub fn decode_system_database<Fs: RawFSInterface>(mut f: SDSSFileIO<Fs>) -> SDSSResult<SysConfig> {
    let rem = f.load_remaining_into_buffer()?;
    let mut store = inf::dec::dec_dict_full::<inf::map::GenericDictSpec>(&rem)?;
    // find auth and sys stores
    let mut auth_store = rkey(&mut store, SYS_KEY_AUTH, DictEntryGeneric::into_dict)?;
    let mut sys_store = rkey(&mut store, SYS_KEY_SYS, DictEntryGeneric::into_dict)?;
    // get our auth
    let auth_root = rkey(&mut auth_store, SYS_KEY_AUTH_ROOT, |dict| {
        let data = dict.into_data()?;
        match data.kind() {
            _ if data.is_null() => Some(None),
            _ => data.into_bin().map(Some),
        }
    })?;
    let auth_users = rkey(&mut auth_store, SYS_KEY_AUTH_USERS, |dict| match dict {
        DictEntryGeneric::Data(dc) if dc.is_null() => Some(None),
        DictEntryGeneric::Map(m) => Some(Some(m)),
        _ => None,
    })?;
    let sys_auth = match (auth_root, auth_users) {
        (Some(root_pass), Some(users)) => {
            let mut usermap = HashMap::new();
            for (user_name, user) in users {
                let mut user_data = user
                    .into_data()
                    .and_then(|d| d.into_list())
                    .ok_or(SDSSError::SysDBCorrupted)?;
                if user_data.len() != 1 {
                    return Err(SDSSError::SysDBCorrupted);
                }
                let password = user_data
                    .remove(0)
                    .into_bin()
                    .ok_or(SDSSError::SysDBCorrupted)?;
                usermap.insert(user_name, SysAuthUser::new(password.into_boxed_slice()));
            }
            Some(RwLock::new(SysAuth::new(
                root_pass.into_boxed_slice(),
                usermap,
            )))
        }
        (None, None) => None,
        _ => return Err(SDSSError::SysDBCorrupted),
    };
    // get our sys
    let sv = rkey(&mut sys_store, SYS_KEY_SYS_SETTINGS_VERSION, |de| {
        de.into_data()?.into_uint()
    })?;
    let sc = rkey(&mut sys_store, SYS_KEY_SYS_STARTUP_COUNTER, |de| {
        de.into_data()?.into_uint()
    })?;
    if !(sys_store.is_empty() & auth_store.is_empty() & store.is_empty()) {
        // the stores have more keys than we expected. something is wrong here
        return Err(SDSSError::SysDBCorrupted);
    }
    Ok(SysConfig::new(sys_auth, SysHostData::new(sc, sv as u32)))
}
