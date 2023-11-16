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
        config::{ConfigAuth, ConfigMode},
        data::{cell::Datacell, DictEntryGeneric, DictGeneric},
        error::{RuntimeResult, StorageError},
        fractal::sys_store::{SysAuth, SysAuthUser, SysConfig, SysHostData, SystemStore},
        storage::v1::{inf, spec, RawFSInterface, SDSSFileIO},
    },
    parking_lot::RwLock,
    std::collections::HashMap,
};

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

impl SystemStoreInitState {
    pub const fn is_created(&self) -> bool {
        matches!(self, Self::Created)
    }
    pub const fn is_existing_updated_root(&self) -> bool {
        matches!(self, Self::UpdatedRoot)
    }
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

impl<Fs: RawFSInterface> SystemStore<Fs> {
    const SYSDB_PATH: &'static str = "sys.db";
    const SYSDB_COW_PATH: &'static str = "sys.db.cow";
    const SYS_KEY_AUTH: &'static str = "auth";
    const SYS_KEY_AUTH_USERS: &'static str = "users";
    const SYS_KEY_SYS: &'static str = "sys";
    const SYS_KEY_SYS_STARTUP_COUNTER: &'static str = "sc";
    const SYS_KEY_SYS_SETTINGS_VERSION: &'static str = "sv";
    pub fn open_or_restore(
        auth: ConfigAuth,
        run_mode: ConfigMode,
    ) -> RuntimeResult<(Self, SystemStoreInitState)> {
        Self::open_with_name(Self::SYSDB_PATH, Self::SYSDB_COW_PATH, auth, run_mode)
    }
    pub fn sync_db(&self, auth: &SysAuth) -> RuntimeResult<()> {
        self._sync_with(Self::SYSDB_PATH, Self::SYSDB_COW_PATH, auth)
    }
    pub fn open_with_name(
        sysdb_name: &str,
        sysdb_cow_path: &str,
        auth: ConfigAuth,
        run_mode: ConfigMode,
    ) -> RuntimeResult<(Self, SystemStoreInitState)> {
        match SDSSFileIO::open_or_create_perm_rw::<spec::SysDBV1>(sysdb_name)? {
            FileOpen::Created(new) => {
                let me = Self::_new(SysConfig::new_auth(auth, run_mode));
                me._sync(new, &me.system_store().auth_data().read())?;
                Ok((me, SystemStoreInitState::Created))
            }
            FileOpen::Existing((ex, _)) => {
                Self::restore_and_sync(ex, auth, run_mode, sysdb_name, sysdb_cow_path)
            }
        }
    }
}

impl<Fs: RawFSInterface> SystemStore<Fs> {
    fn _sync(&self, mut f: SDSSFileIO<Fs>, auth: &SysAuth) -> RuntimeResult<()> {
        let cfg = self.system_store();
        // prepare our flat file
        let mut map: DictGeneric = into_dict!(
            Self::SYS_KEY_SYS => DictEntryGeneric::Map(into_dict!(
                Self::SYS_KEY_SYS_SETTINGS_VERSION => Datacell::new_uint_default(cfg.host_data().settings_version() as _),
                Self::SYS_KEY_SYS_STARTUP_COUNTER => Datacell::new_uint_default(cfg.host_data().startup_counter() as _),
            )),
            Self::SYS_KEY_AUTH => DictGeneric::new(),
        );
        let auth_key = map.get_mut(Self::SYS_KEY_AUTH).unwrap();
        let auth_key = auth_key.as_dict_mut().unwrap();
        auth_key.insert(
            Self::SYS_KEY_AUTH_USERS.into(),
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
    fn _sync_with(&self, target: &str, cow: &str, auth: &SysAuth) -> RuntimeResult<()> {
        let f = SDSSFileIO::create::<spec::SysDBV1>(cow)?;
        self._sync(f, auth)?;
        Fs::fs_rename_file(cow, target)
    }
    fn restore_and_sync(
        f: SDSSFileIO<Fs>,
        auth: ConfigAuth,
        run_mode: ConfigMode,
        fname: &str,
        fcow_name: &str,
    ) -> RuntimeResult<(Self, SystemStoreInitState)> {
        let prev_sysdb = Self::_restore(f, run_mode)?;
        let state;
        // see if settings have changed
        if prev_sysdb
            .auth_data()
            .read()
            .verify_user(SysAuthUser::USER_ROOT, &auth.root_key)
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
            run_mode,
        );
        let slf = Self::_new(new_syscfg);
        // now sync
        slf._sync_with(fname, fcow_name, &slf.system_store().auth_data().read())?;
        Ok((slf, state))
    }
    fn _restore(mut f: SDSSFileIO<Fs>, run_mode: ConfigMode) -> RuntimeResult<SysConfig> {
        let mut sysdb_data =
            inf::dec::dec_dict_full::<inf::map::GenericDictSpec>(&f.load_remaining_into_buffer()?)?;
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
        let sys_auth = SysAuth::new(loaded_users);
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
            & sys_auth.users().contains_key(SysAuthUser::USER_ROOT))
        {
            return Err(StorageError::SysDBCorrupted.into());
        }
        Ok(SysConfig::new(
            RwLock::new(sys_auth),
            SysHostData::new(sc, sv as u32),
            run_mode,
        ))
    }
}
