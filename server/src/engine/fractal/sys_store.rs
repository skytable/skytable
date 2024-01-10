/*
 * Created on Sun Sep 10 2023
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
    crate::engine::{
        config::{ConfigAuth, ConfigMode},
        error::{QueryError, QueryResult},
        storage::safe_interfaces::FSInterface,
    },
    parking_lot::RwLock,
    std::{
        collections::{hash_map::Entry, HashMap},
        marker::PhantomData,
    },
};

#[derive(Debug)]
pub struct SystemStore<Fs> {
    syscfg: SysConfig,
    _fs: PhantomData<Fs>,
}

impl<Fs> SystemStore<Fs> {
    pub fn system_store(&self) -> &SysConfig {
        &self.syscfg
    }
}

#[derive(Debug)]
/// The global system configuration
pub struct SysConfig {
    auth_data: RwLock<SysAuth>,
    host_data: SysHostData,
    run_mode: ConfigMode,
}

impl PartialEq for SysConfig {
    fn eq(&self, other: &Self) -> bool {
        self.run_mode == other.run_mode
            && self.host_data == other.host_data
            && self.auth_data.read().eq(&other.auth_data.read())
    }
}

impl SysConfig {
    /// Initialize a new system config
    pub fn new(auth_data: RwLock<SysAuth>, host_data: SysHostData, run_mode: ConfigMode) -> Self {
        Self {
            auth_data,
            host_data,
            run_mode,
        }
    }
    pub fn new_full(new_auth: ConfigAuth, host_data: SysHostData, run_mode: ConfigMode) -> Self {
        Self::new(
            RwLock::new(SysAuth::new(
                into_dict!(SysAuthUser::USER_ROOT => SysAuthUser::new(
                rcrypt::hash(new_auth.root_key.as_str(), rcrypt::DEFAULT_COST)
                    .unwrap()
                    .into_boxed_slice())),
            )),
            host_data,
            run_mode,
        )
    }
    pub fn new_auth(new_auth: ConfigAuth, run_mode: ConfigMode) -> Self {
        Self::new_full(new_auth, SysHostData::new(0, 0), run_mode)
    }
    #[cfg(test)]
    /// A test-mode default setting with the root password set to `password12345678`
    pub(super) fn test_default() -> Self {
        Self {
            auth_data: RwLock::new(SysAuth::new(
                into_dict!(SysAuthUser::USER_ROOT => SysAuthUser::new(
                rcrypt::hash("password12345678", rcrypt::DEFAULT_COST)
                    .unwrap()
                    .into_boxed_slice())),
            )),
            host_data: SysHostData::new(0, 0),
            run_mode: ConfigMode::Dev,
        }
    }
    /// Returns a handle to the authentication data
    pub fn auth_data(&self) -> &RwLock<SysAuth> {
        &self.auth_data
    }
    /// Returns a reference to host data
    pub fn host_data(&self) -> &SysHostData {
        &self.host_data
    }
}

#[derive(Debug, PartialEq)]
/// The host data section (system.host)
pub struct SysHostData {
    startup_counter: u64,
    settings_version: u32,
}

impl SysHostData {
    /// New [`SysHostData`]
    pub fn new(startup_counter: u64, settings_version: u32) -> Self {
        Self {
            startup_counter,
            settings_version,
        }
    }
    /// Returns the startup counter
    ///
    /// Note:
    /// - If this is `0` -> this is the first boot
    /// - If this is `1` -> this is the second boot (... and so on)
    pub fn startup_counter(&self) -> u64 {
        self.startup_counter
    }
    /// Returns the settings version
    ///
    /// Note:
    /// - If this is `0` -> this is the initial setting (first boot)
    ///
    /// If it stays at 0, this means that the settings were never changed
    pub fn settings_version(&self) -> u32 {
        self.settings_version
    }
}

impl<Fs: FSInterface> SystemStore<Fs> {
    pub fn _new(syscfg: SysConfig) -> Self {
        Self {
            syscfg,
            _fs: PhantomData,
        }
    }
    fn _try_sync_or(&self, auth: &mut SysAuth, rb: impl FnOnce(&mut SysAuth)) -> QueryResult<()> {
        match self.sync_db(auth) {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("failed to sync system store: {e}");
                rb(auth);
                Err(e.into())
            }
        }
    }
    /// Create a new user with the given details
    pub fn create_new_user(&self, username: String, password: String) -> QueryResult<()> {
        // TODO(@ohsayan): we want to be very careful with this
        let _username = username.clone();
        let mut auth = self.system_store().auth_data().write();
        match auth.users.entry(username.into()) {
            Entry::Vacant(ve) => {
                ve.insert(SysAuthUser::new(
                    rcrypt::hash(password, rcrypt::DEFAULT_COST)
                        .unwrap()
                        .into_boxed_slice(),
                ));
                self._try_sync_or(&mut auth, |auth| {
                    auth.users.remove(_username.as_str());
                })
            }
            Entry::Occupied(_) => Err(QueryError::SysAuthError),
        }
    }
    pub fn alter_user(&self, username: String, password: String) -> QueryResult<()> {
        let mut auth = self.system_store().auth_data().write();
        match auth.users.get_mut(username.as_str()) {
            Some(user) => {
                let last_pass_hash = core::mem::replace(
                    &mut user.key,
                    rcrypt::hash(password, rcrypt::DEFAULT_COST)
                        .unwrap()
                        .into_boxed_slice(),
                );
                self._try_sync_or(&mut auth, |auth| {
                    auth.users.get_mut(username.as_str()).unwrap().key = last_pass_hash;
                })
            }
            None => Err(QueryError::SysAuthError),
        }
    }
    pub fn drop_user(&self, username: &str) -> QueryResult<()> {
        let mut auth = self.system_store().auth_data().write();
        if username == SysAuthUser::USER_ROOT {
            // you can't remove root!
            return Err(QueryError::SysAuthError);
        }
        match auth.users.remove_entry(username) {
            Some((username, user)) => self._try_sync_or(&mut auth, |auth| {
                let _ = auth.users.insert(username, user);
            }),
            None => Err(QueryError::SysAuthError),
        }
    }
}

/*
    auth
*/

#[derive(Debug, PartialEq)]
/// The auth data section (system.auth)
pub struct SysAuth {
    users: HashMap<Box<str>, SysAuthUser>,
}

impl SysAuth {
    /// New [`SysAuth`] with the given settings
    pub fn new(users: HashMap<Box<str>, SysAuthUser>) -> Self {
        Self { users }
    }
    pub fn verify_user_check_root<T: AsRef<[u8]> + ?Sized>(
        &self,
        username: &str,
        password: &T,
    ) -> QueryResult<bool> {
        match self.users.get(username) {
            Some(user) if rcrypt::verify(password, user.key()).unwrap() => {
                Ok(username == SysAuthUser::USER_ROOT)
            }
            Some(_) | None => Err(QueryError::SysAuthError),
        }
    }
    /// Verify the user with the given details
    pub fn verify_user<T: AsRef<[u8]> + ?Sized>(
        &self,
        username: &str,
        password: &T,
    ) -> QueryResult<()> {
        self.verify_user_check_root(username, password).map(|_| ())
    }
    pub fn users(&self) -> &HashMap<Box<str>, SysAuthUser> {
        &self.users
    }
}

#[derive(Debug, PartialEq)]
/// The auth user
pub struct SysAuthUser {
    key: Box<[u8]>,
}

impl SysAuthUser {
    pub const USER_ROOT: &'static str = "root";
    /// Create a new [`SysAuthUser`]
    pub fn new(key: Box<[u8]>) -> Self {
        Self { key }
    }
    /// Get the key
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }
}
