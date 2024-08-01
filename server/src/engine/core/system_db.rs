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

use {
    super::RWLIdx,
    crate::engine::{
        error::{QueryError, QueryResult},
        fractal::GlobalInstanceLike,
        mem::unsafe_apis::BoxStr,
        txn::gns::sysctl::{AlterUserTxn, CreateUserTxn, DropUserTxn},
    },
    std::collections::hash_map::Entry,
};

#[derive(Debug)]
pub struct SystemDatabase {
    users: RWLIdx<BoxStr, User>,
}

#[derive(Debug, PartialEq)]
pub struct User {
    phash: Box<[u8]>,
}

impl User {
    pub fn new(password_hash: Box<[u8]>) -> Self {
        Self {
            phash: password_hash,
        }
    }
    pub fn hash(&self) -> &[u8] {
        &self.phash
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum VerifyUser {
    NotFound,
    IncorrectPassword,
    Okay,
    OkayRoot,
}

impl VerifyUser {
    pub fn is_root(&self) -> bool {
        matches!(self, Self::OkayRoot)
    }
}

impl SystemDatabase {
    pub const ROOT_ACCOUNT: &'static str = "root";
    pub fn empty() -> Self {
        Self {
            users: RWLIdx::default(),
        }
    }
    pub fn users(&self) -> &RWLIdx<BoxStr, User> {
        &self.users
    }
    pub fn verify_user(&self, username: &str, password: &[u8]) -> VerifyUser {
        self.users
            .read()
            .get(username)
            .map(|user| {
                if password.is_empty() {
                    return VerifyUser::IncorrectPassword;
                }
                match rcrypt::verify(password, user.hash()) {
                    Ok(true) => {
                        if username == Self::ROOT_ACCOUNT {
                            VerifyUser::OkayRoot
                        } else {
                            VerifyUser::Okay
                        }
                    }
                    Ok(false) => VerifyUser::IncorrectPassword,
                    Err(_) => unreachable!(),
                }
            })
            .unwrap_or(VerifyUser::NotFound)
    }
}

impl SystemDatabase {
    pub fn __raw_create_user(&self, username: BoxStr, password_hash: Box<[u8]>) -> bool {
        match self.users.write().entry(username) {
            Entry::Vacant(ve) => {
                ve.insert(User::new(password_hash));
                true
            }
            Entry::Occupied(_) => false,
        }
    }
    pub fn __raw_delete_user(&self, username: &str) -> bool {
        self.users.write().remove(username).is_some()
    }
    pub fn __raw_alter_user(&self, username: &str, new_password_hash: Box<[u8]>) -> bool {
        match self.users.write().get_mut(username) {
            Some(user) => {
                user.phash = new_password_hash;
                true
            }
            None => false,
        }
    }
}

impl SystemDatabase {
    pub fn create_user(
        &self,
        global: &impl GlobalInstanceLike,
        username: BoxStr,
        password: &str,
    ) -> QueryResult<()> {
        let mut users = self.users.write();
        if users.contains_key(&username) {
            return Err(QueryError::SysAuthError);
        }
        let password_hash = rcrypt::hash(password, rcrypt::DEFAULT_COST).unwrap();
        global.state().gns_driver().driver_context(
            global,
            |drv| drv.commit_event(CreateUserTxn::new(&username, &password_hash)),
            || {},
        )?;
        users.insert(username, User::new(password_hash.into_boxed_slice()));
        Ok(())
    }
    pub fn alter_user(
        &self,
        global: &impl GlobalInstanceLike,
        username: &str,
        password: &str,
    ) -> QueryResult<()> {
        match self.users.write().get_mut(username) {
            Some(user) => {
                let password_hash = rcrypt::hash(password, rcrypt::DEFAULT_COST).unwrap();
                global.state().gns_driver().driver_context(
                    global,
                    |drv| drv.commit_event(AlterUserTxn::new(username, &password_hash)),
                    || {},
                )?;
                user.phash = password_hash.into_boxed_slice();
                Ok(())
            }
            None => Err(QueryError::SysAuthError),
        }
    }
    pub fn drop_user(&self, global: &impl GlobalInstanceLike, username: &str) -> QueryResult<()> {
        let mut users = self.users.write();
        if !users.contains_key(username) {
            return Err(QueryError::SysAuthError);
        }
        global.state().gns_driver().driver_context(
            global,
            |drv| drv.commit_event(DropUserTxn::new(username)),
            || {},
        )?;
        let _ = users.remove(username);
        Ok(())
    }
}
