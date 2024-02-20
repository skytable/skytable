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
        txn::gns::sysctl::{AlterUserTxn, CreateUserTxn, DropUserTxn},
    },
    std::collections::hash_map::Entry,
};

#[derive(Debug)]
pub struct SystemDatabase {
    users: RWLIdx<Box<str>, User>,
}

#[derive(Debug, PartialEq)]
pub struct User {
    password: Box<[u8]>,
}

impl User {
    pub fn new(password: Box<[u8]>) -> Self {
        Self { password }
    }
}

#[derive(Debug, PartialEq)]
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
    pub fn users(&self) -> &RWLIdx<Box<str>, User> {
        &self.users
    }
    pub fn __verify_user(&self, username: &str, password: &[u8]) -> VerifyUser {
        self.users
            .read()
            .get(username)
            .map(|user| {
                if rcrypt::verify(password, &user.password).unwrap() {
                    if username == Self::ROOT_ACCOUNT {
                        VerifyUser::OkayRoot
                    } else {
                        VerifyUser::Okay
                    }
                } else {
                    VerifyUser::IncorrectPassword
                }
            })
            .unwrap_or(VerifyUser::NotFound)
    }
    pub fn __insert_user(&self, username: Box<str>, password: Box<[u8]>) -> bool {
        match self.users.write().entry(username) {
            Entry::Vacant(ve) => {
                ve.insert(User::new(password));
                true
            }
            Entry::Occupied(_) => false,
        }
    }
    pub fn __delete_user(&self, username: &str) -> bool {
        self.users.write().remove(username).is_some()
    }
    pub fn __change_user_password(&self, username: &str, new_password: Box<[u8]>) -> bool {
        match self.users.write().get_mut(username) {
            Some(user) => {
                user.password = new_password;
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
        username: Box<str>,
        password: &str,
    ) -> QueryResult<()> {
        let mut users = self.users.write();
        if users.contains_key(&username) {
            return Err(QueryError::SysAuthError);
        }
        let password_hash = rcrypt::hash(password, rcrypt::DEFAULT_COST).unwrap();
        match global
            .gns_driver()
            .lock()
            .gns_driver()
            .commit_event(CreateUserTxn::new(&username, &password_hash))
        {
            Ok(()) => {
                users.insert(username, User::new(password_hash.into_boxed_slice()));
                Ok(())
            }
            Err(e) => {
                error!("failed to create user: {e}");
                return Err(QueryError::SysTransactionalError);
            }
        }
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
                match global
                    .gns_driver()
                    .lock()
                    .gns_driver()
                    .commit_event(AlterUserTxn::new(username, &password_hash))
                {
                    Ok(()) => {
                        user.password = password_hash.into_boxed_slice();
                        Ok(())
                    }
                    Err(e) => {
                        error!("failed to alter user: {e}");
                        Err(QueryError::SysTransactionalError)
                    }
                }
            }
            None => Err(QueryError::SysAuthError),
        }
    }
    pub fn drop_user(&self, global: &impl GlobalInstanceLike, username: &str) -> QueryResult<()> {
        let mut users = self.users.write();
        if !users.contains_key(username) {
            return Err(QueryError::SysAuthError);
        }
        match global
            .gns_driver()
            .lock()
            .gns_driver()
            .commit_event(DropUserTxn::new(username))
        {
            Ok(()) => {
                let _ = users.remove(username);
                Ok(())
            }
            Err(e) => {
                error!("failed to remove user: {e}");
                Err(QueryError::SysTransactionalError)
            }
        }
    }
}
