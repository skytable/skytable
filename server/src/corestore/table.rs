/*
 * Created on Sat Jul 17 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

#[cfg(test)]
use crate::corestore::{memstore::DdlError, KeyspaceResult};
use crate::{
    actions::ActionResult,
    auth::Authmap,
    corestore::{htable::Coremap, SharedSlice},
    dbnet::prelude::Corestore,
    kvengine::{KVEListmap, KVEStandard, LockedVec},
    protocol::interface::ProtocolSpec,
    util,
};

pub trait DescribeTable {
    type Table;
    fn try_get(table: &Table) -> Option<&Self::Table>;
    fn get<P: ProtocolSpec>(store: &Corestore) -> ActionResult<&Self::Table> {
        match store.estate.table {
            Some((_, ref table)) => {
                // so we do have a table
                match Self::try_get(table) {
                    Some(tbl) => Ok(tbl),
                    None => util::err(P::RSTRING_WRONG_MODEL),
                }
            }
            None => util::err(P::RSTRING_DEFAULT_UNSET),
        }
    }
}

pub struct KVEBlob;

impl DescribeTable for KVEBlob {
    type Table = KVEStandard;
    fn try_get(table: &Table) -> Option<&Self::Table> {
        if let DataModel::KV(ref kve) = table.model_store {
            Some(kve)
        } else {
            None
        }
    }
}

pub struct KVEList;

impl DescribeTable for KVEList {
    type Table = KVEListmap;
    fn try_get(table: &Table) -> Option<&Self::Table> {
        if let DataModel::KVExtListmap(ref kvl) = table.model_store {
            Some(kvl)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum SystemDataModel {
    Auth(Authmap),
}

#[derive(Debug)]
pub struct SystemTable {
    /// data storage
    pub data: SystemDataModel,
}

impl SystemTable {
    pub const fn get_model_ref(&self) -> &SystemDataModel {
        &self.data
    }
    pub fn new(data: SystemDataModel) -> Self {
        Self { data }
    }
    pub fn new_auth(authmap: Authmap) -> Self {
        Self::new(SystemDataModel::Auth(authmap))
    }
}

#[derive(Debug)]
pub enum DataModel {
    KV(KVEStandard),
    KVExtListmap(KVEListmap),
}

// same 8 byte ptrs; any chance of optimizations?

#[derive(Debug)]
/// The underlying table type. This is the place for the other data models (soon!)
pub struct Table {
    /// a key/value store
    model_store: DataModel,
    /// is the table volatile
    volatile: bool,
}

impl Table {
    #[cfg(test)]
    pub const fn from_kve(kve: KVEStandard, volatile: bool) -> Self {
        Self {
            model_store: DataModel::KV(kve),
            volatile,
        }
    }
    #[cfg(test)]
    pub const fn from_kve_listmap(kve: KVEListmap, volatile: bool) -> Self {
        Self {
            model_store: DataModel::KVExtListmap(kve),
            volatile,
        }
    }
    /// Get the key/value store if the table is a key/value store
    #[cfg(test)]
    pub const fn get_kvstore(&self) -> KeyspaceResult<&KVEStandard> {
        #[allow(irrefutable_let_patterns)]
        if let DataModel::KV(kvs) = &self.model_store {
            Ok(kvs)
        } else {
            Err(DdlError::WrongModel)
        }
    }
    pub fn count(&self) -> usize {
        match &self.model_store {
            DataModel::KV(kv) => kv.len(),
            DataModel::KVExtListmap(kv) => kv.len(),
        }
    }
    /// Returns this table's _description_
    pub fn describe_self(&self) -> &'static str {
        match self.get_model_code() {
            // pure KV
            0 if self.is_volatile() => "Keymap { data:(binstr,binstr), volatile:true }",
            0 if !self.is_volatile() => "Keymap { data:(binstr,binstr), volatile:false }",
            1 if self.is_volatile() => "Keymap { data:(binstr,str), volatile:true }",
            1 if !self.is_volatile() => "Keymap { data:(binstr,str), volatile:false }",
            2 if self.is_volatile() => "Keymap { data:(str,str), volatile:true }",
            2 if !self.is_volatile() => "Keymap { data:(str,str), volatile:false }",
            3 if self.is_volatile() => "Keymap { data:(str,binstr), volatile:true }",
            3 if !self.is_volatile() => "Keymap { data:(str,binstr), volatile:false }",
            // KVext => list
            4 if self.is_volatile() => "Keymap { data:(binstr,list<binstr>), volatile:true }",
            4 if !self.is_volatile() => "Keymap { data:(binstr,list<binstr>), volatile:false }",
            5 if self.is_volatile() => "Keymap { data:(binstr,list<str>), volatile:true }",
            5 if !self.is_volatile() => "Keymap { data:(binstr,list<str>), volatile:false }",
            6 if self.is_volatile() => "Keymap { data:(str,list<binstr>), volatile:true }",
            6 if !self.is_volatile() => "Keymap { data:(str,list<binstr>), volatile:false }",
            7 if self.is_volatile() => "Keymap { data:(str,list<str>), volatile:true }",
            7 if !self.is_volatile() => "Keymap { data:(str,list<str>), volatile:false }",
            _ => unsafe { impossible!() },
        }
    }
    pub fn truncate_table(&self) {
        match self.model_store {
            DataModel::KV(ref kv) => kv.truncate_table(),
            DataModel::KVExtListmap(ref kv) => kv.truncate_table(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }
    /// Returns the storage type as an 8-bit uint
    pub const fn storage_type(&self) -> u8 {
        self.volatile as u8
    }
    /// Returns the volatility of the table
    pub const fn is_volatile(&self) -> bool {
        self.volatile
    }
    /// Create a new KVEBlob Table with the provided settings
    pub fn new_pure_kve_with_data(
        data: Coremap<SharedSlice, SharedSlice>,
        volatile: bool,
        k_enc: bool,
        v_enc: bool,
    ) -> Self {
        Self {
            volatile,
            model_store: DataModel::KV(KVEStandard::new(k_enc, v_enc, data)),
        }
    }
    pub fn new_kve_listmap_with_data(
        data: Coremap<SharedSlice, LockedVec>,
        volatile: bool,
        k_enc: bool,
        payload_enc: bool,
    ) -> Self {
        Self {
            volatile,
            model_store: DataModel::KVExtListmap(KVEListmap::new(k_enc, payload_enc, data)),
        }
    }
    pub fn from_model_code(code: u8, volatile: bool) -> Option<Self> {
        macro_rules! pkve {
            ($kenc:expr, $venc:expr) => {
                Self::new_pure_kve_with_data(Coremap::new(), volatile, $kenc, $venc)
            };
        }
        macro_rules! listmap {
            ($kenc:expr, $penc:expr) => {
                Self::new_kve_listmap_with_data(Coremap::new(), volatile, $kenc, $penc)
            };
        }
        let ret = match code {
            // pure kve
            0 => pkve!(false, false),
            1 => pkve!(false, true),
            2 => pkve!(true, true),
            3 => pkve!(true, false),
            // kvext: listmap
            4 => listmap!(false, false),
            5 => listmap!(false, true),
            6 => listmap!(true, false),
            7 => listmap!(true, true),
            _ => return None,
        };
        Some(ret)
    }
    /// Create a new kve with default settings but with provided volatile configuration
    #[cfg(test)]
    pub fn new_kve_with_volatile(volatile: bool) -> Self {
        Self::new_pure_kve_with_data(Coremap::new(), volatile, false, false)
    }
    /// Returns the default kve:
    /// - `k_enc`: `false`
    /// - `v_enc`: `false`
    /// - `volatile`: `false`
    pub fn new_default_kve() -> Self {
        Self::new_pure_kve_with_data(Coremap::new(), false, false, false)
    }
    /// Returns the model code. See [`bytemarks`] for more info
    pub fn get_model_code(&self) -> u8 {
        match self.model_store {
            DataModel::KV(ref kvs) => {
                /*
                bin,bin => 0
                bin,str => 1
                str,str => 2
                str,bin => 3
                */
                let (kenc, venc) = kvs.get_encoding_tuple();
                let ret = kenc as u8 + venc as u8;
                // a little bitmagic goes a long way
                (ret & 1) + ((kenc as u8) << 1)
            }
            DataModel::KVExtListmap(ref kvlistmap) => {
                /*
                bin,list<bin> => 4,
                bin,list<str> => 5,
                str,list<bin> => 6,
                str,list<str> => 7
                */
                let (kenc, venc) = kvlistmap.get_encoding_tuple();
                ((kenc as u8) << 1) + (venc as u8) + 4
            }
        }
    }
    /// Returns the inner data model
    pub fn get_model_ref(&self) -> &DataModel {
        &self.model_store
    }
}
