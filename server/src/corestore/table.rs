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

use crate::corestore::htable::Coremap;
use crate::corestore::memstore::DdlError;
use crate::corestore::Data;
use crate::corestore::KeyspaceResult;
use crate::kvengine::KVEngine;
use crate::storage::bytemarks;

#[derive(Debug)]
pub enum DataModel {
    KV(KVEngine),
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
    /// Get the key/value store if the table is a key/value store
    pub const fn get_kvstore(&self) -> KeyspaceResult<&KVEngine> {
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
        }
    }
    pub fn truncate_table(&self) {
        match self.model_store {
            DataModel::KV(ref kv) => kv.truncate_table(),
        }
    }
    /// Returns the storage type as an 8-bit uint
    pub const fn storage_type(&self) -> u8 {
        self.volatile as u8
    }
    /// Returns the volatility of the table
    pub const fn is_volatile(&self) -> bool {
        self.volatile
    }
    /// Create a new KVE Table with the provided settings
    pub fn new_kve_with_data(
        data: Coremap<Data, Data>,
        volatile: bool,
        k_enc: bool,
        v_enc: bool,
    ) -> Self {
        Self {
            volatile,
            model_store: DataModel::KV(KVEngine::init_with_data(k_enc, v_enc, data)),
        }
    }
    pub fn new_kve_with_encoding(volatile: bool, k_enc: bool, v_enc: bool) -> Self {
        Self {
            volatile,
            model_store: DataModel::KV(KVEngine::init(k_enc, v_enc)),
        }
    }
    pub fn from_model_code(code: u8, volatile: bool) -> Option<Self> {
        let ret = match code {
            0 => Self::new_kve_with_encoding(volatile, false, false),
            1 => Self::new_kve_with_encoding(volatile, false, true),
            2 => Self::new_kve_with_encoding(volatile, true, true),
            3 => Self::new_kve_with_encoding(volatile, true, false),
            _ => return None,
        };
        Some(ret)
    }
    /// Create a new kve with default settings but with provided volatile configuration
    pub fn new_kve_with_volatile(volatile: bool) -> Self {
        Self::new_kve_with_data(Coremap::new(), volatile, false, false)
    }
    /// Returns the default kve:
    /// - `k_enc`: `false`
    /// - `v_enc`: `false`
    /// - `volatile`: `false`
    pub fn new_default_kve() -> Self {
        Self::new_kve_with_data(Coremap::new(), false, false, false)
    }
    /// Returns the model code. See [`bytemarks`] for more info
    pub fn get_model_code(&self) -> u8 {
        match &self.model_store {
            DataModel::KV(kvs) => {
                /*
                bin,bin => 0
                bin,str => 1
                str,str => 2
                str,bin => 3
                */
                let (kbin, vbin) = kvs.get_encoding();
                if kbin {
                    if vbin {
                        // both k + v are str
                        bytemarks::BYTEMARK_MODEL_KV_STR_STR
                    } else {
                        // only k is str
                        bytemarks::BYTEMARK_MODEL_KV_STR_BIN
                    }
                } else if vbin {
                    // k is bin, v is str
                    bytemarks::BYTEMARK_MODEL_KV_BIN_STR
                } else {
                    // both are bin
                    bytemarks::BYTEMARK_MODEL_KV_BIN_BIN
                }
            }
        }
    }
    /// Returns the inner data model
    pub fn get_model_ref(&self) -> &DataModel {
        &self.model_store
    }
}
