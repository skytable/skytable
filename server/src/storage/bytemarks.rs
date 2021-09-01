/*
 * Created on Sun Jul 18 2021
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

#![allow(unused)]

//! # Bytemarks
//!
//! Bytemarks are single bytes that are written to parts of files to provide metadata. This module
//! contains a collection of these

// model
/// KVE model bytemark with key:bin, val:bin
pub const BYTEMARK_MODEL_KV_BIN_BIN: u8 = 0;
/// KVE model bytemark with key:bin, val:str
pub const BYTEMARK_MODEL_KV_BIN_STR: u8 = 1;
/// KVE model bytemark with key:str, val:str
pub const BYTEMARK_MODEL_KV_STR_STR: u8 = 2;
/// KVE model bytemark with key:str, val:bin
pub const BYTEMARK_MODEL_KV_STR_BIN: u8 = 3;
/// KVE model bytemark with key:binstr, val: list<str>
pub const BYTEMARK_MODEL_KV_BINSTR_LIST_STR: u8 = 4;
/// KVE model bytemark with key:binstr, val: list<binstr>
pub const BYTEMARK_MODEL_KV_BINSTR_LIST_BIN: u8 = 5;
/// KVE model bytemark with key:str, val: list<str>
pub const BYTEMARK_MODEL_KV_STR_LIST_STR: u8 = 6;
/// KVE model bytemark with key:str, val: list<binstr>
pub const BYTEMARK_MODEL_KV_STR_LIST_BINSTR: u8 = 6;

// storage bym
/// Persistent storage bytemark
pub const BYTEMARK_STORAGE_PERSISTENT: u8 = 0;
/// Volatile storage bytemark
pub const BYTEMARK_STORAGE_VOLATILE: u8 = 1;
