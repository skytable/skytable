/*
 * Created on Tue Jul 27 2021
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

use crate::corestore::{lazy::Lazy, memstore::ObjectID};
use crate::kvengine::encoding;
use crate::queryengine::ProtocolSpec;
use crate::util::{
    self,
    compiler::{self, cold_err},
};
use core::{fmt, str};
use regex::Regex;

type LazyRegexFn = Lazy<Regex, fn() -> Regex>;

const KEYMAP: &[u8] = "keymap".as_bytes();
const BINSTR: &[u8] = "binstr".as_bytes();
const STR: &[u8] = "str".as_bytes();
const LIST_STR: &[u8] = "list<str>".as_bytes();
const LIST_BINSTR: &[u8] = "list<binstr>".as_bytes();

pub(super) static VALID_CONTAINER_NAME: LazyRegexFn =
    LazyRegexFn::new(|| Regex::new("^[a-zA-Z_][a-zA-Z_0-9]*$").unwrap());
pub(super) static VALID_TYPENAME: LazyRegexFn =
    LazyRegexFn::new(|| Regex::new("^<[a-zA-Z][a-zA-Z0-9]+[^>\\s]?>{1}$").unwrap());

pub(super) fn parse_table_args<'a, P: ProtocolSpec>(
    table_name: &'a [u8],
    model_name: &'a [u8],
) -> Result<(Entity<'a>, u8), &'static [u8]> {
    if compiler::unlikely(!encoding::is_utf8(&table_name) || !encoding::is_utf8(&model_name)) {
        return Err(P::RCODE_ENCODING_ERROR);
    }
    let model_name_str = unsafe { str::from_utf8_unchecked(model_name) };

    // get the entity group
    let entity_group = Entity::from_slice::<P>(table_name)?;
    let splits: Vec<&str> = model_name_str.split('(').collect();
    if compiler::unlikely(splits.len() != 2) {
        return Err(P::RSTRING_BAD_EXPRESSION);
    }

    let model_name_split = unsafe { ucidx!(splits, 0) };
    let model_args_split = unsafe { ucidx!(splits, 1) };

    // model name has to have at least one char while model args should have
    // atleast `)` 1 chars (for example if the model takes no arguments: `smh()`)
    if compiler::unlikely(model_name_split.is_empty() || model_args_split.is_empty()) {
        return Err(P::RSTRING_BAD_EXPRESSION);
    }

    // THIS IS WHERE WE HANDLE THE NEWER MODELS
    if model_name_split.as_bytes() != KEYMAP {
        return Err(P::RSTRING_UNKNOWN_MODEL);
    }

    let non_bracketed_end =
        unsafe { ucidx!(*model_args_split.as_bytes(), model_args_split.len() - 1) != b')' };

    if compiler::unlikely(non_bracketed_end) {
        return Err(P::RSTRING_BAD_EXPRESSION);
    }

    // should be (ty1, ty2)
    let model_args: Vec<&str> = model_args_split[..model_args_split.len() - 1]
        .split(',')
        .map(|v| v.trim())
        .collect();
    if compiler::unlikely(model_args.len() != 2) {
        // nope, someone had fun with commas or they added more args
        // let's check if it was comma fun or if it was arg fun
        return cold_err({
            let all_nonzero = model_args.into_iter().all(|v| !v.is_empty());
            if all_nonzero {
                // arg fun
                Err(P::RSTRING_TOO_MANY_ARGUMENTS)
            } else {
                // comma fun
                Err(P::RSTRING_BAD_EXPRESSION)
            }
        });
    }
    let key_ty = unsafe { ucidx!(model_args, 0) };
    let val_ty = unsafe { ucidx!(model_args, 1) };
    let valid_key_ty = if let Some(idx) = key_ty.chars().position(|v| v.eq(&'<')) {
        VALID_CONTAINER_NAME.is_match(&key_ty[..idx]) && VALID_TYPENAME.is_match(&key_ty[idx..])
    } else {
        VALID_CONTAINER_NAME.is_match(key_ty)
    };
    let valid_val_ty = if let Some(idx) = val_ty.chars().position(|v| v.eq(&'<')) {
        VALID_CONTAINER_NAME.is_match(&val_ty[..idx]) && VALID_TYPENAME.is_match(&val_ty[idx..])
    } else {
        VALID_CONTAINER_NAME.is_match(val_ty)
    };
    if compiler::unlikely(!(valid_key_ty || valid_val_ty)) {
        return Err(P::RSTRING_BAD_EXPRESSION);
    }
    let key_ty = key_ty.as_bytes();
    let val_ty = val_ty.as_bytes();
    let model_code: u8 = match (key_ty, val_ty) {
        // pure KVEBlob
        (BINSTR, BINSTR) => 0,
        (BINSTR, STR) => 1,
        (STR, STR) => 2,
        (STR, BINSTR) => 3,
        // KVExt: listmap
        (BINSTR, LIST_BINSTR) => 4,
        (BINSTR, LIST_STR) => 5,
        (STR, LIST_BINSTR) => 6,
        (STR, LIST_STR) => 7,
        // KVExt bad keytypes (we can't use lists as keys for obvious reasons)
        (LIST_STR, _) | (LIST_BINSTR, _) => return Err(P::RSTRING_BAD_TYPE_FOR_KEY),
        _ => return Err(P::RCODE_UNKNOWN_DATA_TYPE),
    };
    Ok((entity_group, model_code))
}

type ByteSlice<'a> = &'a [u8];

#[derive(PartialEq)]
pub enum Entity<'a> {
    /// Fully qualified syntax (ks:table)
    Full(ByteSlice<'a>, ByteSlice<'a>),
    /// Half entity syntax (only ks/table)
    Single(ByteSlice<'a>),
    /// Partial entity syntax (`:table`)
    Partial(ByteSlice<'a>),
}

#[derive(PartialEq)]
pub enum OwnedEntity {
    Full(ObjectID, ObjectID),
    Single(ObjectID),
    Partial(ObjectID),
}

impl fmt::Debug for OwnedEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OwnedEntity::Full(a, b) => write!(
                f,
                "Full('{}:{}')",
                String::from_utf8_lossy(a),
                String::from_utf8_lossy(b)
            ),
            OwnedEntity::Single(a) => write!(f, "Single('{}')", String::from_utf8_lossy(a)),
            OwnedEntity::Partial(a) => write!(f, "Partial(':{}')", String::from_utf8_lossy(a)),
        }
    }
}

impl<'a> fmt::Debug for Entity<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_owned())
    }
}

impl<'a> Entity<'a> {
    pub fn from_slice<P: ProtocolSpec>(input: ByteSlice<'a>) -> Result<Entity<'a>, &'static [u8]> {
        let parts: Vec<&[u8]> = input.split(|b| *b == b':').collect();
        if compiler::unlikely(parts.is_empty() || parts.len() > 2) {
            return util::err(P::RSTRING_BAD_EXPRESSION);
        }
        // just the table
        let first_entity = unsafe { ucidx!(parts, 0) };
        if parts.len() == 1 {
            Ok(Entity::Single(Self::verify_entity_name::<P>(first_entity)?))
        } else {
            let second_entity = Self::verify_entity_name::<P>(unsafe { ucidx!(parts, 1) })?;
            if first_entity.is_empty() {
                // partial syntax; so the table is in the second position
                Ok(Entity::Partial(second_entity))
            } else {
                let keyspace = Self::verify_entity_name::<P>(first_entity)?;
                let table = Self::verify_entity_name::<P>(second_entity)?;
                Ok(Entity::Full(keyspace, table))
            }
        }
    }
    #[inline(always)]
    fn verify_entity_name<P: ProtocolSpec>(input: &[u8]) -> Result<&[u8], &'static [u8]> {
        let mut valid_name = input.len() < 65
            && encoding::is_utf8(input)
            && unsafe { VALID_CONTAINER_NAME.is_match(str::from_utf8_unchecked(input)) };
        #[cfg(windows)]
        {
            // paths on Windows are case insensitive that's why this is necessary
            valid_name &=
                !(input.eq_ignore_ascii_case(b"PRELOAD") || input.eq_ignore_ascii_case(b"PARTMAP"));
        }
        #[cfg(not(windows))]
        {
            valid_name &= (input != b"PRELOAD") && (input != b"PARTMAP");
        }
        if compiler::likely(valid_name && !input.is_empty()) {
            // valid name
            Ok(input)
        } else if compiler::unlikely(input.is_empty()) {
            // bad expression (something like `:`)
            util::err(P::RSTRING_BAD_EXPRESSION)
        } else if compiler::unlikely(input.eq(b"system")) {
            // system cannot be switched to
            util::err(P::RSTRING_PROTECTED_OBJECT)
        } else {
            // the container has a bad name
            util::err(P::RSTRING_BAD_CONTAINER_NAME)
        }
    }
    pub fn as_owned(&self) -> OwnedEntity {
        unsafe {
            match self {
                Self::Full(a, b) => {
                    OwnedEntity::Full(ObjectID::from_slice(a), ObjectID::from_slice(b))
                }
                Self::Single(a) => OwnedEntity::Single(ObjectID::from_slice(a)),
                Self::Partial(a) => OwnedEntity::Partial(ObjectID::from_slice(a)),
            }
        }
    }
    pub fn into_owned(self) -> OwnedEntity {
        self.as_owned()
    }
}
