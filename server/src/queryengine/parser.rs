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

use crate::corestore::lazy::Lazy;
use crate::corestore::{BorrowedEntityGroup, OwnedEntityGroup};
use crate::kvengine::encoding;
use crate::protocol::responses;
use crate::queryengine::ActionIter;
use crate::util::compiler;
use crate::util::compiler::cold_err;
use crate::util::Unwrappable;
use core::str;
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
pub(super) fn parse_table_args(
    act: &mut ActionIter,
) -> Result<(OwnedEntityGroup, u8), &'static [u8]> {
    let table_name = unsafe { act.next().unsafe_unwrap() };
    let model_name = unsafe { act.next().unsafe_unwrap() };
    if compiler::unlikely(!encoding::is_utf8(&table_name) || !encoding::is_utf8(&model_name)) {
        return Err(responses::groups::ENCODING_ERROR);
    }
    let model_name_str = unsafe { str::from_utf8_unchecked(model_name) };

    // get the entity group
    let entity_group = get_query_entity(table_name)?;
    let splits: Vec<&str> = model_name_str.split('(').collect();
    if compiler::unlikely(splits.len() != 2) {
        return Err(responses::groups::BAD_EXPRESSION);
    }

    let model_name_split = unsafe { ucidx!(splits, 0) };
    let model_args_split = unsafe { ucidx!(splits, 1) };

    // model name has to have at least one char while model args should have
    // atleast `)` 1 chars (for example if the model takes no arguments: `smh()`)
    if compiler::unlikely(model_name_split.is_empty() || model_args_split.is_empty()) {
        return Err(responses::groups::BAD_EXPRESSION);
    }

    // THIS IS WHERE WE HANDLE THE NEWER MODELS
    if model_name_split.as_bytes() != KEYMAP {
        return Err(responses::groups::UNKNOWN_MODEL);
    }

    let non_bracketed_end =
        unsafe { ucidx!(*model_args_split.as_bytes(), model_args_split.len() - 1) != b')' };

    if compiler::unlikely(non_bracketed_end) {
        return Err(responses::groups::BAD_EXPRESSION);
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
                Err(responses::groups::TOO_MANY_ARGUMENTS)
            } else {
                // comma fun
                Err(responses::groups::BAD_EXPRESSION)
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
        return Err(responses::groups::BAD_EXPRESSION);
    }
    let key_ty = key_ty.as_bytes();
    let val_ty = val_ty.as_bytes();
    let model_code: u8 = match (key_ty, val_ty) {
        // pure KVE
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
        (LIST_STR, _) | (LIST_BINSTR, _) => return Err(responses::groups::BAD_TYPE_FOR_KEY),
        _ => return Err(responses::groups::UNKNOWN_DATA_TYPE),
    };
    Ok((
        unsafe {
            // SAFETY: All sizes checked here
            entity_group.into_owned()
        },
        model_code,
    ))
}

#[cfg(test)]
pub(super) fn parse_table_args_test(
    act: &mut std::vec::IntoIter<bytes::Bytes>,
) -> Result<(OwnedEntityGroup, u8), &'static [u8]> {
    let table_name = unsafe { act.next().unsafe_unwrap() };
    let model_name = unsafe { act.next().unsafe_unwrap() };
    if compiler::unlikely(!encoding::is_utf8(&table_name) || !encoding::is_utf8(&model_name)) {
        return Err(responses::groups::ENCODING_ERROR);
    }
    let model_name_str = unsafe { str::from_utf8_unchecked(&model_name) };

    // get the entity group
    let entity_group = get_query_entity(&table_name)?;
    let splits: Vec<&str> = model_name_str.split('(').collect();
    if compiler::unlikely(splits.len() != 2) {
        return Err(responses::groups::BAD_EXPRESSION);
    }
    let model_name_split = unsafe { ucidx!(splits, 0) };
    let model_args_split = unsafe { ucidx!(splits, 1) };

    // model name has to have at least one char while model args should have
    // atleast `)` 1 chars (for example if the model takes no arguments: `smh()`)
    if compiler::unlikely(model_name_split.is_empty() || model_args_split.is_empty()) {
        return Err(responses::groups::BAD_EXPRESSION);
    }

    // THIS IS WHERE WE HANDLE THE NEWER MODELS
    if model_name_split.as_bytes() != KEYMAP {
        return Err(responses::groups::UNKNOWN_MODEL);
    }

    let non_bracketed_end =
        unsafe { ucidx!(*model_args_split.as_bytes(), model_args_split.len() - 1) != b')' };

    if compiler::unlikely(non_bracketed_end) {
        return Err(responses::groups::BAD_EXPRESSION);
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
                Err(responses::groups::TOO_MANY_ARGUMENTS)
            } else {
                // comma fun
                Err(responses::groups::BAD_EXPRESSION)
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
    if compiler::unlikely(!valid_key_ty || !valid_val_ty) {
        return Err(responses::groups::BAD_EXPRESSION);
    }
    let key_ty = key_ty.as_bytes();
    let val_ty = val_ty.as_bytes();
    let model_code: u8 = match (key_ty, val_ty) {
        // pure KVE
        (BINSTR, BINSTR) => 0,
        (BINSTR, STR) => 1,
        (STR, STR) => 2,
        (STR, BINSTR) => 3,
        // KVExt listmap
        (BINSTR, LIST_BINSTR) => 4,
        (BINSTR, LIST_STR) => 5,
        (STR, LIST_BINSTR) => 6,
        (STR, LIST_STR) => 7,
        // KVExt bad keytypes (we can't use lists as keys for obvious reasons)
        (LIST_STR, _) | (LIST_BINSTR, _) => return Err(responses::groups::BAD_TYPE_FOR_KEY),
        _ => return Err(responses::groups::UNKNOWN_DATA_TYPE),
    };
    Ok((
        unsafe {
            // SAFETY: All sizes checked here
            entity_group.into_owned()
        },
        model_code,
    ))
}
pub fn get_query_entity<'a>(input: &'a [u8]) -> Result<BorrowedEntityGroup, &'static [u8]> {
    let y: Vec<&[u8]> = input.split(|v| *v == b':').collect();
    unsafe {
        if y.len() == 1 {
            // just ks
            let ksret = &ucidx!(y, 0);
            #[allow(clippy::if_same_then_else)]
            if compiler::unlikely(ksret.len() > 64 || ksret.is_empty()) {
                Err(responses::groups::BAD_CONTAINER_NAME)
            } else if compiler::unlikely(
                !VALID_CONTAINER_NAME.is_match(str::from_utf8_unchecked(ksret)),
            ) {
                Err(responses::groups::BAD_CONTAINER_NAME)
            } else if compiler::unlikely(ksret.eq(&"system".as_bytes())) {
                Err(responses::groups::PROTECTED_OBJECT)
            } else {
                Ok(BorrowedEntityGroup::from((Some(*ksret), None)))
            }
        } else if y.len() == 2 {
            // tbl + ns
            let ksret = &ucidx!(y, 0);
            let tblret = &ucidx!(y, 1);
            if compiler::unlikely(ksret.len() > 64 || tblret.len() > 64) {
                Err(responses::groups::BAD_CONTAINER_NAME)
            } else if compiler::unlikely(tblret.is_empty() || ksret.is_empty()) {
                Err(responses::groups::BAD_EXPRESSION)
            } else if compiler::unlikely(
                !VALID_CONTAINER_NAME.is_match(str::from_utf8_unchecked(ksret))
                    || !VALID_CONTAINER_NAME.is_match(str::from_utf8_unchecked(tblret)),
            ) {
                Err(responses::groups::BAD_CONTAINER_NAME)
            } else if compiler::unlikely(ksret.eq(&"system".as_bytes())) {
                Err(responses::groups::PROTECTED_OBJECT)
            } else {
                Ok(BorrowedEntityGroup::from((Some(*ksret), Some(*tblret))))
            }
        } else {
            // something wrong
            cold_err(Err(responses::groups::BAD_EXPRESSION))
        }
    }
}
