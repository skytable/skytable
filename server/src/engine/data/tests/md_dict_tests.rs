/*
 * Created on Thu Feb 09 2023
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

use crate::engine::data::{
    cell::Datacell,
    dict::{self, DictEntryGeneric, DictGeneric},
};

#[sky_macros::test]
fn t_simple_flatten() {
    let generic_dict: DictGeneric = into_dict! {
        "a_valid_key" => DictEntryGeneric::Data(100u64.into()),
        "a_null_key" => Datacell::null(),
    };
    let expected: DictGeneric = into_dict!(
        "a_valid_key" => Datacell::new_uint_default(100)
    );
    let ret = dict::rflatten_metadata(generic_dict);
    assert_eq!(ret, expected);
}

#[sky_macros::test]
fn t_simple_patch() {
    let mut current: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(2),
        "b" => Datacell::new_uint_default(3),
        "z" => Datacell::new_sint_default(-100),
    };
    let new: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(1),
        "b" => Datacell::new_uint_default(2),
        "z" => Datacell::null(),
    };
    let expected: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(1),
        "b" => Datacell::new_uint_default(2),
    };
    assert!(dict::rmerge_metadata(&mut current, new));
    assert_eq!(current, expected);
}

#[sky_macros::test]
fn t_bad_patch() {
    let mut current: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(2),
        "b" => Datacell::new_uint_default(3),
        "z" => Datacell::new_sint_default(-100),
    };
    let backup = current.clone();
    let new: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(1),
        "b" => Datacell::new_uint_default(2),
        "z" => Datacell::new_str("omg".into()),
    };
    assert!(!dict::rmerge_metadata(&mut current, new));
    assert_eq!(current, backup);
}

#[sky_macros::test]
fn patch_null_out_dict() {
    let mut current: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(2),
        "b" => Datacell::new_uint_default(3),
        "z" => DictEntryGeneric::Map(into_dict!(
            "c" => Datacell::new_uint_default(1),
            "d" => Datacell::new_uint_default(2)
        )),
    };
    let expected: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(2),
        "b" => Datacell::new_uint_default(3),
    };
    let new: DictGeneric = into_dict! {
        "a" => Datacell::new_uint_default(2),
        "b" => Datacell::new_uint_default(3),
        "z" => Datacell::null(),
    };
    assert!(dict::rmerge_metadata(&mut current, new));
    assert_eq!(current, expected);
}
