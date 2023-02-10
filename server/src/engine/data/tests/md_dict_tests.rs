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
    md_dict::{self, DictEntryGeneric, DictGeneric, MetaDict, MetaDictEntry},
    HSData,
};

#[test]
fn t_simple_flatten() {
    let generic_dict: DictGeneric = into_dict! {
        "a_valid_key" => Some(DictEntryGeneric::Lit(100.into())),
        "a_null_key" => None,
    };
    let expected: MetaDict = into_dict!(
        "a_valid_key" => HSData::UnsignedInt(100)
    );
    let ret = md_dict::rflatten_metadata(generic_dict);
    assert_eq!(ret, expected);
}

#[test]
fn t_simple_patch() {
    let mut current: MetaDict = into_dict! {
        "a" => HSData::UnsignedInt(2),
        "b" => HSData::UnsignedInt(3),
        "z" => HSData::SignedInt(-100),
    };
    let new: DictGeneric = into_dict! {
        "a" => Some(HSData::UnsignedInt(1).into()),
        "b" => Some(HSData::UnsignedInt(2).into()),
        "z" => None,
    };
    let expected: MetaDict = into_dict! {
        "a" => HSData::UnsignedInt(1),
        "b" => HSData::UnsignedInt(2),
    };
    assert!(md_dict::rmerge_metadata(&mut current, new));
    assert_eq!(current, expected);
}

#[test]
fn t_bad_patch() {
    let mut current: MetaDict = into_dict! {
        "a" => HSData::UnsignedInt(2),
        "b" => HSData::UnsignedInt(3),
        "z" => HSData::SignedInt(-100),
    };
    let backup = current.clone();
    let new: DictGeneric = into_dict! {
        "a" => Some(HSData::UnsignedInt(1).into()),
        "b" => Some(HSData::UnsignedInt(2).into()),
        "z" => Some(HSData::String("omg".into()).into()),
    };
    assert!(!md_dict::rmerge_metadata(&mut current, new));
    assert_eq!(current, backup);
}

#[test]
fn patch_null_out_dict() {
    let mut current: MetaDict = into_dict! {
        "a" => HSData::UnsignedInt(2),
        "b" => HSData::UnsignedInt(3),
        "z" => MetaDictEntry::Map(into_dict!(
            "c" => HSData::UnsignedInt(1),
            "d" => HSData::UnsignedInt(2)
        )),
    };
    let expected: MetaDict = into_dict! {
        "a" => HSData::UnsignedInt(2),
        "b" => HSData::UnsignedInt(3),
    };
    let new: DictGeneric = into_dict! {
        "a" => Some(HSData::UnsignedInt(2).into()),
        "b" => Some(HSData::UnsignedInt(3).into()),
        "z" => None,
    };
    assert!(md_dict::rmerge_metadata(&mut current, new));
    assert_eq!(current, expected);
}
