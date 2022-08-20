/*
 * Created on Sun Mar 13 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

use super::{KVEStandard, SharedSlice};

#[test]
fn test_ignore_encoding() {
    let non_unicode_value = b"Hello \xF0\x90\x80World".to_vec();
    let non_unicode_key = non_unicode_value.to_owned();
    let tbl = KVEStandard::default();
    assert!(tbl
        .set(non_unicode_key.into(), non_unicode_value.into())
        .is_ok());
}

#[test]
fn test_bad_unicode_key() {
    let bad_unicode = b"Hello \xF0\x90\x80World".to_vec();
    let tbl = KVEStandard::init(true, false);
    assert!(tbl
        .set(SharedSlice::from(bad_unicode), SharedSlice::from("123"))
        .is_err());
}

#[test]
fn test_bad_unicode_value() {
    let bad_unicode = b"Hello \xF0\x90\x80World".to_vec();
    let tbl = KVEStandard::init(false, true);
    assert!(tbl
        .set(SharedSlice::from("123"), SharedSlice::from(bad_unicode))
        .is_err());
}

#[test]
fn test_bad_unicode_key_value() {
    let bad_unicode = b"Hello \xF0\x90\x80World".to_vec();
    let tbl = KVEStandard::init(true, true);
    assert!(tbl
        .set(
            SharedSlice::from(bad_unicode.clone()),
            SharedSlice::from(bad_unicode)
        )
        .is_err());
}

#[test]
fn test_with_bincode() {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct User {
        username: String,
        password: String,
        uuid: u128,
        score: u32,
        level: u32,
    }
    let tbl = KVEStandard::init(true, false);
    let joe = User {
        username: "Joe".to_owned(),
        password: "Joe123".to_owned(),
        uuid: u128::MAX,
        score: u32::MAX,
        level: u32::MAX,
    };
    assert!(tbl
        .set(
            SharedSlice::from("Joe"),
            SharedSlice::from(bincode::serialize(&joe).unwrap(),),
        )
        .is_ok(),);
    assert_eq!(
        bincode::deserialize::<User>(&tbl.get("Joe".as_bytes()).unwrap().unwrap()).unwrap(),
        joe
    );
}

#[test]
fn test_encoder_ignore() {
    let tbl = KVEStandard::default();
    let encoder = tbl.get_double_encoder();
    assert!(encoder("hello".as_bytes(), b"Hello \xF0\x90\x80World"));
}

#[test]
fn test_encoder_validate_with_non_unicode() {
    let tbl = KVEStandard::init(true, true);
    let encoder = tbl.get_double_encoder();
    assert!(!encoder("hello".as_bytes(), b"Hello \xF0\x90\x80World"));
}
