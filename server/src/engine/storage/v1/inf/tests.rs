/*
 * Created on Sun Aug 13 2023
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

use crate::engine::{
    core::model::{Field, Layer},
    data::{
        cell::Datacell,
        dict::{DictEntryGeneric, DictGeneric},
    },
    storage::v1::rw::BufferedScanner,
};

#[test]
fn dict() {
    let dict: DictGeneric = into_dict! {
        "hello" => Datacell::new_str("world".into()),
        "omg a null?" => Datacell::null(),
        "a big fat dict" => DictEntryGeneric::Map(into_dict!(
            "with a value" => Datacell::new_uint(1002),
            "and a null" => Datacell::null(),
        ))
    };
    let encoded = super::enc::<super::map::PersistMapImpl<super::map::GenericDictSpec>>(&dict);
    let mut scanner = BufferedScanner::new(&encoded);
    let decoded =
        super::dec::<super::map::PersistMapImpl<super::map::GenericDictSpec>>(&mut scanner)
            .unwrap();
    assert_eq!(dict, decoded);
}

#[test]
fn layer() {
    let layer = Layer::list();
    let encoded = super::enc_self(&layer);
    let mut scanner = BufferedScanner::new(&encoded);
    let dec = super::dec_self::<Layer>(&mut scanner).unwrap();
    assert_eq!(layer, dec);
}

#[test]
fn field() {
    let field = Field::new([Layer::list(), Layer::uint64()].into(), true);
    let encoded = super::enc_self(&field);
    let mut scanner = BufferedScanner::new(&encoded);
    let dec = super::dec_self::<Field>(&mut scanner).unwrap();
    assert_eq!(field, dec);
}
