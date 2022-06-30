/*
 * Created on Sun Sep 05 2021
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

#[sky_macros::dbtest_module(table = "(string, string)")]
mod __private {
    use skytable::{types::RawString, Element, RespCode};

    async fn test_bad_encoding_set() {
        query.push("set");
        query.push("x");
        query.push(RawString::from(b"Hello \xF0\x90\x80World".to_vec()));
        runeq!(con, query, Element::RespCode(RespCode::EncodingError));
    }
    async fn test_bad_encoding_update() {
        // first set the keys
        setkeys! {
            con,
            "x": "100"
        }
        // now try to update with a bad value
        query.push("update");
        query.push("x");
        query.push(RawString::from(b"Hello \xF0\x90\x80World".to_vec()));
        runeq!(con, query, Element::RespCode(RespCode::EncodingError));
    }
    async fn test_bad_encoding_uset() {
        query.push("uset");
        query.push("x");
        query.push(RawString::from(b"Hello \xF0\x90\x80World".to_vec()));
        runeq!(con, query, Element::RespCode(RespCode::EncodingError));
    }
    async fn test_bad_encoding_mset() {
        // we'll have one good encoding and one bad encoding
        push!(
            query,
            "mset",
            "x",
            "good value",
            "y",
            // the bad value
            RawString::from(b"Hello \xF0\x90\x80World".to_vec())
        );
        runeq!(con, query, Element::RespCode(RespCode::EncodingError));
    }
}
