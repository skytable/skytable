/*
 * Created on Wed Jul 28 2021
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

const TABLE_DECL_KM_STR_STR_VOLATILE: &str = "Keymap { data:(str,str), volatile:true }";

#[sky_macros::dbtest_module]
mod __private {
    use skytable::{types::Array, Element, RespCode};
    async fn test_inspect_keyspaces() {
        query.push("INSPECT SPACES");
        assert!(matches!(
            con.run_query_raw(&query).await.unwrap(),
            Element::Array(Array::NonNullStr(_))
        ))
    }
    async fn test_inspect_keyspace() {
        query.push(format!("INSPECT SPACE {__MYKS__}"));
        assert!(matches!(
            con.run_query_raw(&query).await.unwrap(),
            Element::Array(Array::NonNullStr(_))
        ))
    }
    async fn test_inspect_current_keyspace() {
        query.push("INSPECT SPACE");
        let ret: Vec<String> = con.run_query(&query).await.unwrap();
        assert!(ret.contains(&__MYTABLE__));
    }
    async fn test_inspect_table() {
        query.push(format!("INSPECT MODEL {__MYTABLE__}"));
        match con.run_query_raw(&query).await.unwrap() {
            Element::String(st) => {
                assert_eq!(st, TABLE_DECL_KM_STR_STR_VOLATILE.to_owned())
            }
            _ => panic!("Bad response for inspect table"),
        }
    }
    async fn test_inspect_current_table() {
        query.push("INSPECT MODEL");
        let ret: String = con.run_query(&query).await.unwrap();
        assert_eq!(ret, TABLE_DECL_KM_STR_STR_VOLATILE);
    }
    async fn test_inspect_table_fully_qualified_entity() {
        query.push(format!("INSPECT MODEL {__MYENTITY__}"));
        match con.run_query_raw(&query).await.unwrap() {
            Element::String(st) => {
                assert_eq!(st, TABLE_DECL_KM_STR_STR_VOLATILE.to_owned())
            }
            _ => panic!("Bad response for inspect table"),
        }
    }
    async fn test_inspect_keyspaces_syntax_error() {
        query.push("INSPECT SPACES iowjfjofoe");
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::ErrorString("bql-invalid-syntax".into()))
        );
    }
    async fn test_inspect_keyspace_syntax_error() {
        query.push("INSPECT SPACE ijfwijifwjo oijfwirfjwo");
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::ErrorString("bql-invalid-syntax".into()))
        );
    }
    async fn test_inspect_table_syntax_error() {
        query.push("INSPECT MODEL ijfwijifwjo oijfwirfjwo");
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::ErrorString("bql-invalid-syntax".into()))
        );
    }
}
