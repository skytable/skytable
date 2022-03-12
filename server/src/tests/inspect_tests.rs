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

#[sky_macros::dbtest_module]
mod __private {
    use skytable::{types::Array, Element, RespCode};
    async fn test_inspect_keyspaces() {
        query.push("INSPECT");
        query.push("KEYSPACES");
        assert!(matches!(
            con.run_query_raw(&query).await.unwrap(),
            Element::Array(Array::Str(_))
        ))
    }
    async fn test_inspect_keyspace() {
        let my_keyspace: &str = __MYENTITY__.split(':').collect::<Vec<&str>>()[0];
        query.push("INSPECT");
        query.push("KEYSPACE");
        query.push(my_keyspace);
        assert!(matches!(
            con.run_query_raw(&query).await.unwrap(),
            Element::Array(Array::Str(_))
        ))
    }
    async fn test_inspect_table() {
        let my_table: &str = __MYENTITY__.split(':').collect::<Vec<&str>>()[1];
        query.push("INSPECT");
        query.push("TABLE");
        query.push(my_table);
        match con.run_query_raw(&query).await.unwrap() {
            Element::String(st) => {
                assert_eq!(st, "Keymap { data:(str,str), volatile:true }".to_owned())
            }
            _ => panic!("Bad response for inspect table"),
        }
    }
    async fn test_inspect_table_fully_qualified_entity() {
        query.push("INSPECT");
        query.push("TABLE");
        query.push(__MYENTITY__);
        match con.run_query_raw(&query).await.unwrap() {
            Element::String(st) => {
                assert_eq!(st, "Keymap { data:(str,str), volatile:true }".to_owned())
            }
            _ => panic!("Bad response for inspect table"),
        }
    }
    async fn test_inspect_keyspaces_syntax_error() {
        query.push("INSPECT");
        query.push("KEYSPACES");
        query.push("iowjfjofoe");
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::ActionError)
        );
    }
    async fn test_inspect_keyspace_syntax_error() {
        query.push("INSPECT");
        query.push("KEYSPACE");
        query.push("ijfwijifwjo");
        query.push("oijfwirfjwo");
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::ActionError)
        );
    }
    async fn test_inspect_table_syntax_error() {
        query.push("INSPECT");
        query.push("TABLE");
        query.push("ijfwijifwjo");
        query.push("oijfwirfjwo");
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::ActionError)
        );
    }
}
