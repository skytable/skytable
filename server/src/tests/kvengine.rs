/*
 * Created on Thu Sep 10 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

//! Tests for the key/value engine and its operations
//!
//! The test functions here might seem slightly _mysterious_ -- but they aren't! The `dbtest` macro from the
//! `sky_macros` crate is what does the magic. It provides each function with an async `stream` to write to.
//! This stream is connected over TCP to a database instance. Once the test completes, the database instance
//! and its data is destroyed; but the spawned database instances are started up in a way to not store any
//! data at all, so this is just a precautionary step.

#[sky_macros::dbtest]
mod __private {
    macro_rules! setkeys {
        ($con:ident, $($key:literal:$value:literal),*) => {
            let mut q = Query::new();
            q.push("MSET");
            let mut count = 0;
            $(
                q.push($key);
                q.push($value);
                count += 1;
            )*
            assert_eq!(
                $con.run_simple_query(&q).await.unwrap(),
                Response::Item(Element::UnsignedInt(count))
            );
        };
    }
    #[cfg(test)]
    use skytable::{Element, Query, RespCode, Response};
    /// Test a HEYA query: The server should return HEY!
    async fn test_heya() {
        query.push("heya");
        let resp = con.run_simple_query(&query).await.unwrap();
        assert_eq!(resp, Response::Item(Element::String("HEY!".to_owned())));
    }

    /// Test a GET query: for a non-existing key
    async fn test_get_single_nil() {
        query.push("get");
        query.push("x");
        let resp = con.run_simple_query(&query).await.unwrap();
        assert_eq!(resp, Response::Item(Element::RespCode(RespCode::NotFound)));
    }

    /// Test a GET query: for an existing key
    async fn test_get_single_okay() {
        query.push("set");
        query.push("x");
        query.push("100");
        let resp = con.run_simple_query(&query).await.unwrap();
        assert_eq!(resp, Response::Item(Element::RespCode(RespCode::Okay)));
        let mut query = Query::new();
        query.push("get");
        query.push("x");
        let resp = con.run_simple_query(&query).await.unwrap();
        assert_eq!(resp, Response::Item(Element::String("100".to_owned())));
    }

    /// Test a GET query with an incorrect number of arguments
    async fn test_get_syntax_error() {
        query.push("get");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
        let mut query = Query::new();
        query.push("get");
        query.push("x");
        query.push("y");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test a SET query: SET a non-existing key, which should return code: 0
    async fn test_set_single_okay() {
        query.push("sEt");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    /// Test a SET query: SET an existing key, which should return code: 2
    async fn test_set_single_overwrite_error() {
        // first set the key
        query.push("set");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // attempt the same thing again
        let mut query = Query::new();
        query.push("set");
        query.push("x");
        query.push("200");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::OverwriteError))
        );
    }

    /// Test a SET query with incorrect number of arugments
    async fn test_set_syntax_error() {
        query.push("set");
        query.push("x");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
        let mut query = Query::new();
        query.push("set");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an UPDATE query: which should return code: 0
    async fn test_update_single_okay() {
        // first set the key
        query.push("set");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // attempt to update it
        let mut query = Query::new();
        query.push("update");
        query.push("x");
        query.push("200");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    /// Test an UPDATE query: which should return code: 1
    async fn test_update_single_nil() {
        // attempt to update it
        query.push("update");
        query.push("x");
        query.push("200");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::NotFound))
        );
    }

    async fn test_update_syntax_error() {
        query.push("update");
        query.push("x");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
        let mut query = Query::new();
        query.push("update");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test a DEL query: which should return int 0
    async fn test_del_single_zero() {
        query.push("del");
        query.push("x");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(0))
        );
    }

    /// Test a DEL query: which should return int 1
    async fn test_del_single_one() {
        // first set the key
        query.push("set");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now delete it
        let mut query = Query::new();
        query.push("del");
        query.push("x");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(1))
        );
    }

    /// Test a DEL query: which should return the number of keys deleted
    async fn test_del_multiple() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        // now delete them
        let mut query = Query::new();
        query.push("del");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
    }

    /// Test a DEL query with an incorrect number of arguments
    async fn test_del_syntax_error() {
        query.push("del");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an EXISTS query
    async fn test_exists_multiple() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        // now check if they exist
        let mut query = Query::new();
        query.push("exists");
        query.push("x");
        query.push("y");
        query.push("z");
        query.push("a");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
    }

    /// Test an EXISTS query with an incorrect number of arguments
    async fn test_exists_syntax_error() {
        query.push("exists");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an MGET query on a single existing key
    async fn test_mget_multiple_okay() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        // now get them
        let mut query = Query::new();
        query.push("mget");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::Array(vec![
                Element::String("100".to_owned()),
                Element::String("200".to_owned()),
                Element::String("300".to_owned())
            ]))
        );
    }

    /// Test an MGET query with different outcomes
    async fn test_mget_multiple_mixed() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        let mut query = Query::new();
        query.push("mget");
        query.push("x");
        query.push("y");
        query.push("a");
        query.push("z");
        query.push("b");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::Array(vec![
                Element::String("100".to_owned()),
                Element::String("200".to_owned()),
                Element::RespCode(RespCode::NotFound),
                Element::String("300".to_owned()),
                Element::RespCode(RespCode::NotFound)
            ]))
        );
    }

    /// Test an MGET query with an incorrect number of arguments
    async fn test_mget_syntax_error() {
        query.push("mget");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an MSET query with a single non-existing key
    async fn test_mset_single_okay() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(1))
        );
    }

    /// Test an MSET query with non-existing keys
    async fn test_mset_multiple_okay() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
    }

    /// Test an MSET query with a mixed set of outcomes
    async fn test_mset_multiple_mixed() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        // now try to set them again with just another new key
        let mut query = Query::new();
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        query.push("a");
        query.push("apple");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(1))
        );
    }

    /// Test an MSET query with the wrong number of arguments
    async fn test_mset_syntax_error_args_one() {
        query.push("mset");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }
    async fn test_mset_syntax_error_args_three() {
        query.push("mset");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an MUPDATE query with a single non-existing key
    async fn test_mupdate_single_okay() {
        // first set the key
        query.push("mset");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(1))
        );
        // now attempt to update it
        // first set the keys
        let mut query = Query::new();
        query.push("mupdate");
        query.push("x");
        query.push("200");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(1))
        );
    }

    /// Test an MUPDATE query with a mixed set of outcomes
    async fn test_mupdate_multiple_mixed() {
        // first set the keys
        query.push("mset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        // now try to update them with just another new key
        let mut query = Query::new();
        query.push("mupdate");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        query.push("a");
        query.push("apple");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
    }

    /// Test an MUPDATE query with the wrong number of arguments
    async fn test_mupdate_syntax_error_args_one() {
        query.push("mupdate");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    async fn test_mupdate_syntax_error_args_three() {
        query.push("mupdate");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an SSET query: which should return code: 0
    async fn test_sset_single_okay() {
        // first set the keys
        query.push("sset");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    /// Test an SSET query: which should return code: 2
    async fn test_sset_single_overwrite_error() {
        // first set the keys
        query.push("set");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now attempt to overwrite it
        let mut query = Query::new();
        query.push("sset");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::OverwriteError))
        );
    }

    /// Test an SSET query: which should return code: 0
    async fn test_sset_multiple_okay() {
        // first set the keys
        query.push("sset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    /// Test an SSET query: which should return code: 2
    async fn test_sset_multiple_overwrite_error() {
        // first set the keys
        query.push("sset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now attempt to sset again with just one new extra key
        let mut query = Query::new();
        query.push("sset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("b");
        query.push("bananas");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::OverwriteError))
        );
    }

    /// Test an SSET query with the wrong number of arguments
    async fn test_sset_syntax_error_args_one() {
        query.push("sset");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    async fn test_sset_syntax_error_args_three() {
        query.push("sset");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an SUPDATE query: which should return code: 0
    async fn test_supdate_single_okay() {
        // set the key
        query.push("sset");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // update it
        let mut query = Query::new();
        query.push("supdate");
        query.push("x");
        query.push("200");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    /// Test an SUPDATE query: which should return code: 1
    async fn test_supdate_single_nil() {
        query.push("supdate");
        query.push("x");
        query.push("200");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::NotFound))
        );
    }

    /// Test an SUPDATE query: which should return code: 0
    async fn test_supdate_multiple_okay() {
        // first set the keys
        query.push("sset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now update all of them
        let mut query = Query::new();
        query.push("supdate");
        query.push("x");
        query.push("200");
        query.push("y");
        query.push("300");
        query.push("z");
        query.push("400");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    async fn test_supdate_multiple_nil() {
        // no keys exist, so we get a nil
        query.push("supdate");
        query.push("x");
        query.push("200");
        query.push("y");
        query.push("300");
        query.push("z");
        query.push("400");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::NotFound))
        );
    }

    /// Test an SUPDATE query with the wrong number of arguments
    async fn test_supdate_syntax_error_args_one() {
        query.push("mupdate");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    async fn test_supdate_syntax_error_args_three() {
        query.push("mupdate");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test an SDEL query: which should return nil
    async fn test_sdel_single_nil() {
        query.push("sdel");
        query.push("x");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::NotFound))
        );
    }

    /// Test an SDEL query: which should return okay
    async fn test_sdel_single_okay() {
        query.push("sset");
        query.push("x");
        query.push("100");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        let mut query = Query::new();
        query.push("sdel");
        query.push("x");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    /// Test an SDEL query: which should return okay
    async fn test_sdel_multiple_okay() {
        // first set the keys
        query.push("sset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now delete them
        let mut query = Query::new();
        query.push("sdel");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }

    async fn test_sdel_multiple_nil() {
        query.push("sdel");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::NotFound))
        );
    }

    /// Test an SDEL query with an incorrect number of arguments
    async fn test_sdel_syntax_error() {
        query.push("sdel");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test a `DBSIZE` query
    async fn test_dbsize() {
        // first set the keys
        query.push("sset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now check the size
        let mut query = Query::new();
        query.push("dbsize");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        let mut query = Query::new();
        query.push("dbsize");
        query.push(__MYENTITY__);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
    }

    /// Test `DBSIZE` with an incorrect number of arguments
    async fn test_dbsize_syntax_error() {
        query.push("dbsize");
        query.push("iroegjoeijgor");
        query.push("roigjoigjj094");
        query.push("ioewjforfifrj");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test `FLUSHDB`
    async fn test_flushdb_okay() {
        // first set the keys
        query.push("sset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now flush the database
        let mut query = Query::new();
        query.push("flushdb");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now check the size
        let mut query = Query::new();
        query.push("dbsize");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(0))
        );
    }

    /// Test `FLUSHDB` with an incorrect number of arguments
    async fn test_flushdb_syntax_error() {
        query.push("flushdb");
        query.push("x");
        query.push("y");
        query.push("z");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test `USET` which returns okay
    ///
    /// `USET` almost always returns okay for the correct number of key(s)/value(s)
    async fn test_uset_all_okay() {
        query.push("uset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
        // now that the keys already exist, do it all over again
        let mut query = Query::new();
        query.push("uset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(3))
        );
    }

    /// Test `USET` with an incorrect number of arguments
    async fn test_uset_syntax_error_args_one() {
        query.push("uset");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    async fn test_uset_syntax_error_args_three() {
        query.push("uset");
        query.push("one");
        query.push("two");
        query.push("three");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }

    /// Test `KEYLEN`
    async fn test_keylen() {
        // first set the key
        query.push("set");
        query.push("x");
        query.push("helloworld");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        // now check for the length
        let mut query = Query::new();
        query.push("keylen");
        query.push("x");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(10))
        );
    }

    /// Test `KEYLEN` with an incorrect number of arguments
    async fn test_keylen_syntax_error_args_one() {
        query.push("keylen");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }
    async fn test_keylen_syntax_error_args_two() {
        query.push("keylen");
        query.push("x");
        query.push("y");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }
    async fn test_mksnap_disabled() {
        query.push("mksnap");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ErrorString(
                "err-snapshot-disabled".to_owned()
            )))
        );
    }
    async fn test_mksnap_sanitization() {
        query.push("mksnap");
        query.push("/var/omgcrazysnappy");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ErrorString(
                "err-invalid-snapshot-name".to_owned()
            )))
        );
        let mut query = Query::new();
        query.push("mksnap");
        query.push("../omgbacktoparent");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ErrorString(
                "err-invalid-snapshot-name".to_owned()
            )))
        );
    }
    async fn test_lskeys_default() {
        query.push("uset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        query.push("a");
        query.push("apples");
        query.push("b");
        query.push("burgers");
        query.push("c");
        query.push("carrots");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(6))
        );
        // now get 'em
        let mut query = Query::new();
        query.push("lskeys");
        let ret = con.run_simple_query(&query).await.unwrap();
        // don't forget that the keys returned are arranged according to their hashes
        let ret_should_have: Vec<String> = vec!["a", "b", "c", "x", "y", "z"]
            .into_iter()
            .map(|element| element.to_owned())
            .collect();
        if let Response::Item(Element::FlatArray(arr)) = ret {
            assert_eq!(ret_should_have.len(), arr.len());
            assert!(ret_should_have.into_iter().all(|key| arr.contains(&key)));
        } else {
            panic!("Expected flat string array");
        }
    }
    async fn test_lskeys_custom_limit() {
        query.push("uset");
        query.push("x");
        query.push("100");
        query.push("y");
        query.push("200");
        query.push("z");
        query.push("300");
        query.push("a");
        query.push("apples");
        query.push("b");
        query.push("burgers");
        query.push("c");
        query.push("carrots");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::UnsignedInt(6))
        );
        let mut query = Query::new();
        query.push("lskeys");
        query.push("1000");
        let ret = con.run_simple_query(&query).await.unwrap();
        // don't forget that the keys returned are arranged according to their hashes
        let ret_should_have: Vec<String> = vec!["a", "b", "c", "x", "y", "z"]
            .into_iter()
            .map(|element| element.to_owned())
            .collect();
        if let Response::Item(Element::FlatArray(arr)) = ret {
            assert_eq!(ret_should_have.len(), arr.len());
            assert!(ret_should_have.into_iter().all(|key| arr.contains(&key)));
        } else {
            panic!("Expected flat string array");
        }
    }
    async fn test_lskeys_entity() {
        setkeys!(
            con,
            "x":100,
            "y":200,
            "z":300
        );
        query.push("lskeys");
        query.push(&__MYENTITY__);
        let ret = con.run_simple_query(&query).await.unwrap();
        let ret_should_have: Vec<String> = vec!["x", "y", "z"]
            .into_iter()
            .map(|element| element.to_owned())
            .collect();
        if let Response::Item(Element::FlatArray(arr)) = ret {
            assert_eq!(ret_should_have.len(), arr.len());
            assert!(ret_should_have.into_iter().all(|key| arr.contains(&key)));
        } else {
            panic!("Expected flat string array");
        }
    }
    async fn test_lskeys_entity_with_count() {
        setkeys!(
            con,
            "x":100,
            "y":200,
            "z":300
        );
        query.push("lskeys");
        query.push(&__MYENTITY__);
        query.push(3);
        let ret = con.run_simple_query(&query).await.unwrap();
        let ret_should_have: Vec<String> = vec!["x", "y", "z"]
            .into_iter()
            .map(|element| element.to_owned())
            .collect();
        if let Response::Item(Element::FlatArray(arr)) = ret {
            assert_eq!(ret_should_have.len(), arr.len());
            assert!(ret_should_have.into_iter().all(|key| arr.contains(&key)));
        } else {
            panic!("Expected flat string array");
        }
    }
    async fn test_lskeys_syntax_error() {
        query.push("lskeys");
        query.push("abcdefg");
        query.push("hijklmn");
        query.push("riufrif");
        query.push("fvnjnvv");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }
    async fn test_pop_syntax_error() {
        query.push("pop");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::ActionError))
        );
    }
    async fn test_pop_all_success() {
        setkeys!(
            con,
            "x":100,
            "y":200,
            "z":300
        );
        query.push(vec!["pop", "x", "y", "z"]);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::Array(vec![
                Element::String("100".to_owned()),
                Element::String("200".to_owned()),
                Element::String("300".to_owned())
            ]))
        )
    }
    async fn test_pop_mixed() {
        setkeys!(
            con,
            "x":100,
            "y":200,
            "z":300
        );
        query.push(vec!["pop", "apple", "arnold", "x", "madonna", "y", "z"]);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::Array(vec![
                Element::RespCode(RespCode::NotFound),
                Element::RespCode(RespCode::NotFound),
                Element::String("100".to_owned()),
                Element::RespCode(RespCode::NotFound),
                Element::String("200".to_owned()),
                Element::String("300".to_owned())
            ]))
        );
    }
}
