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
    use libstress::utils;
    use skytable::types::Array;
    use skytable::{query, Element, Query, RespCode};
    async fn test_create_keyspace() {
        let mut rng = rand::thread_rng();
        let ksname = utils::rand_alphastring(10, &mut rng);
        query.push(format!("create space {ksname}"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_drop_keyspace() {
        let mut rng = rand::thread_rng();
        let ksname = utils::rand_alphastring(10, &mut rng);
        query.push(format!("create space {ksname}"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
        let mut query = Query::new();
        query.push(format!("drop space {ksname}"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_create_table() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push(format!("create model {tblname}(string, string)"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_create_volatile() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push(format!("create model {tblname}(string, string) volatile"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_create_table_fully_qualified_entity() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push(format!("create model {__MYKS__}.{tblname}(string, string)"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_create_table_volatile_fully_qualified_entity() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push(format!(
            "create model {__MYKS__}.{tblname}(string, string) volatile"
        ));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_drop_table() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push(format!("create model {tblname}(string, string)"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
        let query = Query::from(format!("drop model {tblname}"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_drop_table_fully_qualified_entity() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        let my_fqe = __MYKS__.to_owned() + "." + &tblname;
        query.push(format!("create model {my_fqe}(string, string)"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
        let query = Query::from(format!("drop model {my_fqe}"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        );
    }
    async fn test_use() {
        query.push(format!("USE {__MYENTITY__}"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::Okay)
        )
    }
    async fn test_use_syntax_error() {
        query.push(format!("USE {__MYENTITY__} wiwofjwjfio"));
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::RespCode(RespCode::ErrorString("bql-invalid-syntax".into()))
        )
    }
    async fn test_whereami() {
        query.push("whereami");
        assert_eq!(
            con.run_query_raw(&query).await.unwrap(),
            Element::Array(Array::NonNullStr(vec![__MYKS__, __MYTABLE__]))
        );
        runeq!(
            con,
            query!("use default"),
            Element::RespCode(RespCode::Okay)
        );
        runeq!(
            con,
            query!("whereami"),
            Element::Array(Array::NonNullStr(vec!["default".to_owned()]))
        );
        runeq!(
            con,
            query!("use default.default"),
            Element::RespCode(RespCode::Okay)
        );
        runeq!(
            con,
            query!("whereami"),
            Element::Array(Array::NonNullStr(vec![
                "default".to_owned(),
                "default".to_owned()
            ]))
        );
    }
}
