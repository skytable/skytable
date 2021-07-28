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

#[sky_macros::dbtest]
mod __private {
    use libstress::utils;
    use skytable::{Element, Query, RespCode, Response};
    async fn test_create_keyspace() {
        let mut rng = rand::thread_rng();
        let ksname = utils::rand_alphastring(10, &mut rng);
        query.push("create");
        query.push("keyspace");
        query.push(&ksname);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
    async fn test_drop_keyspace() {
        let mut rng = rand::thread_rng();
        let ksname = utils::rand_alphastring(10, &mut rng);
        query.push("create");
        query.push("keyspace");
        query.push(&ksname);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        let mut query = Query::new();
        query.push("drop");
        query.push("keyspace");
        query.push(ksname);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
    async fn test_create_table() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push("create");
        query.push("table");
        query.push(&tblname);
        query.push("keymap(str,str)");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
    async fn test_create_volatile() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push("create");
        query.push("table");
        query.push(&tblname);
        query.push("keymap(str,str)");
        query.push("volatile");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
    async fn test_create_table_fully_qualified_entity() {
        let mykeyspace: &str = __MYENTITY__.split(':').collect::<Vec<&str>>()[0];
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push("create");
        query.push("table");
        query.push(mykeyspace.to_owned() + ":" + &tblname);
        query.push("keymap(str,str)");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
    async fn test_create_table_volatile_fully_qualified_entity() {
        let mykeyspace: &str = __MYENTITY__.split(':').collect::<Vec<&str>>()[0];
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push("create");
        query.push("table");
        query.push(mykeyspace.to_owned() + ":" + &tblname);
        query.push("keymap(str,str)");
        query.push("volatile");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
    async fn test_drop_table() {
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        query.push("create");
        query.push("table");
        query.push(&tblname);
        query.push("keymap(str,str)");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        let mut query = Query::new();
        query.push("drop");
        query.push("table");
        query.push(&tblname);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
    async fn test_drop_table_fully_qualified_entity() {
        let mykeyspace: &str = __MYENTITY__.split(':').collect::<Vec<&str>>()[0];
        let mut rng = rand::thread_rng();
        let tblname = utils::rand_alphastring(10, &mut rng);
        let my_fqe = mykeyspace.to_owned() + ":" + &tblname;
        query.push("create");
        query.push("table");
        query.push(&my_fqe);
        query.push("keymap(str,str)");
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
        let mut query = Query::new();
        query.push("drop");
        query.push("table");
        query.push(my_fqe);
        assert_eq!(
            con.run_simple_query(&query).await.unwrap(),
            Response::Item(Element::RespCode(RespCode::Okay))
        );
    }
}
