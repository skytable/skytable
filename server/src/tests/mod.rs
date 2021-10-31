/*
 * Created on Tue Aug 25 2020
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

//! This module contains automated tests for queries

#[macro_use]
mod macros;
mod ddl_tests;
mod inspect_tests;
mod kvengine;
mod kvengine_encoding;
mod kvengine_list;
mod pipeline;

mod ssl {
    use skytable::aio::TlsConnection;
    use skytable::{Element, Query};
    use std::env;
    #[tokio::test]
    async fn test_ssl() {
        let mut path = env::var("ROOT_DIR").expect("ROOT_DIR unset");
        path.push_str("/cert.pem");
        let mut con = TlsConnection::new("127.0.0.1", 2004, &path).await.unwrap();
        let q = Query::from("heya");
        assert_eq!(
            con.run_simple_query(&q).await.unwrap(),
            Element::String("HEY!".to_owned())
        );
    }
}
