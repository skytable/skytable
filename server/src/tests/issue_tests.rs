/*
 * Created on Fri Aug 12 2022
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

mod issue_276 {
    // Refer to the issue here: https://github.com/skytable/skytable/issues/276
    // Gist of issue: the auth table was cleaned up because the dummy ks in Memstore does not have
    // the system tables
    use skytable::{Element, Query, RespCode};
    #[sky_macros::dbtest_func(port = 2005, auth_testuser = true, skip_if_cfg = "persist-suite")]
    async fn first_run() {
        // create the space
        let r: Element = con
            .run_query(
                Query::from("create")
                    .arg("keyspace")
                    .arg("please_do_not_vanish"),
            )
            .await
            .unwrap();
        assert_eq!(r, Element::RespCode(RespCode::Okay));
        // drop the space
        let r: Element = con
            .run_query(
                Query::from("drop")
                    .arg("keyspace")
                    .arg("please_do_not_vanish"),
            )
            .await
            .unwrap();
        assert_eq!(r, Element::RespCode(RespCode::Okay));
    }
    #[sky_macros::dbtest_func(port = 2005, auth_testuser = true, run_if_cfg = "persist-suite")]
    async fn second_run() {
        // this function is just a dummy fn; if, as described in #276, the auth data is indeed
        // lost, then the server will simply fail to start up
    }
}
