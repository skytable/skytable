/*
 * Created on Thu Nov 30 2023
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

use {sky_macros::dbtest, skytable::query};

#[dbtest]
fn inspect_global_as_root_returns_user_info() {
    let mut db = db!();
    let inspect: String = db.query_parse(&query!("inspect global")).unwrap();
    assert!(inspect.contains("\"users\":"));
}

#[dbtest(switch_user(username = "sneaking_user_info"))]
fn inspect_global_as_std_user_does_not_return_user_info() {
    let mut db = db!();
    let inspect: String = db.query_parse(&query!("inspect global")).unwrap();
    assert!(!inspect.contains("\"users\":"));
}
