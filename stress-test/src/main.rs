/*
 * Created on Wed Jun 16 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]

use libstress::Workpool;
use skytable::Connection;
use skytable::Query;
use skytable::{Element, Response};

fn main() {
    let pool = Workpool::new(
        10,
        || Connection::new("127.0.0.1", 2003).unwrap(),
        |con, query| {
            let ret = con.run_simple_query(&query).unwrap();
            assert_eq!(ret, Response::Item(Element::String("HEY!".to_owned())));
        },
        |_| {},
        false,
    );
    loop {
        pool.execute(Query::from("HEYA"));
    }
}
