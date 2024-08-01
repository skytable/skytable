/*
 * This file is a part of Skytable
 *
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

/*
    while I have manually verified the safety of every component, an excruciatingly painful task involving taking each structure,
    placing it into an executable, checking with valgrind (due to some miri limitations), followed by checking with miri itself...
    a task that took me a few months of day and night work and investigation ... I want to slowly turn these safety checks into a
    "routine" thing so that it's checked with every run.

    Some of these are here in this module.

    -- Sayan (@ohsayan); July, 2024
*/

use crate::engine::{data::cell::Datacell, ql::lex::Token};

#[sky_macros::test]
fn token_vector_swap() {
    let data = vec![
        Datacell::new_list(vec![
            Datacell::new_str("hello".to_owned().into_boxed_str()),
            Datacell::new_str("world".to_owned().into_boxed_str()),
            Datacell::new_list(vec![
                Datacell::new_str("technically".to_owned().into_boxed_str()),
                Datacell::new_str("this".to_owned().into_boxed_str()),
                Datacell::new_str("is".to_owned().into_boxed_str()),
                Datacell::new_str("an".to_owned().into_boxed_str()),
                Datacell::new_bin(b"illegal".to_vec().into_boxed_slice()),
                Datacell::new_bin(b"list".to_vec().into_boxed_slice()),
            ]),
        ]),
        Datacell::new_uint_default(u64::MAX),
    ];
    let token = Token::dc_list(data.clone());
    let taken_list = match &token {
        Token::DCList(dcl) => unsafe {
            // UNSAFE(@ohsayan): this is actually fine since we're the only thread
            Token::take_list(dcl)
        },
        _ => panic!(),
    };
    assert_eq!(taken_list, data);
}
