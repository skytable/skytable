/*
 * Created on Sun Dec 18 2022
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

use super::*;
use crate::engine::ql::{
    ast::{traits::ASTNode, State},
    ddl::Use,
};

/*
    entity
*/

#[test]
fn entity_current() {
    let t = lex_insecure(b"hello").unwrap();
    let mut state = State::new_inplace(&t);
    state.set_space("apps");
    let r = state.try_entity_ref().unwrap();
    assert_eq!(r, ("apps", "hello").into());
}

#[test]
fn entity_full() {
    let t = lex_insecure(b"hello.world").unwrap();
    let mut state = State::new_inplace(&t);
    assert_eq!(
        state.try_entity_ref().unwrap(),
        (("hello"), ("world")).into()
    )
}

/*
    use
*/

#[test]
fn use_new() {
    let t = lex_insecure(b"use myspace").unwrap();
    let mut state = State::new_inplace(&t);
    assert_eq!(
        Use::test_parse_from_state(&mut state).unwrap(),
        Use::Space("myspace".into())
    );
}

#[test]
fn use_null() {
    let t = lex_insecure(b"use null").unwrap();
    let mut state = State::new_inplace(&t);
    assert_eq!(Use::test_parse_from_state(&mut state).unwrap(), Use::Null);
}
