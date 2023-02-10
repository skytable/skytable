/*
 * Created on Thu Feb 09 2023
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

mod create;

use crate::engine::{
    core::{
        space::{Space, SpaceMeta},
        GlobalNS,
    },
    idx::STIndex,
    ql::{
        ast::{compile_test, Statement},
        tests::lex_insecure as lex,
    },
};

/// Creates a space using the given tokens and allows the caller to verify it
fn exec_create_and_verify(gns: &GlobalNS, tok: &str, verify: impl Fn(&Space)) {
    let tok = lex(tok.as_bytes()).unwrap();
    let space = extract_safe!(compile_test(&tok).unwrap(), Statement::CreateSpace(s) => s);
    let space_name = space.space_name;
    Space::exec_create(&gns, space).unwrap();
    verify(
        gns._spaces()
            .read()
            .st_get_cloned(space_name.as_bytes())
            .unwrap()
            .as_ref(),
    );
}

/// Creates an empty space with the given tokens
fn exec_create_empty_verify(gns: &GlobalNS, tok: &str) {
    self::exec_create_and_verify(gns, tok, |space| {
        assert_eq!(
            space,
            &Space::new(Default::default(), SpaceMeta::with_env(into_dict! {}))
        );
    });
}
