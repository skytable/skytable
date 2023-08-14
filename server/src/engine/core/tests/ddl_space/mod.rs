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

mod alter;
mod create;

use crate::engine::{
    core::{space::Space, GlobalNS},
    error::DatabaseResult,
    idx::STIndex,
    ql::{
        ast::{compile_test, Statement},
        tests::lex_insecure as lex,
    },
};

fn exec_verify(
    gns: &GlobalNS,
    query: &str,
    mut exec: impl FnMut(&GlobalNS, Statement<'_>) -> (DatabaseResult<()>, Box<str>),
    mut verify: impl FnMut(DatabaseResult<&Space>),
) {
    let tok = lex(query.as_bytes()).unwrap();
    let ast_node = compile_test(&tok).unwrap();
    let (res, space_name) = exec(gns, ast_node);
    let rl = gns.spaces().read();
    let space_ref = rl.st_get(&space_name);
    let r = res.map(|_| space_ref.unwrap());
    verify(r);
}

/// Creates a space using the given tokens and allows the caller to verify it
fn exec_alter_and_verify(gns: &GlobalNS, tok: &str, verify: impl Fn(DatabaseResult<&Space>)) {
    exec_verify(
        gns,
        tok,
        |gns, stmt| {
            let space = extract_safe!(stmt, Statement::AlterSpace(s) => s);
            let space_name = space.space_name;
            let r = Space::exec_alter(&gns, space);
            (r, space_name.boxed_str())
        },
        verify,
    );
}

/// Creates a space using the given tokens and allows the caller to verify it
fn exec_create_and_verify(gns: &GlobalNS, tok: &str, verify: impl FnMut(DatabaseResult<&Space>)) {
    exec_verify(
        gns,
        tok,
        |gns, stmt| {
            let space = extract_safe!(stmt, Statement::CreateSpace(s) => s);
            let space_name = space.space_name;
            let r = Space::exec_create(&gns, space);
            (r, space_name.boxed_str())
        },
        verify,
    );
}

/// Creates an empty space with the given tokens
fn exec_create_empty_verify(
    gns: &GlobalNS,
    tok: &str,
) -> DatabaseResult<crate::engine::data::uuid::Uuid> {
    let mut name = None;
    self::exec_create_and_verify(gns, tok, |space| {
        assert_eq!(space.unwrap(), &Space::empty());
        name = Some(space.unwrap().get_uuid());
    });
    Ok(name.unwrap())
}
