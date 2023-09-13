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
    core::space::Space,
    data::uuid::Uuid,
    error::DatabaseResult,
    fractal::GlobalInstanceLike,
    ql::{
        ast::{self},
        tests::lex_insecure as lex,
    },
};

fn exec_create(
    gns: &impl GlobalInstanceLike,
    create: &str,
    verify: impl Fn(&Space),
) -> DatabaseResult<Uuid> {
    let tok = lex(create.as_bytes()).unwrap();
    let ast_node =
        ast::parse_ast_node_full::<crate::engine::ql::ddl::crt::CreateSpace>(&tok[2..]).unwrap();
    let name = ast_node.space_name;
    Space::nontransactional_exec_create(gns, ast_node)?;
    gns.namespace().with_space(&name, |space| {
        verify(space);
        Ok(space.get_uuid())
    })
}

fn exec_alter(
    gns: &impl GlobalInstanceLike,
    alter: &str,
    verify: impl Fn(&Space),
) -> DatabaseResult<Uuid> {
    let tok = lex(alter.as_bytes()).unwrap();
    let ast_node =
        ast::parse_ast_node_full::<crate::engine::ql::ddl::alt::AlterSpace>(&tok[2..]).unwrap();
    let name = ast_node.space_name;
    Space::nontransactional_exec_alter(gns, ast_node)?;
    gns.namespace().with_space(&name, |space| {
        verify(space);
        Ok(space.get_uuid())
    })
}

fn exec_create_alter(
    gns: &impl GlobalInstanceLike,
    crt: &str,
    alt: &str,
    verify_post_alt: impl Fn(&Space),
) -> DatabaseResult<Uuid> {
    let uuid_crt = exec_create(gns, crt, |_| {})?;
    let uuid_alt = exec_alter(gns, alt, verify_post_alt)?;
    assert_eq!(uuid_crt, uuid_alt);
    Ok(uuid_alt)
}
