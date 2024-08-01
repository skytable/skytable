/*
 * Created on Fri Sep 22 2023
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

use crate::engine::ql::{
    ast,
    dcl::{self, SysctlCommand},
    tests::lex_insecure,
};

#[sky_macros::test]
fn report_status_simple() {
    let query = lex_insecure(b"sysctl report status").unwrap();
    let q = ast::parse_ast_node_full::<dcl::SysctlCommand>(&query[1..]).unwrap();
    assert_eq!(q, SysctlCommand::ReportStatus)
}

#[sky_macros::test]
fn create_user_simple() {
    let query = lex_insecure(b"sysctl create user sayan with { password: 'mypass123' }").unwrap();
    let q = ast::parse_ast_node_full::<dcl::SysctlCommand>(&query[1..]).unwrap();
    assert_eq!(
        q,
        SysctlCommand::CreateUser(dcl::UserDecl::new(
            "sayan".into(),
            into_dict!("password" => lit!("mypass123"))
        ))
    )
}

#[sky_macros::test]
fn alter_user_simple() {
    let query = lex_insecure(b"sysctl alter user sayan with { password: 'mypass123' }").unwrap();
    let q = ast::parse_ast_node_full::<dcl::SysctlCommand>(&query[1..]).unwrap();
    assert_eq!(
        q,
        SysctlCommand::AlterUser(dcl::UserDecl::new(
            "sayan".into(),
            into_dict!("password" => lit!("mypass123"))
        ))
    )
}

#[sky_macros::test]
fn delete_user_simple() {
    let query = lex_insecure(b"sysctl drop user monster").unwrap();
    let q = ast::parse_ast_node_full::<dcl::SysctlCommand>(&query[1..]).unwrap();
    assert_eq!(
        q,
        SysctlCommand::DropUser(dcl::UserDel::new("monster".into()))
    );
}
