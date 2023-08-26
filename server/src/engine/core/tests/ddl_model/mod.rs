/*
 * Created on Thu Mar 02 2023
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

mod alt;
mod crt;
mod layer;

use crate::engine::{
    core::{model::Model, space::Space, GlobalNS},
    error::DatabaseResult,
    idx::STIndex,
    ql::{
        ast::{parse_ast_node_full, Entity},
        ddl::crt::CreateModel,
        tests::lex_insecure,
    },
};

fn create(s: &str) -> DatabaseResult<Model> {
    let tok = lex_insecure(s.as_bytes()).unwrap();
    let create_model = parse_ast_node_full(&tok[2..]).unwrap();
    Model::process_create(create_model)
}

pub fn exec_create(
    gns: &GlobalNS,
    create_stmt: &str,
    create_new_space: bool,
) -> DatabaseResult<String> {
    let tok = lex_insecure(create_stmt.as_bytes()).unwrap();
    let create_model = parse_ast_node_full::<CreateModel>(&tok[2..]).unwrap();
    let name = match create_model.model_name {
        Entity::Single(tbl) | Entity::Full(_, tbl) => tbl.to_string(),
    };
    if create_new_space {
        gns.test_new_empty_space(&create_model.model_name.into_full().unwrap().0);
    }
    Model::nontransactional_exec_create(gns, create_model).map(|_| name)
}

pub fn exec_create_new_space(gns: &GlobalNS, create_stmt: &str) -> DatabaseResult<()> {
    exec_create(gns, create_stmt, true).map(|_| ())
}

pub fn exec_create_no_create(gns: &GlobalNS, create_stmt: &str) -> DatabaseResult<()> {
    exec_create(gns, create_stmt, false).map(|_| ())
}

fn with_space(gns: &GlobalNS, space_name: &str, f: impl Fn(&Space)) {
    let rl = gns.spaces().read();
    let space = rl.st_get(space_name).unwrap();
    f(space);
}

fn with_model(gns: &GlobalNS, space_id: &str, model_name: &str, f: impl Fn(&Model)) {
    with_space(gns, space_id, |space| {
        let space_rl = space.models().read();
        let model = space_rl.st_get(model_name).unwrap();
        f(model)
    })
}
