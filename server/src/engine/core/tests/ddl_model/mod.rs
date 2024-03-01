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
    core::{model::ModelData, EntityIDRef},
    error::QueryResult,
    fractal::GlobalInstanceLike,
    ql::{ast::parse_ast_node_full, ddl::crt::CreateModel, tests::lex_insecure},
};

fn create(s: &str) -> QueryResult<ModelData> {
    let tok = lex_insecure(s.as_bytes()).unwrap();
    let create_model = parse_ast_node_full(&tok[2..]).unwrap();
    ModelData::process_create(create_model)
}

pub fn exec_create(
    global: &impl GlobalInstanceLike,
    create_stmt: &str,
    create_new_space: bool,
) -> QueryResult<String> {
    let tok = lex_insecure(create_stmt.as_bytes()).unwrap();
    let create_model = parse_ast_node_full::<CreateModel>(&tok[2..]).unwrap();
    let name = create_model.model_name.entity().to_owned();
    if create_new_space {
        global
            .state()
            .namespace()
            .create_empty_test_space(create_model.model_name.space())
    }
    ModelData::transactional_exec_create(global, create_model).map(|_| name)
}

pub fn exec_create_new_space(
    global: &impl GlobalInstanceLike,
    create_stmt: &str,
) -> QueryResult<()> {
    exec_create(global, create_stmt, true).map(|_| ())
}

fn with_model(
    global: &impl GlobalInstanceLike,
    space_id: &str,
    model_name: &str,
    f: impl Fn(&ModelData),
) {
    let models = global.state().namespace().idx_models().read();
    let model = models.get(&EntityIDRef::new(space_id, model_name)).unwrap();
    f(model.data())
}
