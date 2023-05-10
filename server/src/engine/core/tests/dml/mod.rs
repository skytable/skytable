/*
 * Created on Tue May 09 2023
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

mod insert;

use crate::engine::{
    core::{dml, index::Row, model::ModelData, GlobalNS},
    data::lit::LitIR,
    error::DatabaseResult,
    ql::{ast::parse_ast_node_full, dml::ins::InsertStatement, tests::lex_insecure},
    sync,
};

pub(self) fn exec_insert<T>(
    gns: &GlobalNS,
    model: &str,
    insert: &str,
    key_name: &str,
    f: impl Fn(Row) -> T,
) -> DatabaseResult<T> {
    if !gns.spaces().read().contains_key("myspace") {
        gns.test_new_empty_space("myspace");
    }
    let lex_create_model = lex_insecure(model.as_bytes()).unwrap();
    let stmt_create_model = parse_ast_node_full(&lex_create_model[2..]).unwrap();
    ModelData::exec_create(gns, stmt_create_model)?;
    let lex_insert = lex_insecure(insert.as_bytes()).unwrap();
    let stmt_insert = parse_ast_node_full::<InsertStatement>(&lex_insert[1..]).unwrap();
    let entity = stmt_insert.entity();
    dml::insert(gns, stmt_insert)?;
    let guard = sync::atm::cpin();
    gns.with_model(entity, |mdl| {
        let _irm = mdl.intent_read_model();
        let row = mdl
            .primary_index()
            .select(LitIR::from(key_name), &guard)
            .unwrap()
            .clone();
        drop(guard);
        Ok(f(row))
    })
}
