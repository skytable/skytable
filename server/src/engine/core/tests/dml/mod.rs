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

mod delete;
mod insert;

use crate::engine::{
    core::{dml, index::Row, model::ModelData, GlobalNS},
    data::lit::LitIR,
    error::DatabaseResult,
    ql::{
        ast::{parse_ast_node_full, Entity},
        dml::{del::DeleteStatement, ins::InsertStatement},
        tests::lex_insecure,
    },
    sync,
};

fn _exec_only_create_space_model(gns: &GlobalNS, model: &str) -> DatabaseResult<()> {
    if !gns.spaces().read().contains_key("myspace") {
        gns.test_new_empty_space("myspace");
    }
    let lex_create_model = lex_insecure(model.as_bytes()).unwrap();
    let stmt_create_model = parse_ast_node_full(&lex_create_model[2..]).unwrap();
    ModelData::exec_create(gns, stmt_create_model)
}

fn _exec_only_insert_only<T>(
    gns: &GlobalNS,
    insert: &str,
    and_then: impl Fn(Entity) -> T,
) -> DatabaseResult<T> {
    let lex_insert = lex_insecure(insert.as_bytes()).unwrap();
    let stmt_insert = parse_ast_node_full::<InsertStatement>(&lex_insert[1..]).unwrap();
    let entity = stmt_insert.entity();
    dml::insert(gns, stmt_insert)?;
    let r = and_then(entity);
    Ok(r)
}

fn _exec_only_read_key_and_then<T>(
    gns: &GlobalNS,
    entity: Entity,
    key_name: &str,
    and_then: impl Fn(Row) -> T,
) -> DatabaseResult<T> {
    let guard = sync::atm::cpin();
    gns.with_model(entity, |mdl| {
        let _irm = mdl.intent_read_model();
        let row = mdl
            .primary_index()
            .select(LitIR::from(key_name), &guard)
            .unwrap()
            .clone();
        drop(guard);
        Ok(and_then(row))
    })
}

fn _exec_delete_only(gns: &GlobalNS, delete: &str, key: &str) -> DatabaseResult<()> {
    let lex_del = lex_insecure(delete.as_bytes()).unwrap();
    let delete = parse_ast_node_full::<DeleteStatement>(&lex_del[1..]).unwrap();
    let entity = delete.entity();
    dml::delete(gns, delete)?;
    assert_eq!(
        gns.with_model(entity, |model| {
            let _ = model.intent_read_model();
            let g = sync::atm::cpin();
            Ok(model.primary_index().select(key.into(), &g).is_none())
        }),
        Ok(true)
    );
    Ok(())
}

pub(self) fn exec_insert<T: Default>(
    gns: &GlobalNS,
    model: &str,
    insert: &str,
    key_name: &str,
    f: impl Fn(Row) -> T,
) -> DatabaseResult<T> {
    _exec_only_create_space_model(gns, model)?;
    _exec_only_insert_only(gns, insert, |entity| {
        _exec_only_read_key_and_then(gns, entity, key_name, |row| f(row))
    })?
}

pub(self) fn exec_insert_only(gns: &GlobalNS, insert: &str) -> DatabaseResult<()> {
    _exec_only_insert_only(gns, insert, |_| {})
}

pub(self) fn exec_delete(
    gns: &GlobalNS,
    model: &str,
    insert: Option<&str>,
    delete: &str,
    key: &str,
) -> DatabaseResult<()> {
    _exec_only_create_space_model(gns, model)?;
    if let Some(insert) = insert {
        _exec_only_insert_only(gns, insert, |_| {})?;
    }
    _exec_delete_only(gns, delete, key)
}
