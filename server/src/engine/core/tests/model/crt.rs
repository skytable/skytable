/*
 * Created on Sat Mar 04 2023
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

use crate::engine::{
    core::model::{Field, Layer, ModelView},
    data::tag::{DataTag, FullTag},
    error::{DatabaseError, DatabaseResult},
    idx::STIndexSeq,
    ql::{ast::parse_ast_node_full, tests::lex_insecure},
};

fn create(s: &str) -> DatabaseResult<ModelView> {
    let tok = lex_insecure(s.as_bytes()).unwrap();
    let create_model = parse_ast_node_full(&tok[2..]).unwrap();
    ModelView::create_process(create_model)
}

#[test]
fn simple() {
    let ModelView {
        p_key,
        p_tag,
        fields,
    } = create("create model mymodel(username: string, password: binary)").unwrap();
    assert_eq!(p_key.as_ref(), "username");
    assert_eq!(p_tag, FullTag::STR);
    assert_eq!(
        fields.stseq_ord_value().cloned().collect::<Vec<Field>>(),
        [
            Field::new_test([Layer::new_test(FullTag::STR, [0; 2])].into(), false),
            Field::new_test([Layer::new_test(FullTag::BIN, [0; 2])].into(), false)
        ]
    );
}

#[test]
fn idiotic_order() {
    let ModelView {
        p_key,
        p_tag,
        fields,
    } = create("create model mymodel(password: binary, primary username: string)").unwrap();
    assert_eq!(p_key.as_ref(), "username");
    assert_eq!(p_tag, FullTag::STR);
    assert_eq!(
        fields.stseq_ord_value().cloned().collect::<Vec<Field>>(),
        [
            Field::new_test([Layer::new_test(FullTag::BIN, [0; 2])].into(), false),
            Field::new_test([Layer::new_test(FullTag::STR, [0; 2])].into(), false),
        ]
    );
}

#[test]
fn duplicate_primary_key() {
    assert_eq!(
        create("create model mymodel(primary username: string, primary contract_location: binary)")
            .unwrap_err(),
        DatabaseError::DdlModelBadDefinition
    );
}

#[test]
fn duplicate_fields() {
    assert_eq!(
        create("create model mymodel(primary username: string, username: binary)").unwrap_err(),
        DatabaseError::DdlModelBadDefinition
    );
}

#[test]
fn illegal_props() {
    assert_eq!(
        create("create model mymodel(primary username: string, password: binary) with { lol_prop: false }").unwrap_err(),
        DatabaseError::DdlModelBadDefinition
    );
}

#[test]
fn illegal_pk() {
    assert_eq!(
        create(
            "create model mymodel(primary username_bytes: list { type: uint8 }, password: binary)"
        )
        .unwrap_err(),
        DatabaseError::DdlModelBadDefinition
    );
    assert_eq!(
        create("create model mymodel(primary username: float32, password: binary)").unwrap_err(),
        DatabaseError::DdlModelBadDefinition
    );
}
