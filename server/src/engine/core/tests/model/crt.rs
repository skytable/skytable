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

mod validation {
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
        ModelView::process_create(create_model)
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
            create(
                "create model mymodel(primary username: string, primary contract_location: binary)"
            )
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
            create("create model mymodel(primary username: float32, password: binary)")
                .unwrap_err(),
            DatabaseError::DdlModelBadDefinition
        );
    }
}

/*
    Exec
*/

mod exec {
    use crate::engine::{
        core::{
            model::{Field, Layer, ModelView},
            space::Space,
            GlobalNS,
        },
        data::tag::{DataTag, FullTag},
        error::DatabaseResult,
        idx::{STIndex, STIndexSeq},
        ql::{ast::parse_ast_node_full, tests::lex_insecure},
    };

    const SPACE: &str = "myspace";

    pub fn exec_create(
        gns: &GlobalNS,
        create_stmt: &str,
        space_id: &str,
        create_new_space: bool,
    ) -> DatabaseResult<()> {
        if create_new_space {
            assert!(gns.test_new_empty_space(space_id));
        }
        let tok = lex_insecure(create_stmt.as_bytes()).unwrap();
        let create_model = parse_ast_node_full(&tok[2..]).unwrap();
        ModelView::exec_create(gns, space_id.as_bytes(), create_model)
    }

    pub fn exec_create_new_space(
        gns: &GlobalNS,
        create_stmt: &str,
        space_id: &str,
    ) -> DatabaseResult<()> {
        exec_create(gns, create_stmt, space_id, true)
    }

    pub fn exec_create_no_create(
        gns: &GlobalNS,
        create_stmt: &str,
        space_id: &str,
    ) -> DatabaseResult<()> {
        exec_create(gns, create_stmt, space_id, false)
    }

    fn with_space(gns: &GlobalNS, space_name: &str, f: impl Fn(&Space)) {
        let rl = gns.spaces().read();
        let space = rl.st_get(space_name.as_bytes()).unwrap();
        f(space);
    }

    fn with_model(gns: &GlobalNS, space_id: &str, model_name: &str, f: impl Fn(&ModelView)) {
        with_space(gns, space_id, |space| {
            let space_rl = space.models().read();
            let model = space_rl.st_get(model_name.as_bytes()).unwrap();
            f(model)
        })
    }

    #[test]
    fn simple() {
        let gns = GlobalNS::empty();
        exec_create_new_space(
            &gns,
            "create model mymodel(username: string, password: binary)",
            SPACE,
        )
        .unwrap();
        with_model(&gns, SPACE, "mymodel", |model| {
            let models: Vec<(String, Field)> = model
                .fields()
                .stseq_ord_kv()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            assert_eq!(model.p_key.as_ref(), "username");
            assert_eq!(model.p_tag, FullTag::STR);
            assert_eq!(
                models,
                [
                    (
                        "username".to_string(),
                        Field::new_test([Layer::str()].into(), false)
                    ),
                    (
                        "password".to_string(),
                        Field::new_test([Layer::bin()].into(), false)
                    )
                ]
            );
        });
    }
}
