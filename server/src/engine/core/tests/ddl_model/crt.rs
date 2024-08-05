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
    use {
        super::super::create,
        crate::engine::{
            core::model::{DeltaVersion, Field, Layer},
            data::tag::{DataTag, FullTag},
            error::QueryError,
            idx::STIndexSeq,
        },
    };

    #[sky_macros::test]
    fn simple() {
        let model =
            create("create model myspace.mymodel(username: string, password: binary)").unwrap();
        assert_eq!(model.p_key(), "username");
        assert_eq!(model.p_tag(), FullTag::STR);
        assert_eq!(
            model
                .fields()
                .stseq_ord_value()
                .cloned()
                .collect::<Vec<Field>>(),
            [
                Field::new([Layer::new_empty_props(FullTag::STR)].into(), false),
                Field::new([Layer::new_empty_props(FullTag::BIN)].into(), false)
            ]
        );
        assert_eq!(
            model.delta_state().schema_current_version(),
            DeltaVersion::genesis()
        );
    }

    #[sky_macros::test]
    fn list() {
        let model =
            create("create model myspace.mymodel(username: string, notes: [[binary]])").unwrap();
        assert_eq!(model.p_key(), "username");
        assert_eq!(model.p_tag(), FullTag::STR);
        assert_eq!(
            model
                .fields()
                .stseq_ord_value()
                .cloned()
                .collect::<Vec<Field>>(),
            [
                Field::new([Layer::new_empty_props(FullTag::STR)].into(), false),
                Field::new(
                    [
                        Layer::new_empty_props(FullTag::LIST),
                        Layer::new_empty_props(FullTag::LIST),
                        Layer::new_empty_props(FullTag::BIN),
                    ]
                    .into(),
                    false
                )
            ]
        );
        assert_eq!(
            model.delta_state().schema_current_version(),
            DeltaVersion::genesis()
        );
    }

    #[sky_macros::test]
    fn idiotic_order() {
        let model =
            create("create model myspace.mymodel(password: binary, primary username: string)")
                .unwrap();
        assert_eq!(model.p_key(), "username");
        assert_eq!(model.p_tag(), FullTag::STR);
        assert_eq!(
            model
                .fields()
                .stseq_ord_value()
                .cloned()
                .collect::<Vec<Field>>(),
            [
                Field::new([Layer::new_empty_props(FullTag::BIN)].into(), false),
                Field::new([Layer::new_empty_props(FullTag::STR)].into(), false),
            ]
        );
        assert_eq!(
            model.delta_state().schema_current_version(),
            DeltaVersion::genesis()
        );
    }

    #[sky_macros::test]
    fn duplicate_primary_key() {
        assert_eq!(
            create(
                "create model myspace.mymodel(primary username: string, primary contract_location: binary)"
            )
            .unwrap_err(),
            QueryError::QExecDdlModelBadDefinition
        );
    }

    #[sky_macros::test]
    fn duplicate_fields() {
        assert_eq!(
            create("create model myspace.mymodel(primary username: string, username: binary)")
                .unwrap_err(),
            QueryError::QExecDdlModelBadDefinition
        );
    }

    #[sky_macros::test]
    fn illegal_props() {
        assert_eq!(
        create("create model myspace.mymodel(primary username: string, password: binary) with { lol_prop: false }").unwrap_err(),
        QueryError::QExecDdlModelBadDefinition
    );
    }

    #[sky_macros::test]
    fn illegal_pk() {
        assert_eq!(
        create(
            "create model myspace.mymodel(primary username_bytes: list { type: uint8 }, password: binary)"
        )
        .unwrap_err(),
        QueryError::QExecDdlModelBadDefinition
    );
        assert_eq!(
            create("create model myspace.mymodel(primary username: float32, password: binary)")
                .unwrap_err(),
            QueryError::QExecDdlModelBadDefinition
        );
    }
}

/*
    Exec
*/

mod exec {
    use crate::engine::{
        core::{
            model::{DeltaVersion, Field, Layer},
            tests::ddl_model::{exec_create_new_space, with_model},
        },
        data::tag::{DataTag, FullTag},
        fractal::test_utils::TestGlobal,
        idx::STIndexSeq,
    };

    const SPACE: &str = "myspace";

    #[sky_macros::test]
    fn simple() {
        let global = TestGlobal::new_with_driver_id("exec_simple_create");
        exec_create_new_space(
            &global,
            "create model myspace.mymodel(username: string, password: binary)",
        )
        .unwrap();
        with_model(&global, SPACE, "mymodel", |model| {
            let models: Vec<(String, Field)> = model
                .fields()
                .stseq_ord_kv()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            assert_eq!(model.p_key(), "username");
            assert_eq!(model.p_tag(), FullTag::STR);
            assert_eq!(
                models,
                [
                    (
                        "username".to_string(),
                        Field::new([Layer::str()].into(), false)
                    ),
                    (
                        "password".to_string(),
                        Field::new([Layer::bin()].into(), false)
                    )
                ]
            );
            assert_eq!(
                model.delta_state().schema_current_version(),
                DeltaVersion::genesis()
            );
        });
    }
    #[sky_macros::test]
    fn simple_list() {
        let global = TestGlobal::new_with_driver_id("exec_simple_list_create");
        exec_create_new_space(
            &global,
            "create model myspace.mymodel(username: string, notes: [[binary]])",
        )
        .unwrap();
        with_model(&global, SPACE, "mymodel", |model| {
            let models: Vec<(String, Field)> = model
                .fields()
                .stseq_ord_kv()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            assert_eq!(model.p_key(), "username");
            assert_eq!(model.p_tag(), FullTag::STR);
            assert_eq!(
                models,
                [
                    (
                        "username".to_string(),
                        Field::new([Layer::str()].into(), false)
                    ),
                    (
                        "notes".to_string(),
                        Field::new(
                            [
                                Layer::new_empty_props(FullTag::LIST),
                                Layer::new_empty_props(FullTag::LIST),
                                Layer::new_empty_props(FullTag::BIN),
                            ]
                            .into(),
                            false
                        ),
                    )
                ]
            );
            assert_eq!(
                model.delta_state().schema_current_version(),
                DeltaVersion::genesis()
            );
        });
    }
}
