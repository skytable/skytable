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
            core::model::{Field, Layer},
            data::tag::{DataTag, FullTag},
            error::DatabaseError,
            idx::STIndexSeq,
        },
    };

    #[test]
    fn simple() {
        let model = create("create model mymodel(username: string, password: binary)").unwrap();
        assert_eq!(model.p_key.as_ref(), "username");
        assert_eq!(model.p_tag, FullTag::STR);
        assert_eq!(
            model
                .intent_read_model()
                .fields()
                .stseq_ord_value()
                .cloned()
                .collect::<Vec<Field>>(),
            [
                Field::new([Layer::new_test(FullTag::STR, [0; 2])].into(), false),
                Field::new([Layer::new_test(FullTag::BIN, [0; 2])].into(), false)
            ]
        );
    }

    #[test]
    fn idiotic_order() {
        let model =
            create("create model mymodel(password: binary, primary username: string)").unwrap();
        assert_eq!(model.p_key.as_ref(), "username");
        assert_eq!(model.p_tag, FullTag::STR);
        assert_eq!(
            model
                .intent_read_model()
                .fields()
                .stseq_ord_value()
                .cloned()
                .collect::<Vec<Field>>(),
            [
                Field::new([Layer::new_test(FullTag::BIN, [0; 2])].into(), false),
                Field::new([Layer::new_test(FullTag::STR, [0; 2])].into(), false),
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
            model::{Field, Layer},
            tests::model::{exec_create_new_space, with_model},
            GlobalNS,
        },
        data::tag::{DataTag, FullTag},
        idx::STIndexSeq,
    };

    const SPACE: &str = "myspace";

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
                .intent_read_model()
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
                        Field::new([Layer::str()].into(), false)
                    ),
                    (
                        "password".to_string(),
                        Field::new([Layer::bin()].into(), false)
                    )
                ]
            );
        });
    }
}
