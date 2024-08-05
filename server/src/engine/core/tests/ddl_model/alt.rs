/*
 * Created on Mon Mar 06 2023
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
    core::{
        model::{alt::AlterPlan, ModelData},
        tests::ddl_model::{create, exec_create},
        EntityIDRef,
    },
    error::QueryResult,
    fractal::GlobalInstanceLike,
    ql::{ast::parse_ast_node_full, ddl::alt::AlterModel, tests::lex_insecure},
};

fn with_plan(model: &str, plan: &str, f: impl Fn(AlterPlan)) -> QueryResult<()> {
    let model = create(model)?;
    let tok = lex_insecure(plan.as_bytes()).unwrap();
    let alter = parse_ast_node_full(&tok[2..]).unwrap();
    let mv = AlterPlan::fdeltas(&model, alter)?;
    Ok(f(mv))
}
fn plan(model: &str, plan: &str, f: impl Fn(AlterPlan)) {
    with_plan(model, plan, f).unwrap()
}
fn exec_plan(
    global: &impl GlobalInstanceLike,
    new_space: bool,
    model: &str,
    plan: &str,
    f: impl Fn(&ModelData),
) -> QueryResult<()> {
    let mdl_name = exec_create(global, model, new_space)?;
    let prev_uuid = {
        global
            .state()
            .namespace()
            .idx_models()
            .read()
            .get(&EntityIDRef::new("myspace", &mdl_name))
            .map(|mdl| mdl.data().get_uuid())
            .unwrap()
    };
    let tok = lex_insecure(plan.as_bytes()).unwrap();
    let alter = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
    ModelData::transactional_exec_alter(global, alter)?;
    let models = global.state().namespace().idx_models().read();
    let model = models.get(&EntityIDRef::new("myspace", &mdl_name)).unwrap();
    assert_eq!(prev_uuid, model.data().get_uuid());
    f(model.data());
    Ok(())
}

mod plan {
    use crate::{
        engine::{
            core::model::{self, alt::AlterAction, Field, Layer},
            data::tag::{DataTag, FullTag},
            error::QueryError,
        },
        vecfuse,
    };
    /*
        Simple
    */
    #[sky_macros::test]
    fn simple_add() {
        super::plan(
            "create model myspace.mymodel(username: string, password: binary)",
            "alter model myspace.mymodel add myfield { type: string, nullable: true }",
            |plan| {
                assert_eq!(plan.model.entity(), "mymodel");
                assert!(plan.no_lock);
                assert_eq!(
                    plan.action,
                    AlterAction::Add(
                        into_dict! { "myfield" => Field::new([Layer::str()].into(), true) }
                    )
                )
            },
        )
    }
    #[sky_macros::test]
    fn list_add() {
        super::plan(
            "create model myspace.mymodel(username: string, password: binary)",
            "alter model myspace.mymodel add notes { type: [[string]], nullable: true }",
            |plan| {
                assert_eq!(plan.model.entity(), "mymodel");
                assert!(plan.no_lock);
                assert_eq!(
                    plan.action,
                    AlterAction::Add(into_dict! { "notes" =>
                        Field::new(
                            [
                                Layer::new_empty_props(FullTag::LIST),
                                Layer::new_empty_props(FullTag::LIST),
                                Layer::new_empty_props(FullTag::STR),
                            ]
                            .into(),
                            true
                        )
                    })
                );
            },
        )
    }
    #[sky_macros::test]
    fn simple_remove() {
        super::plan(
            "create model myspace.mymodel(username: string, password: binary, useless_field: uint8)",
            "alter model myspace.mymodel remove useless_field",
            |plan| {
                assert_eq!(plan.model.entity(), "mymodel");
                assert!(plan.no_lock);
                assert_eq!(
                    plan.action,
                    AlterAction::Remove(["useless_field".into()].into())
                )
            },
        );
    }
    #[sky_macros::test]
    fn simple_update() {
        // FREEDOM! DAMN THE PASSWORD!
        super::plan(
            "create model myspace.mymodel(username: string, password: binary)",
            "alter model myspace.mymodel update password { nullable: true }",
            |plan| {
                assert_eq!(plan.model.entity(), "mymodel");
                assert!(plan.no_lock);
                assert_eq!(
                    plan.action,
                    AlterAction::Update(into_dict! {
                        "password" => Field::new([Layer::bin()].into(), true)
                    })
                );
            },
        );
    }
    #[sky_macros::test]
    fn update_need_lock() {
        // FIGHT THE NULL
        super::plan(
            "create model myspace.mymodel(username: string, null password: binary)",
            "alter model myspace.mymodel update password { nullable: false }",
            |plan| {
                assert_eq!(plan.model.entity(), "mymodel");
                assert!(!plan.no_lock);
                assert_eq!(
                    plan.action,
                    AlterAction::Update(into_dict! {
                        "password" => Field::new([Layer::bin()].into(), false)
                    })
                );
            },
        );
    }
    /*
        Illegal
    */
    #[sky_macros::test]
    fn illegal_remove_nx() {
        assert_eq!(
            super::with_plan(
                "create model myspace.mymodel(username: string, password: binary)",
                "alter model myspace.mymodel remove password_e2e",
                |_| {}
            )
            .unwrap_err(),
            QueryError::QExecUnknownField
        );
    }
    #[sky_macros::test]
    fn illegal_remove_pk() {
        assert_eq!(
            super::with_plan(
                "create model myspace.mymodel(username: string, password: binary)",
                "alter model myspace.mymodel remove username",
                |_| {}
            )
            .unwrap_err(),
            QueryError::QExecDdlModelAlterIllegal
        );
    }
    #[sky_macros::test]
    fn illegal_add_pk() {
        assert_eq!(
            super::with_plan(
                "create model myspace.mymodel(username: string, password: binary)",
                "alter model myspace.mymodel add username { type: string }",
                |_| {}
            )
            .unwrap_err(),
            QueryError::QExecDdlModelAlterIllegal
        );
    }
    #[sky_macros::test]
    fn illegal_add_ex() {
        assert_eq!(
            super::with_plan(
                "create model myspace.mymodel(username: string, password: binary)",
                "alter model myspace.mymodel add password { type: string }",
                |_| {}
            )
            .unwrap_err(),
            QueryError::QExecDdlModelAlterIllegal
        );
    }
    #[sky_macros::test]
    fn illegal_update_pk() {
        assert_eq!(
            super::with_plan(
                "create model myspace.mymodel(username: string, password: binary)",
                "alter model myspace.mymodel update username { type: string }",
                |_| {}
            )
            .unwrap_err(),
            QueryError::QExecDdlModelAlterIllegal
        );
    }
    #[sky_macros::test]
    fn illegal_update_nx() {
        assert_eq!(
            super::with_plan(
                "create model myspace.mymodel(username: string, password: binary)",
                "alter model myspace.mymodel update username_secret { type: string }",
                |_| {}
            )
            .unwrap_err(),
            QueryError::QExecUnknownField
        );
    }
    fn bad_type_cast(orig_ty: &str, new_ty: &str) {
        let create =
            format!("create model myspace.mymodel(username: string, silly_field: {orig_ty})");
        let alter = format!("alter model myspace.mymodel update silly_field {{ type: {new_ty} }}");
        assert_eq!(
            super::with_plan(&create, &alter, |_| {}).expect_err(&format!(
                "found no error in transformation: {orig_ty} -> {new_ty}"
            )),
            QueryError::QExecDdlInvalidTypeDefinition,
            "failed to match error in transformation: {orig_ty} -> {new_ty}",
        )
    }
    fn enumerated_bad_type_casts<O, N>(orig_ty: O, new_ty: N)
    where
        O: IntoIterator<Item = &'static str>,
        N: IntoIterator<Item = &'static str> + Clone,
    {
        for orig in orig_ty {
            let new_ty = new_ty.clone();
            for new in new_ty {
                bad_type_cast(orig, new);
            }
        }
    }
    #[sky_macros::test]
    fn illegal_bool_direct_cast() {
        enumerated_bad_type_casts(
            ["bool"],
            vecfuse![
                model::TY_UINT,
                model::TY_SINT,
                model::TY_BINARY,
                model::TY_STRING,
                model::TY_LIST
            ],
        );
    }
    #[sky_macros::test]
    fn illegal_uint_direct_cast() {
        enumerated_bad_type_casts(
            model::TY_UINT,
            vecfuse![
                model::TY_BOOL,
                model::TY_SINT,
                model::TY_FLOAT,
                model::TY_BINARY,
                model::TY_STRING,
                model::TY_LIST
            ],
        );
    }
    #[sky_macros::test]
    fn illegal_sint_direct_cast() {
        enumerated_bad_type_casts(
            model::TY_SINT,
            vecfuse![
                model::TY_BOOL,
                model::TY_UINT,
                model::TY_FLOAT,
                model::TY_BINARY,
                model::TY_STRING,
                model::TY_LIST
            ],
        );
    }
    #[sky_macros::test]
    fn illegal_float_direct_cast() {
        enumerated_bad_type_casts(
            model::TY_FLOAT,
            vecfuse![
                model::TY_BOOL,
                model::TY_UINT,
                model::TY_SINT,
                model::TY_BINARY,
                model::TY_STRING,
                model::TY_LIST
            ],
        );
    }
    #[sky_macros::test]
    fn illegal_binary_direct_cast() {
        enumerated_bad_type_casts(
            [model::TY_BINARY],
            vecfuse![
                model::TY_BOOL,
                model::TY_UINT,
                model::TY_SINT,
                model::TY_FLOAT,
                model::TY_STRING,
                model::TY_LIST
            ],
        );
    }
    #[sky_macros::test]
    fn illegal_string_direct_cast() {
        enumerated_bad_type_casts(
            [model::TY_STRING],
            vecfuse![
                model::TY_BOOL,
                model::TY_UINT,
                model::TY_SINT,
                model::TY_FLOAT,
                model::TY_BINARY,
                model::TY_LIST
            ],
        );
    }
    #[sky_macros::test]
    fn illegal_list_direct_cast() {
        enumerated_bad_type_casts(
            ["list { type: string }"],
            vecfuse![
                model::TY_BOOL,
                model::TY_UINT,
                model::TY_SINT,
                model::TY_FLOAT,
                model::TY_BINARY,
                model::TY_STRING
            ],
        );
    }
}

mod exec {
    use crate::engine::{
        core::model::{DeltaVersion, Field, Layer},
        data::tag::{DataTag, FullTag},
        error::QueryError,
        fractal::test_utils::TestGlobal,
        idx::{STIndex, STIndexSeq},
    };

    #[sky_macros::test]
    fn simple_add() {
        let global = TestGlobal::new_with_driver_id("simple_add");
        super::exec_plan(
            &global,
            true,
            "create model myspace.mymodel(username: string, col1: uint64)",
            "alter model myspace.mymodel add (col2 { type: uint32, nullable: true }, col3 { type: uint16, nullable: true })",
            |model| {
                assert_eq!(
                    model
                        .fields()
                        .stseq_ord_kv()
                        .rev()
                        .take(2)
                        .map(|(id, f)| (id.as_str().to_owned(), f.clone()))
                        .collect::<Vec<_>>(),
                    [
                        ("col3".into(), Field::new([Layer::uint16()].into(), true)),
                        ("col2".into(), Field::new([Layer::uint32()].into(), true))
                    ]
                );
                assert_eq!(
                    model.delta_state().schema_current_version(),
                    DeltaVersion::__new(2)
                );
            },
        )
        .unwrap();
    }
    #[sky_macros::test]
    fn simple_remove() {
        let global = TestGlobal::new_with_driver_id("simple_remove");
        super::exec_plan(
            &global,
            true,
            "create model myspace.mymodel(username: string, col1: uint64, col2: uint32, col3: uint16, col4: uint8)",
            "alter model myspace.mymodel remove (col1, col2, col3, col4)",
            |mdl| {
                assert_eq!(
                    mdl
                        .fields()
                        .stseq_ord_kv()
                        .rev()
                        .map(|(a, b)| (a.as_str().to_owned(), b.clone()))
                        .collect::<Vec<_>>(),
                    [("username".into(), Field::new([Layer::str()].into(), false))]
                );
                assert_eq!(
                    mdl.delta_state().schema_current_version(),
                    DeltaVersion::__new(4)
                );
            }
        ).unwrap();
    }
    #[sky_macros::test]
    fn simple_update() {
        let global = TestGlobal::new_with_driver_id("simple_update");
        super::exec_plan(
            &global,
            true,
            "create model myspace.mymodel(username: string, password: binary)",
            "alter model myspace.mymodel update password { nullable: true }",
            |model| {
                assert!(model.fields().st_get("password").unwrap().is_nullable());
                assert_eq!(
                    model.delta_state().schema_current_version(),
                    DeltaVersion::genesis()
                );
            },
        )
        .unwrap();
    }
    #[sky_macros::test]
    fn list_add() {
        let global = TestGlobal::new_with_driver_id("simple_add_list");
        super::exec_plan(
            &global,
            true,
            "create model myspace.mymodel(username: string, password: binary)",
            "alter model myspace.mymodel add notes { type: [[string]] }",
            |model| {
                let field = model.fields().st_get("notes").unwrap();
                assert_eq!(
                    *field,
                    Field::new(
                        [
                            Layer::new_empty_props(FullTag::LIST),
                            Layer::new_empty_props(FullTag::LIST),
                            Layer::new_empty_props(FullTag::STR),
                        ]
                        .into(),
                        false
                    ),
                );
                assert_eq!(
                    model.delta_state().schema_current_version(),
                    DeltaVersion::__new(1)
                );
            },
        )
        .unwrap();
    }
    #[sky_macros::test]
    fn failing_alter_nullable_switch_need_lock() {
        let global = TestGlobal::new_with_driver_id("failing_alter_nullable_switch_need_lock");
        assert_eq!(
            super::exec_plan(
                &global,
                true,
                "create model myspace.mymodel(username: string, null gh_handle: string)",
                "alter model myspace.mymodel update gh_handle { nullable: false }",
                |_| {},
            )
            .unwrap_err(),
            QueryError::QExecNeedLock
        );
    }
}
