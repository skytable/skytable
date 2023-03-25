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

mod plan {
    use crate::{
        engine::{
            core::{
                model::{
                    self,
                    alt::{AlterAction, AlterPlan},
                    Field, Layer,
                },
                tests::model::create,
            },
            error::{DatabaseError, DatabaseResult},
            ql::{ast::parse_ast_node_full, tests::lex_insecure},
        },
        vecfuse,
    };
    fn with_plan(model: &str, plan: &str, f: impl Fn(AlterPlan)) -> DatabaseResult<()> {
        let model = create(model)?;
        let tok = lex_insecure(plan.as_bytes()).unwrap();
        let alter = parse_ast_node_full(&tok[2..]).unwrap();
        let model_write = model.intent_write_model();
        let mv = AlterPlan::fdeltas(&model, &model_write, alter)?;
        Ok(f(mv))
    }
    fn plan(model: &str, plan: &str, f: impl Fn(AlterPlan)) {
        with_plan(model, plan, f).unwrap()
    }
    /*
        Simple
    */
    #[test]
    fn simple_add() {
        plan(
            "create model mymodel(username: string, password: binary)",
            "alter model mymodel add myfield { type: string, nullable: true }",
            |plan| {
                assert_eq!(plan.model.as_str(), "mymodel");
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
    #[test]
    fn simple_remove() {
        plan(
            "create model mymodel(username: string, password: binary, useless_field: uint8)",
            "alter model mymodel remove useless_field",
            |plan| {
                assert_eq!(plan.model.as_str(), "mymodel");
                assert!(plan.no_lock);
                assert_eq!(
                    plan.action,
                    AlterAction::Remove(["useless_field".into()].into())
                )
            },
        );
    }
    #[test]
    fn simple_update() {
        // FREEDOM! DAMN THE PASSWORD!
        plan(
            "create model mymodel(username: string, password: binary)",
            "alter model mymodel update password { nullable: true }",
            |plan| {
                assert_eq!(plan.model.as_str(), "mymodel");
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
    /*
        Illegal
    */
    #[test]
    fn illegal_remove_nx() {
        assert_eq!(
            with_plan(
                "create model mymodel(username: string, password: binary)",
                "alter model mymodel remove password_e2e",
                |_| {}
            )
            .unwrap_err(),
            DatabaseError::DdlModelAlterFieldNotFound
        );
    }
    #[test]
    fn illegal_remove_pk() {
        assert_eq!(
            with_plan(
                "create model mymodel(username: string, password: binary)",
                "alter model mymodel remove username",
                |_| {}
            )
            .unwrap_err(),
            DatabaseError::DdlModelAlterProtectedField
        );
    }
    #[test]
    fn illegal_add_pk() {
        assert_eq!(
            with_plan(
                "create model mymodel(username: string, password: binary)",
                "alter model mymodel add username { type: string }",
                |_| {}
            )
            .unwrap_err(),
            DatabaseError::DdlModelAlterBad
        );
    }
    #[test]
    fn illegal_add_ex() {
        assert_eq!(
            with_plan(
                "create model mymodel(username: string, password: binary)",
                "alter model mymodel add password { type: string }",
                |_| {}
            )
            .unwrap_err(),
            DatabaseError::DdlModelAlterBad
        );
    }
    #[test]
    fn illegal_update_pk() {
        assert_eq!(
            with_plan(
                "create model mymodel(username: string, password: binary)",
                "alter model mymodel update username { type: string }",
                |_| {}
            )
            .unwrap_err(),
            DatabaseError::DdlModelAlterProtectedField
        );
    }
    #[test]
    fn illegal_update_nx() {
        assert_eq!(
            with_plan(
                "create model mymodel(username: string, password: binary)",
                "alter model mymodel update username_secret { type: string }",
                |_| {}
            )
            .unwrap_err(),
            DatabaseError::DdlModelAlterFieldNotFound
        );
    }
    fn bad_type_cast(orig_ty: &str, new_ty: &str) {
        let create = format!("create model mymodel(username: string, silly_field: {orig_ty})");
        let alter = format!("alter model mymodel update silly_field {{ type: {new_ty} }}");
        assert_eq!(
            with_plan(&create, &alter, |_| {}).expect_err(&format!(
                "found no error in transformation: {orig_ty} -> {new_ty}"
            )),
            DatabaseError::DdlModelAlterBadTypedef,
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
    #[test]
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
    #[test]
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
    #[test]
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
    #[test]
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
    #[test]
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
    #[test]
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
    #[test]
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
