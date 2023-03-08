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
    use crate::engine::{
        core::{
            model::{
                alt::{AlterAction, AlterPlan},
                Field, Layer,
            },
            tests::model::create,
        },
        error::{DatabaseError, DatabaseResult},
        ql::{ast::parse_ast_node_full, tests::lex_insecure},
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
}
