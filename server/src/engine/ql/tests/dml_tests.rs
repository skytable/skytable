/*
 * Created on Sun Dec 18 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

use super::*;
mod list_parse {
    use super::*;
    use crate::engine::ql::{ast::parse_ast_node_full, dml::ins::List};

    #[test]
    fn list_mini() {
        let tok = lex_insecure(
            b"
                []
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<List>(&tok[1..]).unwrap();
        assert_eq!(r, vec![])
    }
    #[test]
    fn list() {
        let tok = lex_insecure(
            b"
                [1, 2, 3, 4]
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<List>(&tok[1..]).unwrap();
        assert_eq!(r.as_slice(), into_array![1, 2, 3, 4])
    }
    #[test]
    fn list_pro() {
        let tok = lex_insecure(
            b"
                [
                    [1, 2],
                    [3, 4],
                    [5, 6],
                    []
                ]
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<List>(&tok[1..]).unwrap();
        assert_eq!(
            r.as_slice(),
            into_array![
                into_array![1, 2],
                into_array![3, 4],
                into_array![5, 6],
                into_array![]
            ]
        )
    }
    #[test]
    fn list_pro_max() {
        let tok = lex_insecure(
            b"
                [
                    [[1, 1], [2, 2]],
                    [[], [4, 4]],
                    [[5, 5], [6, 6]],
                    [[7, 7], []]
                ]
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<List>(&tok[1..]).unwrap();
        assert_eq!(
            r.as_slice(),
            into_array![
                into_array![into_array![1, 1], into_array![2, 2]],
                into_array![into_array![], into_array![4, 4]],
                into_array![into_array![5, 5], into_array![6, 6]],
                into_array![into_array![7, 7], into_array![]],
            ]
        )
    }
}

mod tuple_syntax {
    use super::*;
    use crate::engine::ql::{ast::parse_ast_node_full, dml::ins::DataTuple};

    #[test]
    fn tuple_mini() {
        let tok = lex_insecure(b"()").unwrap();
        let r = parse_ast_node_full::<DataTuple>(&tok[1..]).unwrap();
        assert_eq!(r, vec![]);
    }

    #[test]
    fn tuple() {
        let tok = lex_insecure(
            br#"
                (1234, "email@example.com", true)
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<DataTuple>(&tok[1..]).unwrap();
        assert_eq!(
            r.as_slice(),
            into_array_nullable![1234, "email@example.com", true]
        );
    }

    #[test]
    fn tuple_pro() {
        let tok = lex_insecure(
            br#"
                (
                    1234,
                    "email@example.com",
                    true,
                    ["hello", "world", "and", "the", "universe"]
                )
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<DataTuple>(&tok[1..]).unwrap();
        assert_eq!(
            r.as_slice(),
            into_array_nullable![
                1234,
                "email@example.com",
                true,
                into_array!["hello", "world", "and", "the", "universe"]
            ]
        );
    }

    #[test]
    fn tuple_pro_max() {
        let tok = lex_insecure(
            br#"
                (
                    1234,
                    "email@example.com",
                    true,
                    [
                        ["h", "hello"],
                        ["w", "world"],
                        ["a", "and"],
                        ["the"],
                        ["universe"],
                        []
                    ]
                )
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<DataTuple>(&tok[1..]).unwrap();
        assert_eq!(
            r.as_slice(),
            into_array_nullable![
                1234,
                "email@example.com",
                true,
                into_array![
                    into_array!["h", "hello"],
                    into_array!["w", "world"],
                    into_array!["a", "and"],
                    into_array!["the"],
                    into_array!["universe"],
                    into_array![],
                ]
            ]
        );
    }
}
mod map_syntax {
    use super::*;
    use crate::engine::ql::{ast::parse_ast_node_full, dml::ins::DataMap};

    #[test]
    fn map_mini() {
        let tok = lex_insecure(b"{}").unwrap();
        let r = parse_ast_node_full::<DataMap>(&tok[1..]).unwrap();
        assert_eq!(r, null_dict! {})
    }

    #[test]
    fn map() {
        let tok = lex_insecure(
            br#"
                {
                    name: "John Appletree",
                    email: "john@example.com",
                    verified: false,
                    followers: 12345
                }
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<DataMap>(&tok[1..]).unwrap();
        assert_eq!(
            r,
            dict_nullable! {
                "name" => "John Appletree",
                "email" => "john@example.com",
                "verified" => false,
                "followers" => 12345,
            }
        )
    }

    #[test]
    fn map_pro() {
        let tok = lex_insecure(
            br#"
                {
                    name: "John Appletree",
                    email: "john@example.com",
                    verified: false,
                    followers: 12345,
                    tweets_by_day: []
                }
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<DataMap>(&tok[1..]).unwrap();
        assert_eq!(
            r,
            dict_nullable! {
                "name" => "John Appletree",
                "email" => "john@example.com",
                "verified" => false,
                "followers" => 12345,
                "tweets_by_day" => []
            }
        )
    }

    #[test]
    fn map_pro_max() {
        let tok = lex_insecure(br#"
                {
                    name: "John Appletree",
                    email: "john@example.com",
                    verified: false,
                    followers: 12345,
                    tweets_by_day: [
                        ["it's a fresh monday", "monday was tiring"],
                        ["already bored with tuesday", "nope. gotta change stuff, life's getting boring"],
                        ["sunday, going to bed"]
                    ]
                }
            "#)
        .unwrap();
        let r = parse_ast_node_full::<DataMap>(&tok[1..]).unwrap();
        assert_eq!(
            r,
            dict_nullable! {
                "name" => "John Appletree",
                "email" => "john@example.com",
                "verified" => false,
                "followers" => 12345,
                "tweets_by_day" => into_array![
                    into_array![
                        "it's a fresh monday", "monday was tiring"
                    ],
                    into_array![
                        "already bored with tuesday", "nope. gotta change stuff, life's getting boring"
                    ],
                    into_array!["sunday, going to bed"]
                ]
            }
        )
    }
}
mod stmt_insert {
    use {
        super::*,
        crate::engine::ql::{
            ast::parse_ast_node_full,
            dml::{self, ins::InsertStatement},
            lex::Ident,
        },
    };

    #[test]
    fn insert_tuple_mini() {
        let x = lex_insecure(
            br#"
                insert into twitter.users ("sayan")
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<InsertStatement>(&x[1..]).unwrap();
        let e = InsertStatement::new(
            ("twitter", "users").into(),
            into_array_nullable!["sayan"].to_vec().into(),
        );
        assert_eq!(e, r);
    }
    #[test]
    fn insert_tuple() {
        let x = lex_insecure(
            br#"
                insert into twitter.users (
                    "sayan",
                    "Sayan",
                    "sayan@example.com",
                    true,
                    12345,
                    67890
                )
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<InsertStatement>(&x[1..]).unwrap();
        let e = InsertStatement::new(
            ("twitter", "users").into(),
            into_array_nullable!["sayan", "Sayan", "sayan@example.com", true, 12345, 67890]
                .to_vec()
                .into(),
        );
        assert_eq!(e, r);
    }
    #[test]
    fn insert_tuple_pro() {
        let x = lex_insecure(
            br#"
                insert into twitter.users (
                    "sayan",
                    "Sayan",
                    "sayan@example.com",
                    true,
                    12345,
                    67890,
                    null,
                    12345,
                    null
                )
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<InsertStatement>(&x[1..]).unwrap();
        let e = InsertStatement::new(
            ("twitter", "users").into(),
            into_array_nullable![
                "sayan",
                "Sayan",
                "sayan@example.com",
                true,
                12345,
                67890,
                Null,
                12345,
                Null
            ]
            .to_vec()
            .into(),
        );
        assert_eq!(e, r);
    }
    #[test]
    fn insert_map_mini() {
        let tok = lex_insecure(
            br#"
                insert into jotsy.app { username: "sayan" }
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<InsertStatement>(&tok[1..]).unwrap();
        let e = InsertStatement::new(
            ("jotsy", "app").into(),
            dict_nullable! {
                Ident::from("username") => "sayan"
            }
            .into(),
        );
        assert_eq!(e, r);
    }
    #[test]
    fn insert_map() {
        let tok = lex_insecure(
            br#"
                insert into jotsy.app {
                    username: "sayan",
                    name: "Sayan",
                    email: "sayan@example.com",
                    verified: true,
                    following: 12345,
                    followers: 67890
                }
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<InsertStatement>(&tok[1..]).unwrap();
        let e = InsertStatement::new(
            ("jotsy", "app").into(),
            dict_nullable! {
                Ident::from("username") => "sayan",
                Ident::from("name") => "Sayan",
                Ident::from("email") => "sayan@example.com",
                Ident::from("verified") => true,
                Ident::from("following") => 12345,
                Ident::from("followers") => 67890
            }
            .into(),
        );
        assert_eq!(e, r);
    }
    #[test]
    fn insert_map_pro() {
        let tok = lex_insecure(
            br#"
                insert into jotsy.app {
                    username: "sayan",
                    password: "pass123",
                    email: "sayan@example.com",
                    verified: true,
                    following: 12345,
                    followers: 67890,
                    linked_smart_devices: null,
                    bookmarks: 12345,
                    other_linked_accounts: null
                }
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<InsertStatement>(&tok[1..]).unwrap();
        let e = InsertStatement::new(
            ("jotsy", "app").into(),
            dict_nullable! {
                Ident::from("username") => "sayan",
                "password" => "pass123",
                "email" => "sayan@example.com",
                "verified" => true,
                "following" => 12345,
                "followers" => 67890,
                "linked_smart_devices" => Null,
                "bookmarks" => 12345,
                "other_linked_accounts" => Null
            }
            .into(),
        );
        assert_eq!(r, e);
    }
    #[test]
    fn insert_tuple_fnsub() {
        let tok =
            lex_insecure(br#"insert into jotsy.app(@uuidstr(), "sayan", @timesec())"#).unwrap();
        let ret = parse_ast_node_full::<InsertStatement>(&tok[1..]).unwrap();
        let expected = InsertStatement::new(
            ("jotsy", "app").into(),
            into_array_nullable![dml::ins::T_UUIDSTR, "sayan", dml::ins::T_TIMESEC]
                .to_vec()
                .into(),
        );
        assert_eq!(ret, expected);
    }
    #[test]
    fn insert_map_fnsub() {
        let tok = lex_insecure(
            br#"insert into jotsy.app { uuid: @uuidstr(), username: "sayan", signup_time: @timesec() }"#
        ).unwrap();
        let ret = parse_ast_node_full::<InsertStatement>(&tok[1..]).unwrap();
        let expected = InsertStatement::new(
            ("jotsy", "app").into(),
            dict_nullable! {
                "uuid" => dml::ins::T_UUIDSTR,
                Ident::from("username") => "sayan",
                "signup_time" => dml::ins::T_TIMESEC,
            }
            .into(),
        );
        assert_eq!(ret, expected);
    }
}

mod stmt_select {
    use {
        super::*,
        crate::engine::{
            data::lit::Lit,
            ql::{
                ast::{parse_ast_node_full, parse_ast_node_full_with_space},
                dml::{sel::SelectStatement, RelationalExpr},
                lex::Ident,
            },
        },
    };
    #[test]
    fn select_mini() {
        let tok = lex_insecure(
            br#"
                select * from users where username = "sayan"
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<SelectStatement>(&tok[1..], "apps").unwrap();
        let e = SelectStatement::new_test(
            ("apps", "users").into(),
            [].to_vec(),
            true,
            dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"), Lit::new_str("sayan"), RelationalExpr::OP_EQ
                ),
            },
        );
        assert_eq!(r, e);
    }
    #[test]
    fn select() {
        let tok = lex_insecure(
            br#"
                select field1 from users where username = "sayan"
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<SelectStatement>(&tok[1..], "apps").unwrap();
        let e = SelectStatement::new_test(
            ("apps", "users").into(),
            [Ident::from("field1")].to_vec(),
            false,
            dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"), Lit::new_str("sayan"), RelationalExpr::OP_EQ
                ),
            },
        );
        assert_eq!(r, e);
    }
    #[test]
    fn select_pro() {
        let tok = lex_insecure(
            br#"
                select field1 from twitter.users where username = "sayan"
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<SelectStatement>(&tok[1..]).unwrap();
        let e = SelectStatement::new_test(
            ("twitter", "users").into(),
            [Ident::from("field1")].to_vec(),
            false,
            dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"), Lit::new_str("sayan"), RelationalExpr::OP_EQ
                ),
            },
        );
        assert_eq!(r, e);
    }
    #[test]
    fn select_pro_max() {
        let tok = lex_insecure(
            br#"
                select field1, field2 from twitter.users where username = "sayan"
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<SelectStatement>(&tok[1..]).unwrap();
        let e = SelectStatement::new_test(
            ("twitter", "users").into(),
            [Ident::from("field1"), Ident::from("field2")].to_vec(),
            false,
            dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"), Lit::new_str("sayan"), RelationalExpr::OP_EQ
                ),
            },
        );
        assert_eq!(r, e);
    }
}
mod expression_tests {
    use {
        super::*,
        crate::engine::{
            core::query_meta::AssignmentOperator,
            data::lit::Lit,
            ql::{ast::parse_ast_node_full, dml::upd::AssignmentExpression, lex::Ident},
        },
    };
    #[test]
    fn expr_assign() {
        let src = lex_insecure(b"username = 'sayan'").unwrap();
        let r = parse_ast_node_full::<AssignmentExpression>(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(
                Ident::from("username"),
                Lit::new_str("sayan"),
                AssignmentOperator::Assign
            )
        );
    }
    #[test]
    fn expr_add_assign() {
        let src = lex_insecure(b"followers += 100").unwrap();
        let r = parse_ast_node_full::<AssignmentExpression>(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(
                Ident::from("followers"),
                Lit::new_uint(100),
                AssignmentOperator::AddAssign
            )
        );
    }
    #[test]
    fn expr_sub_assign() {
        let src = lex_insecure(b"following -= 150").unwrap();
        let r = parse_ast_node_full::<AssignmentExpression>(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(
                Ident::from("following"),
                Lit::new_uint(150),
                AssignmentOperator::SubAssign
            )
        );
    }
    #[test]
    fn expr_mul_assign() {
        let src = lex_insecure(b"product_qty *= 2").unwrap();
        let r = parse_ast_node_full::<AssignmentExpression>(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(
                Ident::from("product_qty"),
                Lit::new_uint(2),
                AssignmentOperator::MulAssign
            )
        );
    }
    #[test]
    fn expr_div_assign() {
        let src = lex_insecure(b"image_crop_factor /= 2").unwrap();
        let r = parse_ast_node_full::<AssignmentExpression>(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(
                Ident::from("image_crop_factor"),
                Lit::new_uint(2),
                AssignmentOperator::DivAssign
            )
        );
    }
}
mod update_statement {
    use {
        super::*,
        crate::engine::{
            core::query_meta::AssignmentOperator,
            data::lit::Lit,
            ql::{
                ast::{parse_ast_node_full, parse_ast_node_full_with_space},
                dml::{
                    upd::{AssignmentExpression, UpdateStatement},
                    RelationalExpr, WhereClause,
                },
                lex::Ident,
            },
        },
    };
    #[test]
    fn update_mini() {
        let tok = lex_insecure(
            br#"
                update app SET notes += "this is my new note" where username = "sayan"
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<UpdateStatement>(&tok[1..], "apps").unwrap();
        let e = UpdateStatement::new(
            ("apps", "app").into(),
            vec![AssignmentExpression::new(
                Ident::from("notes"),
                Lit::new_str("this is my new note"),
                AssignmentOperator::AddAssign,
            )],
            WhereClause::new(dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"),
                    Lit::new_str("sayan"),
                    RelationalExpr::OP_EQ
                )
            }),
        );
        assert_eq!(r, e);
    }
    #[test]
    fn update() {
        let tok = lex_insecure(
            br#"
                update
                    jotsy.app
                SET
                    notes += "this is my new note",
                    email = "sayan@example.com"
                WHERE
                    username = "sayan"
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<UpdateStatement>(&tok[1..]).unwrap();
        let e = UpdateStatement::new(
            ("jotsy", "app").into(),
            vec![
                AssignmentExpression::new(
                    Ident::from("notes"),
                    Lit::new_str("this is my new note"),
                    AssignmentOperator::AddAssign,
                ),
                AssignmentExpression::new(
                    Ident::from("email"),
                    Lit::new_str("sayan@example.com"),
                    AssignmentOperator::Assign,
                ),
            ],
            WhereClause::new(dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"),
                    Lit::new_str("sayan"),
                    RelationalExpr::OP_EQ
                )
            }),
        );
        assert_eq!(r, e);
    }
}
mod delete_stmt {
    use {
        super::*,
        crate::engine::{
            data::lit::Lit,
            ql::{
                ast::{parse_ast_node_full, parse_ast_node_full_with_space},
                dml::{del::DeleteStatement, RelationalExpr},
                lex::Ident,
            },
        },
    };

    #[test]
    fn delete_mini() {
        let tok = lex_insecure(
            br#"
                delete from users where username = "sayan"
            "#,
        )
        .unwrap();
        let e = DeleteStatement::new_test(
            ("apps", "users").into(),
            dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"),
                    Lit::new_str("sayan"),
                    RelationalExpr::OP_EQ
                )
            },
        );
        assert_eq!(
            parse_ast_node_full_with_space::<DeleteStatement>(&tok[1..], "apps").unwrap(),
            e
        );
    }
    #[test]
    fn delete() {
        let tok = lex_insecure(
            br#"
                delete from twitter.users where username = "sayan"
            "#,
        )
        .unwrap();
        let e = DeleteStatement::new_test(
            ("twitter", "users").into(),
            dict! {
                Ident::from("username") => RelationalExpr::new(
                    Ident::from("username"),
                    Lit::new_str("sayan"),
                    RelationalExpr::OP_EQ
                )
            },
        );
        assert_eq!(
            parse_ast_node_full::<DeleteStatement>(&tok[1..]).unwrap(),
            e
        );
    }
}
mod relational_expr {
    use {
        super::*,
        crate::engine::{
            data::lit::Lit,
            ql::{ast::parse_ast_node_full, dml::RelationalExpr, lex::Ident},
        },
    };

    #[test]
    fn expr_eq() {
        let expr = lex_insecure(b"primary_key = 10").unwrap();
        let r = parse_ast_node_full::<RelationalExpr>(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: Lit::new_uint(10),
                lhs: Ident::from("primary_key"),
                opc: RelationalExpr::OP_EQ
            }
        );
    }
    #[test]
    fn expr_ne() {
        let expr = lex_insecure(b"primary_key != 10").unwrap();
        let r = parse_ast_node_full::<RelationalExpr>(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: Lit::new_uint(10),
                lhs: Ident::from("primary_key"),
                opc: RelationalExpr::OP_NE
            }
        );
    }
    #[test]
    fn expr_gt() {
        let expr = lex_insecure(b"primary_key > 10").unwrap();
        let r = parse_ast_node_full::<RelationalExpr>(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: Lit::new_uint(10),
                lhs: Ident::from("primary_key"),
                opc: RelationalExpr::OP_GT
            }
        );
    }
    #[test]
    fn expr_ge() {
        let expr = lex_insecure(b"primary_key >= 10").unwrap();
        let r = parse_ast_node_full::<RelationalExpr>(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: Lit::new_uint(10),
                lhs: Ident::from("primary_key"),
                opc: RelationalExpr::OP_GE
            }
        );
    }
    #[test]
    fn expr_lt() {
        let expr = lex_insecure(b"primary_key < 10").unwrap();
        let r = parse_ast_node_full::<RelationalExpr>(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: Lit::new_uint(10),
                lhs: Ident::from("primary_key"),
                opc: RelationalExpr::OP_LT
            }
        );
    }
    #[test]
    fn expr_le() {
        let expr = lex_insecure(b"primary_key <= 10").unwrap();
        let r = parse_ast_node_full::<RelationalExpr>(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr::new(
                Ident::from("primary_key"),
                Lit::new_uint(10),
                RelationalExpr::OP_LE
            )
        );
    }
}
mod where_clause {
    use {
        super::*,
        crate::engine::{
            data::lit::Lit,
            ql::{
                ast::parse_ast_node_full,
                dml::{RelationalExpr, WhereClause},
                lex::Ident,
            },
        },
    };
    #[test]
    fn where_single() {
        let tok = lex_insecure(
            br#"
                x = 100
            "#,
        )
        .unwrap();
        let expected = WhereClause::new(dict! {
            Ident::from("x") => RelationalExpr::new(
                Ident::from("x"),
                Lit::new_uint(100),
                RelationalExpr::OP_EQ
            )
        });
        assert_eq!(expected, parse_ast_node_full::<WhereClause>(&tok).unwrap());
    }
    #[test]
    fn where_double() {
        let tok = lex_insecure(
            br#"
                userid = 100 and pass = "password"
            "#,
        )
        .unwrap();
        let expected = WhereClause::new(dict! {
            Ident::from("userid") => RelationalExpr::new(
                Ident::from("userid"),
                Lit::new_uint(100),
                RelationalExpr::OP_EQ
            ),
            Ident::from("pass") => RelationalExpr::new(
                Ident::from("pass"),
                Lit::new_str("password"),
                RelationalExpr::OP_EQ
            )
        });
        assert_eq!(expected, parse_ast_node_full::<WhereClause>(&tok).unwrap());
    }
    #[test]
    fn where_duplicate_condition() {
        let tok = lex_insecure(
            br#"
                userid = 100 and userid > 200
            "#,
        )
        .unwrap();
        assert!(parse_ast_node_full::<WhereClause>(&tok).is_err());
    }
}

mod select_all {
    use {
        super::lex_insecure,
        crate::engine::{
            error::QueryError,
            ql::{ast::parse_ast_node_full_with_space, dml::sel::SelectAllStatement},
        },
    };

    #[test]
    fn select_all_wildcard() {
        let tok = lex_insecure(b"select all * from mymodel limit 100").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<SelectAllStatement>(&tok[2..], "myspace").unwrap(),
            SelectAllStatement::test_new(("myspace", "mymodel").into(), vec![], true, 100)
        );
    }

    #[test]
    fn select_all_multiple_fields() {
        let tok = lex_insecure(b"select all username, password from mymodel limit 100").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<SelectAllStatement>(&tok[2..], "myspace").unwrap(),
            SelectAllStatement::test_new(
                ("myspace", "mymodel").into(),
                into_vec!["username", "password"],
                false,
                100
            )
        );
    }

    #[test]
    fn select_all_missing_limit() {
        let tok = lex_insecure(b"select all * from mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<SelectAllStatement>(&tok[2..], "myspace").unwrap_err(),
            QueryError::QLUnexpectedEndOfStatement
        );
        let tok = lex_insecure(b"select all username, password from mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<SelectAllStatement>(&tok[2..], "myspace").unwrap_err(),
            QueryError::QLUnexpectedEndOfStatement
        );
    }
}

mod truncate {
    use {
        super::lex_insecure,
        crate::engine::{
            core::EntityIDRef,
            ql::{
                ast::{parse_ast_node_full, parse_ast_node_full_with_space},
                dml::trunc::TruncateStmt,
            },
        },
    };

    #[test]
    fn truncate_model() {
        let tok = lex_insecure(b"truncate model myspace.mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full::<TruncateStmt>(&tok[1..]).unwrap(),
            TruncateStmt::Model(EntityIDRef::new("myspace", "mymodel"))
        );
        let tok = lex_insecure(b"truncate model mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<TruncateStmt>(&tok[1..], "otherspace").unwrap(),
            TruncateStmt::Model(EntityIDRef::new("otherspace", "mymodel"))
        );
    }
}

mod insert_dyn_list {
    use crate::engine::{
        data::cell::Datacell,
        ql::{
            ast::parse_ast_node_full,
            dml::ins::InsertStatement,
            lex::{PROTO_PARAM_SYM_LIST_CLOSE, PROTO_PARAM_SYM_LIST_OPEN},
            tests::lex_secure,
        },
    };

    const LIST_CLOSE: char = PROTO_PARAM_SYM_LIST_CLOSE as char;
    const LIST_OPEN: char = PROTO_PARAM_SYM_LIST_OPEN as char;

    #[test]
    fn insert_dyn_list() {
        let query = "insert into twitter.users (?, ?)";
        let params = format!(
            "{query}\x065\nsayan{LIST_OPEN}\x00\x01\x01\x021234\n\x03-1234\n\x041234.5678\n{LIST_OPEN}\x0513\nbinarywithlf\n\x065\nsayan{LIST_CLOSE}{LIST_OPEN}{LIST_CLOSE}{LIST_CLOSE}",
        );
        let x = lex_secure(params.as_bytes(), query.len()).unwrap();
        let r = parse_ast_node_full::<InsertStatement>(&x[1..]).unwrap();
        let e = InsertStatement::new(
            ("twitter", "users").into(),
            into_array_nullable![
                "sayan",
                Datacell::new_list(vec![
                    Datacell::null(),
                    Datacell::new_bool(true),
                    Datacell::new_uint_default(1234),
                    Datacell::new_sint_default(-1234),
                    Datacell::new_float_default(1234.5678),
                    Datacell::new_list(vec![
                        Datacell::new_bin(b"binarywithlf\n".to_vec().into_boxed_slice()),
                        Datacell::new_str("sayan".to_string().into_boxed_str())
                    ]),
                    Datacell::new_list(vec![]),
                ])
            ]
            .to_vec()
            .into(),
        );
        assert_eq!(e, r);
    }
}
