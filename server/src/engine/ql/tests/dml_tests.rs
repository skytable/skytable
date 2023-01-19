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
    use crate::engine::ql::{
        ast::{InplaceData, SubstitutedData},
        dml::ins::parse_list_full,
        lex::LitIR,
    };

    #[test]
    fn list_mini() {
        let tok = lex_insecure(
            b"
                []
            ",
        )
        .unwrap();
        let r = parse_list_full(&tok[1..], InplaceData::new()).unwrap();
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
        let r = parse_list_full(&tok[1..], InplaceData::new()).unwrap();
        assert_eq!(r.as_slice(), into_array![1, 2, 3, 4])
    }
    #[test]
    fn list_param() {
        let tok = lex_secure(
            b"
                [?, ?, ?, ?]
            ",
        )
        .unwrap();
        let data = [
            LitIR::UInt(1),
            LitIR::UInt(2),
            LitIR::UInt(3),
            LitIR::UInt(4),
        ];
        let r = parse_list_full(&tok[1..], SubstitutedData::new(&data)).unwrap();
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
        let r = parse_list_full(&tok[1..], InplaceData::new()).unwrap();
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
    fn list_pro_param() {
        let tok = lex_secure(
            b"
                [
                    [?, ?],
                    [?, ?],
                    [?, ?],
                    []
                ]
            ",
        )
        .unwrap();
        let data = [
            LitIR::UInt(1),
            LitIR::UInt(2),
            LitIR::UInt(3),
            LitIR::UInt(4),
            LitIR::UInt(5),
            LitIR::UInt(6),
        ];
        let r = parse_list_full(&tok[1..], SubstitutedData::new(&data)).unwrap();
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
        let r = parse_list_full(&tok[1..], InplaceData::new()).unwrap();
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
    #[test]
    fn list_pro_max_param() {
        let tok = lex_secure(
            b"
                [
                    [[?, ?], [?, ?]],
                    [[], [?, ?]],
                    [[?, ?], [?, ?]],
                    [[?, ?], []]
                ]
            ",
        )
        .unwrap();
        let data = [
            LitIR::UInt(1),
            LitIR::UInt(1),
            LitIR::UInt(2),
            LitIR::UInt(2),
            LitIR::UInt(4),
            LitIR::UInt(4),
            LitIR::UInt(5),
            LitIR::UInt(5),
            LitIR::UInt(6),
            LitIR::UInt(6),
            LitIR::UInt(7),
            LitIR::UInt(7),
        ];
        let r = parse_list_full(&tok[1..], SubstitutedData::new(&data)).unwrap();
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
    use crate::engine::ql::dml::ins::parse_data_tuple_syntax_full;

    #[test]
    fn tuple_mini() {
        let tok = lex_insecure(b"()").unwrap();
        let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
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
        let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
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
        let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
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
        let r = parse_data_tuple_syntax_full(&tok[1..]).unwrap();
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
    use crate::engine::ql::dml::ins::parse_data_map_syntax_full;

    #[test]
    fn map_mini() {
        let tok = lex_insecure(b"{}").unwrap();
        let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
        assert_eq!(r, nullable_dict! {})
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
        let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
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
        let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
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
        let r = parse_data_map_syntax_full(&tok[1..]).unwrap();
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
            ast::Entity,
            dml::{self, ins::InsertStatement},
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
        let r = dml::ins::parse_insert_full(&x[1..]).unwrap();
        let e = InsertStatement::new(
            Entity::Full(b"twitter", b"users"),
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
        let r = dml::ins::parse_insert_full(&x[1..]).unwrap();
        let e = InsertStatement::new(
            Entity::Full(b"twitter", b"users"),
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
        let r = dml::ins::parse_insert_full(&x[1..]).unwrap();
        let e = InsertStatement::new(
            Entity::Full(b"twitter", b"users"),
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
        let r = dml::ins::parse_insert_full(&tok[1..]).unwrap();
        let e = InsertStatement::new(
            Entity::Full(b"jotsy", b"app"),
            dict_nullable! {
                "username".as_bytes() => "sayan"
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
        let r = dml::ins::parse_insert_full(&tok[1..]).unwrap();
        let e = InsertStatement::new(
            Entity::Full(b"jotsy", b"app"),
            dict_nullable! {
                "username".as_bytes() => "sayan",
                "name".as_bytes() => "Sayan",
                "email".as_bytes() => "sayan@example.com",
                "verified".as_bytes() => true,
                "following".as_bytes() => 12345,
                "followers".as_bytes() => 67890
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
        let r = dml::ins::parse_insert_full(&tok[1..]).unwrap();
        let e = InsertStatement::new(
            Entity::Full(b"jotsy", b"app"),
            dict_nullable! {
                "username".as_bytes() => "sayan",
                "password".as_bytes() => "pass123",
                "email".as_bytes() => "sayan@example.com",
                "verified".as_bytes() => true,
                "following".as_bytes() => 12345,
                "followers".as_bytes() => 67890,
                "linked_smart_devices".as_bytes() => Null,
                "bookmarks".as_bytes() => 12345,
                "other_linked_accounts".as_bytes() => Null
            }
            .into(),
        );
        assert_eq!(r, e);
    }
    #[test]
    fn insert_tuple_fnsub() {
        let tok =
            lex_insecure(br#"insert into jotsy.app(@uuidstr(), "sayan", @timesec())"#).unwrap();
        let ret = dml::ins::parse_insert_full(&tok[1..]).unwrap();
        let expected = InsertStatement::new(
            Entity::Full(b"jotsy", b"app"),
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
        let ret = dml::ins::parse_insert_full(&tok[1..]).unwrap();
        let expected = InsertStatement::new(
            Entity::Full(b"jotsy", b"app"),
            dict_nullable! {
                "uuid".as_bytes() => dml::ins::T_UUIDSTR,
                "username".as_bytes() => "sayan",
                "signup_time".as_bytes() => dml::ins::T_TIMESEC,
            }
            .into(),
        );
        assert_eq!(ret, expected);
    }
}

mod stmt_select {
    use crate::engine::ql::dml::RelationalExpr;

    use {
        super::*,
        crate::engine::ql::{
            ast::Entity,
            dml::{self, sel::SelectStatement},
            lex::LitIR,
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
        let r = dml::sel::parse_select_full(&tok[1..]).unwrap();
        let e = SelectStatement::new_test(
            Entity::Single(b"users"),
            [].to_vec(),
            true,
            dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(), LitIR::Str("sayan"), RelationalExpr::OP_EQ
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
        let r = dml::sel::parse_select_full(&tok[1..]).unwrap();
        let e = SelectStatement::new_test(
            Entity::Single(b"users"),
            [b"field1".as_slice()].to_vec(),
            false,
            dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(), LitIR::Str("sayan"), RelationalExpr::OP_EQ
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
        let r = dml::sel::parse_select_full(&tok[1..]).unwrap();
        let e = SelectStatement::new_test(
            Entity::Full(b"twitter", b"users"),
            [b"field1".as_slice()].to_vec(),
            false,
            dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(), LitIR::Str("sayan"), RelationalExpr::OP_EQ
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
        let r = dml::sel::parse_select_full(&tok[1..]).unwrap();
        let e = SelectStatement::new_test(
            Entity::Full(b"twitter", b"users"),
            [b"field1".as_slice(), b"field2".as_slice()].to_vec(),
            false,
            dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(), LitIR::Str("sayan"), RelationalExpr::OP_EQ
                ),
            },
        );
        assert_eq!(r, e);
    }
}
mod expression_tests {
    use {
        super::*,
        crate::engine::ql::{
            dml::{
                self,
                upd::{AssignmentExpression, Operator},
            },
            lex::LitIR,
        },
    };
    #[test]
    fn expr_assign() {
        let src = lex_insecure(b"username = 'sayan'").unwrap();
        let r = dml::upd::parse_assn_expression_full(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(b"username", LitIR::Str("sayan"), Operator::Assign)
        );
    }
    #[test]
    fn expr_add_assign() {
        let src = lex_insecure(b"followers += 100").unwrap();
        let r = dml::upd::parse_assn_expression_full(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(b"followers", LitIR::UInt(100), Operator::AddAssign)
        );
    }
    #[test]
    fn expr_sub_assign() {
        let src = lex_insecure(b"following -= 150").unwrap();
        let r = dml::upd::parse_assn_expression_full(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(b"following", LitIR::UInt(150), Operator::SubAssign)
        );
    }
    #[test]
    fn expr_mul_assign() {
        let src = lex_insecure(b"product_qty *= 2").unwrap();
        let r = dml::upd::parse_assn_expression_full(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(b"product_qty", LitIR::UInt(2), Operator::MulAssign)
        );
    }
    #[test]
    fn expr_div_assign() {
        let src = lex_insecure(b"image_crop_factor /= 2").unwrap();
        let r = dml::upd::parse_assn_expression_full(&src).unwrap();
        assert_eq!(
            r,
            AssignmentExpression::new(b"image_crop_factor", LitIR::UInt(2), Operator::DivAssign)
        );
    }
}
mod update_statement {
    use {
        super::*,
        crate::engine::ql::{
            ast::Entity,
            dml::{
                self,
                upd::{AssignmentExpression, Operator, UpdateStatement},
                RelationalExpr, WhereClause,
            },
            lex::LitIR,
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
        let r = dml::upd::parse_update_full(&tok[1..]).unwrap();
        let e = UpdateStatement::new(
            Entity::Single(b"app"),
            vec![AssignmentExpression::new(
                b"notes",
                LitIR::Str("this is my new note"),
                Operator::AddAssign,
            )],
            WhereClause::new(dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(),
                    LitIR::Str("sayan"),
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
        let r = dml::upd::parse_update_full(&tok[1..]).unwrap();
        let e = UpdateStatement::new(
            Entity::Full(b"jotsy", b"app"),
            vec![
                AssignmentExpression::new(
                    b"notes",
                    LitIR::Str("this is my new note"),
                    Operator::AddAssign,
                ),
                AssignmentExpression::new(
                    b"email",
                    LitIR::Str("sayan@example.com"),
                    Operator::Assign,
                ),
            ],
            WhereClause::new(dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(),
                    LitIR::Str("sayan"),
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
        crate::engine::ql::{
            ast::Entity,
            dml::{
                del::{self, DeleteStatement},
                RelationalExpr,
            },
            lex::LitIR,
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
            Entity::Single(b"users"),
            dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(),
                    LitIR::Str("sayan"),
                    RelationalExpr::OP_EQ
                )
            },
        );
        let r = del::parse_delete_full(&tok[1..]).unwrap();
        assert_eq!(r, e);
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
            Entity::Full(b"twitter", b"users"),
            dict! {
                "username".as_bytes() => RelationalExpr::new(
                    "username".as_bytes(),
                    LitIR::Str("sayan"),
                    RelationalExpr::OP_EQ
                )
            },
        );
        let r = del::parse_delete_full(&tok[1..]).unwrap();
        assert_eq!(r, e);
    }
}
mod relational_expr {
    use {
        super::*,
        crate::engine::ql::{
            dml::{self, RelationalExpr},
            lex::LitIR,
        },
    };

    #[test]
    fn expr_eq() {
        let expr = lex_insecure(b"primary_key = 10").unwrap();
        let r = dml::parse_relexpr_full(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: LitIR::UInt(10),
                lhs: "primary_key".as_bytes(),
                opc: RelationalExpr::OP_EQ
            }
        );
    }
    #[test]
    fn expr_ne() {
        let expr = lex_insecure(b"primary_key != 10").unwrap();
        let r = dml::parse_relexpr_full(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: LitIR::UInt(10),
                lhs: "primary_key".as_bytes(),
                opc: RelationalExpr::OP_NE
            }
        );
    }
    #[test]
    fn expr_gt() {
        let expr = lex_insecure(b"primary_key > 10").unwrap();
        let r = dml::parse_relexpr_full(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: LitIR::UInt(10),
                lhs: "primary_key".as_bytes(),
                opc: RelationalExpr::OP_GT
            }
        );
    }
    #[test]
    fn expr_ge() {
        let expr = lex_insecure(b"primary_key >= 10").unwrap();
        let r = dml::parse_relexpr_full(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: LitIR::UInt(10),
                lhs: "primary_key".as_bytes(),
                opc: RelationalExpr::OP_GE
            }
        );
    }
    #[test]
    fn expr_lt() {
        let expr = lex_insecure(b"primary_key < 10").unwrap();
        let r = dml::parse_relexpr_full(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr {
                rhs: LitIR::UInt(10),
                lhs: "primary_key".as_bytes(),
                opc: RelationalExpr::OP_LT
            }
        );
    }
    #[test]
    fn expr_le() {
        let expr = lex_insecure(b"primary_key <= 10").unwrap();
        let r = dml::parse_relexpr_full(&expr).unwrap();
        assert_eq!(
            r,
            RelationalExpr::new(
                "primary_key".as_bytes(),
                LitIR::UInt(10),
                RelationalExpr::OP_LE
            )
        );
    }
}
mod where_clause {
    use {
        super::*,
        crate::engine::ql::{
            dml::{self, RelationalExpr, WhereClause},
            lex::LitIR,
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
            "x".as_bytes() => RelationalExpr::new(
                "x".as_bytes(),
                LitIR::UInt(100),
                RelationalExpr::OP_EQ
            )
        });
        assert_eq!(expected, dml::parse_where_clause_full(&tok).unwrap());
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
            "userid".as_bytes() => RelationalExpr::new(
                "userid".as_bytes(),
                LitIR::UInt(100),
                RelationalExpr::OP_EQ
            ),
            "pass".as_bytes() => RelationalExpr::new(
                "pass".as_bytes(),
                LitIR::Str("password"),
                RelationalExpr::OP_EQ
            )
        });
        assert_eq!(expected, dml::parse_where_clause_full(&tok).unwrap());
    }
    #[test]
    fn where_duplicate_condition() {
        let tok = lex_insecure(
            br#"
                userid = 100 and userid > 200
            "#,
        )
        .unwrap();
        assert!(dml::parse_where_clause_full(&tok).is_none());
    }
}
