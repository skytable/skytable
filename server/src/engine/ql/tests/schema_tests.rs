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

use {
    super::{super::lex::Ident, lex_insecure, *},
    crate::engine::data::lit::Lit,
};

mod alter_space {
    use {super::*, crate::engine::ql::ddl::alt::AlterSpace};
    #[sky_macros::test]
    fn alter_space_mini() {
        fullparse_verify_substmt("alter model mymodel with {}", |r: AlterSpace| {
            assert_eq!(r, AlterSpace::new(Ident::from("mymodel"), null_dict! {}));
        })
    }
    #[sky_macros::test]
    fn alter_space() {
        fullparse_verify_substmt(
            r#"
            alter model mymodel with {
                max_entry: 1000,
                driver: "ts-0.8"
            }"#,
            |r: AlterSpace| {
                assert_eq!(
                    r,
                    AlterSpace::new(
                        Ident::from("mymodel"),
                        null_dict! {
                            "max_entry" => Lit::new_uint(1000),
                            "driver" => Lit::new_string("ts-0.8".into())
                        }
                    )
                );
            },
        );
    }
}
mod tymeta {
    use super::*;
    use crate::engine::ql::{
        ast::{parse_ast_node_full, State},
        ddl::syn::{DictTypeMeta, DictTypeMetaSplit},
    };
    #[sky_macros::test]
    fn tymeta_mini() {
        let tok = lex_insecure(b"{}").unwrap();
        let tymeta = parse_ast_node_full::<DictTypeMeta>(&tok).unwrap();
        assert_eq!(tymeta, null_dict!());
    }
    #[sky_macros::test]
    #[should_panic]
    fn tymeta_mini_fail() {
        let tok = lex_insecure(b"{,}").unwrap();
        parse_ast_node_full::<DictTypeMeta>(&tok).unwrap();
    }
    #[sky_macros::test]
    fn tymeta() {
        let tok = lex_insecure(br#"{hello: "world", loading: true, size: 100 }"#).unwrap();
        let tymeta = parse_ast_node_full::<DictTypeMeta>(&tok).unwrap();
        assert_eq!(
            tymeta,
            null_dict! {
                "hello" => Lit::new_string("world".into()),
                "loading" => Lit::new_bool(true),
                "size" => Lit::new_uint(100)
            }
        );
    }
    #[sky_macros::test]
    fn tymeta_pro() {
        // list { maxlen: 100, type: string, unique: true }
        //        ^^^^^^^^^^^^^^^^^^ cursor should be at string
        let tok = lex_insecure(br#"{maxlen: 100, type: string, unique: true }"#).unwrap();
        let mut state = State::new_inplace(&tok);
        let tymeta: DictTypeMeta = ASTNode::test_parse_from_state(&mut state).unwrap();
        assert_eq!(state.cursor(), 6);
        assert!(Token![:].eq(state.fw_read()));
        assert!(Token::Ident(Ident::from("string")).eq(state.fw_read()));
        assert!(Token![,].eq(state.fw_read()));
        let tymeta2: DictTypeMetaSplit = ASTNode::test_parse_from_state(&mut state).unwrap();
        assert!(state.exhausted());
        let mut final_ret = tymeta.into_inner();
        final_ret.extend(tymeta2.into_inner());
        assert_eq!(
            final_ret,
            null_dict! {
                "maxlen" => Lit::new_uint(100),
                "unique" => Lit::new_bool(true)
            }
        )
    }
    #[sky_macros::test]
    fn tymeta_pro_max() {
        // list { maxlen: 100, this: { is: "cool" }, type: string, unique: true }
        //        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ cursor should be at string
        let tok =
            lex_insecure(br#"{maxlen: 100, this: { is: "cool" }, type: string, unique: true }"#)
                .unwrap();
        let mut state = State::new_inplace(&tok);
        let tymeta: DictTypeMeta = ASTNode::test_parse_from_state(&mut state).unwrap();
        assert_eq!(state.cursor(), 14);
        assert!(Token![:].eq(state.fw_read()));
        assert!(Token::Ident(Ident::from("string")).eq(state.fw_read()));
        assert!(Token![,].eq(state.fw_read()));
        let tymeta2: DictTypeMetaSplit = ASTNode::test_parse_from_state(&mut state).unwrap();
        assert!(state.exhausted());
        let mut final_ret = tymeta.into_inner();
        final_ret.extend(tymeta2.into_inner());
        assert_eq!(
            final_ret,
            null_dict! {
                "maxlen" => Lit::new_uint(100),
                "unique" => Lit::new_bool(true),
                "this" => null_dict! {
                    "is" => Lit::new_string("cool".into())
                }
            }
        )
    }
}
mod layer {
    use super::*;
    use crate::engine::ql::{ast::parse_ast_node_multiple_full, ddl::syn::LayerSpec};
    #[sky_macros::test]
    fn layer_mini() {
        let tok = lex_insecure(b"string").unwrap();
        let layers = parse_ast_node_multiple_full::<LayerSpec>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![LayerSpec::new(Ident::from("string"), null_dict! {})]
        );
    }
    #[sky_macros::test]
    fn layer() {
        let tok = lex_insecure(b"string { maxlen: 100 }").unwrap();
        let layers = parse_ast_node_multiple_full::<LayerSpec>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![LayerSpec::new(
                Ident::from("string"),
                null_dict! {
                    "maxlen" => Lit::new_uint(100)
                }
            )]
        );
    }
    #[sky_macros::test]
    fn layer_plus() {
        let tok = lex_insecure(b"list { type: string }").unwrap();
        let layers = parse_ast_node_multiple_full::<LayerSpec>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![
                LayerSpec::new(Ident::from("string"), null_dict! {}),
                LayerSpec::new(Ident::from("list"), null_dict! {})
            ]
        );
    }
    #[sky_macros::test]
    fn layer_pro() {
        let tok = lex_insecure(b"list { unique: true, type: string, maxlen: 10 }").unwrap();
        let layers = parse_ast_node_multiple_full::<LayerSpec>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![
                LayerSpec::new(Ident::from("string"), null_dict! {}),
                LayerSpec::new(
                    Ident::from("list"),
                    null_dict! {
                        "unique" => Lit::new_bool(true),
                        "maxlen" => Lit::new_uint(10),
                    }
                )
            ]
        );
    }
    #[sky_macros::test]
    fn layer_pro_max() {
        let tok = lex_insecure(
            b"list { unique: true, type: string { ascii_only: true, maxlen: 255 }, maxlen: 10 }",
        )
        .unwrap();
        let layers = parse_ast_node_multiple_full::<LayerSpec>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![
                LayerSpec::new(
                    Ident::from("string"),
                    null_dict! {
                        "ascii_only" => Lit::new_bool(true),
                        "maxlen" => Lit::new_uint(255)
                    }
                ),
                LayerSpec::new(
                    Ident::from("list"),
                    null_dict! {
                        "unique" => Lit::new_bool(true),
                        "maxlen" => Lit::new_uint(10),
                    }
                )
            ]
        );
    }

    #[sky_macros::non_miri_test] // FIXME(@ohsayan): massive slowdown with miri..not sure why
    fn fuzz_layer() {
        let tok = b"
            list {
                type: list {
                    maxlen: 100,
                    type: string\x01
                },
                unique: true\x01
            }
        ";
        let expected = vec![
            LayerSpec::new(Ident::from("string"), null_dict!()),
            LayerSpec::new(
                Ident::from("list"),
                null_dict! {
                    "maxlen" => Lit::new_uint(100),
                },
            ),
            LayerSpec::new(
                Ident::from("list"),
                null_dict!("unique" => Lit::new_bool(true)),
            ),
        ];
        fuzz_tokens(tok.as_slice(), |should_pass, new_tok| {
            let layers = parse_ast_node_multiple_full::<LayerSpec>(&new_tok);
            let ok = layers.is_ok();
            if should_pass {
                assert_eq!(layers.unwrap(), expected);
            }
            ok
        });
    }
}
mod fields {
    use {
        super::*,
        crate::engine::ql::{
            ast::parse_ast_node_full,
            ddl::syn::{FieldSpec, LayerSpec},
        },
    };
    #[sky_macros::test]
    fn field_mini() {
        let tok = lex_insecure(b"username: string").unwrap();
        let f = parse_ast_node_full::<FieldSpec>(&tok).unwrap();
        assert_eq!(
            f,
            FieldSpec::new(
                Ident::from("username"),
                [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                false,
                false
            )
        )
    }
    #[sky_macros::test]
    fn field() {
        let tok = lex_insecure(b"primary username: string").unwrap();
        let f = parse_ast_node_full::<FieldSpec>(&tok).unwrap();
        assert_eq!(
            f,
            FieldSpec::new(
                Ident::from("username"),
                [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                false,
                true
            )
        )
    }
    #[sky_macros::test]
    fn field_pro() {
        let tok = lex_insecure(
            b"
                primary username: string {
                    maxlen: 10,
                    ascii_only: true,
                }
            ",
        )
        .unwrap();
        let f = parse_ast_node_full::<FieldSpec>(&tok).unwrap();
        assert_eq!(
            f,
            FieldSpec::new(
                Ident::from("username"),
                [LayerSpec::new(
                    Ident::from("string"),
                    null_dict! {
                        "maxlen" => Lit::new_uint(10),
                        "ascii_only" => Lit::new_bool(true),
                    }
                )]
                .into(),
                false,
                true,
            )
        )
    }
    #[sky_macros::test]
    fn field_pro_max() {
        let tok = lex_insecure(
            b"
                null notes: list {
                    type: string {
                        maxlen: 255,
                        ascii_only: true,
                    },
                    unique: true,
                }
            ",
        )
        .unwrap();
        let f = parse_ast_node_full::<FieldSpec>(&tok).unwrap();
        assert_eq!(
            f,
            FieldSpec::new(
                Ident::from("notes"),
                [
                    LayerSpec::new(
                        Ident::from("string"),
                        null_dict! {
                            "maxlen" => Lit::new_uint(255),
                            "ascii_only" => Lit::new_bool(true),
                        }
                    ),
                    LayerSpec::new(
                        Ident::from("list"),
                        null_dict! {
                            "unique" => Lit::new_bool(true)
                        }
                    ),
                ]
                .into(),
                true,
                false,
            )
        )
    }
}
mod schemas {
    use super::*;
    use crate::engine::ql::ddl::{
        crt::CreateModel,
        syn::{FieldSpec, LayerSpec},
    };
    #[sky_macros::test]
    fn schema_mini() {
        let mut ret = CreateModel::new(
            ("apps", "mymodel").into(),
            vec![
                FieldSpec::new(
                    Ident::from("username"),
                    vec![LayerSpec::new(Ident::from("string"), null_dict! {})],
                    false,
                    true,
                ),
                FieldSpec::new(
                    Ident::from("password"),
                    vec![LayerSpec::new(Ident::from("binary"), null_dict! {})],
                    false,
                    false,
                ),
            ],
            null_dict! {},
            false,
        );
        fullparse_verify_substmt_with_space(
            "create model mymodel(
                primary username: string,
                password: binary
            )",
            "apps",
            |r: CreateModel| assert_eq!(r, ret),
        );
        ret.if_not_exists = true;
        fullparse_verify_substmt_with_space(
            "create model if not exists mymodel(
                primary username: string,
                password: binary
            )",
            "apps",
            |r: CreateModel| assert_eq!(r, ret),
        );
    }
    #[sky_macros::test]
    fn schema() {
        let mut ret = CreateModel::new(
            ("apps", "mymodel").into(),
            vec![
                FieldSpec::new(
                    Ident::from("username"),
                    vec![LayerSpec::new(Ident::from("string"), null_dict! {})],
                    false,
                    true,
                ),
                FieldSpec::new(
                    Ident::from("password"),
                    vec![LayerSpec::new(Ident::from("binary"), null_dict! {})],
                    false,
                    false,
                ),
                FieldSpec::new(
                    Ident::from("profile_pic"),
                    vec![LayerSpec::new(Ident::from("binary"), null_dict! {})],
                    true,
                    false,
                ),
            ],
            null_dict! {},
            false,
        );
        fullparse_verify_substmt_with_space(
            "create model mymodel(
            primary username: string,
            password: binary,
            null profile_pic: binary
        )",
            "apps",
            |r: CreateModel| assert_eq!(r, ret),
        );
        ret.if_not_exists = true;
        fullparse_verify_substmt_with_space(
            "create model if not exists mymodel(
            primary username: string,
            password: binary,
            null profile_pic: binary
        )",
            "apps",
            |r: CreateModel| assert_eq!(r, ret),
        );
    }

    #[sky_macros::test]
    fn schema_pro() {
        let mut ret = CreateModel::new(
            ("apps", "mymodel").into(),
            vec![
                FieldSpec::new(
                    Ident::from("username"),
                    vec![LayerSpec::new(Ident::from("string"), null_dict! {})],
                    false,
                    true,
                ),
                FieldSpec::new(
                    Ident::from("password"),
                    vec![LayerSpec::new(Ident::from("binary"), null_dict! {})],
                    false,
                    false,
                ),
                FieldSpec::new(
                    Ident::from("profile_pic"),
                    vec![LayerSpec::new(Ident::from("binary"), null_dict! {})],
                    true,
                    false,
                ),
                FieldSpec::new(
                    Ident::from("notes"),
                    vec![
                        LayerSpec::new(Ident::from("string"), null_dict! {}),
                        LayerSpec::new(
                            Ident::from("list"),
                            null_dict! {
                                "unique" => Lit::new_bool(true)
                            },
                        ),
                    ],
                    true,
                    false,
                ),
            ],
            null_dict! {},
            false,
        );
        ret.if_not_exists = true;
        fullparse_verify_substmt_with_space(
            "
        create model if not exists mymodel(
            primary username: string,
            password: binary,
            null profile_pic: binary,
            null notes: list {
                type: string,
                unique: true,
            }
        )
        ",
            "apps",
            |r: CreateModel| assert_eq!(ret, r),
        );
    }
    #[sky_macros::test]
    fn schema_pro_max() {
        let mut ret = CreateModel::new(
            ("apps", "mymodel").into(),
            vec![
                FieldSpec::new(
                    Ident::from("username"),
                    vec![LayerSpec::new(Ident::from("string"), null_dict! {})],
                    false,
                    true,
                ),
                FieldSpec::new(
                    Ident::from("password"),
                    vec![LayerSpec::new(Ident::from("binary"), null_dict! {})],
                    false,
                    false,
                ),
                FieldSpec::new(
                    Ident::from("profile_pic"),
                    vec![LayerSpec::new(Ident::from("binary"), null_dict! {})],
                    true,
                    false,
                ),
                FieldSpec::new(
                    Ident::from("notes"),
                    vec![
                        LayerSpec::new(Ident::from("string"), null_dict! {}),
                        LayerSpec::new(
                            Ident::from("list"),
                            null_dict! {
                                "unique" => Lit::new_bool(true)
                            },
                        ),
                    ],
                    true,
                    false,
                ),
            ],
            null_dict! {
                "env" => null_dict! {
                    "free_user_limit" => Lit::new_uint(100),
                },
                "storage_driver" => Lit::new_string("skyheap".into()),
            },
            false,
        );
        ret.if_not_exists = true;
        fullparse_verify_substmt_with_space(
            "
            create model if not exists mymodel(
                primary username: string,
                password: binary,
                null profile_pic: binary,
                null notes: list {
                    type: string,
                    unique: true,
                }
            ) with {
                env: {
                    free_user_limit: 100,
                },
                storage_driver: \"skyheap\"
            }",
            "apps",
            |r: CreateModel| assert_eq!(r, ret),
        );
    }
    #[sky_macros::test]
    fn simple_list_syntax() {
        let simple_list_decls = [
            (
                "[string]",
                vec![
                    LayerSpec::new("string".into(), into_dict!()),
                    LayerSpec::new("list".into(), into_dict!()),
                ],
            ),
            (
                "[[string]]",
                vec![
                    LayerSpec::new("string".into(), into_dict!()),
                    LayerSpec::new("list".into(), into_dict!()),
                    LayerSpec::new("list".into(), into_dict!()),
                ],
            ),
        ];
        for (list_decl, expected) in simple_list_decls {
            let query =
                format!("create model myspace.mymodel(username: string, notes: {list_decl})")
                    .into_bytes();
            let tokens = lex_insecure(&query).unwrap();
            let cm: CreateModel = ast::parse_ast_node_full(&tokens[2..]).unwrap();
            assert_eq!(
                cm,
                CreateModel::new(
                    ("myspace", "mymodel").into(),
                    vec![
                        FieldSpec::new(
                            "username".into(),
                            vec![LayerSpec::new("string".into(), into_dict!())],
                            false,
                            false
                        ),
                        FieldSpec::new("notes".into(), expected, false, false)
                    ],
                    into_dict!(),
                    false,
                )
            );
        }
    }
}

mod dict_field_syntax {
    use super::*;
    use crate::engine::ql::{
        ast::parse_ast_node_full,
        ddl::syn::{ExpandedField, LayerSpec},
    };
    #[sky_macros::test]
    fn field_syn_mini() {
        let tok = lex_insecure(b"username { type: string }").unwrap();
        let ef = parse_ast_node_full::<ExpandedField>(&tok).unwrap();
        assert_eq!(
            ef,
            ExpandedField::new(
                Ident::from("username"),
                vec![LayerSpec::new(Ident::from("string"), null_dict! {})],
                null_dict! {}
            )
        )
    }
    #[sky_macros::test]
    fn field_syn() {
        let tok = lex_insecure(
            b"
                username {
                    nullable: false,
                    type: string,
                }
            ",
        )
        .unwrap();
        let ef = parse_ast_node_full::<ExpandedField>(&tok).unwrap();
        assert_eq!(
            ef,
            ExpandedField::new(
                Ident::from("username"),
                vec![LayerSpec::new(Ident::from("string"), null_dict! {})],
                null_dict! {
                    "nullable" => Lit::new_bool(false),
                },
            )
        );
    }
    #[sky_macros::test]
    fn field_syn_pro() {
        let tok = lex_insecure(
            b"
                username {
                    nullable: false,
                    type: string {
                        minlen: 6,
                        maxlen: 255,
                    },
                    jingle_bells: \"snow\"
                }
            ",
        )
        .unwrap();
        let ef = parse_ast_node_full::<ExpandedField>(&tok).unwrap();
        assert_eq!(
            ef,
            ExpandedField::new(
                Ident::from("username"),
                vec![LayerSpec::new(
                    Ident::from("string"),
                    null_dict! {
                        "minlen" => Lit::new_uint(6),
                        "maxlen" => Lit::new_uint(255),
                    }
                )],
                null_dict! {
                    "nullable" => Lit::new_bool(false),
                    "jingle_bells" => Lit::new_string("snow".into()),
                },
            )
        );
    }
    #[sky_macros::test]
    fn field_syn_pro_max() {
        let tok = lex_insecure(
            b"
                notes {
                    nullable: true,
                    type: list {
                        type: string {
                            ascii_only: true,
                        },
                        unique: true,
                    },
                    jingle_bells: \"snow\"
                }
            ",
        )
        .unwrap();
        let ef = parse_ast_node_full::<ExpandedField>(&tok).unwrap();
        assert_eq!(
            ef,
            ExpandedField::new(
                Ident::from("notes"),
                vec![
                    LayerSpec::new(
                        Ident::from("string"),
                        null_dict! {
                            "ascii_only" => Lit::new_bool(true),
                        }
                    ),
                    LayerSpec::new(
                        Ident::from("list"),
                        null_dict! {
                            "unique" => Lit::new_bool(true),
                        }
                    )
                ],
                null_dict! {
                    "nullable" => Lit::new_bool(true),
                    "jingle_bells" => Lit::new_string("snow".into()),
                },
            )
        );
    }
    #[sky_macros::test]
    fn field_syn_simple_list_syntax() {
        let samples = [
            (
                "notes {
                    nullable: true,
                    type: [[string]],
                    jingle_bells: \"snow\"
                }",
                ExpandedField::new(
                    Ident::from("notes"),
                    vec![
                        LayerSpec::new(Ident::from("string"), into_dict!()),
                        LayerSpec::new(Ident::from("list"), into_dict!()),
                        LayerSpec::new(Ident::from("list"), into_dict!()),
                    ],
                    null_dict! {
                        "nullable" => Lit::new_bool(true),
                        "jingle_bells" => Lit::new_string("snow".into()),
                    },
                ),
            ),
            (
                "notes { type: [[string]] }",
                ExpandedField::new(
                    Ident::from("notes"),
                    vec![
                        LayerSpec::new(Ident::from("string"), into_dict!()),
                        LayerSpec::new(Ident::from("list"), into_dict!()),
                        LayerSpec::new(Ident::from("list"), into_dict!()),
                    ],
                    into_dict!(),
                ),
            ),
            (
                "notes { type: [[string]], }",
                ExpandedField::new(
                    Ident::from("notes"),
                    vec![
                        LayerSpec::new(Ident::from("string"), into_dict!()),
                        LayerSpec::new(Ident::from("list"), into_dict!()),
                        LayerSpec::new(Ident::from("list"), into_dict!()),
                    ],
                    into_dict!(),
                ),
            ),
        ];
        for (sample, expected) in samples {
            let tok = lex_insecure(sample.as_bytes()).unwrap();
            let ef = parse_ast_node_full::<ExpandedField>(&tok).unwrap();
            assert_eq!(ef, expected);
        }
    }
}
mod alter_model_remove {
    use super::*;
    use crate::engine::ql::{
        ast::parse_ast_node_full_with_space,
        ddl::alt::{AlterKind, AlterModel},
    };
    #[sky_macros::test]
    fn alter_mini() {
        let tok = lex_insecure(b"alter model mymodel remove myfield").unwrap();
        let remove = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            remove,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Remove(Box::from([Ident::from("myfield")]))
            )
        );
    }
    #[sky_macros::test]
    fn alter_mini_2() {
        let tok = lex_insecure(b"alter model mymodel remove (myfield)").unwrap();
        let remove = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            remove,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Remove(Box::from([Ident::from("myfield")]))
            )
        );
    }
    #[sky_macros::test]
    fn alter() {
        let tok =
            lex_insecure(b"alter model mymodel remove (myfield1, myfield2, myfield3, myfield4)")
                .unwrap();
        let remove = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            remove,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Remove(Box::from([
                    Ident::from("myfield1"),
                    Ident::from("myfield2"),
                    Ident::from("myfield3"),
                    Ident::from("myfield4"),
                ]))
            )
        );
    }
}
mod alter_model_add {
    use super::*;
    use crate::engine::ql::{
        ast::parse_ast_node_full_with_space,
        ddl::{
            alt::{AlterKind, AlterModel},
            syn::{ExpandedField, LayerSpec},
        },
    };
    #[sky_macros::test]
    fn add_mini() {
        let tok = lex_insecure(
            b"
                alter model mymodel add myfield { type: string }
            ",
        )
        .unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap(),
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Add(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {},
                    )]
                    .into()
                )
            )
        );
    }
    #[sky_macros::test]
    fn add() {
        let tok = lex_insecure(
            b"
                alter model mymodel add myfield { type: string, nullable: true }
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Add(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {
                            "nullable" => Lit::new_bool(true)
                        },
                    )]
                    .into()
                )
            )
        );
    }
    #[sky_macros::test]
    fn add_pro() {
        let tok = lex_insecure(
            b"
                alter model mymodel add (myfield { type: string, nullable: true })
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Add(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {
                            "nullable" => Lit::new_bool(true)
                        },
                    )]
                    .into()
                )
            )
        );
    }
    #[sky_macros::test]
    fn add_pro_max() {
        let tok = lex_insecure(
            b"
                alter model mymodel add (
                    myfield {
                        type: string,
                        nullable: true
                    },
                    another {
                        type: list {
                            type: string {
                                maxlen: 255
                            },
                            unique: true
                        },
                        nullable: false,
                    }
                )
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Add(
                    [
                        ExpandedField::new(
                            Ident::from("myfield"),
                            [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {
                                "nullable" => Lit::new_bool(true)
                            },
                        ),
                        ExpandedField::new(
                            Ident::from("another"),
                            [
                                LayerSpec::new(
                                    Ident::from("string"),
                                    null_dict! {
                                        "maxlen" => Lit::new_uint(255)
                                    }
                                ),
                                LayerSpec::new(
                                    Ident::from("list"),
                                    null_dict! {
                                       "unique" => Lit::new_bool(true)
                                    },
                                )
                            ]
                            .into(),
                            null_dict! {
                                "nullable" => Lit::new_bool(false)
                            },
                        )
                    ]
                    .into()
                )
            )
        );
    }
}
mod alter_model_update {
    use super::*;
    use crate::engine::ql::{
        ast::parse_ast_node_full_with_space,
        ddl::{
            alt::{AlterKind, AlterModel},
            syn::{ExpandedField, LayerSpec},
        },
    };

    #[sky_macros::test]
    fn alter_mini() {
        let tok = lex_insecure(
            b"
                alter model mymodel update myfield { type: string }
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Update(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {},
                    )]
                    .into()
                )
            )
        );
    }
    #[sky_macros::test]
    fn alter_mini_2() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (myfield { type: string })
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Update(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {},
                    )]
                    .into()
                )
            )
        );
    }
    #[sky_macros::test]
    fn alter() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (
                    myfield {
                        type: string,
                        nullable: true,
                    }
                )
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Update(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {
                            "nullable" => Lit::new_bool(true)
                        },
                    )]
                    .into()
                )
            )
        );
    }
    #[sky_macros::test]
    fn alter_pro() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (
                    myfield {
                        type: string,
                        nullable: true,
                    },
                    myfield2 {
                        type: string,
                    }
                )
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Update(
                    [
                        ExpandedField::new(
                            Ident::from("myfield"),
                            [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {
                                "nullable" => Lit::new_bool(true)
                            },
                        ),
                        ExpandedField::new(
                            Ident::from("myfield2"),
                            [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {},
                        )
                    ]
                    .into()
                )
            )
        );
    }
    #[sky_macros::test]
    fn alter_pro_max() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (
                    myfield {
                        type: string {},
                        nullable: true,
                    },
                    myfield2 {
                        type: string {
                            maxlen: 255,
                        },
                    }
                )
            ",
        )
        .unwrap();
        let r = parse_ast_node_full_with_space::<AlterModel>(&tok[2..], "apps").unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                ("apps", "mymodel").into(),
                AlterKind::Update(
                    [
                        ExpandedField::new(
                            Ident::from("myfield"),
                            [LayerSpec::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {
                                "nullable" => Lit::new_bool(true)
                            },
                        ),
                        ExpandedField::new(
                            Ident::from("myfield2"),
                            [LayerSpec::new(
                                Ident::from("string"),
                                null_dict! {"maxlen" => Lit::new_uint(255)}
                            )]
                            .into(),
                            null_dict! {},
                        )
                    ]
                    .into()
                )
            )
        );
    }
}

mod ddl_other_query_tests {
    use {
        super::*,
        crate::engine::ql::{
            ast::{parse_ast_node_full, parse_ast_node_full_with_space},
            ddl::drop::{DropModel, DropSpace},
        },
    };
    #[sky_macros::test]
    fn drop_space() {
        let src = lex_insecure(br"drop space myspace").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropSpace>(&src[2..]).unwrap(),
            DropSpace::new(Ident::from("myspace"), false, false)
        );
        let src = lex_insecure(br"drop space if exists myspace").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropSpace>(&src[2..]).unwrap(),
            DropSpace::new(Ident::from("myspace"), false, true)
        );
    }
    #[sky_macros::test]
    fn drop_space_force() {
        let src = lex_insecure(br"drop space allow not empty myspace").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropSpace>(&src[2..]).unwrap(),
            DropSpace::new(Ident::from("myspace"), true, false)
        );
        let src = lex_insecure(br"drop space if exists allow not empty myspace").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropSpace>(&src[2..]).unwrap(),
            DropSpace::new(Ident::from("myspace"), true, true)
        );
    }
    #[sky_macros::test]
    fn drop_model() {
        let src = lex_insecure(br"drop model mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<DropModel>(&src[2..], "apps").unwrap(),
            DropModel::new(("apps", "mymodel").into(), false, false)
        );
        let src = lex_insecure(br"drop model if exists mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<DropModel>(&src[2..], "apps").unwrap(),
            DropModel::new(("apps", "mymodel").into(), false, true)
        );
    }
    #[sky_macros::test]
    fn drop_model_force() {
        let src = lex_insecure(br"drop model allow not empty mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<DropModel>(&src[2..], "apps").unwrap(),
            DropModel::new(("apps", "mymodel").into(), true, false)
        );
        let src = lex_insecure(br"drop model if exists allow not empty mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full_with_space::<DropModel>(&src[2..], "apps").unwrap(),
            DropModel::new(("apps", "mymodel").into(), true, true)
        );
    }
}
