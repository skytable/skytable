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
    crate::engine::data::{lit::Lit, spec::Dataspec1D},
};
mod inspect {
    use {
        super::*,
        crate::engine::ql::{
            ast::{parse_ast_node_full, Entity, Statement},
            ddl::ins::InspectStatementAST,
        },
    };
    #[test]
    fn inspect_space() {
        let tok = lex_insecure(b"inspect space myspace").unwrap();
        assert_eq!(
            parse_ast_node_full::<InspectStatementAST>(&tok[1..]).unwrap(),
            Statement::InspectSpace(Ident::from("myspace"))
        );
    }
    #[test]
    fn inspect_model() {
        let tok = lex_insecure(b"inspect model users").unwrap();
        assert_eq!(
            parse_ast_node_full::<InspectStatementAST>(&tok[1..]).unwrap(),
            Statement::InspectModel(Entity::Single(Ident::from("users")))
        );
        let tok = lex_insecure(b"inspect model tweeter.users").unwrap();
        assert_eq!(
            parse_ast_node_full::<InspectStatementAST>(&tok[1..]).unwrap(),
            Statement::InspectModel(Entity::Full(Ident::from("tweeter"), Ident::from("users")))
        );
    }
    #[test]
    fn inspect_spaces() {
        let tok = lex_insecure(b"inspect spaces").unwrap();
        assert_eq!(
            parse_ast_node_full::<InspectStatementAST>(&tok[1..]).unwrap(),
            Statement::InspectSpaces
        );
    }
}

mod alter_space {
    use {
        super::*,
        crate::engine::{
            data::{lit::Lit, spec::Dataspec1D},
            ql::{ast::parse_ast_node_full, ddl::alt::AlterSpace},
        },
    };
    #[test]
    fn alter_space_mini() {
        let tok = lex_insecure(b"alter model mymodel with {}").unwrap();
        let r = parse_ast_node_full::<AlterSpace>(&tok[2..]).unwrap();
        assert_eq!(r, AlterSpace::new(Ident::from("mymodel"), null_dict! {}));
    }
    #[test]
    fn alter_space() {
        let tok = lex_insecure(
            br#"
                alter model mymodel with {
                    max_entry: 1000,
                    driver: "ts-0.8"
                }
            "#,
        )
        .unwrap();
        let r = parse_ast_node_full::<AlterSpace>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterSpace::new(
                Ident::from("mymodel"),
                null_dict! {
                    "max_entry" => Lit::UnsignedInt(1000),
                    "driver" => Lit::Str("ts-0.8".into())
                }
            )
        );
    }
}
mod tymeta {
    use super::*;
    use crate::engine::ql::{
        ast::{parse_ast_node_full, traits::ASTNode, State},
        ddl::syn::{DictTypeMeta, DictTypeMetaSplit},
    };
    #[test]
    fn tymeta_mini() {
        let tok = lex_insecure(b"{}").unwrap();
        let tymeta = parse_ast_node_full::<DictTypeMeta>(&tok).unwrap();
        assert_eq!(tymeta, null_dict!());
    }
    #[test]
    #[should_panic]
    fn tymeta_mini_fail() {
        let tok = lex_insecure(b"{,}").unwrap();
        parse_ast_node_full::<DictTypeMeta>(&tok).unwrap();
    }
    #[test]
    fn tymeta() {
        let tok = lex_insecure(br#"{hello: "world", loading: true, size: 100 }"#).unwrap();
        let tymeta = parse_ast_node_full::<DictTypeMeta>(&tok).unwrap();
        assert_eq!(
            tymeta,
            null_dict! {
                "hello" => Lit::Str("world".into()),
                "loading" => Lit::Bool(true),
                "size" => Lit::UnsignedInt(100)
            }
        );
    }
    #[test]
    fn tymeta_pro() {
        // list { maxlen: 100, type: string, unique: true }
        //        ^^^^^^^^^^^^^^^^^^ cursor should be at string
        let tok = lex_insecure(br#"{maxlen: 100, type: string, unique: true }"#).unwrap();
        let mut state = State::new_inplace(&tok);
        let tymeta: DictTypeMeta = ASTNode::from_state(&mut state).unwrap();
        assert_eq!(state.cursor(), 6);
        assert!(Token![:].eq(state.fw_read()));
        assert!(Token::Ident(Ident::from("string")).eq(state.fw_read()));
        assert!(Token![,].eq(state.fw_read()));
        let tymeta2: DictTypeMetaSplit = ASTNode::from_state(&mut state).unwrap();
        assert!(state.exhausted());
        let mut final_ret = tymeta.into_inner();
        final_ret.extend(tymeta2.into_inner());
        assert_eq!(
            final_ret,
            null_dict! {
                "maxlen" => Lit::UnsignedInt(100),
                "unique" => Lit::Bool(true)
            }
        )
    }
    #[test]
    fn tymeta_pro_max() {
        // list { maxlen: 100, this: { is: "cool" }, type: string, unique: true }
        //        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ cursor should be at string
        let tok =
            lex_insecure(br#"{maxlen: 100, this: { is: "cool" }, type: string, unique: true }"#)
                .unwrap();
        let mut state = State::new_inplace(&tok);
        let tymeta: DictTypeMeta = ASTNode::from_state(&mut state).unwrap();
        assert_eq!(state.cursor(), 14);
        assert!(Token![:].eq(state.fw_read()));
        assert!(Token::Ident(Ident::from("string")).eq(state.fw_read()));
        assert!(Token![,].eq(state.fw_read()));
        let tymeta2: DictTypeMetaSplit = ASTNode::from_state(&mut state).unwrap();
        assert!(state.exhausted());
        let mut final_ret = tymeta.into_inner();
        final_ret.extend(tymeta2.into_inner());
        assert_eq!(
            final_ret,
            null_dict! {
                "maxlen" => Lit::UnsignedInt(100),
                "unique" => Lit::Bool(true),
                "this" => null_dict! {
                    "is" => Lit::Str("cool".into())
                }
            }
        )
    }
}
mod layer {
    use super::*;
    use crate::engine::ql::{ast::parse_ast_node_multiple_full, ddl::syn::Layer};
    #[test]
    fn layer_mini() {
        let tok = lex_insecure(b"string").unwrap();
        let layers = parse_ast_node_multiple_full::<Layer>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![Layer::new(Ident::from("string"), null_dict! {})]
        );
    }
    #[test]
    fn layer() {
        let tok = lex_insecure(b"string { maxlen: 100 }").unwrap();
        let layers = parse_ast_node_multiple_full::<Layer>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![Layer::new(
                Ident::from("string"),
                null_dict! {
                    "maxlen" => Lit::UnsignedInt(100)
                }
            )]
        );
    }
    #[test]
    fn layer_plus() {
        let tok = lex_insecure(b"list { type: string }").unwrap();
        let layers = parse_ast_node_multiple_full::<Layer>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![
                Layer::new(Ident::from("string"), null_dict! {}),
                Layer::new(Ident::from("list"), null_dict! {})
            ]
        );
    }
    #[test]
    fn layer_pro() {
        let tok = lex_insecure(b"list { unique: true, type: string, maxlen: 10 }").unwrap();
        let layers = parse_ast_node_multiple_full::<Layer>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![
                Layer::new(Ident::from("string"), null_dict! {}),
                Layer::new(
                    Ident::from("list"),
                    null_dict! {
                        "unique" => Lit::Bool(true),
                        "maxlen" => Lit::UnsignedInt(10),
                    }
                )
            ]
        );
    }
    #[test]
    fn layer_pro_max() {
        let tok = lex_insecure(
            b"list { unique: true, type: string { ascii_only: true, maxlen: 255 }, maxlen: 10 }",
        )
        .unwrap();
        let layers = parse_ast_node_multiple_full::<Layer>(&tok).unwrap();
        assert_eq!(
            layers,
            vec![
                Layer::new(
                    Ident::from("string"),
                    null_dict! {
                        "ascii_only" => Lit::Bool(true),
                        "maxlen" => Lit::UnsignedInt(255)
                    }
                ),
                Layer::new(
                    Ident::from("list"),
                    null_dict! {
                        "unique" => Lit::Bool(true),
                        "maxlen" => Lit::UnsignedInt(10),
                    }
                )
            ]
        );
    }

    #[test]
    #[cfg(not(miri))]
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
            Layer::new(Ident::from("string"), null_dict!()),
            Layer::new(
                Ident::from("list"),
                null_dict! {
                    "maxlen" => Lit::UnsignedInt(100),
                },
            ),
            Layer::new(Ident::from("list"), null_dict!("unique" => Lit::Bool(true))),
        ];
        fuzz_tokens(tok.as_slice(), |should_pass, new_tok| {
            let layers = parse_ast_node_multiple_full::<Layer>(&new_tok);
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
            ddl::syn::{Field, Layer},
            lex::Ident,
        },
    };
    #[test]
    fn field_mini() {
        let tok = lex_insecure(b"username: string").unwrap();
        let f = parse_ast_node_full::<Field>(&tok).unwrap();
        assert_eq!(
            f,
            Field::new(
                Ident::from("username"),
                [Layer::new(Ident::from("string"), null_dict! {})].into(),
                false,
                false
            )
        )
    }
    #[test]
    fn field() {
        let tok = lex_insecure(b"primary username: string").unwrap();
        let f = parse_ast_node_full::<Field>(&tok).unwrap();
        assert_eq!(
            f,
            Field::new(
                Ident::from("username"),
                [Layer::new(Ident::from("string"), null_dict! {})].into(),
                false,
                true
            )
        )
    }
    #[test]
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
        let f = parse_ast_node_full::<Field>(&tok).unwrap();
        assert_eq!(
            f,
            Field::new(
                Ident::from("username"),
                [Layer::new(
                    Ident::from("string"),
                    null_dict! {
                        "maxlen" => Lit::UnsignedInt(10),
                        "ascii_only" => Lit::Bool(true),
                    }
                )]
                .into(),
                false,
                true,
            )
        )
    }
    #[test]
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
        let f = parse_ast_node_full::<Field>(&tok).unwrap();
        assert_eq!(
            f,
            Field::new(
                Ident::from("notes"),
                [
                    Layer::new(
                        Ident::from("string"),
                        null_dict! {
                            "maxlen" => Lit::UnsignedInt(255),
                            "ascii_only" => Lit::Bool(true),
                        }
                    ),
                    Layer::new(
                        Ident::from("list"),
                        null_dict! {
                            "unique" => Lit::Bool(true)
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
    use crate::engine::ql::{
        ast::parse_ast_node_full,
        ddl::{
            crt::CreateModel,
            syn::{Field, Layer},
        },
    };
    #[test]
    fn schema_mini() {
        let tok = lex_insecure(
            b"
                create model mymodel(
                    primary username: string,
                    password: binary
                )
            ",
        )
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let model = parse_ast_node_full::<CreateModel>(tok).unwrap();

        assert_eq!(
            model,
            CreateModel::new(
                Ident::from("mymodel"),
                vec![
                    Field::new(
                        Ident::from("username"),
                        vec![Layer::new(Ident::from("string"), null_dict! {})],
                        false,
                        true,
                    ),
                    Field::new(
                        Ident::from("password"),
                        vec![Layer::new(Ident::from("binary"), null_dict! {})],
                        false,
                        false,
                    )
                ],
                null_dict! {}
            )
        )
    }
    #[test]
    fn schema() {
        let tok = lex_insecure(
            b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary
                )
            ",
        )
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let model = parse_ast_node_full::<CreateModel>(tok).unwrap();

        assert_eq!(
            model,
            CreateModel::new(
                Ident::from("mymodel"),
                vec![
                    Field::new(
                        Ident::from("username"),
                        vec![Layer::new(Ident::from("string"), null_dict! {})],
                        false,
                        true,
                    ),
                    Field::new(
                        Ident::from("password"),
                        vec![Layer::new(Ident::from("binary"), null_dict! {})],
                        false,
                        false,
                    ),
                    Field::new(
                        Ident::from("profile_pic"),
                        vec![Layer::new(Ident::from("binary"), null_dict! {})],
                        true,
                        false,
                    )
                ],
                null_dict! {}
            )
        )
    }

    #[test]
    fn schema_pro() {
        let tok = lex_insecure(
            b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary,
                    null notes: list {
                        type: string,
                        unique: true,
                    }
                )
            ",
        )
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let model = parse_ast_node_full::<CreateModel>(tok).unwrap();

        assert_eq!(
            model,
            CreateModel::new(
                Ident::from("mymodel"),
                vec![
                    Field::new(
                        Ident::from("username"),
                        vec![Layer::new(Ident::from("string"), null_dict! {})],
                        false,
                        true
                    ),
                    Field::new(
                        Ident::from("password"),
                        vec![Layer::new(Ident::from("binary"), null_dict! {})],
                        false,
                        false
                    ),
                    Field::new(
                        Ident::from("profile_pic"),
                        vec![Layer::new(Ident::from("binary"), null_dict! {})],
                        true,
                        false
                    ),
                    Field::new(
                        Ident::from("notes"),
                        vec![
                            Layer::new(Ident::from("string"), null_dict! {}),
                            Layer::new(
                                Ident::from("list"),
                                null_dict! {
                                    "unique" => Lit::Bool(true)
                                }
                            )
                        ],
                        true,
                        false
                    )
                ],
                null_dict! {}
            )
        )
    }

    #[test]
    fn schema_pro_max() {
        let tok = lex_insecure(
            b"
                create model mymodel(
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
                }
            ",
        )
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let model = parse_ast_node_full::<CreateModel>(tok).unwrap();

        assert_eq!(
            model,
            CreateModel::new(
                Ident::from("mymodel"),
                vec![
                    Field::new(
                        Ident::from("username"),
                        vec![Layer::new(Ident::from("string"), null_dict! {})],
                        false,
                        true
                    ),
                    Field::new(
                        Ident::from("password"),
                        vec![Layer::new(Ident::from("binary"), null_dict! {})],
                        false,
                        false
                    ),
                    Field::new(
                        Ident::from("profile_pic"),
                        vec![Layer::new(Ident::from("binary"), null_dict! {})],
                        true,
                        false
                    ),
                    Field::new(
                        Ident::from("notes"),
                        vec![
                            Layer::new(Ident::from("string"), null_dict! {}),
                            Layer::new(
                                Ident::from("list"),
                                null_dict! {
                                    "unique" => Lit::Bool(true)
                                }
                            )
                        ],
                        true,
                        false
                    )
                ],
                null_dict! {
                    "env" => null_dict! {
                        "free_user_limit" => Lit::UnsignedInt(100),
                    },
                    "storage_driver" => Lit::Str("skyheap".into()),
                }
            )
        )
    }
}
mod dict_field_syntax {
    use super::*;
    use crate::engine::ql::{
        ast::parse_ast_node_full,
        ddl::syn::{ExpandedField, Layer},
    };
    #[test]
    fn field_syn_mini() {
        let tok = lex_insecure(b"username { type: string }").unwrap();
        let ef = parse_ast_node_full::<ExpandedField>(&tok).unwrap();
        assert_eq!(
            ef,
            ExpandedField::new(
                Ident::from("username"),
                vec![Layer::new(Ident::from("string"), null_dict! {})],
                null_dict! {}
            )
        )
    }
    #[test]
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
                vec![Layer::new(Ident::from("string"), null_dict! {})],
                null_dict! {
                    "nullable" => Lit::Bool(false),
                },
            )
        );
    }
    #[test]
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
                vec![Layer::new(
                    Ident::from("string"),
                    null_dict! {
                        "minlen" => Lit::UnsignedInt(6),
                        "maxlen" => Lit::UnsignedInt(255),
                    }
                )],
                null_dict! {
                    "nullable" => Lit::Bool(false),
                    "jingle_bells" => Lit::Str("snow".into()),
                },
            )
        );
    }
    #[test]
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
                    Layer::new(
                        Ident::from("string"),
                        null_dict! {
                            "ascii_only" => Lit::Bool(true),
                        }
                    ),
                    Layer::new(
                        Ident::from("list"),
                        null_dict! {
                            "unique" => Lit::Bool(true),
                        }
                    )
                ],
                null_dict! {
                    "nullable" => Lit::Bool(true),
                    "jingle_bells" => Lit::Str("snow".into()),
                },
            )
        );
    }
}
mod alter_model_remove {
    use super::*;
    use crate::engine::ql::{
        ast::parse_ast_node_full,
        ddl::alt::{AlterKind, AlterModel},
        lex::Ident,
    };
    #[test]
    fn alter_mini() {
        let tok = lex_insecure(b"alter model mymodel remove myfield").unwrap();
        let remove = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            remove,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Remove(Box::from([Ident::from("myfield")]))
            )
        );
    }
    #[test]
    fn alter_mini_2() {
        let tok = lex_insecure(b"alter model mymodel remove (myfield)").unwrap();
        let remove = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            remove,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Remove(Box::from([Ident::from("myfield")]))
            )
        );
    }
    #[test]
    fn alter() {
        let tok =
            lex_insecure(b"alter model mymodel remove (myfield1, myfield2, myfield3, myfield4)")
                .unwrap();
        let remove = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            remove,
            AlterModel::new(
                Ident::from("mymodel"),
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
        ast::parse_ast_node_full,
        ddl::{
            alt::{AlterKind, AlterModel},
            syn::{ExpandedField, Layer},
        },
    };
    #[test]
    fn add_mini() {
        let tok = lex_insecure(
            b"
                alter model mymodel add myfield { type: string }
            ",
        )
        .unwrap();
        assert_eq!(
            parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap(),
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Add(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [Layer::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {},
                    )]
                    .into()
                )
            )
        );
    }
    #[test]
    fn add() {
        let tok = lex_insecure(
            b"
                alter model mymodel add myfield { type: string, nullable: true }
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Add(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [Layer::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {
                            "nullable" => Lit::Bool(true)
                        },
                    )]
                    .into()
                )
            )
        );
    }
    #[test]
    fn add_pro() {
        let tok = lex_insecure(
            b"
                alter model mymodel add (myfield { type: string, nullable: true })
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Add(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [Layer::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {
                            "nullable" => Lit::Bool(true)
                        },
                    )]
                    .into()
                )
            )
        );
    }
    #[test]
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
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Add(
                    [
                        ExpandedField::new(
                            Ident::from("myfield"),
                            [Layer::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {
                                "nullable" => Lit::Bool(true)
                            },
                        ),
                        ExpandedField::new(
                            Ident::from("another"),
                            [
                                Layer::new(
                                    Ident::from("string"),
                                    null_dict! {
                                        "maxlen" => Lit::UnsignedInt(255)
                                    }
                                ),
                                Layer::new(
                                    Ident::from("list"),
                                    null_dict! {
                                       "unique" => Lit::Bool(true)
                                    },
                                )
                            ]
                            .into(),
                            null_dict! {
                                "nullable" => Lit::Bool(false)
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
        ast::parse_ast_node_full,
        ddl::{
            alt::{AlterKind, AlterModel},
            syn::{ExpandedField, Layer},
        },
    };

    #[test]
    fn alter_mini() {
        let tok = lex_insecure(
            b"
                alter model mymodel update myfield { type: string }
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Update(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [Layer::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {},
                    )]
                    .into()
                )
            )
        );
    }
    #[test]
    fn alter_mini_2() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (myfield { type: string })
            ",
        )
        .unwrap();
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Update(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [Layer::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {},
                    )]
                    .into()
                )
            )
        );
    }
    #[test]
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
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Update(
                    [ExpandedField::new(
                        Ident::from("myfield"),
                        [Layer::new(Ident::from("string"), null_dict! {})].into(),
                        null_dict! {
                            "nullable" => Lit::Bool(true)
                        },
                    )]
                    .into()
                )
            )
        );
    }
    #[test]
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
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Update(
                    [
                        ExpandedField::new(
                            Ident::from("myfield"),
                            [Layer::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {
                                "nullable" => Lit::Bool(true)
                            },
                        ),
                        ExpandedField::new(
                            Ident::from("myfield2"),
                            [Layer::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {},
                        )
                    ]
                    .into()
                )
            )
        );
    }
    #[test]
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
        let r = parse_ast_node_full::<AlterModel>(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterModel::new(
                Ident::from("mymodel"),
                AlterKind::Update(
                    [
                        ExpandedField::new(
                            Ident::from("myfield"),
                            [Layer::new(Ident::from("string"), null_dict! {})].into(),
                            null_dict! {
                                "nullable" => Lit::Bool(true)
                            },
                        ),
                        ExpandedField::new(
                            Ident::from("myfield2"),
                            [Layer::new(
                                Ident::from("string"),
                                null_dict! {"maxlen" => Lit::UnsignedInt(255)}
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
            ast::{parse_ast_node_full, Entity, Statement},
            ddl::drop::{DropModel, DropSpace, DropStatementAST},
            lex::Ident,
        },
    };
    #[test]
    fn drop_space() {
        let src = lex_insecure(br"drop space myspace").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropStatementAST>(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new(Ident::from("myspace"), false))
        );
    }
    #[test]
    fn drop_space_force() {
        let src = lex_insecure(br"drop space myspace force").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropStatementAST>(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new(Ident::from("myspace"), true))
        );
    }
    #[test]
    fn drop_model() {
        let src = lex_insecure(br"drop model mymodel").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropStatementAST>(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(
                Entity::Single(Ident::from("mymodel")),
                false
            ))
        );
    }
    #[test]
    fn drop_model_force() {
        let src = lex_insecure(br"drop model mymodel force").unwrap();
        assert_eq!(
            parse_ast_node_full::<DropStatementAST>(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(Entity::Single(Ident::from("mymodel")), true))
        );
    }
}
