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

use super::{
    super::{
        lexer::{Lit, Token},
        schema,
    },
    lex_insecure, *,
};
mod inspect {
    use {
        super::*,
        crate::engine::ql::{
            ast::{Entity, Statement},
            ddl,
        },
    };
    #[test]
    fn inspect_space() {
        let tok = lex_insecure(b"inspect space myspace").unwrap();
        assert_eq!(
            ddl::parse_inspect_full(&tok[1..]).unwrap(),
            Statement::InspectSpace(b"myspace")
        );
    }
    #[test]
    fn inspect_model() {
        let tok = lex_insecure(b"inspect model users").unwrap();
        assert_eq!(
            ddl::parse_inspect_full(&tok[1..]).unwrap(),
            Statement::InspectModel(Entity::Single(b"users"))
        );
        let tok = lex_insecure(b"inspect model tweeter.users").unwrap();
        assert_eq!(
            ddl::parse_inspect_full(&tok[1..]).unwrap(),
            Statement::InspectModel(Entity::Full(b"tweeter", b"users"))
        );
    }
    #[test]
    fn inspect_spaces() {
        let tok = lex_insecure(b"inspect spaces").unwrap();
        assert_eq!(
            ddl::parse_inspect_full(&tok[1..]).unwrap(),
            Statement::InspectSpaces
        );
    }
}

mod alter_space {
    use {
        super::*,
        crate::engine::ql::{
            lexer::Lit,
            schema::{self, AlterSpace},
        },
    };
    #[test]
    fn alter_space_mini() {
        let tok = lex_insecure(b"alter model mymodel with {}").unwrap();
        let r = schema::alter_space_full(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterSpace {
                space_name: b"mymodel",
                updated_props: nullable_dict! {}
            }
        );
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
        let r = schema::alter_space_full(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterSpace {
                space_name: b"mymodel",
                updated_props: nullable_dict! {
                    "max_entry" => Lit::UnsignedInt(1000),
                    "driver" => Lit::Str("ts-0.8".into())
                }
            }
        );
    }
}
mod tymeta {
    use super::*;
    #[test]
    fn tymeta_mini() {
        let tok = lex_insecure(b"}").unwrap();
        let (tymeta, okay, cursor, data) = schema::fold_tymeta(&tok);
        assert!(okay);
        assert!(!tymeta.has_more());
        assert_eq!(cursor, 1);
        assert_eq!(data, nullable_dict!());
    }
    #[test]
    fn tymeta_mini_fail() {
        let tok = lex_insecure(b",}").unwrap();
        let (tymeta, okay, cursor, data) = schema::fold_tymeta(&tok);
        assert!(!okay);
        assert!(!tymeta.has_more());
        assert_eq!(cursor, 0);
        assert_eq!(data, nullable_dict!());
    }
    #[test]
    fn tymeta() {
        let tok = lex_insecure(br#"hello: "world", loading: true, size: 100 }"#).unwrap();
        let (tymeta, okay, cursor, data) = schema::fold_tymeta(&tok);
        assert!(okay);
        assert!(!tymeta.has_more());
        assert_eq!(cursor, tok.len());
        assert_eq!(
            data,
            nullable_dict! {
                "hello" => Lit::Str("world".into()),
                "loading" => Lit::Bool(true),
                "size" => Lit::UnsignedInt(100)
            }
        );
    }
    #[test]
    fn tymeta_pro() {
        // list { maxlen: 100, type string, unique: true }
        //        ^^^^^^^^^^^^^^^^^^ cursor should be at string
        let tok = lex_insecure(br#"maxlen: 100, type string, unique: true }"#).unwrap();
        let (tymeta, okay, cursor, data) = schema::fold_tymeta(&tok);
        assert!(okay);
        assert!(tymeta.has_more());
        assert_eq!(cursor, 5);
        let remslice = &tok[cursor + 2..];
        let (tymeta2, okay2, cursor2, data2) = schema::fold_tymeta(remslice);
        assert!(okay2);
        assert!(!tymeta2.has_more());
        assert_eq!(cursor2 + cursor + 2, tok.len());
        let mut final_ret = data;
        final_ret.extend(data2);
        assert_eq!(
            final_ret,
            nullable_dict! {
                "maxlen" => Lit::UnsignedInt(100),
                "unique" => Lit::Bool(true)
            }
        )
    }
    #[test]
    fn tymeta_pro_max() {
        // list { maxlen: 100, this: { is: "cool" }, type string, unique: true }
        //        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ cursor should be at string
        let tok =
            lex_insecure(br#"maxlen: 100, this: { is: "cool" }, type string, unique: true }"#)
                .unwrap();
        let (tymeta, okay, cursor, data) = schema::fold_tymeta(&tok);
        assert!(okay);
        assert!(tymeta.has_more());
        assert_eq!(cursor, 13);
        let remslice = &tok[cursor + 2..];
        let (tymeta2, okay2, cursor2, data2) = schema::fold_tymeta(remslice);
        assert!(okay2);
        assert!(!tymeta2.has_more());
        assert_eq!(cursor2 + cursor + 2, tok.len());
        let mut final_ret = data;
        final_ret.extend(data2);
        assert_eq!(
            final_ret,
            nullable_dict! {
                "maxlen" => Lit::UnsignedInt(100),
                "unique" => Lit::Bool(true),
                "this" => nullable_dict! {
                    "is" => Lit::Str("cool".into())
                }
            }
        )
    }
    #[test]
    fn fuzz_tymeta_normal() {
        // { maxlen: 10, unique: true, users: "sayan" }
        //   ^start
        let tok = lex_insecure(
            b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\x01
                    },
                    users: \"sayan\"\x01
                }
            ",
        )
        .unwrap();
        let expected = nullable_dict! {
            "maxlen" => Lit::UnsignedInt(10),
            "unique" => Lit::Bool(true),
            "auth" => nullable_dict! {
                "maybe" => Lit::Bool(true),
            },
            "users" => Lit::Str("sayan".into())
        };
        fuzz_tokens(&tok, |should_pass, new_src| {
            let (tymeta, okay, cursor, data) = schema::fold_tymeta(&new_src);
            if should_pass {
                assert!(okay, "{:?}", &new_src);
                assert!(!tymeta.has_more());
                assert_eq!(cursor, new_src.len());
                assert_eq!(data, expected);
            } else if okay {
                panic!(
                    "Expected failure but passed for token stream: `{:?}`",
                    new_src
                );
            }
        });
    }
    #[test]
    fn fuzz_tymeta_with_ty() {
        // list { maxlen: 10, unique: true, type string, users: "sayan" }
        //   ^start
        let tok = lex_insecure(
            b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\x01
                    },
                    type string,
                    users: \"sayan\"\x01
                }
            ",
        )
        .unwrap();
        let expected = nullable_dict! {
            "maxlen" => Lit::UnsignedInt(10),
            "unique" => Lit::Bool(true),
            "auth" => nullable_dict! {
                "maybe" => Lit::Bool(true),
            },
        };
        fuzz_tokens(&tok, |should_pass, new_src| {
            let (tymeta, okay, cursor, data) = schema::fold_tymeta(&new_src);
            if should_pass {
                assert!(okay);
                assert!(tymeta.has_more());
                assert!(new_src[cursor] == Token::Ident(b"string"));
                assert_eq!(data, expected);
            } else if okay {
                panic!("Expected failure but passed for token stream: `{:?}`", tok);
            }
        });
    }
}
mod layer {
    use super::*;
    use crate::engine::ql::schema::Layer;
    #[test]
    fn layer_mini() {
        let tok = lex_insecure(b"string)").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len() - 1);
        assert!(okay);
        assert_eq!(
            layers,
            vec![Layer::new_noreset(b"string", nullable_dict! {})]
        );
    }
    #[test]
    fn layer() {
        let tok = lex_insecure(b"string { maxlen: 100 }").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len());
        assert!(okay);
        assert_eq!(
            layers,
            vec![Layer::new_noreset(
                b"string",
                nullable_dict! {
                    "maxlen" => Lit::UnsignedInt(100)
                }
            )]
        );
    }
    #[test]
    fn layer_plus() {
        let tok = lex_insecure(b"list { type string }").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len());
        assert!(okay);
        assert_eq!(
            layers,
            vec![
                Layer::new_noreset(b"string", nullable_dict! {}),
                Layer::new_noreset(b"list", nullable_dict! {})
            ]
        );
    }
    #[test]
    fn layer_pro() {
        let tok = lex_insecure(b"list { unique: true, type string, maxlen: 10 }").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len());
        assert!(okay);
        assert_eq!(
            layers,
            vec![
                Layer::new_noreset(b"string", nullable_dict! {}),
                Layer::new_noreset(
                    b"list",
                    nullable_dict! {
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
            b"list { unique: true, type string { ascii_only: true, maxlen: 255 }, maxlen: 10 }",
        )
        .unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len());
        assert!(okay);
        assert_eq!(
            layers,
            vec![
                Layer::new_noreset(
                    b"string",
                    nullable_dict! {
                        "ascii_only" => Lit::Bool(true),
                        "maxlen" => Lit::UnsignedInt(255)
                    }
                ),
                Layer::new_noreset(
                    b"list",
                    nullable_dict! {
                        "unique" => Lit::Bool(true),
                        "maxlen" => Lit::UnsignedInt(10),
                    }
                )
            ]
        );
    }

    #[test]
    fn fuzz_layer() {
        let tok = lex_insecure(
            b"
            list {
                type list {
                    maxlen: 100,
                    type string\x01
                },
                unique: true\x01
            }
        ",
        )
        .unwrap();
        let expected = vec![
            Layer::new_noreset(b"string", nullable_dict!()),
            Layer::new_noreset(
                b"list",
                nullable_dict! {
                    "maxlen" => Lit::UnsignedInt(100),
                },
            ),
            Layer::new_noreset(b"list", nullable_dict!("unique" => Lit::Bool(true))),
        ];
        fuzz_tokens(&tok, |should_pass, new_tok| {
            let (layers, c, okay) = schema::fold_layers(&new_tok);
            if should_pass {
                assert!(okay);
                assert_eq!(c, new_tok.len());
                assert_eq!(layers, expected);
            } else if okay {
                panic!(
                    "expected failure but passed for token stream: `{:?}`",
                    new_tok
                );
            }
        });
    }
}
mod field_properties {
    use {super::*, crate::engine::ql::schema::FieldProperties};

    #[test]
    fn field_properties_empty() {
        let tok = lex_insecure(b"myfield:").unwrap();
        let (props, c, okay) = schema::parse_field_properties(&tok);
        assert!(okay);
        assert_eq!(c, 0);
        assert_eq!(props, FieldProperties::default());
    }
    #[test]
    fn field_properties_full() {
        let tok = lex_insecure(b"primary null myfield:").unwrap();
        let (props, c, okay) = schema::parse_field_properties(&tok);
        assert_eq!(c, 2);
        assert_eq!(tok[c], Token::Ident(b"myfield"));
        assert!(okay);
        assert_eq!(
            props,
            FieldProperties {
                properties: set!["primary", "null"],
            }
        )
    }
}
mod fields {
    use {
        super::*,
        crate::engine::ql::schema::{Field, Layer},
    };
    #[test]
    fn field_mini() {
        let tok = lex_insecure(
            b"
                username: string,
            ",
        )
        .unwrap();
        let (c, f) = schema::parse_field_full(&tok).unwrap();
        assert_eq!(c, tok.len() - 1);
        assert_eq!(
            f,
            Field {
                field_name: b"username",
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                props: set![],
            }
        )
    }
    #[test]
    fn field() {
        let tok = lex_insecure(
            b"
                primary username: string,    
            ",
        )
        .unwrap();
        let (c, f) = schema::parse_field_full(&tok).unwrap();
        assert_eq!(c, tok.len() - 1);
        assert_eq!(
            f,
            Field {
                field_name: b"username",
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                props: set!["primary"],
            }
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
        let (c, f) = schema::parse_field_full(&tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            f,
            Field {
                field_name: b"username",
                layers: [Layer::new_noreset(
                    b"string",
                    nullable_dict! {
                        "maxlen" => Lit::UnsignedInt(10),
                        "ascii_only" => Lit::Bool(true),
                    }
                )]
                .into(),
                props: set!["primary"],
            }
        )
    }
    #[test]
    fn field_pro_max() {
        let tok = lex_insecure(
            b"
                null notes: list {
                    type string {
                        maxlen: 255,
                        ascii_only: true,
                    },
                    unique: true,
                }
            ",
        )
        .unwrap();
        let (c, f) = schema::parse_field_full(&tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            f,
            Field {
                field_name: b"notes",
                layers: [
                    Layer::new_noreset(
                        b"string",
                        nullable_dict! {
                            "maxlen" => Lit::UnsignedInt(255),
                            "ascii_only" => Lit::Bool(true),
                        }
                    ),
                    Layer::new_noreset(
                        b"list",
                        nullable_dict! {
                            "unique" => Lit::Bool(true)
                        }
                    ),
                ]
                .into(),
                props: set!["null"],
            }
        )
    }
}
mod schemas {
    use crate::engine::ql::schema::{Field, Layer, Model};

    use super::*;
    #[test]
    fn schema_mini() {
        let tok = lex_insecure(
            b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                )
            ",
        )
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let (model, c) = schema::parse_schema_from_tokens_full(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: b"mymodel",
                fields: vec![
                    Field {
                        field_name: b"username",
                        layers: vec![Layer::new_noreset(b"string", nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: b"password",
                        layers: vec![Layer::new_noreset(b"binary", nullable_dict! {})],
                        props: set![]
                    }
                ],
                props: nullable_dict! {}
            }
        )
    }
    #[test]
    fn schema() {
        let tok = lex_insecure(
            b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary,
                )
            ",
        )
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let (model, c) = schema::parse_schema_from_tokens_full(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: b"mymodel",
                fields: vec![
                    Field {
                        field_name: b"username",
                        layers: vec![Layer::new_noreset(b"string", nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: b"password",
                        layers: vec![Layer::new_noreset(b"binary", nullable_dict! {})],
                        props: set![]
                    },
                    Field {
                        field_name: b"profile_pic",
                        layers: vec![Layer::new_noreset(b"binary", nullable_dict! {})],
                        props: set!["null"]
                    }
                ],
                props: nullable_dict! {}
            }
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
                        type string,
                        unique: true,
                    },
                )
            ",
        )
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let (model, c) = schema::parse_schema_from_tokens_full(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: b"mymodel",
                fields: vec![
                    Field {
                        field_name: b"username",
                        layers: vec![Layer::new_noreset(b"string", nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: b"password",
                        layers: vec![Layer::new_noreset(b"binary", nullable_dict! {})],
                        props: set![]
                    },
                    Field {
                        field_name: b"profile_pic",
                        layers: vec![Layer::new_noreset(b"binary", nullable_dict! {})],
                        props: set!["null"]
                    },
                    Field {
                        field_name: b"notes",
                        layers: vec![
                            Layer::new_noreset(b"string", nullable_dict! {}),
                            Layer::new_noreset(
                                b"list",
                                nullable_dict! {
                                    "unique" => Lit::Bool(true)
                                }
                            )
                        ],
                        props: set!["null"]
                    }
                ],
                props: nullable_dict! {}
            }
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
                        type string,
                        unique: true,
                    },
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
        let (model, c) = schema::parse_schema_from_tokens_full(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: b"mymodel",
                fields: vec![
                    Field {
                        field_name: b"username",
                        layers: vec![Layer::new_noreset(b"string", nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: b"password",
                        layers: vec![Layer::new_noreset(b"binary", nullable_dict! {})],
                        props: set![]
                    },
                    Field {
                        field_name: b"profile_pic",
                        layers: vec![Layer::new_noreset(b"binary", nullable_dict! {})],
                        props: set!["null"]
                    },
                    Field {
                        field_name: b"notes",
                        layers: vec![
                            Layer::new_noreset(b"string", nullable_dict! {}),
                            Layer::new_noreset(
                                b"list",
                                nullable_dict! {
                                    "unique" => Lit::Bool(true)
                                }
                            )
                        ],
                        props: set!["null"]
                    }
                ],
                props: nullable_dict! {
                    "env" => nullable_dict! {
                        "free_user_limit" => Lit::UnsignedInt(100),
                    },
                    "storage_driver" => Lit::Str("skyheap".into()),
                }
            }
        )
    }
}
mod dict_field_syntax {
    use super::*;
    use crate::engine::ql::schema::{ExpandedField, Layer};
    #[test]
    fn field_syn_mini() {
        let tok = lex_insecure(b"username { type string }").unwrap();
        let (ef, i) = schema::parse_field_syntax_full::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: b"username",
                layers: vec![Layer::new_noreset(b"string", nullable_dict! {})],
                props: nullable_dict! {},
                reset: false
            }
        )
    }
    #[test]
    fn field_syn() {
        let tok = lex_insecure(
            b"
                username {
                    nullable: false,
                    type string,
                }
            ",
        )
        .unwrap();
        let (ef, i) = schema::parse_field_syntax_full::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: b"username",
                props: nullable_dict! {
                    "nullable" => Lit::Bool(false),
                },
                layers: vec![Layer::new_noreset(b"string", nullable_dict! {})],
                reset: false
            }
        );
    }
    #[test]
    fn field_syn_pro() {
        let tok = lex_insecure(
            b"
                username {
                    nullable: false,
                    type string {
                        minlen: 6,
                        maxlen: 255,
                    },
                    jingle_bells: \"snow\"
                }
            ",
        )
        .unwrap();
        let (ef, i) = schema::parse_field_syntax_full::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: b"username",
                props: nullable_dict! {
                    "nullable" => Lit::Bool(false),
                    "jingle_bells" => Lit::Str("snow".into()),
                },
                layers: vec![Layer::new_noreset(
                    b"string",
                    nullable_dict! {
                        "minlen" => Lit::UnsignedInt(6),
                        "maxlen" => Lit::UnsignedInt(255),
                    }
                )],
                reset: false
            }
        );
    }
    #[test]
    fn field_syn_pro_max() {
        let tok = lex_insecure(
            b"
                notes {
                    nullable: true,
                    type list {
                        type string {
                            ascii_only: true,
                        },
                        unique: true,
                    },
                    jingle_bells: \"snow\"
                }
            ",
        )
        .unwrap();
        let (ef, i) = schema::parse_field_syntax_full::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: b"notes",
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true),
                    "jingle_bells" => Lit::Str("snow".into()),
                },
                layers: vec![
                    Layer::new_noreset(
                        b"string",
                        nullable_dict! {
                            "ascii_only" => Lit::Bool(true),
                        }
                    ),
                    Layer::new_noreset(
                        b"list",
                        nullable_dict! {
                            "unique" => Lit::Bool(true),
                        }
                    )
                ],
                reset: false
            }
        );
    }
}
mod alter_model_remove {
    use super::*;
    #[test]
    fn alter_mini() {
        let tok = lex_insecure(b"alter model mymodel remove myfield").unwrap();
        let mut i = 4;
        let remove = schema::alter_remove_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(remove, [b"myfield".as_slice()].into());
    }
    #[test]
    fn alter_mini_2() {
        let tok = lex_insecure(b"alter model mymodel remove (myfield)").unwrap();
        let mut i = 4;
        let remove = schema::alter_remove_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(remove, [b"myfield".as_slice()].into());
    }
    #[test]
    fn alter() {
        let tok =
            lex_insecure(b"alter model mymodel remove (myfield1, myfield2, myfield3, myfield4)")
                .unwrap();
        let mut i = 4;
        let remove = schema::alter_remove_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            remove,
            [
                b"myfield1".as_slice(),
                b"myfield2".as_slice(),
                b"myfield3".as_slice(),
                b"myfield4".as_slice(),
            ]
            .into()
        );
    }
}
mod alter_model_add {
    use super::*;
    use crate::engine::ql::schema::{ExpandedField, Layer};
    #[test]
    fn add_mini() {
        let tok = lex_insecure(
            b"
                alter model mymodel add myfield { type string }
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_add_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: b"myfield",
                props: nullable_dict! {},
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                reset: false
            }]
        );
    }
    #[test]
    fn add() {
        let tok = lex_insecure(
            b"
                alter model mymodel add myfield { type string, nullable: true }
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_add_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: b"myfield",
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true)
                },
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                reset: false
            }]
        );
    }
    #[test]
    fn add_pro() {
        let tok = lex_insecure(
            b"
                alter model mymodel add (myfield { type string, nullable: true })
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_add_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: b"myfield",
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true)
                },
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                reset: false
            }]
        );
    }
    #[test]
    fn add_pro_max() {
        let tok = lex_insecure(
            b"
                alter model mymodel add (
                    myfield {
                        type string,
                        nullable: true
                    },
                    another {
                        type list {
                            type string {
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
        let mut i = 4;
        let r = schema::alter_add_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [
                ExpandedField {
                    field_name: b"myfield",
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                    reset: false
                },
                ExpandedField {
                    field_name: b"another",
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(false)
                    },
                    layers: [
                        Layer::new_noreset(
                            b"string",
                            nullable_dict! {
                                "maxlen" => Lit::UnsignedInt(255)
                            }
                        ),
                        Layer::new_noreset(
                            b"list",
                            nullable_dict! {
                               "unique" => Lit::Bool(true)
                            },
                        )
                    ]
                    .into(),
                    reset: false
                }
            ]
        );
    }
}
mod alter_model_update {
    use super::*;
    use crate::engine::ql::schema::{ExpandedField, Layer};

    #[test]
    fn alter_mini() {
        let tok = lex_insecure(
            b"
                alter model mymodel update myfield { type string, .. }
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: b"myfield",
                props: nullable_dict! {},
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                reset: true
            }]
        );
    }
    #[test]
    fn alter_mini_2() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (myfield { type string, .. })
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: b"myfield",
                props: nullable_dict! {},
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                reset: true
            }]
        );
    }
    #[test]
    fn alter() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (
                    myfield {
                        type string,
                        nullable: true,
                        ..
                    }
                )
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: b"myfield",
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true)
                },
                layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                reset: true
            }]
        );
    }
    #[test]
    fn alter_pro() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (
                    myfield {
                        type string,
                        nullable: true,
                        ..
                    },
                    myfield2 {
                        type string,
                        ..
                    }
                )
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [
                ExpandedField {
                    field_name: b"myfield",
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                    reset: true
                },
                ExpandedField {
                    field_name: b"myfield2",
                    props: nullable_dict! {},
                    layers: [Layer::new_noreset(b"string", nullable_dict! {})].into(),
                    reset: true
                }
            ]
        );
    }
    #[test]
    fn alter_pro_max() {
        let tok = lex_insecure(
            b"
                alter model mymodel update (
                    myfield {
                        type string {..},
                        nullable: true,
                        ..
                    },
                    myfield2 {
                        type string {
                            maxlen: 255,
                            ..
                        },
                        ..
                    }
                )
            ",
        )
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update_full(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [
                ExpandedField {
                    field_name: b"myfield",
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_reset(b"string", nullable_dict! {})].into(),
                    reset: true
                },
                ExpandedField {
                    field_name: b"myfield2",
                    props: nullable_dict! {},
                    layers: [Layer::new_reset(
                        b"string",
                        nullable_dict! {"maxlen" => Lit::UnsignedInt(255)}
                    )]
                    .into(),
                    reset: true
                }
            ]
        );
    }
}

mod ddl_other_query_tests {
    use {
        super::*,
        crate::engine::ql::{
            ast::{Entity, Statement},
            ddl::{self, DropModel, DropSpace},
        },
    };
    #[test]
    fn drop_space() {
        let src = lex_insecure(br"drop space myspace").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new(b"myspace", false))
        );
    }
    #[test]
    fn drop_space_force() {
        let src = lex_insecure(br"drop space myspace force").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new(b"myspace", true))
        );
    }
    #[test]
    fn drop_model() {
        let src = lex_insecure(br"drop model mymodel").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(Entity::Single(b"mymodel"), false))
        );
    }
    #[test]
    fn drop_model_force() {
        let src = lex_insecure(br"drop model mymodel force").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(Entity::Single(b"mymodel"), true))
        );
    }
}
