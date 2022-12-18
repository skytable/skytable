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
    lex, *,
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
        let tok = lex(b"inspect space myspace").unwrap();
        assert_eq!(
            ddl::parse_inspect_full(&tok[1..]).unwrap(),
            Statement::InspectSpace("myspace".into())
        );
    }
    #[test]
    fn inspect_model() {
        let tok = lex(b"inspect model users").unwrap();
        assert_eq!(
            ddl::parse_inspect_full(&tok[1..]).unwrap(),
            Statement::InspectModel(Entity::Single("users".into()))
        );
        let tok = lex(b"inspect model tweeter.users").unwrap();
        assert_eq!(
            ddl::parse_inspect_full(&tok[1..]).unwrap(),
            Statement::InspectModel(("tweeter", "users").into())
        );
    }
    #[test]
    fn inspect_spaces() {
        let tok = lex(b"inspect spaces").unwrap();
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
        let tok = lex(b"alter model mymodel with {}").unwrap();
        let r = schema::alter_space_full(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterSpace {
                space_name: "mymodel".into(),
                updated_props: nullable_dict! {}
            }
        );
    }
    #[test]
    fn alter_space() {
        let tok = lex(br#"
                alter model mymodel with {
                    max_entry: 1000,
                    driver: "ts-0.8"
                }
            "#)
        .unwrap();
        let r = schema::alter_space_full(&tok[2..]).unwrap();
        assert_eq!(
            r,
            AlterSpace {
                space_name: "mymodel".into(),
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
        let tok = lex(b"}").unwrap();
        let (res, ret) = schema::fold_tymeta(&tok);
        assert!(res.is_okay());
        assert!(!res.has_more());
        assert_eq!(res.pos(), 1);
        assert_eq!(ret, nullable_dict!());
    }
    #[test]
    fn tymeta_mini_fail() {
        let tok = lex(b",}").unwrap();
        let (res, ret) = schema::fold_tymeta(&tok);
        assert!(!res.is_okay());
        assert!(!res.has_more());
        assert_eq!(res.pos(), 0);
        assert_eq!(ret, nullable_dict!());
    }
    #[test]
    fn tymeta() {
        let tok = lex(br#"hello: "world", loading: true, size: 100 }"#).unwrap();
        let (res, ret) = schema::fold_tymeta(&tok);
        assert!(res.is_okay());
        assert!(!res.has_more());
        assert_eq!(res.pos(), tok.len());
        assert_eq!(
            ret,
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
        let tok = lex(br#"maxlen: 100, type string, unique: true }"#).unwrap();
        let (res1, ret1) = schema::fold_tymeta(&tok);
        assert!(res1.is_okay());
        assert!(res1.has_more());
        assert_eq!(res1.pos(), 5);
        let remslice = &tok[res1.pos() + 2..];
        let (res2, ret2) = schema::fold_tymeta(remslice);
        assert!(res2.is_okay());
        assert!(!res2.has_more());
        assert_eq!(res2.pos() + res1.pos() + 2, tok.len());
        let mut final_ret = ret1;
        final_ret.extend(ret2);
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
            lex(br#"maxlen: 100, this: { is: "cool" }, type string, unique: true }"#).unwrap();
        let (res1, ret1) = schema::fold_tymeta(&tok);
        assert!(res1.is_okay());
        assert!(res1.has_more());
        assert_eq!(res1.pos(), 13);
        let remslice = &tok[res1.pos() + 2..];
        let (res2, ret2) = schema::fold_tymeta(remslice);
        assert!(res2.is_okay());
        assert!(!res2.has_more());
        assert_eq!(res2.pos() + res1.pos() + 2, tok.len());
        let mut final_ret = ret1;
        final_ret.extend(ret2);
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
        let tok = lex(b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\x01
                    },
                    users: \"sayan\"\x01
                }
            ")
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
            let (ret, dict) = schema::fold_tymeta(&new_src);
            if should_pass {
                assert!(ret.is_okay(), "{:?}", &new_src);
                assert!(!ret.has_more());
                assert_eq!(ret.pos(), new_src.len());
                assert_eq!(dict, expected);
            } else if ret.is_okay() {
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
        let tok = lex(b"
                    maxlen: 10,
                    unique: true,
                    auth: {
                        maybe: true\x01
                    },
                    type string,
                    users: \"sayan\"\x01
                }
            ")
        .unwrap();
        let expected = nullable_dict! {
            "maxlen" => Lit::UnsignedInt(10),
            "unique" => Lit::Bool(true),
            "auth" => nullable_dict! {
                "maybe" => Lit::Bool(true),
            },
        };
        fuzz_tokens(&tok, |should_pass, new_src| {
            let (ret, dict) = schema::fold_tymeta(&new_src);
            if should_pass {
                assert!(ret.is_okay());
                assert!(ret.has_more());
                assert!(new_src[ret.pos()] == Token::Ident("string".into()));
                assert_eq!(dict, expected);
            } else if ret.is_okay() {
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
        let tok = lex(b"string)").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len() - 1);
        assert!(okay);
        assert_eq!(
            layers,
            vec![Layer::new_noreset("string".into(), nullable_dict! {})]
        );
    }
    #[test]
    fn layer() {
        let tok = lex(b"string { maxlen: 100 }").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len());
        assert!(okay);
        assert_eq!(
            layers,
            vec![Layer::new_noreset(
                "string".into(),
                nullable_dict! {
                    "maxlen" => Lit::UnsignedInt(100)
                }
            )]
        );
    }
    #[test]
    fn layer_plus() {
        let tok = lex(b"list { type string }").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len());
        assert!(okay);
        assert_eq!(
            layers,
            vec![
                Layer::new_noreset("string".into(), nullable_dict! {}),
                Layer::new_noreset("list".into(), nullable_dict! {})
            ]
        );
    }
    #[test]
    fn layer_pro() {
        let tok = lex(b"list { unique: true, type string, maxlen: 10 }").unwrap();
        let (layers, c, okay) = schema::fold_layers(&tok);
        assert_eq!(c, tok.len());
        assert!(okay);
        assert_eq!(
            layers,
            vec![
                Layer::new_noreset("string".into(), nullable_dict! {}),
                Layer::new_noreset(
                    "list".into(),
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
        let tok = lex(
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
                    "string".into(),
                    nullable_dict! {
                        "ascii_only" => Lit::Bool(true),
                        "maxlen" => Lit::UnsignedInt(255)
                    }
                ),
                Layer::new_noreset(
                    "list".into(),
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
        let tok = lex(b"
            list {
                type list {
                    maxlen: 100,
                    type string\x01
                },
                unique: true\x01
            }
        ")
        .unwrap();
        let expected = vec![
            Layer::new_noreset("string".into(), nullable_dict!()),
            Layer::new_noreset(
                "list".into(),
                nullable_dict! {
                    "maxlen" => Lit::UnsignedInt(100),
                },
            ),
            Layer::new_noreset("list".into(), nullable_dict!("unique" => Lit::Bool(true))),
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
        let tok = lex(b"myfield:").unwrap();
        let (props, c, okay) = schema::parse_field_properties(&tok);
        assert!(okay);
        assert_eq!(c, 0);
        assert_eq!(props, FieldProperties::default());
    }
    #[test]
    fn field_properties_full() {
        let tok = lex(b"primary null myfield:").unwrap();
        let (props, c, okay) = schema::parse_field_properties(&tok);
        assert_eq!(c, 2);
        assert_eq!(tok[c], Token::Ident("myfield".into()));
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
        let tok = lex(b"
                username: string,
            ")
        .unwrap();
        let (c, f) = schema::parse_field(&tok).unwrap();
        assert_eq!(c, tok.len() - 1);
        assert_eq!(
            f,
            Field {
                field_name: "username".into(),
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                props: set![],
            }
        )
    }
    #[test]
    fn field() {
        let tok = lex(b"
                primary username: string,    
            ")
        .unwrap();
        let (c, f) = schema::parse_field(&tok).unwrap();
        assert_eq!(c, tok.len() - 1);
        assert_eq!(
            f,
            Field {
                field_name: "username".into(),
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                props: set!["primary"],
            }
        )
    }
    #[test]
    fn field_pro() {
        let tok = lex(b"
                primary username: string {
                    maxlen: 10,
                    ascii_only: true,
                }
            ")
        .unwrap();
        let (c, f) = schema::parse_field(&tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            f,
            Field {
                field_name: "username".into(),
                layers: [Layer::new_noreset(
                    "string".into(),
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
        let tok = lex(b"
                null notes: list {
                    type string {
                        maxlen: 255,
                        ascii_only: true,
                    },
                    unique: true,
                }
            ")
        .unwrap();
        let (c, f) = schema::parse_field(&tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            f,
            Field {
                field_name: "notes".into(),
                layers: [
                    Layer::new_noreset(
                        "string".into(),
                        nullable_dict! {
                            "maxlen" => Lit::UnsignedInt(255),
                            "ascii_only" => Lit::Bool(true),
                        }
                    ),
                    Layer::new_noreset(
                        "list".into(),
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
        let tok = lex(b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                )
            ")
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: "mymodel".into(),
                fields: vec![
                    Field {
                        field_name: "username".into(),
                        layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: "password".into(),
                        layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                        props: set![]
                    }
                ],
                props: nullable_dict! {}
            }
        )
    }
    #[test]
    fn schema() {
        let tok = lex(b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary,
                )
            ")
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: "mymodel".into(),
                fields: vec![
                    Field {
                        field_name: "username".into(),
                        layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: "password".into(),
                        layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                        props: set![]
                    },
                    Field {
                        field_name: "profile_pic".into(),
                        layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                        props: set!["null"]
                    }
                ],
                props: nullable_dict! {}
            }
        )
    }

    #[test]
    fn schema_pro() {
        let tok = lex(b"
                create model mymodel(
                    primary username: string,
                    password: binary,
                    null profile_pic: binary,
                    null notes: list {
                        type string,
                        unique: true,
                    },
                )
            ")
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: "mymodel".into(),
                fields: vec![
                    Field {
                        field_name: "username".into(),
                        layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: "password".into(),
                        layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                        props: set![]
                    },
                    Field {
                        field_name: "profile_pic".into(),
                        layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                        props: set!["null"]
                    },
                    Field {
                        field_name: "notes".into(),
                        layers: vec![
                            Layer::new_noreset("string".into(), nullable_dict! {}),
                            Layer::new_noreset(
                                "list".into(),
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
        let tok = lex(b"
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
            ")
        .unwrap();
        let tok = &tok[2..];

        // parse model
        let (model, c) = schema::parse_schema_from_tokens(tok).unwrap();
        assert_eq!(c, tok.len());
        assert_eq!(
            model,
            Model {
                model_name: "mymodel".into(),
                fields: vec![
                    Field {
                        field_name: "username".into(),
                        layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                        props: set!["primary"]
                    },
                    Field {
                        field_name: "password".into(),
                        layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                        props: set![]
                    },
                    Field {
                        field_name: "profile_pic".into(),
                        layers: vec![Layer::new_noreset("binary".into(), nullable_dict! {})],
                        props: set!["null"]
                    },
                    Field {
                        field_name: "notes".into(),
                        layers: vec![
                            Layer::new_noreset("string".into(), nullable_dict! {}),
                            Layer::new_noreset(
                                "list".into(),
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
        let tok = lex(b"username { type string }").unwrap();
        let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: "username".into(),
                layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                props: nullable_dict! {},
                reset: false
            }
        )
    }
    #[test]
    fn field_syn() {
        let tok = lex(b"
                username {
                    nullable: false,
                    type string,
                }
            ")
        .unwrap();
        let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: "username".into(),
                props: nullable_dict! {
                    "nullable" => Lit::Bool(false),
                },
                layers: vec![Layer::new_noreset("string".into(), nullable_dict! {})],
                reset: false
            }
        );
    }
    #[test]
    fn field_syn_pro() {
        let tok = lex(b"
                username {
                    nullable: false,
                    type string {
                        minlen: 6,
                        maxlen: 255,
                    },
                    jingle_bells: \"snow\"
                }
            ")
        .unwrap();
        let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: "username".into(),
                props: nullable_dict! {
                    "nullable" => Lit::Bool(false),
                    "jingle_bells" => Lit::Str("snow".into()),
                },
                layers: vec![Layer::new_noreset(
                    "string".into(),
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
        let tok = lex(b"
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
            ")
        .unwrap();
        let (ef, i) = schema::parse_field_syntax::<true>(&tok).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            ef,
            ExpandedField {
                field_name: "notes".into(),
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true),
                    "jingle_bells" => Lit::Str("snow".into()),
                },
                layers: vec![
                    Layer::new_noreset(
                        "string".into(),
                        nullable_dict! {
                            "ascii_only" => Lit::Bool(true),
                        }
                    ),
                    Layer::new_noreset(
                        "list".into(),
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
    use crate::engine::ql::RawSlice;
    #[test]
    fn alter_mini() {
        let tok = lex(b"alter model mymodel remove myfield").unwrap();
        let mut i = 4;
        let remove = schema::alter_remove(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(remove, [RawSlice::from("myfield")].into());
    }
    #[test]
    fn alter_mini_2() {
        let tok = lex(b"alter model mymodel remove (myfield)").unwrap();
        let mut i = 4;
        let remove = schema::alter_remove(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(remove, [RawSlice::from("myfield")].into());
    }
    #[test]
    fn alter() {
        let tok =
            lex(b"alter model mymodel remove (myfield1, myfield2, myfield3, myfield4)").unwrap();
        let mut i = 4;
        let remove = schema::alter_remove(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            remove,
            [
                RawSlice::from("myfield1"),
                RawSlice::from("myfield2"),
                RawSlice::from("myfield3"),
                RawSlice::from("myfield4")
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
        let tok = lex(b"
                alter model mymodel add myfield { type string }
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_add(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: "myfield".into(),
                props: nullable_dict! {},
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                reset: false
            }]
        );
    }
    #[test]
    fn add() {
        let tok = lex(b"
                alter model mymodel add myfield { type string, nullable: true }
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_add(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: "myfield".into(),
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true)
                },
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                reset: false
            }]
        );
    }
    #[test]
    fn add_pro() {
        let tok = lex(b"
                alter model mymodel add (myfield { type string, nullable: true })
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_add(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: "myfield".into(),
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true)
                },
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                reset: false
            }]
        );
    }
    #[test]
    fn add_pro_max() {
        let tok = lex(b"
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
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_add(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [
                ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: false
                },
                ExpandedField {
                    field_name: "another".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(false)
                    },
                    layers: [
                        Layer::new_noreset(
                            "string".into(),
                            nullable_dict! {
                                "maxlen" => Lit::UnsignedInt(255)
                            }
                        ),
                        Layer::new_noreset(
                            "list".into(),
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
        let tok = lex(b"
                alter model mymodel update myfield { type string, .. }
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: "myfield".into(),
                props: nullable_dict! {},
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                reset: true
            }]
        );
    }
    #[test]
    fn alter_mini_2() {
        let tok = lex(b"
                alter model mymodel update (myfield { type string, .. })
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: "myfield".into(),
                props: nullable_dict! {},
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                reset: true
            }]
        );
    }
    #[test]
    fn alter() {
        let tok = lex(b"
                alter model mymodel update (
                    myfield {
                        type string,
                        nullable: true,
                        ..
                    }
                )
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [ExpandedField {
                field_name: "myfield".into(),
                props: nullable_dict! {
                    "nullable" => Lit::Bool(true)
                },
                layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                reset: true
            }]
        );
    }
    #[test]
    fn alter_pro() {
        let tok = lex(b"
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
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [
                ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: true
                },
                ExpandedField {
                    field_name: "myfield2".into(),
                    props: nullable_dict! {},
                    layers: [Layer::new_noreset("string".into(), nullable_dict! {})].into(),
                    reset: true
                }
            ]
        );
    }
    #[test]
    fn alter_pro_max() {
        let tok = lex(b"
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
            ")
        .unwrap();
        let mut i = 4;
        let r = schema::alter_update(&tok[i..], &mut i).unwrap();
        assert_eq!(i, tok.len());
        assert_eq!(
            r.as_ref(),
            [
                ExpandedField {
                    field_name: "myfield".into(),
                    props: nullable_dict! {
                        "nullable" => Lit::Bool(true)
                    },
                    layers: [Layer::new_reset("string".into(), nullable_dict! {})].into(),
                    reset: true
                },
                ExpandedField {
                    field_name: "myfield2".into(),
                    props: nullable_dict! {},
                    layers: [Layer::new_reset(
                        "string".into(),
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
        let src = lex(br"drop space myspace").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new("myspace".into(), false))
        );
    }
    #[test]
    fn drop_space_force() {
        let src = lex(br"drop space myspace force").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropSpace(DropSpace::new("myspace".into(), true))
        );
    }
    #[test]
    fn drop_model() {
        let src = lex(br"drop model mymodel").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(Entity::Single("mymodel".into()), false))
        );
    }
    #[test]
    fn drop_model_force() {
        let src = lex(br"drop model mymodel force").unwrap();
        assert_eq!(
            ddl::parse_drop_full(&src[1..]).unwrap(),
            Statement::DropModel(DropModel::new(Entity::Single("mymodel".into()), true))
        );
    }
}
