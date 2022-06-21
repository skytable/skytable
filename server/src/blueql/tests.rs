/*
 * Created on Tue Jun 14 2022
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
    ast::{Compiler, Entity, FieldConfig, Statement},
    error::LangError,
    lexer::{Keyword, Lexer, Token, Type, TypeExpression},
};

macro_rules! src {
    ($name:ident, $($src:expr),* $(,)?) => {
        const $name: &[&[u8]] = &[$($src.as_bytes()),*];
    };
}

mod lexer {
    //! Lexer tests
    use super::*;

    #[test]
    fn lex_ident() {
        let src = b"mytbl";
        assert_eq!(
            Lexer::lex(src).unwrap(),
            vec![Token::Identifier("mytbl".into())]
        )
    }

    #[test]
    fn lex_keyword() {
        let src = b"create";
        assert_eq!(
            Lexer::lex(src).unwrap(),
            vec![Token::Keyword(Keyword::Create)]
        )
    }

    #[test]
    fn lex_number() {
        let src = b"123456";
        assert_eq!(Lexer::lex(src).unwrap(), vec![Token::Number(123456)])
    }

    #[test]
    fn lex_full() {
        let src = b"create model tweet";
        assert_eq!(
            Lexer::lex(src).unwrap(),
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Model),
                Token::Identifier("tweet".into())
            ]
        );
    }

    #[test]
    fn lex_combined_tokens() {
        let src = b"create model tweet(name: string, pic: binary, posts: list<string>)";
        assert_eq!(
            Lexer::lex(src).unwrap(),
            vec![
                Keyword::Create.into(),
                Keyword::Model.into(),
                "tweet".into(),
                Token::OpenParen,
                Token::Identifier("name".into()),
                Token::Colon,
                Type::String.into(),
                Token::Comma,
                Token::Identifier("pic".into()),
                Token::Colon,
                Type::Binary.into(),
                Token::Comma,
                Token::Identifier("posts".into()),
                Token::Colon,
                Type::List.into(),
                Token::OpenAngular,
                Type::String.into(),
                Token::CloseAngular,
                Token::CloseParen
            ]
        );
    }

    #[test]
    fn lex_quoted_string() {
        let src_a = "'hello, world🦀!'".as_bytes();
        let src_b = r#" "hello, world🦀!" "#.as_bytes();
        let src_c = r#" "\"hello, world🦀!\"" "#.as_bytes();
        assert_eq!(
            Lexer::lex(src_a).unwrap(),
            vec![Token::QuotedString("hello, world🦀!".into())]
        );
        assert_eq!(
            Lexer::lex(src_b).unwrap(),
            vec![Token::QuotedString("hello, world🦀!".into())]
        );
        assert_eq!(
            Lexer::lex(src_c).unwrap(),
            vec![Token::QuotedString("\"hello, world🦀!\"".into())]
        )
    }

    #[test]
    fn lex_fail_unknown_chars() {
        const SOURCES: &[&[u8]] = &[
            b"!", b"@", b"#", b"$", b"%", b"^", b"&", b"*", b"[", b"]", b"{", b"}", b"|", b"\\",
            b"/", b"~", b"`", b";", b"hello?",
        ];
        for source in SOURCES {
            assert_eq!(Lexer::lex(source).unwrap_err(), LangError::UnexpectedChar);
        }
    }

    #[test]
    fn lex_fail_unclosed_litstring() {
        const SOURCES: &[&[u8]] = &[b"'hello, world", br#""hello, world"#];
        for source in SOURCES {
            assert_eq!(
                Lexer::lex(source).unwrap_err(),
                LangError::InvalidStringLiteral
            );
        }
    }

    #[test]
    fn lex_fail_litnum() {
        src!(SOURCES, "12345f", "123!", "123'");
        for source in SOURCES {
            assert_eq!(
                Lexer::lex(source).unwrap_err(),
                LangError::InvalidNumericLiteral
            );
        }
    }
}

mod ast {
    //! AST tests

    #[test]
    fn parse_entity_name_test() {
        assert_eq!(
            Compiler::new(&Lexer::lex(b"hello").unwrap())
                .parse_entity_name()
                .unwrap(),
            Entity::Current("hello".into())
        );
        assert_eq!(
            Compiler::new(&Lexer::lex(b"hello.world").unwrap())
                .parse_entity_name()
                .unwrap(),
            Entity::Full("hello".into(), "world".into())
        );
    }

    use super::*;
    #[cfg(test)]
    fn setup_src_stmt() -> (Vec<u8>, Statement) {
        let src =
            b"create model twitter.tweet(username: string, password: binary, posts: list<string>) volatile"
                .to_vec();
        let stmt = Statement::CreateModel {
            entity: Entity::Full("twitter".into(), "tweet".into()),
            model: FieldConfig {
                types: vec![
                    TypeExpression(vec![Type::String]),
                    TypeExpression(vec![Type::Binary]),
                    TypeExpression(vec![Type::List, Type::String]),
                ],
                names: vec!["username".into(), "password".into(), "posts".into()],
            },
            volatile: true,
        };
        (src, stmt)
    }

    #[test]
    fn stmt_create_named_unnamed_mixed() {
        let src = b"create model twitter.tweet(username: string, binary)".to_vec();
        assert_eq!(
            Compiler::compile(&src).unwrap_err(),
            LangError::BadExpression
        );
    }
    #[test]
    fn stmt_create_unnamed() {
        let src = b"create model twitter.passwords(string, binary)".to_vec();
        let expected = Statement::CreateModel {
            entity: Entity::Full("twitter".into(), "passwords".into()),
            model: FieldConfig {
                names: vec![],
                types: vec![
                    TypeExpression(vec![Type::String]),
                    TypeExpression(vec![Type::Binary]),
                ],
            },
            volatile: false,
        };
        assert_eq!(Compiler::compile(&src).unwrap(), expected);
    }
    #[test]
    fn stmt_drop_space() {
        assert_eq!(
            Compiler::compile(b"drop space twitter force").unwrap(),
            Statement::DropSpace {
                entity: "twitter".into(),
                force: true
            }
        );
    }
    #[test]
    fn stmt_drop_model() {
        assert_eq!(
            Compiler::compile(b"drop model twitter.tweet force").unwrap(),
            Statement::DropModel {
                entity: Entity::Full("twitter".into(), "tweet".into()),
                force: true
            }
        );
    }
    #[test]
    fn stmt_inspect_space() {
        assert_eq!(
            Compiler::compile(b"inspect space twitter").unwrap(),
            Statement::InspectSpace(Some("twitter".into()))
        );
    }
    #[test]
    fn stmt_inspect_model() {
        assert_eq!(
            Compiler::compile(b"inspect model twitter.tweet").unwrap(),
            Statement::InspectModel(Some(Entity::Full("twitter".into(), "tweet".into())))
        );
    }
    #[test]
    fn compile_full() {
        let (src, stmt) = setup_src_stmt();
        assert_eq!(Compiler::compile(&src).unwrap(), stmt)
    }
    #[test]
    fn bad_model_code() {
        let get_model_code = |src| {
            let l = Lexer::lex(src).unwrap();
            let stmt = Compiler::new(&l)
                .parse_fields(Entity::Current("jotsy".into()))
                .unwrap();
            match stmt {
                Statement::CreateModel { model, .. } => model.get_model_code(),
                x => panic!("Expected model found {:?}", x),
            }
        };
        // check rules
        // first type cannot be list
        src!(
            SRC,
            // rule: first cannot be list
            "(list, string)",
            "(list, binary)",
            "(list<string>, string)",
            "(list<binary>, string)",
            // rule: cannot have more than two args
            "(list<string>, string, string)",
            // rule: non-compound types cannot have type arguments
            "(string<binary>, binary<string>)",
            // rule: fields can't be named
            "(id: string, posts: list<string>)",
            // rule: nested lists are disallowed
            "(string, list<list<string>>)"
        );
        for src in SRC {
            assert_eq!(
                get_model_code(src).unwrap_err(),
                LangError::UnsupportedModelDeclaration,
                "{}",
                String::from_utf8_lossy(src)
            );
        }
    }
}
