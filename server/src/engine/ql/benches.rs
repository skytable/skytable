/*
 * Created on Wed Nov 16 2022
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

/*
    All benches should be aggregate costs of full execution. This means that when for example
    you're writing a benchmark for something like parsing a `select` statement, you should calculate
    the total time of execution including lexing, parsing and allocating. Hopefully in the future we can
    implement a testing framework that enables us to find the total tiered cost of execution for each stage
    and hence enable us to iterate on the weakness and fix it. Maybe even visualize it? That'd be amazing
    and maybe would be something I'll work on around 0.9.

    -- Sayan (@ohsayan)
*/

extern crate test;

use {
    crate::engine::ql::{lex::Ident, tests::lex_insecure},
    test::Bencher,
};

mod lexer {
    use {
        super::*,
        crate::engine::{
            data::{lit::Lit, spec::Dataspec1D},
            ql::lex::Token,
        },
    };
    #[bench]
    fn lex_number(b: &mut Bencher) {
        let src = b"1234567890";
        let expected = vec![Token::Lit(1234567890_u64.into())];
        b.iter(|| assert_eq!(lex_insecure(src).unwrap(), expected));
    }
    #[bench]
    fn lex_bool(b: &mut Bencher) {
        let s = b"true";
        let e = vec![Token::Lit(true.into())];
        b.iter(|| assert_eq!(lex_insecure(s).unwrap(), e));
    }
    #[bench]
    fn lex_string_noescapes(b: &mut Bencher) {
        let s = br#"'hello, world!'"#;
        let e = vec![Token::Lit("hello, world!".into())];
        b.iter(|| assert_eq!(lex_insecure(s).unwrap(), e));
    }
    #[bench]
    fn lex_string_with_escapes(b: &mut Bencher) {
        let s = br#"'hello, world! this is within a \'quote\''"#;
        let e = vec![Token::Lit("hello, world! this is within a 'quote'".into())];
        b.iter(|| assert_eq!(lex_insecure(s).unwrap(), e));
    }
    #[bench]
    fn lex_raw_literal(b: &mut Bencher) {
        let src = b"\r44\ne69b10ffcc250ae5091dec6f299072e23b0b41d6a739";
        let expected = vec![Token::Lit(Lit::Bin(
            b"e69b10ffcc250ae5091dec6f299072e23b0b41d6a739",
        ))];
        b.iter(|| assert_eq!(lex_insecure(src).unwrap(), expected));
    }
}

mod ast {
    use {
        super::*,
        crate::engine::ql::ast::{Entity, InplaceData, State},
    };
    #[bench]
    fn parse_entity_single(b: &mut Bencher) {
        let e = Entity::Single(Ident::from("user"));
        b.iter(|| {
            let src = lex_insecure(b"user").unwrap();
            let mut state = State::new(&src, InplaceData::new());
            let re = Entity::attempt_process_entity_result(&mut state).unwrap();
            assert_eq!(e, re);
            assert!(state.exhausted());
        })
    }
    #[bench]
    fn parse_entity_double(b: &mut Bencher) {
        let e = Entity::Full(Ident::from("tweeter"), Ident::from("user"));
        b.iter(|| {
            let src = lex_insecure(b"tweeter.user").unwrap();
            let mut state = State::new(&src, InplaceData::new());
            let re = Entity::attempt_process_entity_result(&mut state).unwrap();
            assert_eq!(e, re);
            assert!(state.exhausted());
        })
    }
}

mod ddl_queries {
    use {
        super::*,
        crate::engine::ql::{
            ast::{compile, Entity, InplaceData, Statement},
            lex::InsecureLexer,
        },
    };
    mod use_stmt {
        use super::*;
        #[bench]
        fn use_space(b: &mut Bencher) {
            let src = b"use myspace";
            let expected = Statement::Use(Entity::Single(Ident::from("myspace")));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn use_model(b: &mut Bencher) {
            let src = b"use myspace.mymodel";
            let expected =
                Statement::Use(Entity::Full(Ident::from("myspace"), Ident::from("mymodel")));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
    }
    mod inspect_stmt {
        use super::*;
        #[bench]
        fn inspect_space(b: &mut Bencher) {
            let src = b"inspect space myspace";
            let expected = Statement::InspectSpace(Ident::from("myspace"));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn inspect_model_single_entity(b: &mut Bencher) {
            let src = b"inspect model mymodel";
            let expected = Statement::InspectModel(Entity::Single(Ident::from("mymodel")));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn inspect_model_full_entity(b: &mut Bencher) {
            let src = b"inspect model myspace.mymodel";
            let expected = Statement::InspectModel(Entity::Full(
                Ident::from("myspace"),
                Ident::from("mymodel"),
            ));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn inspect_spaces(b: &mut Bencher) {
            let src = b"inspect spaces";
            let expected = Statement::InspectSpaces;
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
    }
    mod drop_stmt {
        use {
            super::*,
            crate::engine::ql::ddl::drop::{DropModel, DropSpace},
        };
        #[bench]
        fn drop_space(b: &mut Bencher) {
            let src = b"drop space myspace";
            let expected = Statement::DropSpace(DropSpace::new(Ident::from("myspace"), false));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn drop_space_force(b: &mut Bencher) {
            let src = b"drop space myspace force";
            let expected = Statement::DropSpace(DropSpace::new(Ident::from("myspace"), true));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn drop_model_single(b: &mut Bencher) {
            let src = b"drop model mymodel";
            let expected = Statement::DropModel(DropModel::new(
                Entity::Single(Ident::from("mymodel")),
                false,
            ));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn drop_model_single_force(b: &mut Bencher) {
            let src = b"drop model mymodel force";
            let expected =
                Statement::DropModel(DropModel::new(Entity::Single(Ident::from("mymodel")), true));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn drop_model_full(b: &mut Bencher) {
            let src = b"drop model myspace.mymodel";
            let expected = Statement::DropModel(DropModel::new(
                Entity::Full(Ident::from("myspace"), Ident::from("mymodel")),
                false,
            ));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
        #[bench]
        fn drop_model_full_force(b: &mut Bencher) {
            let src = b"drop model myspace.mymodel force";
            let expected = Statement::DropModel(DropModel::new(
                Entity::Full(Ident::from("myspace"), Ident::from("mymodel")),
                true,
            ));
            b.iter(|| {
                let lexed = InsecureLexer::lex(src).unwrap();
                assert_eq!(compile(&lexed, InplaceData::new()).unwrap(), expected);
            });
        }
    }
}
