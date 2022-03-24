/*
 * Created on Tue Jul 27 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

use super::parser;

mod parser_ddl_tests {
    use super::parser::Entity;
    macro_rules! byvec {
        ($($element:expr),*) => {
            vec![
                $(
                    $element.as_bytes()
                ),*
            ]
        };
    }
    fn parse_table_args_test(input: Vec<&'static [u8]>) -> Result<(Entity<'_>, u8), &'static [u8]> {
        super::parser::parse_table_args(input[0], input[1])
    }
    use crate::protocol::responses;
    #[test]
    fn test_table_args_valid() {
        // binstr, binstr
        let it = byvec!("mytbl", "keymap(binstr,binstr)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 0);

        // binstr, str
        let it = byvec!("mytbl", "keymap(binstr,str)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 1);

        // str, str
        let it = byvec!("mytbl", "keymap(str,str)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 2);

        // str, binstr
        let it = byvec!("mytbl", "keymap(str,binstr)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 3);

        // now test kvext: listmap
        // binstr, list<binstr>
        let it = byvec!("mytbl", "keymap(binstr,list<binstr>)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 4);

        // binstr, list<str>
        let it = byvec!("mytbl", "keymap(binstr,list<str>)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 5);

        // str, list<binstr>
        let it = byvec!("mytbl", "keymap(str,list<binstr>)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 6);

        // str, list<str>
        let it = byvec!("mytbl", "keymap(str,list<str>)");
        let (tbl_name, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tbl_name, Entity::Single(b"mytbl"));
        assert_eq!(mcode, 7);
    }
    #[test]
    fn test_table_bad_ident() {
        let it = byvec!("1one", "keymap(binstr,binstr)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_CONTAINER_NAME
        );
        let it = byvec!("%whywouldsomeone", "keymap(binstr,binstr)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_CONTAINER_NAME
        );
    }
    #[test]
    fn test_table_whitespaced_datatypes() {
        let it = byvec!("mycooltbl", "keymap(binstr, binstr)");
        let (tblid, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tblid, Entity::Single(b"mycooltbl"));
        assert_eq!(mcode, 0);

        let it = byvec!("mycooltbl", "keymap(binstr, str)");
        let (tblid, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tblid, Entity::Single(b"mycooltbl"));
        assert_eq!(mcode, 1);

        let it = byvec!("mycooltbl", "keymap(str, str)");
        let (tblid, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tblid, Entity::Single(b"mycooltbl"));
        assert_eq!(mcode, 2);

        let it = byvec!("mycooltbl", "keymap(str, binstr)");
        let (tblid, mcode) = parse_table_args_test(it).unwrap();
        assert_eq!(tblid, Entity::Single(b"mycooltbl"));
        assert_eq!(mcode, 3);
    }

    #[test]
    fn test_table_badty() {
        let it = byvec!("mycooltbl", "keymap(wth, str)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
        let it = byvec!("mycooltbl", "keymap(wth, wth)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
        let it = byvec!("mycooltbl", "keymap(str, wth)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
        let it = byvec!("mycooltbl", "keymap(wth1, wth2)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
    }
    #[test]
    fn test_table_bad_model() {
        let it = byvec!("mycooltbl", "wthmap(wth, wth)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::UNKNOWN_MODEL
        );
        let it = byvec!("mycooltbl", "wthmap(str, str)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::UNKNOWN_MODEL
        );
        let it = byvec!("mycooltbl", "wthmap()");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::UNKNOWN_MODEL
        );
    }
    #[test]
    fn test_table_malformed_expr() {
        let it = byvec!("mycooltbl", "keymap(");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(,");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(,,");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap),");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap),,");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap),,)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(,)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(,,)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap,");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap,,");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap,,)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(str,");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(str,str");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(str,str,");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(str,str,)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = byvec!("mycooltbl", "keymap(str,str,),");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
    }

    #[test]
    fn test_table_too_many_args() {
        let it = byvec!("mycooltbl", "keymap(str, str, str)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::TOO_MANY_ARGUMENTS
        );

        // this should be valid for not-yet-known data types too
        let it = byvec!("mycooltbl", "keymap(wth, wth, wth)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::TOO_MANY_ARGUMENTS
        );
    }

    #[test]
    fn test_bad_key_type() {
        let it = byvec!("myverycooltbl", "keymap(list<str>, str)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_TYPE_FOR_KEY
        );
        let it = byvec!("myverycooltbl", "keymap(list<binstr>, binstr)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_TYPE_FOR_KEY
        );
        // for consistency checks
        let it = byvec!("myverycooltbl", "keymap(list<str>, binstr)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_TYPE_FOR_KEY
        );
        let it = byvec!("myverycooltbl", "keymap(list<binstr>, str)");
        assert_eq!(
            parse_table_args_test(it).unwrap_err(),
            responses::groups::BAD_TYPE_FOR_KEY
        );
    }
}

mod entity_parser_tests {
    use super::parser::Entity;
    use crate::protocol::responses;
    #[test]
    fn test_query_full_entity_okay() {
        let x = byt!("ks:tbl");
        assert_eq!(Entity::from_slice(&x).unwrap(), Entity::Full(b"ks", b"tbl"));
    }
    #[test]
    fn test_query_half_entity() {
        let x = byt!("tbl");
        assert_eq!(Entity::from_slice(&x).unwrap(), Entity::Single(b"tbl"))
    }
    #[test]
    fn test_query_partial_entity() {
        let x = byt!(":tbl");
        assert_eq!(Entity::from_slice(&x).unwrap(), Entity::Partial(b"tbl"))
    }
    #[test]
    fn test_query_entity_badexpr() {
        let x = byt!("ks:");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!(":");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("::");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("::ks");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("ks::tbl");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("ks::");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("ks::tbl::");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("::ks::tbl::");
        assert_eq!(
            Entity::from_slice(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
    }

    #[test]
    fn test_bad_entity_name() {
        let ename = byt!("$var");
        assert_eq!(
            Entity::from_slice(&ename).unwrap_err(),
            responses::groups::BAD_CONTAINER_NAME
        );
    }
}
