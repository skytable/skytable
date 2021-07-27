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

macro_rules! byt {
    ($f:expr) => {
        bytes::Bytes::from($f)
    };
}
macro_rules! bi {
    ($($x:expr),+ $(,)?) => {{
        vec![$(bytes::Bytes::from($x),)*].into_iter()
    }};
}

mod parser_ddl_tests {
    use super::parser::parse_table_args;
    use crate::corestore::memstore::ObjectID;
    use crate::protocol::responses;
    #[test]
    fn test_table_args_valid() {
        // create table [mytbl keymap(str, str)]
        let it = vec![byt!("mytbl"), byt!("keymap(binstr,binstr)")].into_iter();
        let (tbl_name, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tbl_name, unsafe { ObjectID::from_slice("mytbl") });
        assert_eq!(mcode, 0);

        let it = vec![byt!("mytbl"), byt!("keymap(binstr,str)")].into_iter();
        let (tbl_name, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tbl_name, unsafe { ObjectID::from_slice("mytbl") });
        assert_eq!(mcode, 1);

        let it = vec![byt!("mytbl"), byt!("keymap(str,str)")].into_iter();
        let (tbl_name, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tbl_name, unsafe { ObjectID::from_slice("mytbl") });
        assert_eq!(mcode, 2);

        let it = vec![byt!("mytbl"), byt!("keymap(str,binstr)")].into_iter();
        let (tbl_name, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tbl_name, unsafe { ObjectID::from_slice("mytbl") });
        assert_eq!(mcode, 3);
    }
    #[test]
    fn test_table_bad_ident() {
        let it = vec![byt!("1one"), byt!("keymap(binstr,binstr)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = vec![byt!("%whywouldsomeone"), byt!("keymap(binstr,binstr)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
    }
    #[test]
    fn test_table_whitespaced_datatypes() {
        let it = vec![byt!("mycooltbl"), byt!("keymap(binstr, binstr)")].into_iter();
        let (tblid, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tblid, unsafe { ObjectID::from_slice("mycooltbl") });
        assert_eq!(mcode, 0);

        let it = vec![byt!("mycooltbl"), byt!("keymap(binstr, str)")].into_iter();
        let (tblid, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tblid, unsafe { ObjectID::from_slice("mycooltbl") });
        assert_eq!(mcode, 1);

        let it = vec![byt!("mycooltbl"), byt!("keymap(str, str)")].into_iter();
        let (tblid, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tblid, unsafe { ObjectID::from_slice("mycooltbl") });
        assert_eq!(mcode, 2);

        let it = vec![byt!("mycooltbl"), byt!("keymap(str, binstr)")].into_iter();
        let (tblid, mcode) = parse_table_args(it).unwrap();
        assert_eq!(tblid, unsafe { ObjectID::from_slice("mycooltbl") });
        assert_eq!(mcode, 3);
    }

    #[test]
    fn test_table_badty() {
        let it = vec![byt!("mycooltbl"), byt!("keymap(wth, str)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
        let it = vec![byt!("mycooltbl"), byt!("keymap(wth, wth)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
        let it = vec![byt!("mycooltbl"), byt!("keymap(str, wth)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
        let it = vec![byt!("mycooltbl"), byt!("keymap(wth1, wth2)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::UNKNOWN_DATA_TYPE
        );
    }
    #[test]
    fn test_table_bad_model() {
        let it = vec![byt!("mycooltbl"), byt!("wthmap(wth, wth)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::UNKNOWN_MODEL
        );
        let it = vec![byt!("mycooltbl"), byt!("wthmap(str, str)")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::UNKNOWN_MODEL
        );
        let it = vec![byt!("mycooltbl"), byt!("wthmap()")].into_iter();
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::UNKNOWN_MODEL
        );
    }
    #[test]
    fn test_table_malformed_expr() {
        let it = bi!("mycooltbl", "keymap(");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(,");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(,,");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap),");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap),,");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap),,)");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(,)");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(,,)");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap,");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap,,");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap,,)");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(str,");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(str,str");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(str,str,");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(str,str,)");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let it = bi!("mycooltbl", "keymap(str,str,),");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
    }

    #[test]
    fn test_table_too_many_args() {
        let it = bi!("mycooltbl", "keymap(str, str, str)");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::TOO_MANY_ARGUMENTS
        );

        // this should be valid for not-yet-known data types too
        let it = bi!("mycooltbl", "keymap(wth, wth, wth)");
        assert_eq!(
            parse_table_args(it).unwrap_err(),
            responses::groups::TOO_MANY_ARGUMENTS
        );
    }
}

mod entity_parser_tests {
    use super::parser::get_query_entity;
    use crate::protocol::responses;
    #[test]
    fn test_query_full_entity_okay() {
        let x = byt!("ks:tbl");
        assert_eq!(
            get_query_entity(&x).unwrap(),
            (Some("ks".as_bytes()), Some("tbl".as_bytes()))
        );
    }
    #[test]
    fn test_query_half_entity() {
        let x = byt!("tbl");
        assert_eq!(
            get_query_entity(&x).unwrap(),
            (None, Some("tbl".as_bytes()))
        )
    }
    #[test]
    fn test_query_entity_badexpr() {
        let x = byt!("ks:");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!(":");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!(":tbl");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("::");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("::ks");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("ks::tbl");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("ks::");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("ks::tbl::");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
        let x = byt!("::ks::tbl::");
        assert_eq!(
            get_query_entity(&x).unwrap_err(),
            responses::groups::BAD_EXPRESSION
        );
    }
}
