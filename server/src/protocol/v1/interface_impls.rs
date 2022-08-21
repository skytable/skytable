/*
 * Created on Mon May 02 2022
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
    crate::{
        dbnet::QueryWithAdvance,
        protocol::{interface::ProtocolSpec, ParseError, Skyhash1},
    },
    ::sky_macros::compiled_eresp_bytes_v1 as eresp,
};

impl ProtocolSpec for Skyhash1 {
    // spec information
    const PROTOCOL_VERSION: f32 = 1.0;
    const PROTOCOL_VERSIONSTRING: &'static str = "Skyhash-1.0";

    // type symbols
    const TSYMBOL_STRING: u8 = b'+';
    const TSYMBOL_BINARY: u8 = b'?';
    const TSYMBOL_FLOAT: u8 = b'%';
    const TSYMBOL_INT64: u8 = b':';
    const TSYMBOL_TYPED_ARRAY: u8 = b'@';
    const TSYMBOL_TYPED_NON_NULL_ARRAY: u8 = b'^';
    const TSYMBOL_ARRAY: u8 = b'&';
    const TSYMBOL_FLAT_ARRAY: u8 = b'_';

    // typed array
    const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8] = b"\0";

    // metaframe
    const SIMPLE_QUERY_HEADER: &'static [u8] = b"*";
    const PIPELINED_QUERY_FIRST_BYTE: u8 = b'$';

    // respcodes
    const RCODE_OKAY: &'static [u8] = eresp!("0");
    const RCODE_NIL: &'static [u8] = eresp!("1");
    const RCODE_OVERWRITE_ERR: &'static [u8] = eresp!("2");
    const RCODE_ACTION_ERR: &'static [u8] = eresp!("3");
    const RCODE_PACKET_ERR: &'static [u8] = eresp!("4");
    const RCODE_SERVER_ERR: &'static [u8] = eresp!("5");
    const RCODE_OTHER_ERR_EMPTY: &'static [u8] = eresp!("6");
    const RCODE_UNKNOWN_ACTION: &'static [u8] = eresp!("Unknown action");
    const RCODE_WRONGTYPE_ERR: &'static [u8] = eresp!("7");
    const RCODE_UNKNOWN_DATA_TYPE: &'static [u8] = eresp!("8");
    const RCODE_ENCODING_ERROR: &'static [u8] = eresp!("9");

    // respstrings
    const RSTRING_SNAPSHOT_BUSY: &'static [u8] = eresp!("err-snapshot-busy");
    const RSTRING_SNAPSHOT_DISABLED: &'static [u8] = eresp!("err-snapshot-disabled");
    const RSTRING_SNAPSHOT_DUPLICATE: &'static [u8] = eresp!("duplicate-snapshot");
    const RSTRING_SNAPSHOT_ILLEGAL_NAME: &'static [u8] = eresp!("err-invalid-snapshot-name");
    const RSTRING_ERR_ACCESS_AFTER_TERMSIG: &'static [u8] = eresp!("err-access-after-termsig");

    // keyspace related resps
    const RSTRING_DEFAULT_UNSET: &'static [u8] = eresp!("default-container-unset");
    const RSTRING_CONTAINER_NOT_FOUND: &'static [u8] = eresp!("container-not-found");
    const RSTRING_STILL_IN_USE: &'static [u8] = eresp!("still-in-use");
    const RSTRING_PROTECTED_OBJECT: &'static [u8] = eresp!("err-protected-object");
    const RSTRING_WRONG_MODEL: &'static [u8] = eresp!("wrong-model");
    const RSTRING_ALREADY_EXISTS: &'static [u8] = eresp!("err-already-exists");
    const RSTRING_NOT_READY: &'static [u8] = eresp!("not-ready");
    const RSTRING_DDL_TRANSACTIONAL_FAILURE: &'static [u8] = eresp!("transactional-failure");
    const RSTRING_UNKNOWN_DDL_QUERY: &'static [u8] = eresp!("unknown-ddl-query");
    const RSTRING_BAD_EXPRESSION: &'static [u8] = eresp!("malformed-expression");
    const RSTRING_UNKNOWN_MODEL: &'static [u8] = eresp!("unknown-model");
    const RSTRING_TOO_MANY_ARGUMENTS: &'static [u8] = eresp!("too-many-args");
    const RSTRING_CONTAINER_NAME_TOO_LONG: &'static [u8] = eresp!("container-name-too-long");
    const RSTRING_BAD_CONTAINER_NAME: &'static [u8] = eresp!("bad-container-name");
    const RSTRING_UNKNOWN_INSPECT_QUERY: &'static [u8] = eresp!("unknown-inspect-query");
    const RSTRING_UNKNOWN_PROPERTY: &'static [u8] = eresp!("unknown-property");
    const RSTRING_KEYSPACE_NOT_EMPTY: &'static [u8] = eresp!("keyspace-not-empty");
    const RSTRING_BAD_TYPE_FOR_KEY: &'static [u8] = eresp!("bad-type-for-key");
    const RSTRING_LISTMAP_BAD_INDEX: &'static [u8] = eresp!("bad-list-index");
    const RSTRING_LISTMAP_LIST_IS_EMPTY: &'static [u8] = eresp!("list-is-empty");

    // elements
    const ELEMRESP_HEYA: &'static [u8] = b"+4\nHEY!\n";

    // full responses
    const FULLRESP_RCODE_PACKET_ERR: &'static [u8] = b"*1\n!1\n4\n";
    const FULLRESP_RCODE_WRONG_TYPE: &'static [u8] = b"*1\n!1\n7\n";

    // auth rcodes/strings
    const AUTH_ERROR_ALREADYCLAIMED: &'static [u8] = eresp!("err-auth-already-claimed");
    const AUTH_CODE_BAD_CREDENTIALS: &'static [u8] = eresp!("10");
    const AUTH_ERROR_DISABLED: &'static [u8] = eresp!("err-auth-disabled");
    const AUTH_CODE_PERMS: &'static [u8] = eresp!("11");
    const AUTH_ERROR_ILLEGAL_USERNAME: &'static [u8] = eresp!("err-auth-illegal-username");
    const AUTH_ERROR_FAILED_TO_DELETE_USER: &'static [u8] = eresp!("err-auth-deluser-fail");

    // bql respstrings
    const BQL_BAD_EXPRESSION: &'static [u8] = eresp!("bql-bad-expression");
    const BQL_EXPECTED_STMT: &'static [u8] = eresp!("bql-expected-statement");
    const BQL_INVALID_NUMERIC_LITERAL: &'static [u8] = eresp!("bql-bad-numeric-literal");
    const BQL_INVALID_STRING_LITERAL: &'static [u8] = eresp!("bql-bad-string-literal");
    const BQL_INVALID_SYNTAX: &'static [u8] = eresp!("bql-invalid-syntax");
    const BQL_UNEXPECTED_EOF: &'static [u8] = eresp!("bql-unexpected-eof");
    const BQL_UNKNOWN_CREATE_QUERY: &'static [u8] = eresp!("bql-unknown-create-query");
    const BQL_UNSUPPORTED_MODEL_DECL: &'static [u8] = eresp!("bql-unsupported-model-decl");
    const BQL_UNEXPECTED_CHAR: &'static [u8] = eresp!("bql-unexpected-char");

    const NEEDS_TERMINAL_LF: bool = true;

    fn decode_packet(input: &[u8]) -> Result<QueryWithAdvance, ParseError> {
        Skyhash1::parse(input)
    }
}
