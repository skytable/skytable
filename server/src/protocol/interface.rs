/*
 * Created on Tue Apr 26 2022
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
    super::ParseError,
    crate::{
        corestore::booltable::{BytesBoolTable, BytesNicheLUT},
        dbnet::QueryWithAdvance,
    },
};

/// The `ProtocolSpec` trait is used to define the character set and pre-generated elements
/// and responses for a protocol version.
pub trait ProtocolSpec: Send + Sync {
    // spec information

    /// The Skyhash protocol version
    const PROTOCOL_VERSION: f32;
    /// The Skyhash protocol version string (Skyhash-x.y)
    const PROTOCOL_VERSIONSTRING: &'static str;

    // type symbols
    /// Type symbol for unicode strings
    const TSYMBOL_STRING: u8;
    /// Type symbol for blobs
    const TSYMBOL_BINARY: u8;
    /// Type symbol for float
    const TSYMBOL_FLOAT: u8;
    /// Type symbok for int64
    const TSYMBOL_INT64: u8;
    /// Type symbol for typed array
    const TSYMBOL_TYPED_ARRAY: u8;
    /// Type symbol for typed non-null array
    const TSYMBOL_TYPED_NON_NULL_ARRAY: u8;
    /// Type symbol for an array
    const TSYMBOL_ARRAY: u8;
    /// Type symbol for a flat array
    const TSYMBOL_FLAT_ARRAY: u8;

    // charset
    /// The line-feed character or separator
    const LF: u8 = b'\n';

    // metaframe
    /// The header for simple queries
    const SIMPLE_QUERY_HEADER: &'static [u8];
    /// The header for pipelined queries (excluding length, obviously)
    const PIPELINED_QUERY_FIRST_BYTE: u8;

    // typed array
    /// Null element represenation for a typed array
    const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8];

    // respcodes
    /// Respcode 0: Okay
    const RCODE_OKAY: &'static [u8];
    /// Respcode 1: Nil
    const RCODE_NIL: &'static [u8];
    /// Respcode 2: Overwrite error
    const RCODE_OVERWRITE_ERR: &'static [u8];
    /// Respcode 3: Action error
    const RCODE_ACTION_ERR: &'static [u8];
    /// Respcode 4: Packet error
    const RCODE_PACKET_ERR: &'static [u8];
    /// Respcode 5: Server error
    const RCODE_SERVER_ERR: &'static [u8];
    /// Respcode 6: Other error
    const RCODE_OTHER_ERR_EMPTY: &'static [u8];
    /// Respcode 7: Unknown action
    const RCODE_UNKNOWN_ACTION: &'static [u8];
    /// Respcode 8: Wrongtype error
    const RCODE_WRONGTYPE_ERR: &'static [u8];
    /// Respcode 9: Unknown data type error
    const RCODE_UNKNOWN_DATA_TYPE: &'static [u8];
    /// Respcode 10: Encoding error
    const RCODE_ENCODING_ERROR: &'static [u8];

    // respstrings
    /// Respstring when snapshot engine is busy
    const RSTRING_SNAPSHOT_BUSY: &'static [u8];
    /// Respstring when snapshots are disabled
    const RSTRING_SNAPSHOT_DISABLED: &'static [u8];
    /// Respstring when duplicate snapshot creation is attempted
    const RSTRING_SNAPSHOT_DUPLICATE: &'static [u8];
    /// Respstring when snapshot has illegal chars
    const RSTRING_SNAPSHOT_ILLEGAL_NAME: &'static [u8];
    /// Respstring when a **very bad error** happens (use after termsig)
    const RSTRING_ERR_ACCESS_AFTER_TERMSIG: &'static [u8];
    /// Respstring when the default container is unset
    const RSTRING_DEFAULT_UNSET: &'static [u8];
    /// Respstring when the container is not found
    const RSTRING_CONTAINER_NOT_FOUND: &'static [u8];
    /// Respstring when the container is still in use, but a _free_ op is attempted
    const RSTRING_STILL_IN_USE: &'static [u8];
    /// Respstring when a protected container is attempted to be accessed/modified
    const RSTRING_PROTECTED_OBJECT: &'static [u8];
    /// Respstring when an action is not suitable for the current table model
    const RSTRING_WRONG_MODEL: &'static [u8];
    /// Respstring when the container already exists
    const RSTRING_ALREADY_EXISTS: &'static [u8];
    /// Respstring when the container is not ready
    const RSTRING_NOT_READY: &'static [u8];
    /// Respstring when a DDL transaction fails
    const RSTRING_DDL_TRANSACTIONAL_FAILURE: &'static [u8];
    /// Respstring when an unknow DDL query is run (`CREATE BLAH`, for example)
    const RSTRING_UNKNOWN_DDL_QUERY: &'static [u8];
    /// Respstring when a bad DDL expression is run
    const RSTRING_BAD_EXPRESSION: &'static [u8];
    /// Respstring when an unsupported model is attempted to be used during table creation
    const RSTRING_UNKNOWN_MODEL: &'static [u8];
    /// Respstring when too many arguments are passed to a DDL query
    const RSTRING_TOO_MANY_ARGUMENTS: &'static [u8];
    /// Respstring when the container name is too long
    const RSTRING_CONTAINER_NAME_TOO_LONG: &'static [u8];
    /// Respstring when the container name
    const RSTRING_BAD_CONTAINER_NAME: &'static [u8];
    /// Respstring when an unknown inspect query is run (`INSPECT blah`, for example)
    const RSTRING_UNKNOWN_INSPECT_QUERY: &'static [u8];
    /// Respstring when an unknown table property is passed during table creation
    const RSTRING_UNKNOWN_PROPERTY: &'static [u8];
    /// Respstring when a non-empty keyspace is attempted to be dropped
    const RSTRING_KEYSPACE_NOT_EMPTY: &'static [u8];
    /// Respstring when a bad type is provided for a key in the K/V engine (like using a `list`
    /// for the key)
    const RSTRING_BAD_TYPE_FOR_KEY: &'static [u8];
    /// Respstring when a non-existent index is attempted to be accessed in a list
    const RSTRING_LISTMAP_BAD_INDEX: &'static [u8];
    /// Respstring when a list is empty and we attempt to access/modify it
    const RSTRING_LISTMAP_LIST_IS_EMPTY: &'static [u8];

    // element responses
    /// A string element containing the text "HEY!"
    const ELEMRESP_HEYA: &'static [u8];

    // full responses
    /// A **full response** for a packet error
    const FULLRESP_RCODE_PACKET_ERR: &'static [u8];
    /// A **full response** for a wrongtype error
    const FULLRESP_RCODE_WRONG_TYPE: &'static [u8];

    // LUTs
    /// A LUT for SET operations
    const SET_NLUT: BytesNicheLUT = BytesNicheLUT::new(
        Self::RCODE_ENCODING_ERROR,
        Self::RCODE_OKAY,
        Self::RCODE_OVERWRITE_ERR,
    );
    /// A LUT for lists
    const OKAY_BADIDX_NIL_NLUT: BytesNicheLUT = BytesNicheLUT::new(
        Self::RCODE_NIL,
        Self::RCODE_OKAY,
        Self::RSTRING_LISTMAP_BAD_INDEX,
    );
    /// A LUT for SET operations
    const OKAY_OVW_BLUT: BytesBoolTable =
        BytesBoolTable::new(Self::RCODE_OKAY, Self::RCODE_OVERWRITE_ERR);
    /// A LUT for UPDATE operations
    const UPDATE_NLUT: BytesNicheLUT = BytesNicheLUT::new(
        Self::RCODE_ENCODING_ERROR,
        Self::RCODE_OKAY,
        Self::RCODE_NIL,
    );
    const SKYHASH_PARSE_ERROR_LUT: [&'static [u8]; 4] = [
        Self::FULLRESP_RCODE_PACKET_ERR,
        Self::FULLRESP_RCODE_PACKET_ERR,
        Self::FULLRESP_RCODE_WRONG_TYPE,
        Self::FULLRESP_RCODE_WRONG_TYPE,
    ];

    // auth error respstrings
    /// respstring: already claimed (user was already claimed)
    const AUTH_ERROR_ALREADYCLAIMED: &'static [u8];
    /// respcode(10): bad credentials (either bad creds or invalid user)
    const AUTH_CODE_BAD_CREDENTIALS: &'static [u8];
    /// respstring: auth is disabled
    const AUTH_ERROR_DISABLED: &'static [u8];
    /// respcode(11): Insufficient permissions (same for anonymous user)
    const AUTH_CODE_PERMS: &'static [u8];
    /// respstring: ID is too long
    const AUTH_ERROR_ILLEGAL_USERNAME: &'static [u8];
    /// respstring: ID is protected/in use
    const AUTH_ERROR_FAILED_TO_DELETE_USER: &'static [u8];

    // BlueQL respstrings
    const BQL_BAD_EXPRESSION: &'static [u8];
    const BQL_EXPECTED_STMT: &'static [u8];
    const BQL_INVALID_NUMERIC_LITERAL: &'static [u8];
    const BQL_INVALID_STRING_LITERAL: &'static [u8];
    const BQL_INVALID_SYNTAX: &'static [u8];
    const BQL_UNEXPECTED_EOF: &'static [u8];
    const BQL_UNKNOWN_CREATE_QUERY: &'static [u8];
    const BQL_UNSUPPORTED_MODEL_DECL: &'static [u8];
    const BQL_UNEXPECTED_CHAR: &'static [u8];

    /// The body is terminated by a linefeed
    const NEEDS_TERMINAL_LF: bool;

    fn decode_packet(input: &[u8]) -> Result<QueryWithAdvance, ParseError>;
}
