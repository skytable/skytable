/*
 * Created on Sat Aug 22 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

//! Primitives for generating Skyhash compatible responses

pub mod groups {
    #![allow(unused)]
    //! # Pre-compiled response **elements**
    //! These are pre-compiled response groups and **not** complete responses. If complete
    //! responses are required, user protocol::responses::fresp
    use ::sky_macros::compiled_eresp_bytes as eresp;
    /// Response code 0 as a array element
    pub const OKAY: &[u8] = eresp!("0");
    /// Response code 1 as a array element
    pub const NIL: &[u8] = eresp!("1");
    /// Response code 2 as a array element
    pub const OVERWRITE_ERR: &[u8] = eresp!("2");
    /// Response code 3 as a array element
    pub const ACTION_ERR: &[u8] = eresp!("3");
    /// Response code 4 as a array element
    pub const PACKET_ERR: &[u8] = eresp!("4");
    /// Response code 5 as a array element
    pub const SERVER_ERR: &[u8] = eresp!("5");
    /// Response code 6 as a array element
    pub const OTHER_ERR_EMPTY: &[u8] = eresp!("6");
    /// Response group element with string "HEYA"
    pub const HEYA: &[u8] = "+4\nHEY!".as_bytes();
    /// "Unknown action" error response
    pub const UNKNOWN_ACTION: &[u8] = eresp!("Unknown action");
    /// Response code 7
    pub const WRONGTYPE_ERR: &[u8] = eresp!("7");
    /// Response code 8
    pub const UNKNOWN_DATA_TYPE: &[u8] = eresp!("8");
    /// Response code 9 as an array element
    pub const ENCODING_ERROR: &[u8] = eresp!("9");
    /// Snapshot busy error
    pub const SNAPSHOT_BUSY: &[u8] = eresp!("err-snapshot-busy");
    /// Snapshot disabled (other error)
    pub const SNAPSHOT_DISABLED: &[u8] = eresp!("err-snapshot-disabled");
    /// Duplicate snapshot
    pub const SNAPSHOT_DUPLICATE: &[u8] = eresp!("duplicate-snapshot");
    /// Snapshot has illegal name (other error)
    pub const SNAPSHOT_ILLEGAL_NAME: &[u8] = eresp!("err-invalid-snapshot-name");
    /// Access after termination signal (other error)
    pub const ERR_ACCESS_AFTER_TERMSIG: &[u8] = eresp!("err-access-after-termsig");

    // keyspace related resps
    /// The default container was not set
    pub const DEFAULT_UNSET: &[u8] = eresp!("default-container-unset");
    /// The container was not found
    pub const CONTAINER_NOT_FOUND: &[u8] = eresp!("container-not-found");
    /// The container is still in use and so cannot be removed
    pub const STILL_IN_USE: &[u8] = eresp!("still-in-use");
    /// This is a protected object and hence cannot be accessed
    pub const PROTECTED_OBJECT: &[u8] = eresp!("err-protected-object");
    /// The action was applied against the wrong model
    pub const WRONG_MODEL: &[u8] = eresp!("wrong-model");
    /// The container already exists
    pub const ALREADY_EXISTS: &[u8] = eresp!("err-already-exists");
    /// The container is not ready
    pub const NOT_READY: &[u8] = eresp!("not-ready");
    /// A transactional failure occurred
    pub const DDL_TRANSACTIONAL_FAILURE: &[u8] = eresp!("transactional-failure");
    /// An unknown DDL query was run
    pub const UNKNOWN_DDL_QUERY: &[u8] = eresp!("unknown-ddl-query");
    /// The expression for a DDL query was malformed
    pub const BAD_EXPRESSION: &[u8] = eresp!("malformed-expression");
    /// An unknown model was passed in a DDL query
    pub const UNKNOWN_MODEL: &[u8] = eresp!("unknown-model");
    /// Too many arguments were passed to model constructor
    pub const TOO_MANY_ARGUMENTS: &[u8] = eresp!("too-many-args");
    /// The container name is too long
    pub const CONTAINER_NAME_TOO_LONG: &[u8] = eresp!("container-name-too-long");
    /// The container name contains invalid characters
    pub const BAD_CONTAINER_NAME: &[u8] = eresp!("bad-container-name");
    /// An unknown inspect query
    pub const UNKNOWN_INSPECT_QUERY: &[u8] = eresp!("unknown-inspect-query");
    /// An unknown table property was passed
    pub const UNKNOWN_PROPERTY: &[u8] = eresp!("unknown-property");
    /// The keyspace is not empty and hence cannot be removed
    pub const KEYSPACE_NOT_EMPTY: &[u8] = eresp!("keyspace-not-empty");
    /// Bad type supplied in a DDL query for the key
    pub const BAD_TYPE_FOR_KEY: &[u8] = eresp!("bad-type-for-key");
    /// The index for the provided list was non-existent
    pub const LISTMAP_BAD_INDEX: &[u8] = eresp!("bad-list-index");
    /// The list is empty
    pub const LISTMAP_LIST_IS_EMPTY: &[u8] = eresp!("list-is-empty");
}

pub mod full_responses {
    #![allow(unused)]
    //! # Pre-compiled **responses**
    //! These are pre-compiled **complete** responses. This means that they should
    //! be written off directly to the stream and should **not be preceded by any response metaframe**

    /// Response code: 0 (Okay)
    pub const R_OKAY: &[u8] = "*!1\n0\n".as_bytes();
    /// Response code: 1 (Nil)
    pub const R_NIL: &[u8] = "*!1\n1\n".as_bytes();
    /// Response code: 2 (Overwrite Error)
    pub const R_OVERWRITE_ERR: &[u8] = "*!1\n2\n".as_bytes();
    /// Response code: 3 (Action Error)
    pub const R_ACTION_ERR: &[u8] = "*!1\n3\n".as_bytes();
    /// Response code: 4 (Packet Error)
    pub const R_PACKET_ERR: &[u8] = "*!1\n4\n".as_bytes();
    /// Response code: 5 (Server Error)
    pub const R_SERVER_ERR: &[u8] = "*!1\n5\n".as_bytes();
    /// Response code: 6 (Other Error _without description_)
    pub const R_OTHER_ERR_EMPTY: &[u8] = "*!1\n6\n".as_bytes();
    /// Response code: 7; wrongtype
    pub const R_WRONGTYPE_ERR: &[u8] = "*!1\n7".as_bytes();
    /// Response code: 8; unknown data type
    pub const R_UNKNOWN_DATA_TYPE: &[u8] = "*!1\n8\n".as_bytes();
    /// A heya response
    pub const R_HEYA: &[u8] = "*+4\nHEY!\n".as_bytes();
    /// An other response with description: "Unknown action"
    pub const R_UNKNOWN_ACTION: &[u8] = "*!14\nUnknown action\n".as_bytes();
    /// A 0 uint64 reply
    pub const R_ONE_INT_REPLY: &[u8] = "*:1\n1\n".as_bytes();
    /// A 1 uint64 reply
    pub const R_ZERO_INT_REPLY: &[u8] = "*:1\n0\n".as_bytes();
    /// Snapshot busy (other error)
    pub const R_SNAPSHOT_BUSY: &[u8] = "*!17\nerr-snapshot-busy\n".as_bytes();
    /// Snapshot disabled (other error)
    pub const R_SNAPSHOT_DISABLED: &[u8] = "*!21\nerr-snapshot-disabled\n".as_bytes();
    /// Snapshot has illegal name (other error)
    pub const R_SNAPSHOT_ILLEGAL_NAME: &[u8] = "*!25\nerr-invalid-snapshot-name\n".as_bytes();
    /// Access after termination signal (other error)
    pub const R_ERR_ACCESS_AFTER_TERMSIG: &[u8] = "*!24\nerr-access-after-termsig\n".as_bytes();
}
