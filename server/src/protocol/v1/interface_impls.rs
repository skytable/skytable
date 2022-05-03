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
        corestore::buffers::Integer64,
        dbnet::connection::{QueryWithAdvance, RawConnection, Stream},
        protocol::{
            interface::{ProtocolRead, ProtocolSpec, ProtocolWrite},
            ParseError, Skyhash1,
        },
        util::FutureResult,
        IoResult,
    },
    ::sky_macros::compiled_eresp_bytes_v1 as eresp,
    tokio::io::AsyncWriteExt,
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
    const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8] = b"\0\n";

    // metaframe
    const SIMPLE_QUERY_HEADER: &'static [u8] = b"*1\n";
    const PIPELINED_QUERY_FIRST_BYTE: u8 = b'*';

    // respcodes
    /// Response code 0 as a array element
    const RCODE_OKAY: &'static [u8] = eresp!("0");
    /// Response code 1 as a array element
    const RCODE_NIL: &'static [u8] = eresp!("1");
    /// Response code 2 as a array element
    const RCODE_OVERWRITE_ERR: &'static [u8] = eresp!("2");
    /// Response code 3 as a array element
    const RCODE_ACTION_ERR: &'static [u8] = eresp!("3");
    /// Response code 4 as a array element
    const RCODE_PACKET_ERR: &'static [u8] = eresp!("4");
    /// Response code 5 as a array element
    const RCODE_SERVER_ERR: &'static [u8] = eresp!("5");
    /// Response code 6 as a array element
    const RCODE_OTHER_ERR_EMPTY: &'static [u8] = eresp!("6");
    /// "Unknown action" error response
    const RCODE_UNKNOWN_ACTION: &'static [u8] = eresp!("Unknown action");
    /// Response code 7
    const RCODE_WRONGTYPE_ERR: &'static [u8] = eresp!("7");
    /// Response code 8
    const RCODE_UNKNOWN_DATA_TYPE: &'static [u8] = eresp!("8");
    /// Response code 9 as an array element
    const RCODE_ENCODING_ERROR: &'static [u8] = eresp!("9");

    // respstrings

    /// Snapshot busy error
    const RSTRING_SNAPSHOT_BUSY: &'static [u8] = eresp!("err-snapshot-busy");
    /// Snapshot disabled (other error)
    const RSTRING_SNAPSHOT_DISABLED: &'static [u8] = eresp!("err-snapshot-disabled");
    /// Duplicate snapshot
    const RSTRING_SNAPSHOT_DUPLICATE: &'static [u8] = eresp!("duplicate-snapshot");
    /// Snapshot has illegal name (other error)
    const RSTRING_SNAPSHOT_ILLEGAL_NAME: &'static [u8] = eresp!("err-invalid-snapshot-name");
    /// Access after termination signal (other error)
    const RSTRING_ERR_ACCESS_AFTER_TERMSIG: &'static [u8] = eresp!("err-access-after-termsig");

    // keyspace related resps
    /// The default container was not set
    const RSTRING_DEFAULT_UNSET: &'static [u8] = eresp!("default-container-unset");
    /// The container was not found
    const RSTRING_CONTAINER_NOT_FOUND: &'static [u8] = eresp!("container-not-found");
    /// The container is still in use and so cannot be removed
    const RSTRING_STILL_IN_USE: &'static [u8] = eresp!("still-in-use");
    /// This is a protected object and hence cannot be accessed
    const RSTRING_PROTECTED_OBJECT: &'static [u8] = eresp!("err-protected-object");
    /// The action was applied against the wrong model
    const RSTRING_WRONG_MODEL: &'static [u8] = eresp!("wrong-model");
    /// The container already exists
    const RSTRING_ALREADY_EXISTS: &'static [u8] = eresp!("err-already-exists");
    /// The container is not ready
    const RSTRING_NOT_READY: &'static [u8] = eresp!("not-ready");
    /// A transactional failure occurred
    const RSTRING_DDL_TRANSACTIONAL_FAILURE: &'static [u8] = eresp!("transactional-failure");
    /// An unknown DDL query was run
    const RSTRING_UNKNOWN_DDL_QUERY: &'static [u8] = eresp!("unknown-ddl-query");
    /// The expression for a DDL query was malformed
    const RSTRING_BAD_EXPRESSION: &'static [u8] = eresp!("malformed-expression");
    /// An unknown model was passed in a DDL query
    const RSTRING_UNKNOWN_MODEL: &'static [u8] = eresp!("unknown-model");
    /// Too many arguments were passed to model constructor
    const RSTRING_TOO_MANY_ARGUMENTS: &'static [u8] = eresp!("too-many-args");
    /// The container name is too long
    const RSTRING_CONTAINER_NAME_TOO_LONG: &'static [u8] = eresp!("container-name-too-long");
    /// The container name contains invalid characters
    const RSTRING_BAD_CONTAINER_NAME: &'static [u8] = eresp!("bad-container-name");
    /// An unknown inspect query
    const RSTRING_UNKNOWN_INSPECT_QUERY: &'static [u8] = eresp!("unknown-inspect-query");
    /// An unknown table property was passed
    const RSTRING_UNKNOWN_PROPERTY: &'static [u8] = eresp!("unknown-property");
    /// The keyspace is not empty and hence cannot be removed
    const RSTRING_KEYSPACE_NOT_EMPTY: &'static [u8] = eresp!("keyspace-not-empty");
    /// Bad type supplied in a DDL query for the key
    const RSTRING_BAD_TYPE_FOR_KEY: &'static [u8] = eresp!("bad-type-for-key");
    /// The index for the provided list was non-existent
    const RSTRING_LISTMAP_BAD_INDEX: &'static [u8] = eresp!("bad-list-index");
    /// The list is empty
    const RSTRING_LISTMAP_LIST_IS_EMPTY: &'static [u8] = eresp!("list-is-empty");

    // elements
    const ELEMRESP_HEYA: &'static [u8] = b"+4\nHEY!\n";

    // full responses
    const FULLRESP_RCODE_PACKET_ERR: &'static [u8] = b"*1\n!1\n4\n";
    const FULLRESP_RCODE_WRONG_TYPE: &'static [u8] = b"*1\n!1\n7\n";
}

impl<Strm, T> ProtocolRead<Skyhash1, Strm> for T
where
    T: RawConnection<Skyhash1, Strm> + Send + Sync,
    Strm: Stream,
{
    fn try_query(&self) -> Result<QueryWithAdvance, ParseError> {
        Skyhash1::parse(self.get_buffer())
    }
}

impl<Strm, T> ProtocolWrite<Skyhash1, Strm> for T
where
    T: RawConnection<Skyhash1, Strm> + Send + Sync,
    Strm: Stream,
{
    fn write_string<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        string: &'life1 str,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: 'ret_life,
    {
        Box::pin(async move {
            let stream = self.get_mut_stream();
            // tsymbol
            stream.write_all(&[Skyhash1::TSYMBOL_STRING]).await?;
            // length
            let len_bytes = Integer64::from(string.len());
            stream.write_all(&len_bytes).await?;
            // LF
            stream.write_all(&[Skyhash1::LF]).await?;
            // payload
            stream.write_all(string.as_bytes()).await?;
            // final LF
            stream.write_all(&[Skyhash1::LF]).await
        })
    }
    fn write_binary<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        binary: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: 'ret_life,
    {
        Box::pin(async move {
            let stream = self.get_mut_stream();
            // tsymbol
            stream.write_all(&[Skyhash1::TSYMBOL_BINARY]).await?;
            // length
            let len_bytes = Integer64::from(binary.len());
            stream.write_all(&len_bytes).await?;
            // LF
            stream.write_all(&[Skyhash1::LF]).await?;
            // payload
            stream.write_all(binary).await?;
            // final LF
            stream.write_all(&[Skyhash1::LF]).await
        })
    }
    fn write_usize<'life0, 'ret_life>(
        &'life0 mut self,
        size: usize,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: 'ret_life,
    {
        Box::pin(async move { self.write_int64(size as _).await })
    }
    fn write_int64<'life0, 'ret_life>(
        &'life0 mut self,
        int: u64,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: 'ret_life,
    {
        Box::pin(async move {
            let stream = self.get_mut_stream();
            // tsymbol
            stream.write_all(&[Skyhash1::TSYMBOL_INT64]).await?;
            // get body and sizeline
            let body = Integer64::from(int);
            let body_len = Integer64::from(body.len());
            // len of body
            stream.write_all(&body_len).await?;
            // sizeline LF
            stream.write_all(&[Skyhash1::LF]).await?;
            // body
            stream.write_all(&body).await?;
            // LF
            stream.write_all(&[Skyhash1::LF]).await
        })
    }
    fn write_float<'life0, 'ret_life>(
        &'life0 mut self,
        float: f32,
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        Self: 'ret_life,
    {
        Box::pin(async move {
            let stream = self.get_mut_stream();
            // tsymbol
            stream.write_all(&[Skyhash1::TSYMBOL_FLOAT]).await?;
            // get body and sizeline
            let body = float.to_string();
            let body = body.as_bytes();
            let sizeline = Integer64::from(body.len());
            // sizeline
            stream.write_all(&sizeline).await?;
            // sizeline LF
            stream.write_all(&[Skyhash1::LF]).await?;
            // body
            stream.write_all(body).await?;
            // LF
            stream.write_all(&[Skyhash1::LF]).await
        })
    }
    fn write_typed_array_element<'life0, 'life1, 'ret_life>(
        &'life0 mut self,
        element: &'life1 [u8],
    ) -> FutureResult<'ret_life, IoResult<()>>
    where
        'life0: 'ret_life,
        'life1: 'ret_life,
        Self: 'ret_life,
    {
        Box::pin(async move {
            let stream = self.get_mut_stream();
            // len
            stream.write_all(&Integer64::from(element.len())).await?;
            // LF
            stream.write_all(&[Skyhash1::LF]).await?;
            // body
            stream.write_all(element).await?;
            // LF
            stream.write_all(&[Skyhash1::LF]).await
        })
    }
}
