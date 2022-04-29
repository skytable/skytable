/*
 * Created on Fri Apr 29 2022
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
    interface::{ProtocolRead, ProtocolSpec, ProtocolWrite},
    ParseError, Skyhash2,
};
use crate::{
    corestore::buffers::Integer64,
    dbnet::connection::{QueryWithAdvance, RawConnection, Stream},
    util::FutureResult,
    IoResult,
};
use tokio::io::AsyncWriteExt;

impl ProtocolSpec for Skyhash2 {
    const TSYMBOL_STRING: u8 = b'+';
    const TSYMBOL_BINARY: u8 = b'?';
    const TSYMBOL_FLOAT: u8 = b'%';
    const TSYMBOL_INT64: u8 = b':';
    const TSYMBOL_TYPED_ARRAY: u8 = b'@';
    const TSYMBOL_TYPED_NON_NULL_ARRAY: u8 = b'^';
    const TSYMBOL_ARRAY: u8 = b'&';
    const TSYMBOL_FLAT_ARRAY: u8 = b'_';
    const TYPE_TYPED_ARRAY_ELEMENT_NULL: &'static [u8] = b"\0";
    const SIMPLE_QUERY_HEADER: &'static [u8] = b"*";
    const PIPELINED_QUERY_FIRST_BYTE: u8 = b'$';
}

impl<Strm, T> ProtocolRead<Skyhash2, Strm> for T
where
    T: RawConnection<Skyhash2, Strm> + Send + Sync,
    Strm: Stream,
{
    fn try_query(&self) -> Result<QueryWithAdvance, ParseError> {
        Skyhash2::parse(self.get_buffer())
    }
}

impl<Strm, T> ProtocolWrite<Skyhash2, Strm> for T
where
    T: RawConnection<Skyhash2, Strm> + Send + Sync,
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
            stream.write_all(&[Skyhash2::TSYMBOL_STRING]).await?;
            // length
            let len_bytes = Integer64::from(string.len());
            stream.write_all(&len_bytes).await?;
            // LF
            stream.write_all(&[Skyhash2::LF]).await?;
            // payload
            stream.write_all(string.as_bytes()).await
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
            stream.write_all(&[Skyhash2::TSYMBOL_BINARY]).await?;
            // length
            let len_bytes = Integer64::from(binary.len());
            stream.write_all(&len_bytes).await?;
            // LF
            stream.write_all(&[Skyhash2::LF]).await?;
            // payload
            stream.write_all(binary).await
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
        Box::pin(async move {
            let stream = self.get_mut_stream();
            // tsymbol
            stream.write_all(&[Skyhash2::TSYMBOL_INT64]).await?;
            // body
            stream.write_all(&Integer64::from(size)).await?;
            // LF
            stream.write_all(&[Skyhash2::LF]).await
        })
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
            stream.write_all(&[Skyhash2::TSYMBOL_INT64]).await?;
            // body
            stream.write_all(&Integer64::from(int)).await?;
            // LF
            stream.write_all(&[Skyhash2::LF]).await
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
            stream.write_all(&[Skyhash2::TSYMBOL_FLOAT]).await?;
            // body
            stream.write_all(float.to_string().as_bytes()).await?;
            // LF
            stream.write_all(&[Skyhash2::LF]).await
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
            stream.write_all(&[Skyhash2::LF]).await?;
            // body
            stream.write_all(element).await
        })
    }
}
