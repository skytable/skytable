/*
 * Created on Thu Aug 12 2021
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

use crate::corestore::buffers::Integer64;
use crate::corestore::Data;
use crate::dbnet::connection::ProtocolConnectionExt;
use crate::protocol::responses::groups;
use crate::IoResult;
use core::marker::PhantomData;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

pub struct Writer<'a, T, Strm> {
    tsymbol: [u8; 1],
    con: &'a mut T,
    _owned: PhantomData<Strm>,
}

impl<'a, T, Strm> Writer<'a, T, Strm>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    pub unsafe fn new(con: &'a mut T, tsymbol: u8) -> Self {
        Self {
            tsymbol: [tsymbol; 1],
            con,
            _owned: PhantomData,
        }
    }
    pub async fn write_nil(&mut self) -> IoResult<()> {
        self.con.write_response(groups::NIL).await
    }
    pub async fn write_rawstring(&mut self, payload: impl AsRef<[u8]>) -> IoResult<()> {
        let payload = payload.as_ref();
        let raw_stream = unsafe { self.con.raw_stream() };
        raw_stream.write_all(&self.tsymbol).await?; // first write tsymbol
        let bytes = Integer64::from(payload.len());
        raw_stream.write_all(&bytes).await?; // then len
        raw_stream.write_all(&[b'\n']).await?; // LF
        raw_stream.write_all(payload).await?; // payload
        raw_stream.write_all(&[b'\n']).await?; // final LF
        Ok(())
    }
    pub async fn write_encoding_error(&mut self) -> IoResult<()> {
        self.con.write_response(groups::ENCODING_ERROR).await
    }
    pub async fn write_server_err(&mut self) -> IoResult<()> {
        self.con.write_response(groups::SERVER_ERR).await
    }
}

/// Write a raw mono group with a custom tsymbol
pub async unsafe fn write_raw_mono<T, Strm>(
    con: &mut T,
    tsymbol: u8,
    payload: &Data,
) -> IoResult<()>
where
    T: ProtocolConnectionExt<Strm>,
    Strm: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    let raw_stream = unsafe { con.raw_stream() };
    raw_stream.write_all(&[tsymbol; 1]).await?; // first write tsymbol
    let bytes = Integer64::from(payload.len());
    raw_stream.write_all(&bytes).await?; // then len
    raw_stream.write_all(&[b'\n']).await?; // LF
    raw_stream.write_all(payload).await?; // payload
    raw_stream.write_all(&[b'\n']).await?; // final LF
    Ok(())
}
