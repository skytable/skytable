/*
 * Created on Mon Aug 17 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! Utilities for generating responses, which are only used by the `server`
//!

use bytes::Bytes;
use libtdb::terrapipe::RespCodes;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;

/// # The `Writable` trait
/// All trait implementors are given access to an asynchronous stream to which
/// they must write a response.
///
pub trait Writable {
    /*
    HACK(@ohsayan): Since `async` is not supported in traits just yet, we will have to
    use explicit declarations for asynchoronous functions
    */
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + Sync + 's>>;
}

/// A `BytesWrapper` object wraps around a `Bytes` object that might have been pulled
/// from `CoreDB`.
///
/// This wrapper exists to prevent trait implementation conflicts when
/// an impl for `fmt::Display` may be implemented upstream
#[derive(Debug, PartialEq)]
pub struct BytesWrapper(pub Bytes);

/// This indicates the beginning of a response group in a response.
///
/// It holds the number of items to be written and writes:
/// ```text
/// #<self.0.to_string().len().to_string().into_bytes()>\n
/// &<self.0.to_string()>\n
/// ```
#[derive(Debug, PartialEq)]
pub struct GroupBegin(pub usize);

impl BytesWrapper {
    pub fn finish_into_bytes(self) -> Bytes {
        self.0
    }
}

impl Writable for Vec<u8> {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut BufWriter<TcpStream>,
            resp: Vec<u8>,
        ) -> Result<(), Box<dyn Error>> {
            con.write(&resp).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}

impl Writable for BytesWrapper {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut BufWriter<TcpStream>,
            bytes: Bytes,
        ) -> Result<(), Box<dyn Error>> {
            // First write a `+` character to the stream since this is a
            // string (we represent `String`s as `Byte` objects internally)
            // and since `Bytes` are effectively `String`s we will append the
            // type operator `+` to the stream
            con.write(&[b'+']).await?;
            // Now get the size of the Bytes object as bytes
            let size = bytes.len().to_string().into_bytes();
            // Write this to the stream
            con.write(&size).await?;
            // Now write a LF character
            con.write(&[b'\n']).await?;
            // Now write the REAL bytes (of the object)
            con.write(&bytes).await?;
            // Now write another LF
            con.write(&[b'\n']).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self.finish_into_bytes()))
    }
}

impl Writable for RespCodes {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut BufWriter<TcpStream>,
            code: RespCodes,
        ) -> Result<(), Box<dyn Error>> {
            // Self's tsymbol is !
            // The length of the response code is 1
            // And we need a newline
            con.write(&[b'!', b'1', b'\n']).await?;
            // We need to get the u8 version of the response code
            let code: u8 = code.into();
            // We need the UTF8 equivalent of the response code
            let code_bytes = code.to_string().into_bytes();
            con.write(&code_bytes).await?;
            // Now append a newline
            con.write(&[b'\n']).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}

impl Writable for GroupBegin {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut BufWriter<TcpStream>,
            size: usize,
        ) -> Result<(), Box<dyn Error>> {
            con.write(b"#2\n*1\n").await?;
            // First write a `#` which indicates that the next bytes give the
            // prefix length
            con.write(&[b'#']).await?;
            let group_len_as_bytes = size.to_string().into_bytes();
            let group_prefix_len_as_bytes = (group_len_as_bytes.len() + 1).to_string().into_bytes();
            // Now write Self's len as bytes
            con.write(&group_prefix_len_as_bytes).await?;
            // Now write a LF and '&' which signifies the beginning of a datagroup
            con.write(&[b'\n', b'&']).await?;
            // Now write the number of items in the datagroup as bytes
            con.write(&group_len_as_bytes).await?;
            // Now write a '\n' character
            con.write(&[b'\n']).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self.0))
    }
}

impl Writable for usize {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut BufWriter<TcpStream>,
            val: usize,
        ) -> Result<(), Box<dyn Error>> {
            con.write(b":").await?;
            let usize_bytes = val.to_string().into_bytes();
            let usize_bytes_len = usize_bytes.len().to_string().into_bytes();
            con.write(&usize_bytes_len).await?;
            con.write(b"\n").await?;
            con.write(&usize_bytes).await?;
            con.write(b"\n").await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}
