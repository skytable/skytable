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
use std::io::Error as IoError;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

/// # The `Writable` trait
/// All trait implementors are given access to an asynchronous stream to which
/// they must write a response.
///
/// Every `write()` call makes a call to the [`IsConnection`](./IsConnection)'s
/// `write_lowlevel` function, which in turn writes something to the underlying stream.
///
/// Do note that this write **doesn't gurantee immediate completion** as the underlying
/// stream might use buffering. So, the best idea would be to use to use the `flush()`
/// call on the stream.
pub trait Writable {
    /*
    HACK(@ohsayan): Since `async` is not supported in traits just yet, we will have to
    use explicit declarations for asynchoronous functions
    */
    fn write<'s>(
        self,
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + Sync + 's>>;
}

pub trait IsConnection: std::marker::Sync + std::marker::Send {
    fn write_lowlevel<'s>(
        &'s mut self,
        bytes: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = Result<usize, IoError>> + Send + Sync + 's>>;
}

impl IsConnection for BufWriter<TcpStream> {
    fn write_lowlevel<'s>(
        &'s mut self,
        bytes: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = Result<usize, IoError>> + Send + Sync + 's>> {
        Box::pin(self.write(bytes))
    }
}

impl IsConnection for SslStream<TcpStream> {
    fn write_lowlevel<'s>(
        &'s mut self,
        bytes: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = Result<usize, IoError>> + Send + Sync + 's>> {
        Box::pin(self.write(bytes))
    }
}

impl IsConnection for BufWriter<SslStream<TcpStream>> {
    fn write_lowlevel<'s>(
        &'s mut self,
        bytes: &'s [u8],
    ) -> Pin<Box<dyn Future<Output = Result<usize, IoError>> + Send + Sync + 's>> {
        Box::pin(self.write(bytes))
    }
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
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut impl IsConnection,
            resp: Vec<u8>,
        ) -> Result<(), Box<dyn Error>> {
            con.write_lowlevel(&resp).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}

impl Writable for &'static [u8] {
    fn write<'s>(
        self,
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut impl IsConnection,
            resp: &[u8],
        ) -> Result<(), Box<dyn Error>> {
            con.write_lowlevel(&resp).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, &self))
    }
}

impl Writable for BytesWrapper {
    fn write<'s>(
        self,
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut impl IsConnection,
            bytes: Bytes,
        ) -> Result<(), Box<dyn Error>> {
            // First write a `+` character to the stream since this is a
            // string (we represent `String`s as `Byte` objects internally)
            // and since `Bytes` are effectively `String`s we will append the
            // type operator `+` to the stream
            con.write_lowlevel(&[b'+']).await?;
            // Now get the size of the Bytes object as bytes
            let size = bytes.len().to_string().into_bytes();
            // Write this to the stream
            con.write_lowlevel(&size).await?;
            // Now write a LF character
            con.write_lowlevel(&[b'\n']).await?;
            // Now write the REAL bytes (of the object)
            con.write_lowlevel(&bytes).await?;
            // Now write another LF
            con.write_lowlevel(&[b'\n']).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self.finish_into_bytes()))
    }
}

impl Writable for RespCodes {
    fn write<'s>(
        self,
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut impl IsConnection,
            code: RespCodes,
        ) -> Result<(), Box<dyn Error>> {
            if let RespCodes::OtherError(Some(e)) = code {
                // Since this is an other error which contains a description
                // we'll write !<no_of_bytes> followed by the string
                con.write_lowlevel(&[b'!']).await?;
                // Convert the string into a vector of bytes
                let e = e.to_string().into_bytes();
                // Now get the length of the byte vector and turn it into
                // a string and then into a byte vector
                let len_as_bytes = e.len().to_string().into_bytes();
                // Write the length
                con.write_lowlevel(&len_as_bytes).await?;
                // Then an LF
                con.write_lowlevel(&[b'\n']).await?;
                // Then the error string
                con.write_lowlevel(&e).await?;
                // Then another LF
                con.write_lowlevel(&[b'\n']).await?;
                // And now we're done
                return Ok(());
            }
            // Self's tsymbol is !
            // The length of the response code is 1
            // And we need a newline
            con.write_lowlevel(&[b'!', b'1', b'\n']).await?;
            // We need to get the u8 version of the response code
            let code: u8 = code.into();
            // We need the UTF8 equivalent of the response code
            let code_bytes = code.to_string().into_bytes();
            con.write_lowlevel(&code_bytes).await?;
            // Now append a newline
            con.write_lowlevel(&[b'\n']).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}

impl Writable for GroupBegin {
    fn write<'s>(
        self,
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut impl IsConnection,
            size: usize,
        ) -> Result<(), Box<dyn Error>> {
            con.write_lowlevel(b"#2\n*1\n").await?;
            // First write a `#` which indicates that the next bytes give the
            // prefix length
            con.write_lowlevel(&[b'#']).await?;
            let group_len_as_bytes = size.to_string().into_bytes();
            let group_prefix_len_as_bytes = (group_len_as_bytes.len() + 1).to_string().into_bytes();
            // Now write Self's len as bytes
            con.write_lowlevel(&group_prefix_len_as_bytes).await?;
            // Now write a LF and '&' which signifies the beginning of a datagroup
            con.write_lowlevel(&[b'\n', b'&']).await?;
            // Now write the number of items in the datagroup as bytes
            con.write_lowlevel(&group_len_as_bytes).await?;
            // Now write a '\n' character
            con.write_lowlevel(&[b'\n']).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self.0))
    }
}

impl Writable for usize {
    fn write<'s>(
        self,
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(
            con: &mut impl IsConnection,
            val: usize,
        ) -> Result<(), Box<dyn Error>> {
            con.write_lowlevel(b":").await?;
            let usize_bytes = val.to_string().into_bytes();
            let usize_bytes_len = usize_bytes.len().to_string().into_bytes();
            con.write_lowlevel(&usize_bytes_len).await?;
            con.write_lowlevel(b"\n").await?;
            con.write_lowlevel(&usize_bytes).await?;
            con.write_lowlevel(b"\n").await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}

impl Writable for u64 {
    fn write<'s>(
        self,
        con: &'s mut impl IsConnection,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<(dyn Error + 'static)>>> + Send + Sync + 's)>>
    {
        async fn write_bytes(con: &mut impl IsConnection, val: u64) -> Result<(), Box<dyn Error>> {
            con.write_lowlevel(b":").await?;
            let usize_bytes = val.to_string().into_bytes();
            let usize_bytes_len = usize_bytes.len().to_string().into_bytes();
            con.write_lowlevel(&usize_bytes_len).await?;
            con.write_lowlevel(b"\n").await?;
            con.write_lowlevel(&usize_bytes).await?;
            con.write_lowlevel(b"\n").await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}
