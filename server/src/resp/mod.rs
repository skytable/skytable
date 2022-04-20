/*
 * Created on Mon Aug 17 2020
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

#![allow(clippy::needless_lifetimes)]

//! Utilities for generating responses, which are only used by the `server`
//!
use crate::corestore::buffers::Integer64;
use crate::corestore::memstore::ObjectID;
use crate::util::FutureResult;
use bytes::Bytes;
use std::io::Error as IoError;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
pub mod writer;

pub const TSYMBOL_UNICODE_STRING: u8 = b'+';
pub const TSYMBOL_FLOAT: u8 = b'%';

type FutureIoResult<'s> = FutureResult<'s, Result<(), IoError>>;

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
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s>;
}

pub trait IsConnection: std::marker::Sync + std::marker::Send {
    fn write_lowlevel<'s>(&'s mut self, bytes: &'s [u8]) -> FutureIoResult<'s>;
}

impl<T> IsConnection for T
where
    T: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync,
{
    fn write_lowlevel<'s>(&'s mut self, bytes: &'s [u8]) -> FutureIoResult<'s> {
        Box::pin(self.write_all(bytes))
    }
}

/// A `BytesWrapper` object wraps around a `Bytes` object that might have been pulled
/// from `Corestore`.
///
/// This wrapper exists to prevent trait implementation conflicts when
/// an impl for `fmt::Display` may be implemented upstream
#[derive(Debug, PartialEq)]
pub struct BytesWrapper(pub Bytes);

impl BytesWrapper {
    pub fn finish_into_bytes(self) -> Bytes {
        self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct StringWrapper(pub String);

impl Writable for StringWrapper {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move {
            con.write_lowlevel(&[TSYMBOL_UNICODE_STRING]).await?;
            // Now get the size of the Bytes object as bytes
            let size = Integer64::from(self.0.len());
            // Write this to the stream
            con.write_lowlevel(&size).await?;
            // Now write a LF character
            con.write_lowlevel(&[b'\n']).await?;
            // Now write the REAL bytes (of the object)
            con.write_lowlevel(self.0.as_bytes()).await?;
            Ok(())
        })
    }
}

impl Writable for Vec<u8> {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move { con.write_lowlevel(&self).await })
    }
}

impl<const N: usize> Writable for [u8; N] {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move { con.write_lowlevel(&self).await })
    }
}

impl Writable for &'static [u8] {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move { con.write_lowlevel(self).await })
    }
}

impl Writable for &'static str {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move {
            // First write a `+` character to the stream since this is a
            // string (we represent `String`s as `Byte` objects internally)
            // and since `Bytes` are effectively `String`s we will append the
            // type operator `+` to the stream
            con.write_lowlevel(&[TSYMBOL_UNICODE_STRING]).await?;
            // Now get the size of the Bytes object as bytes
            let size = Integer64::from(self.len());
            // Write this to the stream
            con.write_lowlevel(&size).await?;
            // Now write a LF character
            con.write_lowlevel(&[b'\n']).await?;
            // Now write the REAL bytes (of the object)
            con.write_lowlevel(self.as_bytes()).await?;
            Ok(())
        })
    }
}

impl Writable for BytesWrapper {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move {
            // First write a `+` character to the stream since this is a
            // string (we represent `String`s as `Byte` objects internally)
            // and since `Bytes` are effectively `String`s we will append the
            // type operator `+` to the stream
            let bytes = self.finish_into_bytes();
            con.write_lowlevel(&[TSYMBOL_UNICODE_STRING]).await?;
            // Now get the size of the Bytes object as bytes
            let size = Integer64::from(bytes.len());
            // Write this to the stream
            con.write_lowlevel(&size).await?;
            // Now write a LF character
            con.write_lowlevel(&[b'\n']).await?;
            // Now write the REAL bytes (of the object)
            con.write_lowlevel(&bytes).await?;
            Ok(())
        })
    }
}

impl Writable for usize {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move {
            con.write_lowlevel(b":").await?;
            let usize_bytes = Integer64::from(self);
            con.write_lowlevel(&usize_bytes).await?;
            con.write_lowlevel(b"\n").await?;
            Ok(())
        })
    }
}

impl Writable for u64 {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move {
            con.write_lowlevel(b":").await?;
            let usize_bytes = Integer64::from(self);
            con.write_lowlevel(&usize_bytes).await?;
            con.write_lowlevel(b"\n").await?;
            Ok(())
        })
    }
}

impl Writable for ObjectID {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move {
            // First write a `+` character to the stream since this is a
            // string (we represent `String`s as `Byte` objects internally)
            // and since `Bytes` are effectively `String`s we will append the
            // type operator `+` to the stream
            con.write_lowlevel(&[TSYMBOL_UNICODE_STRING]).await?;
            // Now get the size of the Bytes object as bytes
            let size = Integer64::from(self.len());
            // Write this to the stream
            con.write_lowlevel(&size).await?;
            // Now write a LF character
            con.write_lowlevel(&[b'\n']).await?;
            // Now write the REAL bytes (of the object)
            con.write_lowlevel(&self).await?;
            Ok(())
        })
    }
}

impl Writable for f32 {
    fn write<'s>(self, con: &'s mut impl IsConnection) -> FutureIoResult<'s> {
        Box::pin(async move {
            let payload = self.to_string();
            con.write_lowlevel(&[TSYMBOL_FLOAT]).await?;
            con.write_lowlevel(payload.as_bytes()).await?;
            con.write_lowlevel(&[b'\n']).await?;
            Ok(())
        })
    }
}
