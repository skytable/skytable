/*
 * Created on Mon Aug 17 2020
 *
 * This file is a part of the source code for the Terrabase database
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
/// As we will eventually move towards a second
/// iteration of the structure of response packets, we will need to let several
/// items to be able to write to the stream.
/*
HACK(@ohsayan): Since `async` is not supported in traits just yet, we will have to
use explicit declarations for asynchoronous functions
*/
#[derive(Debug, PartialEq)]
pub struct BytesWrapper(Bytes);

impl BytesWrapper {
    pub fn from_bytes(bytes: Bytes) -> Self {
        BytesWrapper(bytes)
    }
    pub fn finish_into_bytes(self) -> Bytes {
        self.0
    }
}

impl<T> From<T> for BytesWrapper
where
    T: Into<Vec<u8>>,
{
    fn from(vec: T) -> Self {
        BytesWrapper(Bytes::from(vec.into()))
    }
}

pub trait Writable {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + Sync + 's>>;
}

impl Writable for BytesWrapper {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<dyn Error>>> + Send + Sync + 's)>>
    where
        Self: Sync,
    {
        async fn write_bytes(
            con: &mut BufWriter<TcpStream>,
            resp: Vec<u8>,
        ) -> Result<(), Box<dyn Error>> {
            con.write_all(&resp).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self.finish_into_bytes().to_vec()))
    }
}

impl<T> Writable for T
where
    T: ToString,
{
    fn write<'s>(
        self,
        _: &'s mut tokio::io::BufWriter<tokio::net::TcpStream>,
    ) -> std::pin::Pin<
        std::boxed::Box<
            (dyn std::future::Future<
                Output = std::result::Result<
                    (),
                    std::boxed::Box<(dyn std::error::Error + 'static)>,
                >,
            > + std::marker::Send
                 + std::marker::Sync
                 + 's),
        >,
    > {
        // Writing a `T` which can be represented as a string is essentially like
        // writing a positive response
        todo!()
    }
}
