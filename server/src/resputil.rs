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

use corelib::builders::{self, response::*};
use std::error::Error;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::TcpStream;

/// # `ExceptFor`
///
/// This object provides a way to build responses which return the specialized
/// type `^`, that is the `Except` response. It implements most of the response
/// traits and hence can be added **as a group** or can be added **to a group**.
///
/// To clarify what's going on here - we must keep this in mind: The response code `1`
/// also implies a `Nil` value, since it signals that the value doesn't exist. The
/// "Except" response is a more efficient way of handling such `Nil` values. So let's say
/// that we ran `EXISTS x y z` and only x exists. If we naively create a response,
/// this will be returned:
/// ```text
/// &3\n
/// !0\n
/// !1\n
/// !1\n
/// ```
/// The last two lines are very wasteful, as soon as the query gets larger. For an example,
/// say we did 100 EXISTS, we'd return 100 `!0`s which is simple a waste of bytes. To augment
/// this, the `ExceptFor` type exists. Though we could've returned the count of the number of elements
/// which existed, it would not provide any information on what existed and what didn't which might be
/// needed at times.
pub struct ExceptFor {
    df_ext: Vec<u8>,
}

const EXFOR_CAP: usize = 2 * 10;

impl ExceptFor {
    pub fn new() -> Self {
        let mut df_ext = Vec::with_capacity(EXFOR_CAP + 2);
        df_ext.push(b'^');
        ExceptFor { df_ext }
    }
    pub fn with_space_for(howmany: usize) -> Self {
        let mut df_ext = vec![b'^'];
        df_ext.reserve(howmany);
        ExceptFor { df_ext }
    }
    /// This will essentially add 'idx,' to the `df_ext` field as bytes
    pub fn add(&mut self, idx: usize) {
        self.df_ext.extend(idx.to_string().into_bytes());
        self.df_ext.push(b',');
    }
}

impl IntoRespGroup for ExceptFor {
    fn into_resp_group(self) -> RGTuple {
        // self_len is the length of the exceptfor line in bytes
        let self_len = self.df_ext.len().to_string().into_bytes();
        // The size_of(self_len) + size_of('#1#<bytes>')
        let mut metalayout = Vec::with_capacity(self_len.len() + 4);
        let mut dataframe = Vec::with_capacity(self.df_ext.len() + 3);
        metalayout.extend(&[b'#', b'2', b'#']);
        metalayout.extend(self_len);
        // Preallocate capacity for the dataframe: df_ext.len() + len("&1\n")
        dataframe.extend(&[b'&', b'1', b'\n']);
        unsafe {
            // Add a newline
            let self_ptr = &self as *const _;
            let self_mut_ptr = self_ptr as *mut ExceptFor;
            let mut_ref = &mut (*self_mut_ptr);
            let _ = mem::replace(&mut mut_ref.df_ext[(*self_mut_ptr).df_ext.len() - 1], b'\n');
        }
        dataframe.extend(self.df_ext);
        (metalayout, dataframe)
    }
}

#[cfg(test)]
#[test]
fn test_intorespgroup_trait_impl_exceptfor() {
    let mut exceptfor = ExceptFor::new();
    exceptfor.add(1);
    exceptfor.add(2);
    let (ml, df) = exceptfor.into_resp_group();
    assert_eq!("#2#5".as_bytes().to_owned(), ml);
    assert_eq!("&1\n^1,2\n".as_bytes().to_owned(), df);
}

impl IntoResponse for ExceptFor {
    fn into_response(self) -> Response {
        let (mut metalayout_ext, dataframe) = self.into_resp_group();
        metalayout_ext.push(b'\n');
        let mut metaline = Vec::with_capacity(builders::MLINE_BUF);
        metaline.extend(&[b'*', b'!']);
        metaline.extend(dataframe.len().to_string().into_bytes());
        metaline.push(b'!');
        metaline.extend(metalayout_ext.len().to_string().into_bytes());
        metaline.push(b'\n');
        (metaline, metalayout_ext, dataframe)
    }
}

#[cfg(test)]
#[test]
fn test_intoresponse_trait_impl_exceptfor() {
    let mut exceptfor = ExceptFor::new();
    exceptfor.add(1);
    exceptfor.add(3);
    let (r1, r2, r3) = exceptfor.into_response();
    let r = [r1, r2, r3].concat();
    assert_eq!("*!8!5\n#2#5\n&1\n^1,3\n".as_bytes().to_owned(), r);
}

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
pub trait Writable {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send + Sync + 's>>;
}

impl Writable for (Vec<u8>, Vec<u8>, Vec<u8>) {
    fn write<'s>(
        self,
        con: &'s mut BufWriter<TcpStream>,
    ) -> Pin<Box<(dyn Future<Output = Result<(), Box<dyn Error>>> + Send + Sync + 's)>>
    where
        Self: Sync,
    {
        async fn write_bytes(
            con: &mut BufWriter<TcpStream>,
            (abyte, bbyte, cbyte): Response,
        ) -> Result<(), Box<dyn Error>> {
            con.write_all(&abyte).await?;
            con.write_all(&bbyte).await?;
            con.write_all(&cbyte).await?;
            Ok(())
        }
        Box::pin(write_bytes(con, self))
    }
}
