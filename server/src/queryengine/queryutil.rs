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

//! Utilities for handling queries
//!

use crate::protocol::Connection;
use corelib::builders::response::*;
use corelib::TResult;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

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
pub struct ExceptFor(Vec<usize>);

const EXCEPTFOR_CAP: usize = 10;

impl ExceptFor {
    /// Create a new `ExceptFor` instance
    pub fn new() -> Self {
        ExceptFor(Vec::with_capacity(EXCEPTFOR_CAP))
    }
    /// Add an index to `ExceptFor`
    pub fn add(&mut self, idx: usize) {
        self.0.push(idx);
    }
    /// Drop the object returning the inner-vector
    pub fn finish_into_vec(self) -> Vec<usize> {
        self.0
    }
}

impl IntoRespGroup for ExceptFor {
    fn into_resp_group(self) -> (Vec<u8>, Vec<u8>) {
        let mut except_for_line = Vec::with_capacity((self.0.len() * 2) + 2);
        except_for_line.push(b'^');
        let mut it = self.0.into_iter().peekable();
        while let Some(item) = it.next() {
            except_for_line.extend(item.to_string().as_bytes());
            if it.peek().is_some() {
                except_for_line.push(b',');
            } else {
                except_for_line.push(b'\n');
            }
        }
        let mut metalayout_ext = Vec::with_capacity(EXCEPTFOR_CAP);
        metalayout_ext.push(b'#');
        metalayout_ext.push(b'1');
        metalayout_ext.push(b'#');
        metalayout_ext.extend(except_for_line.len().to_string().as_bytes());
        let dataframe_ext = [vec![b'&', b'1', b'\n'], except_for_line].concat();
        (metalayout_ext, dataframe_ext)
    }
}

impl IntoResponse for ExceptFor {
    fn into_response(self) -> Response {
        let (mut metalayout_ext, df_ext) = self.into_resp_group();
        metalayout_ext.push(b'\n');
        let metaline = [
            &[b'*', b'!'],
            df_ext.len().to_string().as_bytes(),
            &[b'!'],
            metalayout_ext.len().to_string().as_bytes(),
            &[b'\n'],
        ]
        .concat();
        (metaline, metalayout_ext, df_ext)
    }
}

#[test]
fn test_exceptfor() {
    let mut exfor = ExceptFor::new();
    exfor.add(1);
    exfor.add(2);
    let (r1, r2, r3) = exfor.into_response();
    let r = [r1, r2, r3].concat();
    assert_eq!("*!8!5\n#1#5\n&1\n^1,2\n".as_bytes().to_owned(), r);
}

pub trait Writable {
    fn write<'s, T>(
        self,
        con: &mut Connection,
    ) -> Pin<Box<dyn Future<Output = T> + Send + Sync + 's>>;
}

impl Writable for (Vec<u8>, Vec<u8>, Vec<u8>) {
    fn write<'s, T>(
        self,
        con: &mut Connection,
    ) -> Pin<Box<(dyn Future<Output = T> + Send + Sync + 's)>> {
        async fn write_bytes(con: &mut Connection, tuple: Response) -> Result<(), Box<dyn Error>> {
            con.write_response(tuple).await
        }
        todo!()
    }
}
