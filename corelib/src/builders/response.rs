/*
 * Created on Tue Aug 04 2020
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

//! # The `Response` module
//! This module can be used to build responses that can be sent to clients.
//! It prepares packets following the Terrapipe protocol.

use super::{DF_BUF, MLINE_BUF, ML_BUF};
use crate::terrapipe::RespCodes;
use bytes::Bytes;

/// The size to skip through, dataframe bytes
type PRTuple = (usize, Vec<u8>);

/// The bytes for the sizes (inclusive of the `#` character), the bytes for the df
type RGTuple = (Vec<u8>, Vec<u8>);

/// The metaline, metalayout and dataframe in order
pub type Response = (Vec<u8>, Vec<u8>, Vec<u8>);

pub struct BytesWrapper(pub Bytes);

/// This trait will return: ([<symbol>, val.into_bytes(), '\n'], len(1+val.len()))
pub trait PreResp {
    fn into_pre_resp(self) -> PRTuple;
}

/// Trait implementors should return a data group in the response packet.
/// It should have a structure, which has the following general format:
/// ```text
/// &<n>\n
/// <symbol>item[0]\n
/// <symbol>item[1]\n
/// ...
/// <symbol>item[n-1]\n
/// <symbol>item[n]\n
/// ```
pub trait IntoRespGroup {
    fn into_resp_group(self) -> RGTuple;
}

/// Trait implementors must return a **complete** response packet  
/// A complete response packet looks like:
/// ```text
/// <* or $>!<CONTENT_LENGTH>!<METALAYOUT_LENGTH>\n
/// #a#b#c#d ...\n
/// < --- data --- >
/// ```
pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl<T> PreResp for T
where
    T: ToString,
{
    fn into_pre_resp(self) -> PRTuple {
        let self_bytes = self.to_string().into_bytes();
        let mut df_bytes = Vec::with_capacity(2 + self_bytes.len());
        df_bytes.push(b'+');
        df_bytes.extend(self_bytes);
        df_bytes.push(b'\n');
        (df_bytes.len() - 1, df_bytes)
    }
}

impl PreResp for BytesWrapper {
    fn into_pre_resp(self) -> PRTuple {
        let df_bytes = [&[b'+'], &self.0[..], &[b'\n']].concat();
        (df_bytes.len() - 1, df_bytes)
    }
}

impl PreResp for RespCodes {
    fn into_pre_resp(self) -> PRTuple {
        let bytes = match self {
            RespCodes::Okay => "!0\n".as_bytes().to_owned(),
            RespCodes::NotFound => "!1\n".as_bytes().to_owned(),
            RespCodes::OverwriteError => "!2\n".as_bytes().to_owned(),
            RespCodes::PacketError => "!3\n".as_bytes().to_owned(),
            RespCodes::ActionError => "!4\n".as_bytes().to_owned(),
            RespCodes::ServerError => "!5\n".as_bytes().to_owned(),
            RespCodes::OtherError(maybe_err) => {
                if let Some(err) = maybe_err {
                    let mut vc = Vec::with_capacity(err.len() + 2);
                    vc.push(b'!');
                    vc.extend(err.into_bytes());
                    vc.push(b'\n');
                    vc
                } else {
                    "!6\n".as_bytes().to_owned()
                }
            }
        };
        (bytes.len() - 1, bytes)
    }
}

#[cfg(test)]
#[test]
fn test_preresp_trait_impl_respcodes() {
    let okay_resp = RespCodes::Okay.into_pre_resp();
    assert_eq!(2, okay_resp.0);
    assert_eq!("!0\n".as_bytes().to_owned(), okay_resp.1);
}

#[cfg(test)]
#[test]
fn test_preresp_trait_impl_blanket() {
    let blanket_resp = 23.into_pre_resp();
    assert_eq!(blanket_resp.0, 3);
    assert_eq!("+23\n".as_bytes().to_owned(), blanket_resp.1);
}

#[cfg(test)]
#[test]
fn test_preresp_trait_impl_byteswrapper() {
    let bytes_wrapper = BytesWrapper(Bytes::from("coolvalue"));
    let bytes_wrapper_resp = bytes_wrapper.into_pre_resp();
    assert_eq!(10, bytes_wrapper_resp.0);
    assert_eq!("+coolvalue\n".as_bytes().to_owned(), bytes_wrapper_resp.1);
}

// For responses which just have one response code as a group
impl IntoRespGroup for RespCodes {
    fn into_resp_group(self) -> RGTuple {
        let (size, data) = self.into_pre_resp();
        let self_bytes = size.to_string().into_bytes();
        let mut metalayout_ext = Vec::with_capacity(3 + self_bytes.len());
        metalayout_ext.extend(&[b'#', b'2', b'#']);
        metalayout_ext.extend(size.to_string().into_bytes());
        let mut dataframe_ext = Vec::with_capacity(3 + data.len());
        dataframe_ext.extend(&[b'&', b'1', b'\n']);
        dataframe_ext.extend(data);
        (metalayout_ext, dataframe_ext)
    }
}

impl IntoRespGroup for BytesWrapper {
    fn into_resp_group(self) -> RGTuple {
        let (size, data) = self.into_pre_resp();
        let size_string = size.to_string().into_bytes();
        let mut metalayout_ext = Vec::with_capacity(3 + (size_string.len()));
        metalayout_ext.extend(&[b'#', b'2', b'#']);
        metalayout_ext.extend(size_string);
        let mut dataframe_ext = Vec::with_capacity(3 + data.len());
        dataframe_ext.extend(&[b'&', b'1', b'\n']);
        dataframe_ext.extend(data);
        (metalayout_ext, dataframe_ext)
    }
}

impl IntoRespGroup for RespGroup {
    fn into_resp_group(self) -> RGTuple {
        // Get the number of items in the data group, convert it into it's UTF-8
        // equivalent.
        let self_size_into_bytes = self.sizes.len().to_string().into_bytes();
        let mut sizeline = Vec::with_capacity(2 + self_size_into_bytes.len());
        sizeline.push(b'&');
        sizeline.extend(self_size_into_bytes);
        sizeline.push(b'\n');
        // Now we have the &<n>\n line
        // All we need to know is: add this line to the data bytes
        // also we need to add the len of the sizeline - 1 to the sizes
        let sizes: Vec<u8> = self
            .sizes
            .into_iter()
            .map(|size| {
                let size_as_bytes = size.to_string().into_bytes();
                let mut vc = Vec::with_capacity(size_as_bytes.len() + 1);
                vc.push(b'#');
                vc.extend(size_as_bytes);
                vc
            })
            .flatten()
            .collect();
        let metalayout = [
            vec![b'#'],
            (sizeline.len() - 1).to_string().into_bytes().to_vec(),
            sizes,
        ]
        .concat();
        let dataframe = [sizeline, self.df_bytes].concat();
        (metalayout, dataframe)
    }
}
// For responses which just have one value
impl<T> IntoRespGroup for T
where
    T: ToString,
{
    fn into_resp_group(self) -> RGTuple {
        let st = self.to_string().into_bytes();
        let st_len = (st.len() + 1).to_string().into_bytes();
        let mut metalayout = Vec::with_capacity(3 + st_len.len());
        metalayout.extend(&[b'#', b'2', b'#']);
        metalayout.extend(st_len);
        let mut dataframe = Vec::with_capacity(5 + st.len());
        dataframe.extend(&[b'&', b'1', b'\n', b'+']);
        dataframe.extend(st);
        dataframe.push(b'\n');
        (metalayout, dataframe)
    }
}
#[cfg(test)]
#[test]
fn test_respgroup_trait_impl_datagroup() {
    let mut dg = RespGroup::new();
    dg.add_item("HEYA");
    dg.add_item(String::from("sayan"));
    let (layout, df) = dg.into_resp_group();
    assert_eq!("&2\n+HEYA\n+sayan\n".as_bytes().to_vec(), df);
    assert_eq!("#2#5#6".as_bytes().to_vec(), layout);
    let one_string_response = "OKAY".into_resp_group();
    assert_eq!("&1\n+OKAY\n".as_bytes().to_owned(), one_string_response.1);
    assert_eq!("#2#5".as_bytes().to_owned(), one_string_response.0);
    let mut dg = RespGroup::new();
    dg.add_item(100);
    let (layout, df) = dg.into_resp_group();
    assert_eq!("&1\n+100\n".as_bytes().to_owned(), df);
    assert_eq!("#2#4".as_bytes().to_owned(), layout);
}

#[cfg(test)]
#[test]
fn test_respgroup_trait_impl_respcodes() {
    let dg = RespCodes::Okay.into_resp_group();
    assert_eq!("#2#2".as_bytes().to_owned(), dg.0);
    assert_eq!("&1\n!0\n".as_bytes().to_owned(), dg.1);
}

#[cfg(test)]
#[test]
fn test_respgroup_trait_impl_blanket() {
    let dg = "four".into_resp_group();
    assert_eq!("#2#5".as_bytes().to_owned(), dg.0);
    assert_eq!("&1\n+four\n".as_bytes().to_owned(), dg.1);
}

#[cfg(test)]
#[test]
fn test_respgroup_trait_impl_byteswrapper() {
    let dg = BytesWrapper(Bytes::from("coolvalue")).into_resp_group();
    assert_eq!("#2#10".as_bytes().to_owned(), dg.0);
    assert_eq!("&1\n+coolvalue\n".as_bytes().to_owned(), dg.1);
}

/// A response group which is a data group in a dataframe
pub struct RespGroup {
    /// The skips sequence as `usize`s (i.e `#1#2#3...`)
    sizes: Vec<usize>,
    /// The bytes which can be appended to an existing dataframe
    df_bytes: Vec<u8>,
}

impl RespGroup {
    /// Create a new `RespGroup`
    pub fn new() -> Self {
        RespGroup {
            sizes: Vec::with_capacity(ML_BUF),
            df_bytes: Vec::with_capacity(DF_BUF),
        }
    }
    /// Add an item to the `RespGroup`
    pub fn add_item<T: PreResp>(&mut self, item: T) {
        let (size, append_bytes) = item.into_pre_resp();
        self.df_bytes.extend(append_bytes);
        self.sizes.push(size);
    }
}

pub enum ResponseBuilder {
    Simple(SResp),
    // TODO(@ohsayan): Add pipelined response builder
}

impl ResponseBuilder {
    pub fn new_simple() -> SResp {
        SResp::new()
    }
}

pub struct SResp {
    metaline: Vec<u8>,
    metalayout: Vec<u8>,
    dataframe: Vec<u8>,
}

impl SResp {
    pub fn new() -> Self {
        let mut metaline = Vec::with_capacity(MLINE_BUF);
        metaline.push(b'*');
        metaline.push(b'!');
        SResp {
            metaline,
            metalayout: Vec::with_capacity(ML_BUF),
            dataframe: Vec::with_capacity(DF_BUF),
        }
    }
    pub fn add_group<T: IntoRespGroup>(&mut self, group: T) {
        let (metalayout_ext, dataframe_ext) = group.into_resp_group();
        self.dataframe.extend(dataframe_ext);
        self.metalayout.extend(metalayout_ext);
    }
}

impl IntoResponse for SResp {
    fn into_response(self) -> Response {
        /* UNSAFE(@ohsayan): We know what we're doing here: We convert an immutable reference
        to an `SRESP` to avoid `concat`ing over and over again
         */
        unsafe {
            // Convert the immutable references to mutable references
            // because we don't want to use concat()
            let self_ptr = &self as *const _;
            let self_mut = self_ptr as *mut SResp;
            // We need to add a newline to the metalayout
            (*self_mut).metalayout.push(b'\n');
            // Now add the content length + ! + metalayout length + '\n'
            (*self_mut)
                .metaline
                .extend(self.dataframe.len().to_string().into_bytes());
            (*self_mut).metaline.push(b'!');
            (*self_mut)
                .metaline
                .extend((*self_mut).metalayout.len().to_string().into_bytes());
            (*self_mut).metaline.push(b'\n');
        } // The raw pointers are dropped here
        (self.metaline, self.metalayout, self.dataframe)
    }
}

// For an entire response which only comprises of a single value
impl<T> IntoResponse for T
where
    T: ToString,
{
    fn into_response(self) -> Response {
        let (mut metalayout, dataframe) = self.to_string().into_resp_group();
        metalayout.push(b'\n');
        let df_len_bytes = dataframe.len().to_string().into_bytes();
        let ml_len_bytes = metalayout.len().to_string().into_bytes();
        let mut metaline = Vec::with_capacity(5 + df_len_bytes.len() + ml_len_bytes.len());
        metaline.extend(&[b'*', b'!']);
        metaline.extend(df_len_bytes);
        metaline.push(b'!');
        metaline.extend(ml_len_bytes);
        metaline.push(b'\n');
        (metaline, metalayout, dataframe)
    }
}

impl IntoResponse for BytesWrapper {
    fn into_response(self) -> Response {
        let (metalayout, dataframe) = self.into_resp_group();
        let df_len_bytes = dataframe.len().to_string().into_bytes();
        let ml_len_bytes = metalayout.len().to_string().into_bytes();
        let mut metaline = Vec::with_capacity(4 + df_len_bytes.len() + ml_len_bytes.len());
        metaline.extend(&[b'*', b'!']);
        metaline.extend(df_len_bytes);
        metaline.push(b'!');
        metaline.extend(ml_len_bytes);
        (metaline, metalayout, dataframe)
    }
}

impl IntoResponse for RespCodes {
    fn into_response(self) -> Response {
        let (mut metalayout, dataframe) = self.into_resp_group();
        metalayout.push(b'\n');
        let df_len_bytes = dataframe.len().to_string().into_bytes();
        let ml_len_bytes = metalayout.len().to_string().into_bytes();
        let mut metaline = Vec::with_capacity(4 + df_len_bytes.len() + ml_len_bytes.len());
        metaline.extend(&[b'*', b'!']);
        metaline.extend(df_len_bytes);
        metaline.push(b'!');
        metaline.extend(ml_len_bytes);
        metaline.push(b'\n');
        (metaline, metalayout, dataframe)
    }
}

#[cfg(test)]
#[test]
fn test_intoresponse_trait_impl_datagroup() {
    let mut datagroup = RespGroup::new();
    datagroup.add_item("HEY!");
    datagroup.add_item("four");
    datagroup.add_item(RespCodes::Okay);
    let mut builder = ResponseBuilder::new_simple();
    builder.add_group(datagroup);
    let resp = builder.into_response();
    assert_eq!(
        "*!18!9\n#2#5#5#2\n&3\n+HEY!\n+four\n!0\n"
            .as_bytes()
            .to_owned(),
        [resp.0, resp.1, resp.2].concat()
    );
    let mut builder = ResponseBuilder::new_simple();
    builder.add_group(RespCodes::Okay);
    let resp = builder.into_response();
    assert_eq!(
        "*!6!5\n#2#2\n&1\n!0\n".as_bytes().to_owned(),
        [resp.0, resp.1, resp.2].concat()
    );
    let mut builder = ResponseBuilder::new_simple();
    builder.add_group("four");
    let resp = builder.into_response();
    assert_eq!(
        "*!9!5\n#2#5\n&1\n+four\n".as_bytes().to_owned(),
        [resp.0, resp.1, resp.2].concat()
    );
}
