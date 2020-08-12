/*
 * Created on Sun Aug 09 2020
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

//! This v2 of the Terrapipe protocol, or more like an attempt to build the
//! first stable version of Terrapipe.  
//! **WARNING ⚠**:  This is **purely experimental**  
//! **Note:** Checking with older benchmarks, the new evaluation would add to about
//! 1 microsec of processing time, as opposed to the 0.1 microsec processing time
//! of the previous layout. Nothing is finalized yet, but this is what it looks like
//! currently

#[derive(Debug, PartialEq)]
pub struct Action(Vec<String>);

type Dataset = Vec<String>;

pub enum Actions {
    DEL(Dataset),
    EXISTS(Dataset),
    GET(Dataset),
    HEYA,
    SET(Dataset),
    UPDATE(Dataset),
}

pub fn parse_df(buf: Vec<u8>, sizes: Vec<usize>, nc: usize) -> Option<Vec<Action>> {
    let (mut i, mut pos) = (0, 0);
    if buf.len() < 1 || sizes.len() < 1 {
        // Having fun, eh? Why're you giving empty dataframes?
        return None;
    }
    let mut tokens = Vec::with_capacity(nc);
    while i < sizes.len() && pos < buf.len() {
        // Allocate everything first
        unsafe {
            let cursize = sizes.get_unchecked(0);
            i += 1; // We've just read a line push it ahead
                    // Get the current line-> pos..pos+cursize+1
            let curline = match buf.get(pos..pos + cursize + 1) {
                Some(line) => line,
                None => return None,
            };
            // We've read `cursize` number of elements, so skip them
            // Also skip the newline
            pos += cursize + 1;
            if *curline.get_unchecked(0) == b'&' {
                // A valid action array
                let mut cursize = 0usize; // The number of elements in this action array
                let mut k = 1; // Skip the '&' character in `curline`
                while k < (curline.len() - 1) {
                    let cur_dig: usize = match curline.get_unchecked(k).checked_sub(48) {
                        Some(dig) => {
                            if dig > 9 {
                                // For the UTF8 character to be a number (0-9)
                                // `dig` must be lesser than 9, since `48` is the UTF8
                                // code for 0
                                return None;
                            } else {
                                dig.into()
                            }
                        }
                        None => return None,
                    };
                    cursize = (cursize * 10) + cur_dig;
                    k += 1;
                }
                let mut toks: Vec<String> = sizes
                    .iter()
                    .take(cursize)
                    .map(|sz| String::with_capacity(*sz))
                    .collect();
                let mut l = 0;
                // We now know the array size, so let's parse it!
                // Get all the sizes of the array elements
                let arr_elem_sizes = match sizes.get(i..(i + cursize)) {
                    Some(sizes) => sizes,
                    None => return None,
                };
                i += cursize; // We've already read `cursize` items from the `sizes` array
                arr_elem_sizes
                    .into_iter()
                    .zip(toks.iter_mut())
                    .for_each(|(size, empty_buf)| {
                        let extracted = match buf.get(pos..pos + size) {
                            Some(ex) => ex,
                            None => return (),
                        };
                        pos += size + 1; // Advance `pos` by `sz` and `1` for the newline
                        l += 1; // Move ahead
                        *empty_buf = String::from_utf8_lossy(extracted).to_string();
                    });
                if toks.len() != cursize {
                    return None;
                }
                // We're done with parsing the entire array, return it
                tokens.push(Action(toks));
            } else {
                i += 1;
                continue;
            }
        }
    }
    Some(tokens)
}

mod builders {
    pub const MLINE_BUF: usize = 46;
    pub const ML_BUF: usize = 64;
    pub const DF_BUF: usize = 256;
    mod response {
        use super::{DF_BUF, MLINE_BUF, ML_BUF};
        use crate::terrapipe::RespCodes;

        /// The the size to skip through, dataframe bytes
        type PRTuple = (usize, Vec<u8>);

        /// The bytes for the sizes (inclusive of the `#` character), the bytes for the df
        type RespTuple = (Vec<u8>, Vec<u8>);

        /// This trait will return: ([<symbol>, val.as_bytes(), '\n'], len(1+val.len()))
        pub trait PreResp {
            fn into_pre_resp(self) -> PRTuple;
        }

        /// Trait implementors should return a data group in the response packet. It have
        /// a structure, which has the following general format:
        /// ```text
        /// &<n>\n
        /// <symbol>item[0]\n
        /// <symbol>item[1]\n
        /// ...
        /// <symbol>item[n-1]\n
        /// <symbol>item[n]\n
        /// ```
        pub trait IntoRespGroup {
            fn into_resp_group(self) -> RespTuple;
        }

        /// Trait implementors must return a **complete** response packet  
        /// A complete response packet looks like:
        /// ```text
        /// <* or $>!<CONTENT_LENGTH>!<METALAYOUT_LENGTH>\n
        /// #a#b#c#d ...\n
        /// < --- data --- >
        /// ```
        pub trait IntoResponse {
            fn into_response(self) -> Vec<u8>;
        }

        impl PreResp for String {
            fn into_pre_resp(self) -> PRTuple {
                let df_bytes = [&[b'+'], self.as_bytes(), &[b'\n']].concat();
                (df_bytes.len() - 1, df_bytes)
            }
        }

        impl PreResp for &str {
            fn into_pre_resp(self) -> PRTuple {
                let df_bytes = [&[b'+'], self.as_bytes(), &[b'\n']].concat();
                (df_bytes.len() - 1, df_bytes)
            }
        }

        impl PreResp for RespCodes {
            fn into_pre_resp(self) -> PRTuple {
                let bytes = match self {
                    RespCodes::Okay => "!0\n".as_bytes().to_owned(),
                    RespCodes::NotFound => "!1\n".as_bytes().to_owned(),
                    RespCodes::OverwriteError => "!2\n".as_bytes().to_owned(),
                    RespCodes::InvalidMetaframe => "!3\n".as_bytes().to_owned(),
                    RespCodes::ArgumentError => "!4\n".as_bytes().to_owned(),
                    RespCodes::ServerError => "!5\n".as_bytes().to_owned(),
                    RespCodes::OtherError(maybe_err) => {
                        if let Some(err) = maybe_err {
                            [&[b'!'], err.as_bytes(), &[b'\n']].concat()
                        } else {
                            "!6\n".as_bytes().to_owned()
                        }
                    }
                };
                (bytes.len() - 1, bytes)
            }
        }

        // For responses which just have one response code as a group
        impl IntoRespGroup for RespCodes {
            fn into_resp_group(self) -> RespTuple {
                let (size, data) = self.into_pre_resp();
                let metalayout_ext = [&[b'#', b'2', b'#'], size.to_string().as_bytes()].concat();
                let dataframe_ext = [vec![b'&', b'1', b'\n'], data].concat();
                (metalayout_ext, dataframe_ext)
            }
        }

        impl IntoRespGroup for RespGroup {
            fn into_resp_group(self) -> RespTuple {
                // Get the number of items in the data group, convert it into it's UTF-8
                // equivalent.
                let sizeline =
                    [&[b'&'], self.sizes.len().to_string().as_bytes(), &[b'\n']].concat();
                // Now we have the &<n>\n line
                // All we need to know is: add this line to the data bytes
                // also we need to add the len of the sizeline - 1 to the sizes
                let sizes: Vec<u8> = self
                    .sizes
                    .into_iter()
                    .map(|size| [&[b'#'], size.to_string().as_bytes()].concat())
                    .flatten()
                    .collect();
                let metalayout = [
                    vec![b'#'],
                    (sizeline.len() - 1).to_string().as_bytes().to_vec(),
                    sizes,
                ]
                .concat();
                let dataframe = [sizeline, self.df_bytes].concat();
                (metalayout, dataframe)
            }
        }
        // For responses which just have one String
        impl IntoRespGroup for String {
            fn into_resp_group(self) -> RespTuple {
                let metalayout =
                    [&[b'#', b'2', b'#'], (self.len() + 1).to_string().as_bytes()].concat();
                let dataframe =
                    [&[b'&', b'1', b'\n'], &[b'+'][..], self.as_bytes(), &[b'\n']].concat();
                (metalayout, dataframe)
            }
        }
        // For responses which just have one str
        impl IntoRespGroup for &str {
            fn into_resp_group(self) -> RespTuple {
                let metalayout =
                    [&[b'#', b'2', b'#'], (self.len() + 1).to_string().as_bytes()].concat();
                let dataframe =
                    [&[b'&', b'1', b'\n'], &[b'+'][..], self.as_bytes(), &[b'\n']].concat();
                (metalayout, dataframe)
            }
        }
        #[cfg(test)]
        #[test]
        fn test_data_group_resp_trait_impl() {
            let mut dg = RespGroup::new();
            dg.add_item("HEYA");
            dg.add_item(String::from("sayan"));
            let (layout, df) = dg.into_resp_group();
            assert_eq!("&2\n+HEYA\n+sayan\n".as_bytes().to_vec(), df);
            assert_eq!("#2#5#6".as_bytes().to_vec(), layout);
            let one_string_response = "OKAY".into_resp_group();
            assert_eq!("&1\n+OKAY\n".as_bytes().to_owned(), one_string_response.1);
            assert_eq!("#2#5".as_bytes().to_owned(), one_string_response.0);
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
            fn into_response(self) -> Vec<u8> {
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
                        .extend(self.dataframe.len().to_string().as_bytes());
                    (*self_mut).metaline.push(b'!');
                    (*self_mut)
                        .metaline
                        .extend((*self_mut).metalayout.len().to_string().as_bytes());
                    (*self_mut).metaline.push(b'\n');
                } // The raw pointers are dropped here
                [self.metaline, self.metalayout, self.dataframe].concat()
            }
        }

        // For an entire response which only comprises of a string
        impl IntoResponse for String {
            fn into_response(self) -> Vec<u8> {
                let (metalayout, dataframe) = self.into_resp_group();
                let metaline = [
                    &[b'*', b'!'],
                    dataframe.len().to_string().as_bytes(),
                    &[b'!'],
                    metalayout.len().to_string().as_bytes(),
                    &[b'\n'],
                ]
                .concat();
                [metaline, metalayout, dataframe].concat()
            }
        }

        // For an entire response which only comprises of a string
        impl IntoResponse for &str {
            fn into_response(self) -> Vec<u8> {
                let (metalayout, dataframe) = self.into_resp_group();
                let metaline = [
                    &[b'*', b'!'],
                    dataframe.len().to_string().as_bytes(),
                    &[b'!'],
                    metalayout.len().to_string().as_bytes(),
                    &[b'\n'],
                ]
                .concat();
                [metaline, metalayout, dataframe].concat()
            }
        }

        #[cfg(test)]
        #[test]
        fn test_datagroup() {
            let mut datagroup = RespGroup::new();
            datagroup.add_item("HEY!");
            datagroup.add_item("four");
            datagroup.add_item(RespCodes::Okay);
            let mut builder = ResponseBuilder::new_simple();
            builder.add_group(datagroup);
            assert_eq!(
                "*!18!9\n#2#5#5#2\n&3\n+HEY!\n+four\n!0\n"
                    .as_bytes()
                    .to_owned(),
                builder.into_response()
            );
            let mut builder = ResponseBuilder::new_simple();
            builder.add_group(RespCodes::Okay);
            assert_eq!(
                "*!6!5\n#2#2\n&1\n!0\n".as_bytes().to_owned(),
                builder.into_response()
            );
            let mut builder = ResponseBuilder::new_simple();
            builder.add_group("four");
            assert_eq!(
                "*!9!5\n#2#5\n&1\n+four\n".as_bytes().to_owned(),
                builder.into_response()
            );
        }
    }
}

#[cfg(test)]
#[test]
fn test_df() {
    let ss: Vec<usize> = vec![2, 3, 5, 6, 6];
    let df = "&4\nGET\nsayan\nfoobar\nopnsrc\n".as_bytes().to_owned();
    let parsed = parse_df(df, ss, 1).unwrap();
    assert_eq!(
        parsed,
        vec![Action(vec![
            "GET".to_owned(),
            "sayan".to_owned(),
            "foobar".to_owned(),
            "opnsrc".to_owned()
        ])]
    );
}
