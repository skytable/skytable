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

//! This module provides methods to deserialize an incoming response packet

use corelib::builders::MLINE_BUF;
use corelib::de::*;
use corelib::terrapipe::*;
use std::fmt;

/// Errors that may occur while parsing responses from the server
#[derive(Debug, PartialEq)]
pub enum ClientResult {
    RespCode(RespCodes, usize),
    InvalidResponse(usize),
    Response(Vec<DataGroup>, usize),
    Empty(usize),
    Incomplete(usize),
}

impl fmt::Display for ClientResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ClientResult::*;
        use RespCodes::*;
        match self {
            RespCode(r, _) => match r {
                Okay => Ok(()),
                NotFound => writeln!(f, "ERROR: Couldn't find the key"),
                OverwriteError => writeln!(f, "ERROR: Existing values cannot be overwritten"),
                InvalidMetaframe => writeln!(f, "ERROR: Invalid metaframe"),
                ArgumentError => writeln!(f, "ERROR: The command is not in the correct format"),
                ServerError => writeln!(f, "ERROR: The server had an internal error"),
                OtherError(e) => match e {
                    None => writeln!(f, "ERROR: Some unknown error occurred"),
                    Some(e) => writeln!(f, "ERROR: {}", e),
                },
            },
            InvalidResponse(_) => write!(f, "ERROR: The server sent an invalid response"),
            Response(_, _) => unimplemented!(),
            Empty(_) => write!(f, ""),
            Incomplete(_) => write!(f, "ERROR: The server sent an incomplete response"),
        }
    }
}

struct Metaline {
    content_size: usize,
    metalayout_size: usize,
    resp_type: ActionType,
}

impl Metaline {
    pub fn from_navigator(nav: &mut Navigator) -> Option<Self> {
        if let Some(mline) = nav.get_line(Some(MLINE_BUF)) {
            if mline.len() < 5 {
                return None;
            }
            let resp_type = match unsafe { mline.get_unchecked(0) } {
                b'$' => ActionType::Pipeline,
                b'*' => ActionType::Simple,
                _ => return None,
            };
            if resp_type == ActionType::Pipeline {
                // TODO(@ohsayan): Enable pipelined responses to be parsed
                unimplemented!("Pipelined responses cannot be parsed yet");
            }
            if let Some(sizes) = get_frame_sizes(unsafe { mline.get_unchecked(1..) }) {
                return Some(Metaline {
                    content_size: unsafe { *sizes.get_unchecked(0) },
                    metalayout_size: unsafe { *sizes.get_unchecked(1) },
                    resp_type,
                });
            }
        }
        None
    }
}

#[derive(Debug)]
struct Metalayout(Vec<usize>);

impl Metalayout {
    pub fn from_navigator(nav: &mut Navigator, mlayoutsize: usize) -> Option<Self> {
        if let Some(layout) = nav.get_line(Some(mlayoutsize)) {
            if let Some(skip_sequence) = get_skip_sequence(&layout) {
                return Some(Metalayout(skip_sequence));
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct Response {
    pub data: Vec<String>,
    pub resptype: ActionType,
}

impl Response {
    pub fn from_navigator(mut nav: Navigator) -> ClientResult {
        if let Some(metaline) = Metaline::from_navigator(&mut nav) {
            if let Some(layout) = Metalayout::from_navigator(&mut nav, metaline.metalayout_size) {
                if let Some(content) = nav.get_exact(metaline.content_size) {
                    let data = parse_df(content, layout.0, 1);
                    if let Some(data) = data {
                        return ClientResult::Response(data, nav.get_pos_usize());
                    }
                }
            }
        }
        ClientResult::InvalidResponse(nav.get_pos_usize())
    }
}
