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

use corelib::de::*;
use corelib::terrapipe::*;
use std::fmt;

/// Errors that may occur while parsing responses from the server
#[derive(Debug, PartialEq)]
pub enum ClientResult {
    RespCode(RespCodes, usize),
    InvalidResponse(usize),
    Response(Vec<String>, usize),
    Empty(usize),
    Incomplete(usize),
}

impl fmt::Display for ClientResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ClientResult::*;
        match self {
            RespCode(r, _) => r.fmt(f),
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
    respcode: RespCodes,
}

impl Metaline {
    pub fn from_navigator(nav: &mut Navigator) -> Option<Self> {
        if let Some(mline) = nav.get_line(Some(DEF_QMETALINE_BUFSIZE)) {
            if mline.len() < 7 {
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
            let respcode = match RespCodes::from_utf8(unsafe { *mline.get_unchecked(2) }) {
                Some(rc) => rc,
                None => return None,
            };
            if let Some(sizes) = get_frame_sizes(unsafe { mline.get_unchecked(3..) }) {
                return Some(Metaline {
                    content_size: unsafe { *sizes.get_unchecked(0) },
                    metalayout_size: unsafe { *sizes.get_unchecked(1) },
                    resp_type,
                    respcode,
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
            let mut is_other_error = false;
            match metaline.respcode {
                RespCodes::Okay => (),
                RespCodes::OtherError(_) => is_other_error = true,
                x @ _ => return ClientResult::RespCode(x, nav.get_pos_usize()),
            }
            if metaline.content_size == 0 && metaline.metalayout_size == 0 {
                return ClientResult::RespCode(metaline.respcode, nav.get_pos_usize());
            }
            if let Some(layout) = Metalayout::from_navigator(&mut nav, metaline.metalayout_size) {
                if let Some(content) = nav.get_exact(metaline.content_size) {
                    let data = extract_idents(content, layout.0);
                    if is_other_error {
                        if data.len() == 1 {
                            return ClientResult::RespCode(
                                RespCodes::OtherError(Some(unsafe {
                                    data.get_unchecked(0).clone()
                                })),
                                nav.get_pos_usize(),
                            );
                        }
                    }
                    return ClientResult::Response(data, nav.get_pos_usize());
                }
            }
        }
        ClientResult::InvalidResponse(nav.get_pos_usize())
    }
}
