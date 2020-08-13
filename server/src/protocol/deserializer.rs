/*
 * Created on Thu Jul 30 2020
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

//! This module provides deserialization primitives for query packets

use corelib::de::*;
use corelib::terrapipe::{ActionType, RespCodes, DEF_QMETALINE_BUFSIZE};

/// Result of parsing a query
/// This is **not** the same as `std`'s `Result<T, E>` but instead is an `enum`
/// which represents the outcome when a query is parsed
#[derive(Debug)]
pub enum QueryParseResult {
    /// A successfully parsed `Query` along with an `usize` which specifies
    /// the amount by which the `buffer` must advance
    Parsed((Query, usize)),
    /// The query parsing failed and returned a Response code as an error
    RespCode(RespCodes),
    /// The query packet is incomplete
    Incomplete,
}

/// A metaline object which represents a metaline in the Terrapipe protocol's
/// query packet
struct Metaline {
    /// The content size, inclusive of the newlines. This is sent by the client
    /// driver
    content_size: usize,
    /// The metaline size, inclusive of the newline character. This is also sent
    /// by the client driver
    metalayout_size: usize,
    /// The action type - whether it is a pipelined operation or a simple query
    actiontype: ActionType,
}

impl Metaline {
    /// Create a new metaline from a `Navigator` instance
    ///
    /// This will use the navigator to extract the metaline
    pub fn from_navigator(nav: &mut Navigator) -> Option<Self> {
        if let Some(mline) = nav.get_line(Some(DEF_QMETALINE_BUFSIZE)) {
            // The minimum metaline length is five characters
            // if not - clearly something is wrong
            if mline.len() < 5 {
                return None;
            }
            // The first byte is always a `*` or `$` depending on the
            // type of query
            // UNSAFE(@ohsayan): This is safe because we already know the size
            let actiontype = match unsafe { mline.get_unchecked(0) } {
                b'$' => ActionType::Pipeline,
                b'*' => ActionType::Simple,
                _ => return None,
            };
            // Get the frame sizes: the first index is the content size
            // and the second index is the metalayout size
            // UNSAFE(@ohsayan): This is safe because we already know the size
            if let Some(sizes) = get_frame_sizes(unsafe { &mline.get_unchecked(1..) }) {
                return Some(Metaline {
                    content_size: unsafe { *sizes.get_unchecked(0) },
                    metalayout_size: unsafe { *sizes.get_unchecked(1) },
                    actiontype,
                });
            }
        }
        None
    }
}

/// A metalayout object which represents the Terrapipe protocol's metalayout line
///
/// This is nothing more than a wrapper around `Vec<usize>` which provides a more
/// convenient API
#[derive(Debug)]
struct Metalayout(Vec<usize>);

impl Metalayout {
    /// Create a new metalayout from a `Navigator` instance
    ///
    /// This uses the navigator to navigate over the buffer
    pub fn from_navigator(nav: &mut Navigator, mlayoutsize: usize) -> Option<Self> {
        // We pass `mlayoutsize` to `get_line` since we already know where the
        // metalayout ends
        if let Some(layout) = nav.get_line(Some(mlayoutsize)) {
            if let Some(skip_sequence) = get_skip_sequence(&layout) {
                return Some(Metalayout(skip_sequence));
            }
        }
        None
    }
}

/// # A `Query` object
#[derive(Debug, PartialEq)]
pub struct Query {
    /// A stream of tokens parsed from the dataframe
    pub data: Vec<Action>,
    /// The type of query - `Simple` or `Pipeline`
    pub actiontype: ActionType,
}

impl Query {
    /// Create a new `Query` instance from a `Navigator`
    ///
    /// This function will use the private `Metalayout` and `Metaline` objects
    /// to extract information on the format of the dataframe and then it will
    /// parse the dataframe itself
    pub fn from_navigator(mut nav: Navigator) -> QueryParseResult {
        if let Some(metaline) = Metaline::from_navigator(&mut nav) {
            if let Some(metalayout) = Metalayout::from_navigator(&mut nav, metaline.metalayout_size)
            {
                if let Some(content) = nav.get_exact(metaline.content_size) {
                    // TODO(@ohsayan): Add nc for pipelined commands here
                    let data = parse_df(content, metalayout.0, 1);
                    if let Some(df) = data {
                        // Return the parsed query and the amount by which the buffer
                        // must `advance`
                        return QueryParseResult::Parsed((
                            Query {
                                data: df,
                                actiontype: metaline.actiontype,
                            },
                            nav.get_pos_usize(),
                        ));
                    }
                } else {
                    // Since we couldn't get the slice, this means that the
                    // query packet was incomplete, return that error
                    return QueryParseResult::Incomplete;
                }
            }
        }
        // If we're here - it clearly means that the metaline/metalayout failed
        // to parse - we return a standard invalid metaframe `RespCodes`
        QueryParseResult::RespCode(RespCodes::InvalidMetaframe)
    }
}

#[cfg(test)]
#[test]
fn test_query() {
    use bytes::{Buf, BytesMut};
    let mut mybuf = BytesMut::from("*!17!9\n#2#3#5#3\n&3\nSET\nsayan\n123\n".as_bytes());
    let resulting_data_should_be: Action = Action::new(
        "SET sayan 123"
            .split_whitespace()
            .map(|val| val.to_string())
            .collect(),
    );
    let nav = Navigator::new(&mut mybuf);
    let query = Query::from_navigator(nav);
    println!("{:#?}", query);
    if let QueryParseResult::Parsed((query, forward)) = query {
        assert_eq!(
            query,
            Query {
                data: vec![resulting_data_should_be],
                actiontype: ActionType::Simple,
            }
        );
        mybuf.advance(forward);
        assert_eq!(mybuf.len(), 0);
    } else {
        panic!("Query parsing failed");
    }
}
