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

use bytes::BytesMut;
use corelib::terrapipe::DEF_QMETALINE_BUFSIZE;
use corelib::terrapipe::{ActionType, RespCodes};
use std::io::Cursor;

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

/// A navigator is a wrapper around a `Cursor` which efficiently navigates over
/// a mutable `BytesMut` object
pub struct Navigator<'a> {
    /// The cursor
    cursor: Cursor<&'a [u8]>,
}
impl<'a> Navigator<'a> {
    /// Create a new `Navigator` instance
    pub fn new(buffer: &'a mut BytesMut) -> Self {
        Navigator {
            cursor: Cursor::new(&buffer[..]),
        }
    }
    /// Get a line from a buffer
    ///
    /// The `beforehint` argument provides a clue to the `Navigator` about the
    /// point till which the line must end. This prevents checking the entire buffer.
    /// Note that this `beforehint` is optional and in case no hint as available,
    /// just pass `None`
    pub fn get_line(&mut self, beforehint: Option<usize>) -> Option<&'a [u8]> {
        let ref mut cursor = self.cursor;
        let start = cursor.position() as usize;
        let end = match beforehint {
            // The end will be the current position + the moved position
            Some(hint) => (start + hint),
            None => cursor.get_ref().len() - 1,
        };
        for i in start..end {
            // If the current character is a `\n` byte, then return this slice
            if cursor.get_ref()[i] == b'\n' {
                if let Some(slice) = cursor.get_ref().get(start..i) {
                    // Only move the cursor ahead if the bytes could be fetched
                    // otherwise the next time we try to get anything, the
                    // cursor would crash. If we don't change the cursor position
                    // we will keep moving over stale data
                    cursor.set_position((i + 1) as u64);
                    return Some(slice);
                }
            }
        }
        // If we are here, then the slice couldn't be extracted,
        None
    }
    /// Get an exact number of bytes from a buffer
    pub fn get_exact(&mut self, exact: usize) -> Option<&'a [u8]> {
        let ref mut cursor = self.cursor;
        // The start position should be set to the current position of the
        // cursor, otherwise we'll move from start, which is erroneous
        let start = cursor.position() as usize;
        // The end position will be the current position + number of bytes to be read
        let end = start + exact;
        if let Some(chunk) = cursor.get_ref().get(start..end) {
            // Move the cursor ahead - only if we could get the slice
            self.cursor.set_position(end as u64);
            Some(chunk)
        } else {
            // If we're here, then the slice couldn't be extracted, probably
            // because it doesn't exist. Return `None`
            None
        }
    }
    /// Get the cursor's position as an `usize`
    fn get_pos_usize(&self) -> usize {
        self.cursor.position() as usize
    }
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
                println!("Did we?");
                return None;
            }
            // The first byte is always a `*` or `$` depending on the
            // type of query
            let actiontype = match mline[0] {
                b'$' => ActionType::Pipeline,
                b'*' => ActionType::Simple,
                _ => return None,
            };
            // Get the frame sizes: the first index is the content size
            // and the second index is the metalayout size
            if let Some(sizes) = get_frame_sizes(&mline[1..]) {
                return Some(Metaline {
                    content_size: sizes[0],
                    metalayout_size: sizes[1],
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
    pub data: Vec<String>,
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
                    let data = extract_idents(content, metalayout.0);
                    // Return the parsed query and the amount by which the buffer
                    // must `advance`
                    return QueryParseResult::Parsed((
                        Query {
                            data,
                            actiontype: metaline.actiontype,
                        },
                        nav.get_pos_usize(),
                    ));
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

/// Get the frame sizes from a metaline
fn get_frame_sizes(metaline: &[u8]) -> Option<Vec<usize>> {
    if let Some(s) = extract_sizes_splitoff(metaline, b'!', 2) {
        if s.len() == 2 {
            Some(s)
        } else {
            None
        }
    } else {
        None
    }
}

/// Get the skip sequence from the metalayout line
fn get_skip_sequence(metalayout: &[u8]) -> Option<Vec<usize>> {
    let l = metalayout.len() / 2;
    extract_sizes_splitoff(metalayout, b'#', l)
}

/// Extract `usize`s from any buffer which when converted into UTF-8
/// looks like: '<SEP>123<SEP>456<SEP>567\n', where `<SEP>` is the separator
/// which in the case of the metaline is a `0x21` byte or a `0x23` byte in the
/// case of the metalayout line
fn extract_sizes_splitoff(buf: &[u8], splitoff: u8, sizehint: usize) -> Option<Vec<usize>> {
    let mut sizes = Vec::with_capacity(sizehint);
    let len = buf.len();
    let mut i = 0;
    while i < len {
        if buf[i] == splitoff {
            // This is a hash
            let mut res: usize = 0;
            // Move to the next element
            i = i + 1;
            while i < len {
                // Only proceed if the current byte is not the separator
                if buf[i] != splitoff {
                    // Make sure we don't go wrong here
                    // 48 is the unicode byte for 0 so 48-48 should give 0
                    // Also the subtraction shouldn't give something greater
                    // than 9, otherwise it is a different character
                    let num: usize = match buf[i].checked_sub(48) {
                        Some(s) => s.into(),
                        None => return None,
                    };
                    if num > 9 {
                        return None;
                    }
                    res = res * 10 + num;
                    i = i + 1;
                    continue;
                } else {
                    break;
                }
            }
            sizes.push(res.into());
            continue;
        } else {
            // Technically, we should never reach here, but if we do
            // clearly, it's an error by the client-side driver
            return None;
        }
    }
    Some(sizes)
}
/// Extract the tokens from the slice using the `skip_sequence`
fn extract_idents(buf: &[u8], skip_sequence: Vec<usize>) -> Vec<String> {
    skip_sequence
        .into_iter()
        .scan(buf.into_iter(), |databuf, size| {
            let tok: Vec<u8> = databuf.take(size).map(|val| *val).collect();
            let _ = databuf.next();
            // FIXME(@ohsayan): This is quite slow, we'll have to use SIMD in the future
            Some(String::from_utf8_lossy(&tok).to_string())
        })
        .collect()
}

#[cfg(test)]
#[test]
fn test_navigator() {
    use bytes::BytesMut;
    let mut mybytes = BytesMut::from("*!5!2\n1#\nHEYA\n".as_bytes());
    let mut nav = Navigator::new(&mut mybytes);
    assert_eq!(Some("*!5!2".as_bytes()), nav.get_line(Some(46)));
    assert_eq!(Some("1#".as_bytes()), nav.get_line(Some(3)));
    assert_eq!(Some("HEYA".as_bytes()), nav.get_line(Some(5)));
}

#[cfg(test)]
#[test]
fn test_query() {
    use bytes::{Buf, BytesMut};
    let mut mybuf = BytesMut::from("*!14!7\n#3#5#3\nSET\nsayan\n123\n".as_bytes());
    let resulting_data_should_be: Vec<String> = "SET sayan 123"
        .split_whitespace()
        .map(|val| val.to_string())
        .collect();
    let nav = Navigator::new(&mut mybuf);
    let query = Query::from_navigator(nav);
    if let QueryParseResult::Parsed((query, forward)) = query {
        assert_eq!(
            query,
            Query {
                data: resulting_data_should_be,
                actiontype: ActionType::Simple,
            }
        );
        mybuf.advance(forward);
        assert_eq!(mybuf.len(), 0);
    } else {
        panic!("Query parsing failed");
    }
}
