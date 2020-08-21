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
    pub data: Vec<DataGroup>,
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
        QueryParseResult::RespCode(RespCodes::PacketError)
    }
}

#[cfg(test)]
#[test]
fn test_query() {
    use bytes::{Buf, BytesMut};
    let mut mybuf = BytesMut::from("*!17!9\n#2#3#5#3\n&3\nSET\nsayan\n123\n".as_bytes());
    let resulting_data_should_be: DataGroup = DataGroup::new(
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

mod parser_v2 {
    //! The 2<sup>nd</sup> Generation Parser for Terrapipe
    /*
    NOTE: I haven't used any recursion, because:
    1. I like things to be explicit
    2. I don't like huge stacks
    And that's why I've done, what I've done, here.
    */

    /// # `ActionGroup`
    ///
    /// The `ActionGroup` is an "Action Group" in the dataframe as described by the
    /// Terrapipe protocol. An `ActionGroup` contains all the elements required to
    /// execute an `Action`. The `ActionGroup` contains the "action" itself.
    /// It may look like:
    /// ```rust
    /// ["GET", "x", "y"]
    /// ```
    #[derive(Debug, PartialEq)]
    pub struct ActionGroup(Vec<String>);

    #[derive(Debug, PartialEq)]
    /// Outcome of parsing a query
    pub enum ParseResult {
        /// The packet is incomplete, i.e more data needs to be read
        Incomplete,
        /// The packet is corrupted, in the sense that it contains invalid data
        BadPacket,
        /// A successfully parsed query
        Query(Vec<ActionGroup>),
    }

    /// # The Query parser
    ///
    /// The query parser, well, parses query packets! Query packets look like this:
    /// ```text
    /// #<size_of_next_line>\n
    /// *<no_of_actions>\n
    /// #<size_of_next_line>\n
    /// &<no_of_elements_in_actiongroup>\n
    /// #<size_of_next_line>\n
    /// element[0]\n
    /// #<size_of_next_line>\n
    /// element[1]\n
    /// ...
    /// element[n]\n
    /// #<size_of_next_line>\n
    /// &<no_of_elements_in_this_actiongroup>\n
    /// ...
    /// ```
    ///
    pub fn parse(buf: &[u8]) -> ParseResult {
        if buf.len() < 6 {
            // A packet that has less than 6 characters? Nonsense!
            return ParseResult::Incomplete;
        }
        let mut pos = 0;
        if buf[pos] != b'#' {
            return ParseResult::BadPacket;
        } else {
            pos += 1;
        }
        let next_line = match read_line_and_return_next_line(&mut pos, &buf) {
            Some(line) => line,
            None => {
                // This is incomplete
                return ParseResult::Incomplete;
            }
        };
        pos += 1; // Skip LF
                  // Find out the number of actions that we have to do
        let mut action_size = 0usize;
        if next_line[0] == b'*' {
            let mut line_iter = next_line.into_iter().skip(1).peekable();
            while let Some(dig) = line_iter.next() {
                let curdig: usize = match dig.checked_sub(48) {
                    Some(dig) => {
                        if dig > 9 {
                            return ParseResult::BadPacket;
                        } else {
                            dig.into()
                        }
                    }
                    None => return ParseResult::BadPacket,
                };
                action_size = (action_size * 10) + curdig;
            }
        // This line gives us the number of actions
        } else {
            return ParseResult::BadPacket;
        }
        let mut items: Vec<ActionGroup> = Vec::with_capacity(action_size);
        while pos < buf.len() && items.len() <= action_size {
            match buf[pos] {
                b'#' => {
                    pos += 1; // Skip '#'
                    let next_line = match read_line_and_return_next_line(&mut pos, &buf) {
                        Some(line) => line,
                        None => {
                            // This is incomplete
                            return ParseResult::Incomplete;
                        }
                    }; // Now we have the current line
                    pos += 1; // Skip the newline
                              // Move the cursor ahead by the number of bytes that we just read
                              // Let us check the current char
                    match next_line[0] {
                        b'&' => {
                            // This is an array
                            // Now let us parse the array size
                            let mut current_array_size = 0usize;
                            let mut linepos = 1; // Skip the '&' character
                            while linepos < next_line.len() {
                                let curdg: usize = match next_line[linepos].checked_sub(48) {
                                    Some(dig) => {
                                        if dig > 9 {
                                            // If `dig` is greater than 9, then the current
                                            // UTF-8 char isn't a number
                                            return ParseResult::BadPacket;
                                        } else {
                                            dig.into()
                                        }
                                    }
                                    None => return ParseResult::BadPacket,
                                };
                                current_array_size = (current_array_size * 10) + curdg; // Increment the size
                                linepos += 1; // Move the position ahead, since we just read another char
                            }
                            // Now we know the array size, good!
                            let mut actiongroup = Vec::with_capacity(current_array_size);
                            // Let's loop over to get the elements till the size of this array
                            let mut current_element = 0usize;
                            while pos < buf.len() && current_element < current_array_size {
                                let mut element_size = 0usize;
                                while pos < buf.len() && buf[pos] != b'\n' {
                                    if buf[pos] == b'#' {
                                        pos += 1; // skip the '#' character
                                        let curdig: usize = match buf[pos].checked_sub(48) {
                                            Some(dig) => {
                                                if dig > 9 {
                                                    // If `dig` is greater than 9, then the current
                                                    // UTF-8 char isn't a number
                                                    return ParseResult::BadPacket;
                                                } else {
                                                    dig.into()
                                                }
                                            }
                                            None => return ParseResult::BadPacket,
                                        };
                                        element_size = (element_size * 10) + curdig; // Increment the size
                                        pos += 1; // Move the position ahead, since we just read another char
                                    } else {
                                        return ParseResult::BadPacket;
                                    }
                                }
                                pos += 1;
                                // We now know the item size
                                let mut value = String::with_capacity(element_size);
                                let extracted = match buf.get(pos..pos + element_size) {
                                    Some(s) => s,
                                    None => return ParseResult::Incomplete,
                                };
                                pos += element_size; // Move the position ahead
                                value.push_str(&String::from_utf8_lossy(extracted));
                                pos += 1; // Skip the newline
                                actiongroup.push(value);
                                current_element += 1;
                            }
                            items.push(ActionGroup(actiongroup));
                        }
                        _ => return ParseResult::BadPacket,
                    }
                    continue;
                }
                _ => {
                    // Since the variant '#' would does all the array
                    // parsing business, we should never reach here unless
                    // the packet is invalid
                    return ParseResult::BadPacket;
                }
            }
        }
        if buf.get(pos).is_none() {
            // Either more data was sent or some data was missing
            if items.len() == action_size {
                ParseResult::Query(items)
            } else {
                ParseResult::Incomplete
            }
        } else {
            ParseResult::BadPacket
        }
    }
    fn read_line_and_return_next_line<'a>(pos: &mut usize, buf: &'a [u8]) -> Option<&'a [u8]> {
        let mut next_line_size = 0usize;
        while pos < &mut buf.len() && buf[*pos] != b'\n' {
            // 48 is the UTF-8 code for '0'
            let curdig: usize = match buf[*pos].checked_sub(48) {
                Some(dig) => {
                    if dig > 9 {
                        // If `dig` is greater than 9, then the current
                        // UTF-8 char isn't a number
                        return None;
                    } else {
                        dig.into()
                    }
                }
                None => return None,
            };
            next_line_size = (next_line_size * 10) + curdig; // Increment the size
            *pos += 1; // Move the position ahead, since we just read another char
        }
        *pos += 1; // Skip the newline
                   // We now know the size of the next line
        let next_line = match buf.get(*pos..*pos + next_line_size) {
            Some(line) => line,
            None => {
                // This is incomplete
                return None;
            }
        }; // Now we have the current line
           // Move the cursor ahead by the number of bytes that we just read
        *pos += next_line_size;
        Some(next_line)
    }

    #[test]
    fn test_parser() {
        let input = "#2\n*1\n#2\n&3\n#3\nGET\n#1\nx\n#2\nex\n"
            .to_owned()
            .into_bytes();
        let res = parse(&input);
        let res_should_be = ParseResult::Query(vec![ActionGroup(vec![
            "GET".to_owned(),
            "x".to_owned(),
            "ex".to_owned(),
        ])]);
        assert_eq!(res, res_should_be);
    }
}
