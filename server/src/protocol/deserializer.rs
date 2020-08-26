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

/*
NOTE: I haven't used any recursion, because:
1. I like things to be explicit
2. I don't like huge stacks
And that's why I've done, what I've done, here.
*/

use std::vec::IntoIter;

/// # `ActionGroup`
///
/// The `ActionGroup` is an "Action Group" in the dataframe as described by the
/// Terrapipe protocol. An `ActionGroup` contains all the elements required to
/// execute an `Action`. The `ActionGroup` contains the "action" itself.
/// It may look like:
/// ```text
/// ["GET", "x", "y"]
/// ```
#[derive(Debug, PartialEq)]
pub struct ActionGroup(Vec<String>);

impl ActionGroup {
    /// Returns how many arguments are there excluding the name of the action
    pub fn howmany(&self) -> usize {
        self.0.len() - 1
    }
    pub fn get_first(&self) -> Option<&String> {
        self.0.get(0)
    }
}

impl IntoIterator for ActionGroup {
    type Item = String;
    type IntoIter = std::iter::Skip<IntoIter<String>>;
    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        self.0.into_iter().skip(1).into_iter()
    }
}

#[derive(Debug, PartialEq)]
pub enum Query {
    Simple(ActionGroup),
    Pipelined(Vec<ActionGroup>),
}

#[derive(Debug, PartialEq)]
/// Outcome of parsing a query
pub enum ParseResult {
    /// The packet is incomplete, i.e more data needs to be read
    Incomplete,
    /// The packet is corrupted, in the sense that it contains invalid data
    BadPacket,
    /// A successfully parsed query
    ///
    /// The second field is the number of bytes that should be discarded from the buffer as it has already
    /// been read
    Query(Query, usize),
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

    /*
    We first get the metaframe, which looks something like:
    ```
    #<numchars_in_next_line>\n
    !<num_of_datagroups>\n
    ```
    */
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
                                None => {
                                    return ParseResult::BadPacket;
                                }
                            };
                            current_array_size = (current_array_size * 10) + curdg; // Increment the size
                            linepos += 1; // Move the position ahead, since we just read another char
                        }
                        // Now we know the array size, good!
                        let mut actiongroup = Vec::with_capacity(current_array_size);
                        // Let's loop over to get the elements till the size of this array
                        while pos < buf.len() && actiongroup.len() < current_array_size {
                            let mut element_size = 0usize;
                            if buf[pos] == b'#' {
                                pos += 1; // skip the '#' character
                            } else {
                                return ParseResult::BadPacket;
                            }
                            while pos < buf.len() && buf[pos] != b'\n' {
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
                                    None => {
                                        return ParseResult::BadPacket;
                                    }
                                };
                                element_size = (element_size * 10) + curdig; // Increment the size
                                pos += 1; // Move the position ahead, since we just read another char
                            }
                            pos += 1; // Skip the newline
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
                        }

                        items.push(ActionGroup(actiongroup));
                    }
                    _ => {
                        return ParseResult::BadPacket;
                    }
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
            if items.len() == 1 {
                ParseResult::Query(Query::Simple(items.remove(0)), pos)
            } else {
                ParseResult::Query(Query::Pipelined(items), pos)
            }
        } else {
            ParseResult::Incomplete
        }
    } else {
        ParseResult::BadPacket
    }
}
/// Read a size line and return the following line
///
/// This reads a line that begins with the number, i.e make sure that
/// the **`#` character is skipped**
///
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
    let res_should_be = ParseResult::Query(
        Query::Simple(ActionGroup(vec![
            "GET".to_owned(),
            "x".to_owned(),
            "ex".to_owned(),
        ])),
        input.len(),
    );
    assert_eq!(res, res_should_be);
    let input = "#2\n*2\n#2\n&3\n#3\nGET\n#1\nx\n#2\nex\n"
        .to_owned()
        .into_bytes();
    let res = parse(&input);
    let res_should_be = ParseResult::Incomplete;
    assert_eq!(res, res_should_be);
    let input = "#2\n*A\n#2\n&3\n#3\nGET\n#1\nx\n#2\nex\n"
        .to_owned()
        .into_bytes();
    let res = parse(&input);
    let res_should_be = ParseResult::BadPacket;
    assert_eq!(res, res_should_be);
    let input = "#2\n*1\n#2\n&3\n#3\nSET\n#19\nbeinghumanisawesome\n#4\ntrue\n"
        .as_bytes()
        .to_owned();
    let res = parse(&input);
    let res_should_be = ParseResult::Query(
        Query::Simple(ActionGroup(vec![
            "SET".to_owned(),
            "beinghumanisawesome".to_owned(),
            "true".to_owned(),
        ])),
        input.len(),
    );
    assert_eq!(res, res_should_be);
}
