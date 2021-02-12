/*
 * Created on Tue Aug 04 2020
 *
 * This file is a part of Skybase
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

use libsky::terrapipe::RespCodes;
use libsky::util::terminal;
use std::fmt;

#[derive(Debug, PartialEq)]
/// A response datagroup
///
/// This contains all the elements returned by a certain action. So let's say you did
/// something like `MGET x y`, then the values of x and y will be in a single datagroup.
pub struct DataGroup(Vec<DataType>);

/// A data type as defined by the Terrapipe protocol
///
///
/// Every variant stays in an `Option` for convenience while parsing. It's like we first
/// create a `Variant(None)` variant. Then we read the data which corresponds to it, and then we
/// replace `None` with the appropriate object. When we first detect the type, we use this as a way of matching
/// avoiding duplication by writing another `DataType` enum
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum DataType {
    /// A string value
    Str(Option<String>),
    /// A response code (it is kept as `String` for "other error" types)
    RespCode(Option<String>),
    /// An unsigned 64-bit integer, equivalent to an `u64`
    UnsignedInt(Option<Result<u64, std::num::ParseIntError>>),
}

impl fmt::Display for DataGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for element in self.0.iter() {
            match element {
                DataType::Str(Some(val)) => write!(f, "\"{}\" ", val)?,
                DataType::Str(None) => (),
                DataType::UnsignedInt(Some(Ok(int))) => write!(f, "{}", int)?,
                DataType::UnsignedInt(Some(Err(_))) => terminal::write_error("[Parse Error]")?,
                DataType::UnsignedInt(None) => (),
                DataType::RespCode(Some(rc)) => {
                    if rc.len() == 1 {
                        if let Some(rcode) = RespCodes::from_str(&rc, None) {
                            match rcode {
                                RespCodes::Okay => terminal::write_info("(Okay) ")?,
                                RespCodes::NotFound => terminal::write_info("(Nil) ")?,
                                RespCodes::OverwriteError => {
                                    terminal::write_error("(Overwrite Error) ")?
                                }
                                RespCodes::ActionError => terminal::write_error("(Action Error) ")?,
                                RespCodes::PacketError => terminal::write_error("(Packet Error) ")?,
                                RespCodes::ServerError => terminal::write_error("(Server Error) ")?,
                                RespCodes::OtherError(_) => {
                                    terminal::write_error("(Other Error) ")?
                                }
                            }
                        }
                    } else {
                        terminal::write_error(format!("[ERROR: '{}'] ", rc))?;
                    }
                }
                _ => unimplemented!(),
            }
        }
        Ok(())
    }
}

/// Errors that may occur while parsing responses from the server
///
/// Every variant, except `Incomplete` has an `usize` field, which is used to advance the
/// buffer
#[derive(Debug, PartialEq)]
pub enum ClientResult {
    /// The response was Invalid
    InvalidResponse(usize),
    /// The response is a valid response and has been parsed into a vector of datagroups
    Response(Vec<DataGroup>, usize),
    /// The response was empty, which means that the remote end closed the connection
    Empty(usize),
    /// The response is incomplete
    Incomplete,
}

impl fmt::Display for ClientResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ClientResult::*;
        match self {
            InvalidResponse(_) => write!(f, "ERROR: The server sent an invalid response"),
            Response(_, _) => unimplemented!(),
            Empty(_) => write!(f, ""),
            Incomplete => write!(f, "ERROR: The server sent an incomplete response"),
        }
    }
}

/// Parse a response packet
pub fn parse(buf: &[u8]) -> ClientResult {
    if buf.len() < 6 {
        // A packet that has less than 6 characters? Nonsense!
        return ClientResult::Incomplete;
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
        return ClientResult::InvalidResponse(pos);
    } else {
        pos += 1;
    }
    let next_line = match read_line_and_return_next_line(&mut pos, &buf) {
        Some(line) => line,
        None => {
            // This is incomplete
            return ClientResult::Incomplete;
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
                        return ClientResult::InvalidResponse(pos);
                    } else {
                        dig.into()
                    }
                }
                None => return ClientResult::InvalidResponse(pos),
            };
            action_size = (action_size * 10) + curdig;
        }
    // This line gives us the number of actions
    } else {
        return ClientResult::InvalidResponse(pos);
    }
    let mut items: Vec<DataGroup> = Vec::with_capacity(action_size);
    while pos < buf.len() && items.len() <= action_size {
        match buf[pos] {
            b'#' => {
                pos += 1; // Skip '#'
                let next_line = match read_line_and_return_next_line(&mut pos, &buf) {
                    Some(line) => line,
                    None => {
                        // This is incomplete
                        return ClientResult::Incomplete;
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
                                        return ClientResult::InvalidResponse(pos);
                                    } else {
                                        dig.into()
                                    }
                                }
                                None => return ClientResult::InvalidResponse(pos),
                            };
                            current_array_size = (current_array_size * 10) + curdg; // Increment the size
                            linepos += 1; // Move the position ahead, since we just read another char
                        }
                        // Now we know the array size, good!
                        let mut actiongroup: Vec<DataType> = Vec::with_capacity(current_array_size);
                        // Let's loop over to get the elements till the size of this array
                        while pos < buf.len() && actiongroup.len() < current_array_size {
                            let mut element_size = 0usize;
                            let datatype = match buf[pos] {
                                b'+' => DataType::Str(None),
                                b'!' => DataType::RespCode(None),
                                b':' => DataType::UnsignedInt(None),
                                x @ _ => unimplemented!("Type '{}' not implemented", char::from(x)),
                            };
                            pos += 1; // We've got the tsymbol above, so skip it
                            while pos < buf.len() && buf[pos] != b'\n' {
                                let curdig: usize = match buf[pos].checked_sub(48) {
                                    Some(dig) => {
                                        if dig > 9 {
                                            // If `dig` is greater than 9, then the current
                                            // UTF-8 char isn't a number
                                            return ClientResult::InvalidResponse(pos);
                                        } else {
                                            dig.into()
                                        }
                                    }
                                    None => return ClientResult::InvalidResponse(pos),
                                };
                                element_size = (element_size * 10) + curdig; // Increment the size
                                pos += 1; // Move the position ahead, since we just read another char
                            }
                            pos += 1;
                            // We now know the item size
                            let mut value = String::with_capacity(element_size);
                            let extracted = match buf.get(pos..pos + element_size) {
                                Some(s) => s,
                                None => return ClientResult::Incomplete,
                            };
                            pos += element_size; // Move the position ahead
                            value.push_str(&String::from_utf8_lossy(extracted));
                            pos += 1; // Skip the newline
                            actiongroup.push(match datatype {
                                DataType::Str(_) => DataType::Str(Some(value)),
                                DataType::RespCode(_) => DataType::RespCode(Some(value)),
                                DataType::UnsignedInt(_) => {
                                    DataType::UnsignedInt(Some(value.parse()))
                                }
                            });
                        }
                        items.push(DataGroup(actiongroup));
                    }
                    _ => return ClientResult::InvalidResponse(pos),
                }
                continue;
            }
            _ => {
                // Since the variant '#' would does all the array
                // parsing business, we should never reach here unless
                // the packet is invalid
                return ClientResult::InvalidResponse(pos);
            }
        }
    }
    if buf.get(pos).is_none() {
        // Either more data was sent or some data was missing
        if items.len() == action_size {
            if items.len() == 1 {
                ClientResult::Response(items, pos)
            } else {
                // The CLI does not support batch queries
                unimplemented!();
            }
        } else {
            ClientResult::Incomplete
        }
    } else {
        ClientResult::InvalidResponse(pos)
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

#[cfg(test)]
#[test]
fn test_parser() {
    let res = "#2\n*1\n#2\n&1\n+4\nHEY!\n".as_bytes().to_owned();
    assert_eq!(
        parse(&res),
        ClientResult::Response(
            vec![DataGroup(vec![DataType::Str(Some("HEY!".to_owned()))])],
            res.len()
        )
    );
    let res = "#2\n*1\n#2\n&1\n!1\n0\n".as_bytes().to_owned();
    assert_eq!(
        parse(&res),
        ClientResult::Response(
            vec![DataGroup(vec![DataType::RespCode(Some("0".to_owned()))])],
            res.len()
        )
    );
}
