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

//! # `de`
//! The `de` module provides primitives for deserialization primitives for parsing
//! query and response packets

use crate::terrapipe::RespCodes;
use bytes::BytesMut;
use std::fmt;
use std::ops::Deref;
use std::vec::IntoIter;

/// A navigator is a wrapper around an indexed-based position tracker which
/// efficiently navigates over a `BytesMut` object
pub struct Navigator<'a> {
    /// The buffer which is a `&[u8]`
    buf: &'a [u8],
    /// The current position of this `Navigator` instance
    position: usize,
}
impl<'a> Navigator<'a> {
    /// Create a new `Navigator` instance
    pub fn new<'b: 'a>(buffer: &'b BytesMut) -> Self {
        Navigator {
            buf: &buffer,
            position: 0,
        }
    }
    /// Get a line from a buffer
    ///
    /// The `beforehint` argument provides a clue to the `Navigator` about the
    /// point till which the line must end. This prevents checking the entire buffer.
    /// Note that this `beforehint` is optional and in case no hint as available,
    /// just pass `None`
    pub fn get_line(&mut self, beforehint: Option<usize>) -> Option<&'a [u8]> {
        let start = self.position;
        let end = match beforehint {
            // The end will be the current position + the moved position - 1
            Some(hint) => (start + hint),
            None => self.buf.len() - 1,
        };
        for i in start..end {
            // If the current character is a `\n` byte, then return this slice
            if let Some(rf) = self.buf.get(i) {
                if *rf == b'\n' {
                    if let Some(slice) = self.buf.get(start..i) {
                        // Only move the Navigator ahead if the bytes could be fetched
                        // otherwise the next time we try to get anything, the
                        // Navigator would crash. If we don't change the Navigator's position
                        // we will keep moving over stale data
                        self.position = i + 1;
                        return Some(slice);
                    }
                }
            }
        }
        // If we are here, then the slice couldn't be extracted,
        None
    }
    /// Get an exact number of bytes from a buffer
    pub fn get_exact(&mut self, exact: usize) -> Option<&'a [u8]> {
        // The start position should be set to the current position of the
        // Navigator, otherwise we'll move from start, which is erroneous
        let start = self.position;
        // The end position will be the current position + number of bytes to be read
        let end = start + exact;
        if let Some(chunk) = self.buf.get(start..end) {
            // Move the Navigator ahead - only if we could get the slice
            self.position = end;
            Some(chunk)
        } else {
            // If we're here, then the slice couldn't be extracted, probably
            // because it doesn't exist. Return `None`
            None
        }
    }
    /// Get the current position of the navigator
    pub fn get_pos_usize(&self) -> usize {
        self.position
    }
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

/// Get the frame sizes from a metaline
pub fn get_frame_sizes(metaline: &[u8]) -> Option<Vec<usize>> {
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
pub fn get_skip_sequence(metalayout: &[u8]) -> Option<Vec<usize>> {
    let l = metalayout.len() / 2;
    extract_sizes_splitoff(metalayout, b'#', l)
}

/// Extract `usize`s from any buffer which when converted into UTF-8
/// looks like: '<SEP>123<SEP>456<SEP>567\n', where `<SEP>` is the separator
/// which in the case of the metaline is a `0x21` byte or a `0x23` byte in the
/// case of the metalayout line
pub fn extract_sizes_splitoff(buf: &[u8], splitoff: u8, sizehint: usize) -> Option<Vec<usize>> {
    let mut sizes = Vec::with_capacity(sizehint);
    let len = buf.len();
    let mut i = 0;
    while i < len {
        // UNSAFE(@ohsayan): This is safe because we already know the size
        if unsafe { *buf.get_unchecked(i) } == splitoff {
            // This is a hash
            let mut res: usize = 0;
            // Move to the next element
            i = i + 1;
            while i < len {
                // Only proceed if the current byte is not the separator
                // UNSAFE(@ohsayan): This is safe because we already know the size
                if unsafe { *buf.get_unchecked(i) } != splitoff {
                    // Make sure we don't go wrong here
                    // 48 is the unicode byte for 0 so 48-48 should give 0
                    // Also the subtraction shouldn't give something greater
                    // than 9, otherwise it is a different character
                    // UNSAFE(@ohsayan): This is safe because we already know the size
                    let num: usize = match unsafe { *buf.get_unchecked(i) }.checked_sub(48) {
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

/// A wrapper around a `Vec<String>` which represents a data group in the dataframe
#[derive(Debug, PartialEq)]
pub struct DataGroup(pub Vec<String>);

impl fmt::Display for DataGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::util::terminal::*;
        /*
        TODO(@ohsayan): Implement proper formatting for the response. That is,
        for `!` print the respective error code, for `+` print the corresponding
        array or single-value
        */
        if self.len() == 0 {
            // The server returned a zero-sized array
            return write!(f, "[]");
        }
        for token in self.iter() {
            if token.len() == 0 {
                write!(f, "\"\"")?;
                continue;
            }
            let mut byter = token.bytes().peekable();
            match byter.next().unwrap() {
                b'!' => {
                    // This is an error
                    match byter.next() {
                        Some(tok) => {
                            match RespCodes::from_utf8(tok) {
                                Some(code) => {
                                    use RespCodes::*;
                                    match code {
                                        Okay => write_okay("(Okay)")?,
                                        NotFound => {
                                            // `NotFound` is the same as a `Nil` value
                                            write_error("(Nil)")?;
                                        }
                                        OverwriteError => {
                                            write_error(
                                                "ERROR: Existing values cannot be overwritten",
                                            )?;
                                        }
                                        ActionError => {
                                            write_error("ERROR: An invalid request was sent")?;
                                        }
                                        PacketError => write_error(
                                            "ERROR: The action is not in the correct format",
                                        )?,
                                        ServerError => write_error(
                                            "ERROR: An error occurred on the serve-side",
                                        )?,
                                        OtherError(_) => {
                                            let rem = byter.collect::<Vec<u8>>();
                                            if rem.len() == 0 {
                                                write_error("ERROR: An unknown error occurred")?;
                                            } else {
                                                write_error(format!(
                                                    "ERROR: {}",
                                                    String::from_utf8_lossy(&rem)
                                                ))?;
                                            }
                                        }
                                    }
                                }
                                None => {
                                    let rem = byter.collect::<Vec<u8>>();
                                    if rem.len() == 0 {
                                        write_error("ERROR: An unknown error occurred")?;
                                    } else {
                                        write_error(format!(
                                            "ERROR: '{}{}'",
                                            char::from(tok),
                                            String::from_utf8_lossy(&rem)
                                        ))?;
                                    }
                                }
                            }
                        }
                        None => write_error("ERROR: An unknown error occurred")?,
                    }
                }
                b'+' => {
                    // This is a positive response
                    let rem = byter.collect::<Vec<u8>>();
                    write!(f, "\"{}\"", String::from_utf8_lossy(&rem))?;
                }
                x @ _ => {
                    // Unknown response
                    let rem = byter.collect::<Vec<u8>>();
                    write_warning(format!(
                        "Unknown response: \"{}{}\"",
                        x,
                        String::from_utf8_lossy(&rem)
                    ))?;
                }
            }
            write!(f, "\n")?;
        }
        Ok(())
    }
}
impl IntoIterator for DataGroup {
    type Item = String;
    type IntoIter = IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl DataGroup {
    /// Create a new `DataGroup`
    pub fn new(v: Vec<String>) -> Self {
        DataGroup(v)
    }
    /// Drops the `DataGroup` instance returning the `Vec<String>` that it held
    pub fn finish_into_vector(self) -> Vec<String> {
        self.0
    }
}

impl Deref for DataGroup {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// # The Dataframe Parser
///
/// This functions accepts:
/// - a `buf` as a slice (`&[u8]`) which should be the dataframe
/// of a query or response packet
/// - `sizes` which is the result of parsing the metalayout and is referred to as the
/// "skip sequence", since it dictates the size of each data item in a datagroup
/// - `nc` is essentially an optional argument, but still needs to be passed. This is
/// only there to avoid too many relocations in the case of pipelined queries. A minimum of
/// one should be passed for standard performance
pub fn parse_df(buf: &[u8], sizes: Vec<usize>, nc: usize) -> Option<Vec<DataGroup>> {
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
                tokens.push(DataGroup(toks));
            } else {
                i += 1;
                continue;
            }
        }
    }
    Some(tokens)
}

#[cfg(test)]
#[test]
fn test_df() {
    let ss: Vec<usize> = vec![2, 3, 5, 6, 6];
    let df = "&4\nGET\nsayan\nfoobar\nopnsrc\n".as_bytes().to_owned();
    let parsed = parse_df(&df, ss, 1).unwrap();
    assert_eq!(
        parsed,
        vec![DataGroup(vec![
            "GET".to_owned(),
            "sayan".to_owned(),
            "foobar".to_owned(),
            "opnsrc".to_owned()
        ])]
    );
}
