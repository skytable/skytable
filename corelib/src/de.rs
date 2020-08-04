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
//! The `de` module provides primitive for deserialization primitives for parsing
//! query and response packets

use bytes::BytesMut;
use std::io::Cursor;

/// The size of the read buffer in bytes
pub const BUF_CAP: usize = 8 * 1024; // 8 KB per-connection

/// A navigator is a wrapper around a `Cursor` which efficiently navigates over
/// a mutable `BytesMut` object
pub struct Navigator<'a> {
    /// The cursor
    cursor: Cursor<&'a [u8]>,
}
impl<'a> Navigator<'a> {
    /// Create a new `Navigator` instance
    pub fn new<'b: 'a>(buffer: &'b BytesMut) -> Self {
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
            // The end will be the current position + the moved position - 1
            Some(hint) => (start + hint),
            None => cursor.get_ref().len() - 1,
        };
        for i in start..end {
            // If the current character is a `\n` byte, then return this slice
            if let Some(rf) = cursor.get_ref().get(i) {
                if *rf == b'\n' {
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
    pub fn get_pos_usize(&self) -> usize {
        self.cursor.position() as usize
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
/// Extract the tokens from the slice using the `skip_sequence`
pub fn extract_idents(buf: &[u8], skip_sequence: Vec<usize>) -> Vec<String> {
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
fn test_extract_idents() {
    let testbuf = "set\nsayan\n17\n".as_bytes().to_vec();
    let skip_sequence: Vec<usize> = vec![3, 5, 2];
    let res = extract_idents(&testbuf, skip_sequence);
    assert_eq!(
        vec!["set".to_owned(), "sayan".to_owned(), "17".to_owned()],
        res
    );
    let badbuf = vec![0, 0, 159, 146, 150];
    let skip_sequence: Vec<usize> = vec![1, 2];
    let res = extract_idents(&badbuf, skip_sequence);
    assert_eq!(res[1], "��");
}
