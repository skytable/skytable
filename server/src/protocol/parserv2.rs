/*
 * Created on Mon May 10 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

#[derive(Debug)]
pub(super) struct Parser<'a> {
    cursor: usize,
    buffer: &'a [u8],
}

#[derive(Debug)]
enum ParseError {
    NotEnough,
    UnexpectedByte,
}

type ParseResult<T> = Result<T, ParseError>;

impl<'a> Parser<'a> {
    pub const fn new(buffer: &'a [u8]) -> Self {
        Parser {
            cursor: 0usize,
            buffer,
        }
    }
    fn read_until(&self, until: usize) -> ParseResult<&[u8]> {
        if let Some(b) = self.buffer.get(self.cursor + 1..until) {
            Ok(b)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    fn read_sizeline(&mut self) -> ParseResult<usize> {
        if let Some(b'#') = self.buffer.get(self.cursor) {
            // Good, we found a #; time to move ahead
            self.incr_cursor();
            let started_at = self.cursor;
            let mut stopped_at = self.cursor;
            while self.cursor < self.buffer.len() {
                if self.buffer[self.cursor] == b'\n' {
                    // Oh no! Newline reached, time to break the loop
                    // But before that ... we read the newline, so let's advance the cursor
                    self.incr_cursor();
                    break;
                }
                // So this isn't an LF, great! Let's forward the stopped_at position
                stopped_at += 1;
                self.incr_cursor();
            }
            Self::parse_into_usize(&self.buffer[started_at..stopped_at])
        } else {
            // A sizeline should begin with a '#'; this one doesn't so it's a bad packet; ugh
            Err(ParseError::UnexpectedByte)
        }
    }
    fn incr_cursor(&mut self) {
        self.cursor += 1;
    }
    fn parse_into_usize(bytes: &[u8]) -> ParseResult<usize> {
        let mut byte_iter = bytes.into_iter();
        let mut item_usize = 0usize;
        while let Some(dig) = byte_iter.next() {
            let curdig: usize = match dig.checked_sub(48) {
                Some(dig) => {
                    if dig > 9 {
                        return Err(ParseError::UnexpectedByte);
                    } else {
                        dig.into()
                    }
                }
                None => return Err(ParseError::UnexpectedByte),
            };
            item_usize = (item_usize * 10) + curdig;
        }
        Ok(item_usize)
    }
}

#[test]
fn test_sizeline_parse() {
    let sizeline = "#125\n".as_bytes();
    let mut parser = Parser::new(&sizeline);
    assert_eq!(125, parser.read_sizeline().unwrap());
}
