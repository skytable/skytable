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
pub enum ParseError {
    NotEnough,
    UnexpectedByte,
}

type ActionGroup = Vec<Vec<u8>>;

#[derive(Debug, PartialEq)]
pub enum Query {
    SimpleQuery(ActionGroup),
    PipelinedQuery(Vec<ActionGroup>),
}

type ParseResult<T> = Result<T, ParseError>;

impl<'a> Parser<'a> {
    pub const fn new(buffer: &'a [u8]) -> Self {
        Parser {
            cursor: 0usize,
            buffer,
        }
    }
    /// Read from the current cursor position to `until` number of positions ahead
    /// This **will forward the cursor itself** if the bytes exist or it will just return a `NotEnough` error
    fn read_until(&mut self, until: usize) -> ParseResult<&[u8]> {
        if let Some(b) = self.buffer.get(self.cursor..self.cursor + until) {
            self.cursor += until;
            Ok(b)
        } else {
            Err(ParseError::NotEnough)
        }
    }
    /// This returns the position at which the line parsing began and the position at which the line parsing
    /// stopped, in other words, you should be able to do self.buffer[started_at..stopped_at] to get a line
    /// and do it unchecked. This **will move the internal cursor ahead**
    fn read_line(&mut self) -> (usize, usize) {
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
        (started_at, stopped_at)
    }
    /// This function will return the number of bytes this sizeline has (this is usually the number of items in
    /// the following line)
    /// This **will forward the cursor itself**
    fn read_sizeline(&mut self) -> ParseResult<usize> {
        if let Some(b'#') = self.buffer.get(self.cursor) {
            // Good, we found a #; time to move ahead
            self.incr_cursor();
            // Now read the remaining line
            let (started_at, stopped_at) = self.read_line();
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
    /// This will return the number of datagroups present in this query packet
    ///
    /// This **will forward the cursor itself**
    fn parse_metaframe_get_datagroup_count(&mut self) -> ParseResult<usize> {
        // This will give us the `#<m>\n`
        let metaframe_sizeline = self.read_sizeline()?;
        // Now we want to read `*<n>\n`
        let our_chunk = self.read_until(metaframe_sizeline)?;
        if our_chunk[0] == b'*' {
            // Good, this will tell us the number of actions
            // Let us attempt to read the usize from this point onwards
            // that is excluding the '!' (so 1..)
            // also push the cursor ahead because we want to ignore the LF char
            // as read_until won't skip the newline
            let ret = Self::parse_into_usize(&our_chunk[1..])?;
            self.incr_cursor();
            Ok(ret)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// This will return the number of items in a datagroup
    fn parse_datagroup_get_group_size(&mut self) -> ParseResult<usize> {
        // This will give us `#<p>\n`
        let dataframe_sizeline = self.read_sizeline()?;
        // Now we want to read `&<q>\n`
        let our_chunk = self.read_until(dataframe_sizeline)?;
        if our_chunk[0] == b'&' {
            // Good, so this is indeed a datagroup!
            // Let us attempt to read the usize from this point onwards
            // excluding the '&' char (so 1..)
            // also push the cursor ahead
            let ret = Self::parse_into_usize(&our_chunk[1..])?;
            self.incr_cursor();
            Ok(ret)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// This will read a datagroup element and return an **owned vector** containing the bytes
    /// for the next datagroup element
    fn parse_next_datagroup_element(&mut self) -> ParseResult<Vec<u8>> {
        // So we need to read the sizeline for this element first!
        let element_size = self.read_sizeline()?;
        // Now we want to read the element itself
        let mut ret = Vec::with_capacity(element_size);
        ret.extend_from_slice(self.read_until(element_size)?);
        // Now move the cursor ahead as read_until doesn't do anything with the newline
        self.incr_cursor();
        // We'll just return this since that's all we have to do!
        Ok(ret)
    }
    fn parse_next_actiongroup(&mut self) -> ParseResult<Vec<Vec<u8>>> {
        let len = self.parse_datagroup_get_group_size()?;
        let mut elements = Vec::with_capacity(len);
        // so we expect `len` count of elements; let's iterate and get each element in turn
        for _ in 0..len {
            elements.push(self.parse_next_datagroup_element()?);
        }
        if self.parse_next_datagroup_element().is_ok() {
            // they sent one more element when we already have what we expected? that's unexpected
            Err(ParseError::UnexpectedByte)
        } else {
            Ok(elements)
        }
    }
    pub fn parse(mut self) -> Result<(Query, usize), ParseError> {
        let number_of_queries = self.parse_metaframe_get_datagroup_count()?;
        if number_of_queries == 1 {
            // This is a simple query
            let single_group = self.parse_next_actiongroup()?;
            if self.parse_next_actiongroup().is_ok() {
                // they sent one more actiongroup? that's unexpected
                Err(ParseError::UnexpectedByte)
            } else {
                Ok((Query::SimpleQuery(single_group), self.cursor))
            }
        } else {
            // This is a pipelined query
            // We'll first make space for all the actiongroups
            let mut queries = Vec::with_capacity(number_of_queries);
            for _ in 0..number_of_queries {
                queries.push(self.parse_next_actiongroup()?);
            }
            if self.parse_next_actiongroup().is_ok() {
                // they sent one more actiongroup? that's unexpected
                Err(ParseError::UnexpectedByte)
            } else {
                Ok((Query::PipelinedQuery(queries), self.cursor))
            }
        }
    }
}

#[test]
fn test_sizeline_parse() {
    let sizeline = "#125\n".as_bytes();
    let mut parser = Parser::new(&sizeline);
    assert_eq!(125, parser.read_sizeline().unwrap());
    assert_eq!(parser.cursor, sizeline.len());
}

#[test]
fn test_metaframe_parse() {
    let metaframe = "#2\n*2\n".as_bytes();
    let mut parser = Parser::new(&metaframe);
    assert_eq!(2, parser.parse_metaframe_get_datagroup_count().unwrap());
    assert_eq!(parser.cursor, metaframe.len());
}

#[test]
fn test_actiongroup_size_parse() {
    let dataframe_layout = "#6\n&12345\n".as_bytes();
    let mut parser = Parser::new(&dataframe_layout);
    assert_eq!(12345, parser.parse_datagroup_get_group_size().unwrap());
    assert_eq!(parser.cursor, dataframe_layout.len());
}

#[test]
fn test_read_datagroup_element() {
    let element_with_block = "#5\nsayan\n".as_bytes();
    let mut parser = Parser::new(&element_with_block);
    assert_eq!(
        String::from("sayan").into_bytes(),
        parser.parse_next_datagroup_element().unwrap()
    );
    assert_eq!(parser.cursor, element_with_block.len());
}

#[test]
fn test_parse_actiongroup_single() {
    let actiongroup = "#2\n&2\n#3\nGET\n#5\nsayan\n".as_bytes();
    let mut parser = Parser::new(&actiongroup);
    assert_eq!(
        vec![
            String::from("GET").into_bytes(),
            String::from("sayan").into_bytes()
        ],
        parser.parse_next_actiongroup().unwrap()
    );
    assert_eq!(parser.cursor, actiongroup.len());
}

#[test]
fn test_complete_query_packet_parse() {
    let query_packet = "#2\n*1\n#2\n&2\n#3\nGET\n#3\nfoo\n".as_bytes();
    let (res, forward_by) = Parser::new(&query_packet).parse().unwrap();
    assert_eq!(
        res,
        Query::SimpleQuery(vec![
            "GET".as_bytes().to_owned(),
            "foo".as_bytes().to_owned()
        ])
    );
    assert_eq!(forward_by, query_packet.len());
}
