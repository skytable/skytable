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

use std::hint::unreachable_unchecked;

/// The header magic (a '\r' or CR)
///
/// This demarcates distinct query packets; all queries begin with this byte
const START_HEADER_MAGIC: u8 = 0x0D;

#[derive(Debug)]
pub(super) struct Parser<'a> {
    cursor: usize,
    buffer: &'a [u8],
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// Didn't get the number of expected bytes
    NotEnough,
    /// The query contains an unexpected byte
    UnexpectedByte,
    /// The packet simply contains invalid data
    BadPacket,
    /// A data type was given but the parser failed to serialize it into this type
    DataTypeParseError,
    /// A data type that the server doesn't know was passed into the query
    ///
    /// This is a frequent problem that can arise between different server editions as more data types
    /// can be added with changing server versions
    UnknownDatatype,
}

type ActionGroup = Vec<Vec<u8>>;

#[derive(Debug, PartialEq)]
pub enum Query {
    SimpleQuery(ActionGroup),
    PipelinedQuery(Vec<ActionGroup>),
}

#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum DataType {
    /// Arrays can be nested! Their `<tsymbol>` is `&`
    Array(Vec<DataType>),
    /// A String value; `<tsymbol>` is `+`
    String(String),
    /// An unsigned integer value; `<tsymbol>` is `:`
    UnsignedInt(u64),
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
    fn read_sizeline(&mut self, opt_char: Option<u8>) -> ParseResult<usize> {
        if let Some(b) = self.buffer.get(self.cursor) {
            if *b == opt_char.unwrap_or(b'#') {
                // Good, we found a opt_char; time to move ahead
                self.incr_cursor();
                // Now read the remaining line
                let (started_at, stopped_at) = self.read_line();
                return Self::parse_into_usize(&self.buffer[started_at..stopped_at]);
            } else {
                // A sizeline should begin with a opt_char; this one doesn't so it's a bad packet; ugh
                Err(ParseError::UnexpectedByte)
            }
        } else {
            Err(ParseError::NotEnough)
        }
    }
    fn incr_cursor(&mut self) {
        self.cursor += 1;
    }
    fn will_cursor_give_char(&self, ch: u8, this_if_nothing_ahead: bool) -> ParseResult<bool> {
        self.buffer.get(self.cursor).map_or(
            if this_if_nothing_ahead {
                Ok(true)
            } else {
                Err(ParseError::NotEnough)
            },
            |v| Ok(*v == ch),
        )
    }
    fn will_cursor_give_linefeed(&self) -> ParseResult<bool> {
        self.will_cursor_give_char(b'\n', false)
    }
    fn parse_into_usize(bytes: &[u8]) -> ParseResult<usize> {
        let mut byte_iter = bytes.into_iter();
        let mut item_usize = 0usize;
        while let Some(dig) = byte_iter.next() {
            // 48 is the ASCII code for 0, and 57 is the ascii code for 9
            // so if 0 is given, the subtraction should give 0; similarly
            // if 9 is given, the subtraction should give us 9!
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
            // The usize can overflow; check that case
            let product = match item_usize.checked_mul(10) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeParseError),
            };
            let sum = match product.checked_add(curdig) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeParseError),
            };
            item_usize = sum;
        }
        Ok(item_usize)
    }
    fn parse_into_u64(bytes: &[u8]) -> ParseResult<u64> {
        let mut byte_iter = bytes.into_iter();
        let mut item_u64 = 0u64;
        while let Some(dig) = byte_iter.next() {
            // 48 is the ASCII code for 0, and 57 is the ascii code for 9
            // so if 0 is given, the subtraction should give 0; similarly
            // if 9 is given, the subtraction should give us 9!
            let curdig: u64 = match dig.checked_sub(48) {
                Some(dig) => {
                    if dig > 9 {
                        return Err(ParseError::UnexpectedByte);
                    } else {
                        dig.into()
                    }
                }
                None => return Err(ParseError::UnexpectedByte),
            };
            // Now the entire u64 can overflow, so let's attempt to check it
            let product = match item_u64.checked_mul(10) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeParseError),
            };
            let sum = match product.checked_add(curdig) {
                Some(not_overflowed) => not_overflowed,
                None => return Err(ParseError::DataTypeParseError),
            };
            item_u64 = sum;
        }
        Ok(item_u64)
    }
    /// This will return the number of datagroups present in this query packet
    ///
    /// This **will forward the cursor itself**
    fn parse_metaframe_get_datagroup_count(&mut self) -> ParseResult<usize> {
        // This will give us the `\r<m>\n`
        let metaframe_sizeline = self.read_sizeline(Some(START_HEADER_MAGIC))?;
        // Now we want to read `*<n>\n`
        let our_chunk = self.read_until(metaframe_sizeline)?;
        if our_chunk[0] == b'*' {
            // Good, this will tell us the number of actions
            // Let us attempt to read the usize from this point onwards
            // that is excluding the '!' (so 1..)
            // also push the cursor ahead because we want to ignore the LF char
            // as read_until won't skip the newline
            let ret = Self::parse_into_usize(&our_chunk[1..])?;
            if self.will_cursor_give_linefeed()? {
                // check if the cursor points at the terminal character LF
                // as it does, we can freely move past
                self.incr_cursor();
                Ok(ret)
            } else {
                Err(ParseError::UnexpectedByte)
            }
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// This will return the number of items in a datagroup
    fn parse_datagroup_get_group_size(&mut self) -> ParseResult<usize> {
        // This will give us `#<p>\n`
        let dataframe_sizeline = self.read_sizeline(None)?;
        // Now we want to read `&<q>\n`
        let our_chunk = self.read_until(dataframe_sizeline)?;
        if our_chunk[0] == b'&' {
            // Good, so this is indeed a datagroup!
            // Let us attempt to read the usize from this point onwards
            // excluding the '&' char (so 1..)
            // also push the cursor ahead
            let ret = Self::parse_into_usize(&our_chunk[1..])?;
            if self.will_cursor_give_linefeed()? {
                // check if the cursor points at the terminal character LF
                // since it does, we'll move past
                self.incr_cursor();
                Ok(ret)
            } else {
                Err(ParseError::UnexpectedByte)
            }
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// Get the next element **without** the tsymbol
    ///
    /// This function **does not forward the newline**
    fn __get_next_element(&mut self) -> ParseResult<&[u8]> {
        let string_sizeline = self.read_line();
        let string_size =
            Self::parse_into_usize(&self.buffer[string_sizeline.0..string_sizeline.1])?;
        let our_chunk = self.read_until(string_size)?;
        Ok(our_chunk)
    }
    /// The cursor should have passed the `+` tsymbol
    fn parse_next_string(&mut self) -> ParseResult<String> {
        let our_string_chunk = self.__get_next_element()?;
        let our_string = String::from_utf8_lossy(&our_string_chunk).to_string();
        if self.will_cursor_give_linefeed()? {
            // there is a lf after the end of the string; great!
            // let's skip that now
            self.incr_cursor();
            // let's return our string
            Ok(our_string)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// The cursor should have passed the `:` tsymbol
    fn parse_next_u64(&mut self) -> ParseResult<u64> {
        let our_u64_chunk = self.__get_next_element()?;
        let our_u64 = Self::parse_into_u64(our_u64_chunk)?;
        if self.will_cursor_give_linefeed()? {
            // line feed after u64; heck yeah!
            self.incr_cursor();
            // return it
            Ok(our_u64)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    /// The cursor should be **at the tsymbol**
    fn parse_next_element(&mut self) -> ParseResult<DataType> {
        if let Some(tsymbol) = self.buffer.get(self.cursor) {
            // so we have a tsymbol; nice, let's match it
            // but advance the cursor before doing that
            self.incr_cursor();
            let ret = match *tsymbol {
                b'+' => DataType::String(self.parse_next_string()?),
                b':' => DataType::UnsignedInt(self.parse_next_u64()?),
                b'&' => DataType::Array(self.parse_next_array()?),
                _ => return Err(ParseError::UnknownDatatype),
            };
            Ok(ret)
        } else {
            // Not enough bytes to read an element
            Err(ParseError::NotEnough)
        }
    }
    /// The tsymbol `&` should have been passed!
    fn parse_next_array(&mut self) -> ParseResult<Vec<DataType>> {
        let (start, stop) = self.read_line();
        let array_size = Self::parse_into_usize(&self.buffer[start..stop])?;
        let mut array = Vec::with_capacity(array_size);
        for _ in 0..array_size {
            array.push(self.parse_next_element()?);
        }
        Ok(array)
    }
    /// This will read a datagroup element and return an **owned vector** containing the bytes
    /// for the next datagroup element
    fn parse_next_datagroup_element(&mut self) -> ParseResult<Vec<u8>> {
        // So we need to read the sizeline for this element first!
        let element_size = self.read_sizeline(None)?;
        // Now we want to read the element itself
        let mut ret = Vec::with_capacity(element_size);
        ret.extend_from_slice(self.read_until(element_size)?);
        if self.will_cursor_give_linefeed()? {
            // check if the cursor points at the terminal character LF
            // We'll just return this since that's all we have to do!
            // we just checked that the cursor points at the LF, so let's also move ahead before returning
            self.incr_cursor();
            Ok(ret)
        } else {
            Err(ParseError::UnexpectedByte)
        }
    }
    fn parse_next_actiongroup(&mut self) -> ParseResult<Vec<Vec<u8>>> {
        let len = self.parse_datagroup_get_group_size()?;
        let mut elements = Vec::with_capacity(len);
        // so we expect `len` count of elements; let's iterate and get each element in turn
        for _ in 0..len {
            elements.push(self.parse_next_datagroup_element()?);
        }
        Ok(elements)
    }
    pub fn parse(mut self) -> Result<(Query, usize), ParseError> {
        let number_of_queries = self.parse_metaframe_get_datagroup_count()?;
        if number_of_queries == 0 {
            // how on earth do you expect us to execute 0 queries? waste of bandwidth
            return Err(ParseError::BadPacket);
        }
        if number_of_queries == 1 {
            // This is a simple query
            let single_group = self.parse_next_actiongroup()?;
            // The below line defaults to false if no item is there in the buffer
            // or it checks if the next time is a \r char; if it is, then it is the beginning
            // of the next query
            if self
                .will_cursor_give_char(b'\r', true)
                .unwrap_or_else(|_| unsafe {
                    // This will never be the case because we'll always get a result and no error value
                    // as we've passed true which will yield Ok(true) even if there is no byte ahead
                    unreachable_unchecked()
                })
            {
                Ok((Query::SimpleQuery(single_group), self.cursor))
            } else {
                // the next item isn't the beginning of a query but something else?
                // that doesn't look right!
                Err(ParseError::UnexpectedByte)
            }
        } else {
            // This is a pipelined query
            // We'll first make space for all the actiongroups
            let mut queries = Vec::with_capacity(number_of_queries);
            for _ in 0..number_of_queries {
                queries.push(self.parse_next_actiongroup()?);
            }
            if self.will_cursor_give_char(b'\r', true)? {
                Ok((Query::PipelinedQuery(queries), self.cursor))
            } else {
                Err(ParseError::UnexpectedByte)
            }
        }
    }
}

#[test]
fn test_sizeline_parse() {
    let sizeline = "#125\n".as_bytes();
    let mut parser = Parser::new(&sizeline);
    assert_eq!(125, parser.read_sizeline(None).unwrap());
    assert_eq!(parser.cursor, sizeline.len());
}

#[test]
#[should_panic]
fn test_fail_sizeline_parse_wrong_firstbyte() {
    let sizeline = "125\n".as_bytes();
    let mut parser = Parser::new(&sizeline);
    parser.read_sizeline(None).unwrap();
}

#[test]
fn test_metaframe_parse() {
    let metaframe = "\r2\n*2\n".as_bytes();
    let mut parser = Parser::new(&metaframe);
    assert_eq!(2, parser.parse_metaframe_get_datagroup_count().unwrap());
    assert_eq!(parser.cursor, metaframe.len());
}

#[test]
fn test_cursor_next_char() {
    let bytes = &[b'\n'];
    assert!(Parser::new(&bytes[..])
        .will_cursor_give_char(b'\n', false)
        .unwrap());
    let bytes = &[];
    assert!(Parser::new(&bytes[..])
        .will_cursor_give_char(b'\r', true)
        .unwrap());
    let bytes = &[];
    assert!(
        Parser::new(&bytes[..])
            .will_cursor_give_char(b'\n', false)
            .unwrap_err()
            == ParseError::NotEnough
    );
}

#[test]
fn test_metaframe_parse_fail() {
    // First byte should be CR and not $
    let metaframe = "$2\n*2\n".as_bytes();
    let mut parser = Parser::new(&metaframe);
    assert_eq!(
        parser.parse_metaframe_get_datagroup_count().unwrap_err(),
        ParseError::UnexpectedByte
    );
    // Give a wrong length approximation
    let metaframe = "\r1\n*2\n".as_bytes();
    assert_eq!(
        Parser::new(&metaframe)
            .parse_metaframe_get_datagroup_count()
            .unwrap_err(),
        ParseError::UnexpectedByte
    );
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
    let query_packet = "\r2\n*1\n#2\n&2\n#3\nGET\n#3\nfoo\n".as_bytes();
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

#[test]
#[should_panic]
fn test_query_parse_fail() {
    // this packet has an extra \n, where it should have been nothing or a \r
    let query_packet = "\r2\n*1\n#2\n&2\n#3\nGET\n#3\nfoo\n\n".as_bytes();
    Parser::new(&query_packet).parse().unwrap();
}

#[test]
fn test_query_parse_pass_part_of_next_query() {
    // we read a part of the next query, we should happily ignore it (`\r2\n*1\n`)
    let query_packet = "\r2\n*1\n#2\n&2\n#3\nGET\n#3\nfoo\n\r2\n*1\n".as_bytes();
    let (ret, forward_by) = Parser::new(&query_packet).parse().unwrap();
    assert_eq!(
        ret,
        Query::SimpleQuery(vec![
            "GET".to_owned().into_bytes(),
            "foo".to_owned().into_bytes()
        ])
    );
    // the cursor should be at the '\n' byte
    assert!(forward_by == query_packet.len() - "\r2\n*1\n".len());
}

#[test]
fn test_query_fail_not_enough() {
    let query_packet = "\r2".as_bytes();
    assert_eq!(
        Parser::new(&query_packet).parse().err().unwrap(),
        ParseError::NotEnough
    );
    let query_packet = "\r2\n*".as_bytes();
    assert_eq!(
        Parser::new(&query_packet).parse().err().unwrap(),
        ParseError::NotEnough
    );
    let metaframe = "\r2\n*".as_bytes();
    assert_eq!(
        Parser::new(&metaframe)
            .parse_metaframe_get_datagroup_count()
            .unwrap_err(),
        ParseError::NotEnough
    );
    let metaframe = "\r2\n*1\n".as_bytes();
    // we just got the metaframe, but there are more bytes, so this is incomplete!
    assert_eq!(
        Parser::new(&metaframe).parse().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn test_query_incomplete_dataframes() {
    let metaframe_and_dataframe_layout = "\r2\n*1\n#2\n&2\n".as_bytes();
    // we got the number of queries and the size of the data group, but no data; this is incomplete
    assert_eq!(
        Parser::new(&metaframe_and_dataframe_layout)
            .parse()
            .unwrap_err(),
        ParseError::NotEnough
    );
    let metaframe_and_dataframe_layout = "\r2\n*1\n#2\n&2\n#3\nGET\n".as_bytes();
    // we got the number of queries and the size of the data group, but a part of the data; this is incomplete
    assert_eq!(
        Parser::new(&metaframe_and_dataframe_layout)
            .parse()
            .unwrap_err(),
        ParseError::NotEnough
    );
    // incomplete sayan? that shouldn't work anyway heh ;)
    let metaframe_and_dataframe_layout = "\r2\n*1\n#2\n&2\n#3\nGET\n#5\nsaya".as_bytes();
    // we got the number of queries and the size of the data group, but a part of the data; this is incomplete
    assert_eq!(
        Parser::new(&metaframe_and_dataframe_layout)
            .parse()
            .unwrap_err(),
        ParseError::NotEnough
    );
    let metaframe_and_dataframe_layout = "\r2\n*1\n#2\n&2\n#3\nGET\n#5\nsayan".as_bytes();
    // we got the entire thing but not a newline; technically this is incomplete
    assert_eq!(
        Parser::new(&metaframe_and_dataframe_layout)
            .parse()
            .unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn test_parse_next_string() {
    let bytes = "5\nsayan\n".as_bytes();
    let st = Parser::new(&bytes).parse_next_string().unwrap();
    assert_eq!(st, "sayan".to_owned());
}

#[test]
fn test_parse_next_u64() {
    let max = 18446744073709551615;
    assert!(u64::MAX == max);
    let bytes = "20\n18446744073709551615\n".as_bytes();
    let our_u64 = Parser::new(&bytes).parse_next_u64().unwrap();
    assert_eq!(our_u64, max);
    // now overflow the u64
    let bytes = "21\n184467440737095516156\n".as_bytes();
    let our_u64 = Parser::new(&bytes).parse_next_u64().unwrap_err();
    assert_eq!(our_u64, ParseError::DataTypeParseError);
}

#[test]
fn test_parse_next_element_string() {
    let bytes = "+5\nsayan\n".as_bytes();
    let next_element = Parser::new(&bytes).parse_next_element().unwrap();
    assert_eq!(next_element, DataType::String("sayan".to_owned()));
}

#[test]
fn test_parse_next_element_u64() {
    let bytes = ":20\n18446744073709551615\n".as_bytes();
    let our_u64 = Parser::new(&bytes).parse_next_element().unwrap();
    assert_eq!(our_u64, DataType::UnsignedInt(u64::MAX));
}

#[test]
fn test_parse_next_element_array() {
    let bytes = "&3\n+4\nMGET\n+3\nfoo\n+3\nbar\n".as_bytes();
    let mut parser = Parser::new(&bytes);
    let array = parser.parse_next_element().unwrap();
    assert_eq!(
        array,
        DataType::Array(vec![
            DataType::String("MGET".to_owned()),
            DataType::String("foo".to_owned()),
            DataType::String("bar".to_owned())
        ])
    );
    assert_eq!(parser.cursor, bytes.len());
}

#[test]
fn test_parse_nested_array() {
    // let's test a nested array
    let bytes =
        "&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&2\n+6\nreally\n+4\nhard\n"
            .as_bytes();
    let mut parser = Parser::new(&bytes);
    let array = parser.parse_next_element().unwrap();
    assert_eq!(
        array,
        DataType::Array(vec![
            DataType::String("ACT".to_owned()),
            DataType::String("foo".to_owned()),
            DataType::Array(vec![
                DataType::String("sayan".to_owned()),
                DataType::String("is".to_owned()),
                DataType::String("working".to_owned()),
                DataType::Array(vec![
                    DataType::String("really".to_owned()),
                    DataType::String("hard".to_owned())
                ])
            ])
        ])
    );
    assert_eq!(parser.cursor, bytes.len());
}

#[test]
fn test_parse_multitype_array() {
    // let's test a nested array
    let bytes = "&3\n+3\nACT\n+3\nfoo\n&4\n+5\nsayan\n+2\nis\n+7\nworking\n&2\n:2\n23\n+5\napril\n"
        .as_bytes();
    let mut parser = Parser::new(&bytes);
    let array = parser.parse_next_element().unwrap();
    assert_eq!(
        array,
        DataType::Array(vec![
            DataType::String("ACT".to_owned()),
            DataType::String("foo".to_owned()),
            DataType::Array(vec![
                DataType::String("sayan".to_owned()),
                DataType::String("is".to_owned()),
                DataType::String("working".to_owned()),
                DataType::Array(vec![
                    DataType::UnsignedInt(23),
                    DataType::String("april".to_owned())
                ])
            ])
        ])
    );
    assert_eq!(parser.cursor, bytes.len());
}
