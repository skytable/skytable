/*
 * Created on Tue Apr 12 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use super::Parser;
use crate::protocol::ParseError;
use std::iter::Map;
use std::vec::IntoIter as VecIntoIter;

type IterPacketWithLen = Map<VecIntoIter<Vec<u8>>, fn(Vec<u8>) -> (usize, Vec<u8>)>;
type Packets = Vec<Vec<u8>>;

macro_rules! v {
    () => {
        vec![]
    };
    ($literal:literal) => {
        $literal.to_vec()
    };
}

fn ensure_exhausted(p: &Parser) {
    assert!(!p.not_exhausted());
    assert!(p.exhausted());
}

fn ensure_remaining(p: &Parser, r: usize) {
    assert_eq!(p.remaining(), r);
    assert!(p.has_remaining(r));
}

fn ensure_not_exhausted(p: &Parser) {
    assert!(p.not_exhausted());
    assert!(!p.exhausted());
}

fn get_slices(slices: &[&[u8]]) -> Packets {
    slices.iter().map(|slc| slc.to_vec()).collect()
}

fn ensure_zero_reads(parser: &mut Parser) {
    let r = parser.read_until(0).unwrap();
    unsafe {
        let slice = r.as_slice();
        assert_eq!(slice, b"");
        assert!(slice.is_empty());
    }
}

// We do this intentionally for "heap simulation"
fn slices() -> Packets {
    const SLICE_COLLECTION: &[&[u8]] = &[
        b"",
        b"a",
        b"ab",
        b"abc",
        b"abcd",
        b"abcde",
        b"abcdef",
        b"abcdefg",
        b"abcdefgh",
        b"abcdefghi",
        b"abcdefghij",
        b"abcdefghijk",
        b"abcdefghijkl",
        b"abcdefghijklm",
    ];
    get_slices(SLICE_COLLECTION)
}

fn get_slices_with_len(slices: Packets) -> IterPacketWithLen {
    slices.into_iter().map(|slc| (slc.len(), slc))
}

fn slices_with_len() -> IterPacketWithLen {
    get_slices_with_len(slices())
}

fn slices_lf() -> Packets {
    const SLICE_COLLECTION: &[&[u8]] = &[
        b"",
        b"a\n",
        b"ab\n",
        b"abc\n",
        b"abcd\n",
        b"abcde\n",
        b"abcdef\n",
        b"abcdefg\n",
        b"abcdefgh\n",
        b"abcdefghi\n",
        b"abcdefghij\n",
        b"abcdefghijk\n",
        b"abcdefghijkl\n",
        b"abcdefghijklm\n",
    ];
    get_slices(SLICE_COLLECTION)
}

fn slices_lf_with_len() -> IterPacketWithLen {
    get_slices_with_len(slices_lf())
}

// "actual" tests
// data_end_ptr
#[test]
fn data_end_ptr() {
    for (len, src) in slices_with_len() {
        let parser = Parser::new(&src);
        unsafe {
            assert_eq!(parser.data_end_ptr(), src.as_ptr().add(len));
        }
    }
}

// cursor_ptr
#[test]
fn cursor_ptr() {
    for src in slices() {
        let parser = Parser::new(&src);
        assert_eq!(parser.cursor_ptr(), src.as_ptr())
    }
}
#[test]
fn cursor_ptr_with_incr() {
    for src in slices() {
        let mut parser = Parser::new(&src);
        unsafe {
            parser.incr_cursor_by(src.len());
            assert_eq!(parser.cursor_ptr(), src.as_ptr().add(src.len()));
        }
    }
}

// remaining
#[test]
fn remaining() {
    for (len, src) in slices_with_len() {
        let parser = Parser::new(&src);
        assert_eq!(parser.remaining(), len);
    }
}
#[test]
fn remaining_with_incr() {
    for (len, src) in slices_with_len() {
        let mut parser = Parser::new(&src);
        unsafe {
            // no change
            parser.incr_cursor_by(0);
            assert_eq!(parser.remaining(), len);
            if len != 0 {
                // move one byte ahead. should reach EOA or len - 1
                parser.incr_cursor();
                assert_eq!(parser.remaining(), len - 1);
                // move the cursor to the end; should reach EOA
                parser.incr_cursor_by(len - 1);
                assert_eq!(parser.remaining(), 0);
            }
        }
    }
}

// has_remaining
#[test]
fn has_remaining() {
    for (len, src) in slices_with_len() {
        let parser = Parser::new(&src);
        assert!(parser.has_remaining(len), "should have {len} remaining")
    }
}
#[test]
fn has_remaining_with_incr() {
    for (len, src) in slices_with_len() {
        let mut parser = Parser::new(&src);
        unsafe {
            // no change
            parser.incr_cursor_by(0);
            assert!(parser.has_remaining(len));
            if len != 0 {
                // move one byte ahead. should reach EOA or len - 1
                parser.incr_cursor();
                assert!(parser.has_remaining(len - 1));
                // move the cursor to the end; should reach EOA
                parser.incr_cursor_by(len - 1);
                assert!(!parser.has_remaining(1));
                // should always be true
                assert!(parser.has_remaining(0));
            }
        }
    }
}

// exhausted
#[test]
fn exhausted() {
    for src in slices() {
        let parser = Parser::new(&src);
        if src.is_empty() {
            assert!(parser.exhausted());
        } else {
            assert!(!parser.exhausted())
        }
    }
}
#[test]
fn exhausted_with_incr() {
    for (len, src) in slices_with_len() {
        let mut parser = Parser::new(&src);
        if len == 0 {
            assert!(parser.exhausted());
        } else {
            assert!(!parser.exhausted());
            unsafe {
                parser.incr_cursor();
                if len == 1 {
                    assert!(parser.exhausted());
                } else {
                    assert!(!parser.exhausted());
                    parser.incr_cursor_by(len - 1);
                    assert!(parser.exhausted());
                }
            }
        }
    }
}

// not_exhausted
#[test]
fn not_exhausted() {
    for src in slices() {
        let parser = Parser::new(&src);
        if src.is_empty() {
            assert!(!parser.not_exhausted());
        } else {
            assert!(parser.not_exhausted())
        }
    }
}
#[test]
fn not_exhausted_with_incr() {
    for (len, src) in slices_with_len() {
        let mut parser = Parser::new(&src);
        if len == 0 {
            assert!(!parser.not_exhausted());
        } else {
            assert!(parser.not_exhausted());
            unsafe {
                parser.incr_cursor();
                if len == 1 {
                    assert!(!parser.not_exhausted());
                } else {
                    assert!(parser.not_exhausted());
                    parser.incr_cursor_by(len - 1);
                    assert!(!parser.not_exhausted());
                }
            }
        }
    }
}

// read_until
#[test]
fn read_until_empty() {
    let b = v!(b"");
    let mut parser = Parser::new(&b);
    ensure_zero_reads(&mut parser);
    assert_eq!(parser.read_until(1).unwrap_err(), ParseError::NotEnough);
}

#[test]
fn read_until_nonempty() {
    for (len, src) in slices_with_len() {
        let mut parser = Parser::new(&src);
        // should always work
        ensure_zero_reads(&mut parser);
        // now read the entire length; should always work
        let r = parser.read_until(len).unwrap();
        unsafe {
            let slice = r.as_slice();
            assert_eq!(slice, src.as_slice());
            assert_eq!(slice.len(), len);
        }
        // even after the buffer is exhausted, `0` should always work
        ensure_zero_reads(&mut parser);
    }
}

#[test]
fn read_until_not_enough() {
    for (len, src) in slices_with_len() {
        let mut parser = Parser::new(&src);
        ensure_zero_reads(&mut parser);
        // try to read more than the amount of data bufferred
        assert_eq!(
            parser.read_until(len + 1).unwrap_err(),
            ParseError::NotEnough
        );
        // should the above fail, zero reads should still work
        ensure_zero_reads(&mut parser);
    }
}

#[test]
fn read_until_more_bytes() {
    let sample1 = v!(b"abcd1");
    let mut p1 = Parser::new(&sample1);
    unsafe {
        assert_eq!(
            p1.read_until(&sample1.len() - 1).unwrap().as_slice(),
            &sample1[..&sample1.len() - 1]
        );
        // ensure we have not exhasuted
        ensure_not_exhausted(&p1);
        ensure_remaining(&p1, 1);
    }
    let sample2 = v!(b"abcd1234567890!@#$");
    let mut p2 = Parser::new(&sample2);
    unsafe {
        assert_eq!(p2.read_until(4).unwrap().as_slice(), &sample2[..4]);
        // ensure we have not exhasuted
        ensure_not_exhausted(&p2);
        ensure_remaining(&p2, sample2.len() - 4);
    }
}

// read_line
#[test]
fn read_line_special_case_only_lf() {
    let b = v!(b"\n");
    let mut parser = Parser::new(&b);
    unsafe {
        let r = parser.read_line().unwrap();
        let slice = r.as_slice();
        assert_eq!(slice, b"");
        assert!(slice.is_empty());
    };
    // ensure it is exhausted
    ensure_exhausted(&parser);
}

#[test]
fn read_line() {
    for (len, src) in slices_lf_with_len() {
        let mut parser = Parser::new(&src);
        if len == 0 {
            // should be empty, so NotEnough
            assert_eq!(parser.read_line().unwrap_err(), ParseError::NotEnough);
        } else {
            // should work
            unsafe {
                assert_eq!(
                    parser.read_line().unwrap().as_slice(),
                    &src.as_slice()[..len - 1]
                );
            }
            // now, we attempt to read which should work
            ensure_zero_reads(&mut parser);
        }
        // ensure it is exhausted
        ensure_exhausted(&parser);
        // now, we attempt to read another line which should fail
        assert_eq!(parser.read_line().unwrap_err(), ParseError::NotEnough);
        // ensure that cursor is at end
        unsafe {
            assert_eq!(parser.cursor_ptr(), src.as_ptr().add(len));
        }
    }
}

#[test]
fn read_line_more_bytes() {
    let sample1 = v!(b"abcd\n1");
    let mut p1 = Parser::new(&sample1);
    let line = p1.read_line().unwrap();
    unsafe {
        assert_eq!(line.as_slice(), b"abcd");
    }
    // we should still have one remaining
    ensure_not_exhausted(&p1);
    ensure_remaining(&p1, 1);
}

#[test]
fn read_line_subsequent_lf() {
    let sample1 = v!(b"abcd\n1\n");
    let mut p1 = Parser::new(&sample1);
    let line = p1.read_line().unwrap();
    unsafe {
        assert_eq!(line.as_slice(), b"abcd");
    }
    // we should still have two octets remaining
    ensure_not_exhausted(&p1);
    ensure_remaining(&p1, 2);
    // and we should be able to read in another line
    let line = p1.read_line().unwrap();
    unsafe {
        assert_eq!(line.as_slice(), b"1");
    }
    ensure_exhausted(&p1);
}

#[test]
fn read_line_pedantic_okay() {
    for (len, src) in slices_lf_with_len() {
        let mut parser = Parser::new(&src);
        if len == 0 {
            // should be empty, so NotEnough
            assert_eq!(
                parser.read_line_pedantic().unwrap_err(),
                ParseError::NotEnough
            );
        } else {
            // should work
            unsafe {
                assert_eq!(
                    parser.read_line_pedantic().unwrap().as_slice(),
                    &src.as_slice()[..len - 1]
                );
            }
            // now, we attempt to read which should work
            ensure_zero_reads(&mut parser);
        }
        // ensure it is exhausted
        ensure_exhausted(&parser);
        // now, we attempt to read another line which should fail
        assert_eq!(
            parser.read_line_pedantic().unwrap_err(),
            ParseError::NotEnough
        );
        // ensure that cursor is at end
        unsafe {
            assert_eq!(parser.cursor_ptr(), src.as_ptr().add(len));
        }
    }
}

#[test]
fn read_line_pedantic_fail_empty() {
    let payload = v!(b"");
    assert_eq!(
        Parser::new(&payload).read_line_pedantic().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn read_line_pedantic_fail_only_lf() {
    let payload = v!(b"\n");
    assert_eq!(
        Parser::new(&payload).read_line_pedantic().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn read_line_pedantic_fail_only_lf_extra_data() {
    let payload = v!(b"\n1");
    assert_eq!(
        Parser::new(&payload).read_line_pedantic().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn read_usize_fail_empty() {
    let payload = v!(b"");
    assert_eq!(
        Parser::new(&payload).read_usize().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn read_usize_fail_no_lf() {
    let payload = v!(b"1");
    assert_eq!(
        Parser::new(&payload).read_usize().unwrap_err(),
        ParseError::NotEnough
    );
}

#[test]
fn read_usize_okay() {
    let payload = v!(b"1\n");
    assert_eq!(Parser::new(&payload).read_usize().unwrap(), 1);
    let payload = v!(b"1234\n");
    assert_eq!(Parser::new(&payload).read_usize().unwrap(), 1234);
}
