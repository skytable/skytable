/*
 * Created on Wed Sep 20 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

use crate::engine::mem::scanner::{BufferedScanner, ScannerDecodeResult};

fn s(b: &[u8]) -> BufferedScanner {
    BufferedScanner::new(b)
}

/*
    lf separated
*/

#[sky_macros::test]
fn read_u64_lf_separated() {
    let mut s = s(b"18446744073709551615\n");
    assert_eq!(
        s.try_next_ascii_u64_lf_separated_or_restore_cursor()
            .unwrap(),
        u64::MAX
    );
    assert_eq!(s.cursor(), s.buffer_len());
}

#[sky_macros::test]
fn read_u64_lf_separated_missing() {
    let mut s = s(b"18446744073709551615");
    assert!(s
        .try_next_ascii_u64_lf_separated_or_restore_cursor()
        .is_none());
    assert_eq!(s.cursor(), 0);
}

#[sky_macros::test]
fn read_u64_lf_separated_invalid() {
    let mut scn = s(b"1844674407370955161A\n");
    assert!(scn
        .try_next_ascii_u64_lf_separated_or_restore_cursor()
        .is_none());
    assert_eq!(scn.cursor(), 0);
    let mut scn = s(b"?1844674407370955161A\n");
    assert!(scn
        .try_next_ascii_u64_lf_separated_or_restore_cursor()
        .is_none());
    assert_eq!(scn.cursor(), 0);
}

#[sky_macros::test]
fn read_u64_lf_separated_zero() {
    let mut s = s(b"0\n");
    assert_eq!(
        s.try_next_ascii_u64_lf_separated_or_restore_cursor()
            .unwrap(),
        0
    );
    assert_eq!(s.cursor(), s.buffer_len());
}

#[sky_macros::test]
fn read_u64_lf_overflow() {
    let mut s = s(b"184467440737095516155\n");
    assert!(s
        .try_next_ascii_u64_lf_separated_or_restore_cursor()
        .is_none());
    assert_eq!(s.cursor(), 0);
}

/*
    lf separated allow unbuffered
*/

#[sky_macros::test]
fn incomplete_read_u64_okay() {
    let mut scn = s(b"18446744073709551615\n");
    assert_eq!(
        scn.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor(),
        ScannerDecodeResult::Value(u64::MAX)
    );
    assert_eq!(scn.cursor(), scn.buffer_len());
}

#[sky_macros::test]
fn incomplete_read_u64_missing_lf() {
    let mut scn = s(b"18446744073709551615");
    assert_eq!(
        scn.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor(),
        ScannerDecodeResult::NeedMore
    );
    assert_eq!(scn.cursor(), 0);
}

#[sky_macros::test]
fn incomplete_read_u64_lf_error() {
    let mut scn = s(b"1844674407370955161A\n");
    assert_eq!(
        scn.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor(),
        ScannerDecodeResult::Error
    );
    assert_eq!(scn.cursor(), 0);
    let mut scn = s(b"?1844674407370955161A\n");
    assert_eq!(
        scn.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor(),
        ScannerDecodeResult::Error
    );
    assert_eq!(scn.cursor(), 0);
}

#[sky_macros::test]
fn incomplete_read_u64_lf_zero() {
    let mut scn = s(b"0\n");
    assert_eq!(
        scn.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor(),
        ScannerDecodeResult::Value(0)
    )
}

#[sky_macros::test]
fn incomplete_read_u64_lf_overflow() {
    let mut s = s(b"184467440737095516155\n");
    assert_eq!(
        s.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor(),
        ScannerDecodeResult::Error
    );
    assert_eq!(s.cursor(), 0);
}

/*
    lf separated i64
*/

fn concat(a: impl ToString, b: impl ToString) -> Vec<u8> {
    let (a, b) = (a.to_string(), b.to_string());
    let mut s = String::with_capacity(a.len() + b.len());
    s.push_str(a.as_str());
    s.push_str(b.as_str());
    s.into_bytes()
}

#[sky_macros::test]
fn read_i64_lf_separated_okay() {
    let buf = concat(i64::MAX, "\n");
    let mut scn = s(&buf);
    assert_eq!(
        scn.try_next_ascii_i64_separated_by::<b'\n'>(),
        (true, i64::MAX)
    );
    assert_eq!(scn.cursor(), scn.buffer_len());
    let buf = concat(i64::MIN, "\n");
    let mut scn = s(&buf);
    assert_eq!(
        scn.try_next_ascii_i64_separated_by::<b'\n'>(),
        (true, i64::MIN)
    );
    assert_eq!(scn.cursor(), scn.buffer_len());
}

#[sky_macros::test]
fn read_i64_lf_separated_missing() {
    let buf = concat(i64::MAX, "");
    let mut scn = s(&buf);
    assert_eq!(
        scn.try_next_ascii_i64_separated_by::<b'\n'>(),
        (false, i64::MAX)
    );
    assert_eq!(scn.cursor(), scn.buffer_len());
    let buf = concat(i64::MIN, "");
    let mut scn = s(&buf);
    assert_eq!(
        scn.try_next_ascii_i64_separated_by::<b'\n'>(),
        (false, i64::MIN)
    );
    assert_eq!(scn.cursor(), scn.buffer_len());
}

#[sky_macros::test]
fn read_i64_lf_separated_invalid() {
    let buf = concat(i64::MAX, "A\n");
    let mut scn = s(&buf);
    assert_eq!(
        scn.try_next_ascii_i64_separated_by::<b'\n'>(),
        (false, i64::MAX)
    );
    assert_eq!(scn.cursor(), scn.buffer_len() - 1);
    let buf = concat("A", format!("{}\n", i64::MIN));
    let mut scn = s(&buf);
    assert_eq!(scn.try_next_ascii_i64_separated_by::<b'\n'>(), (false, 0));
    assert_eq!(scn.cursor(), 0);
}

#[sky_macros::test]
fn read_i64_lf_overflow() {
    let buf = concat(u64::MAX, "\n");
    let mut scn = s(&buf);
    assert_eq!(
        scn.try_next_ascii_i64_separated_by::<b'\n'>(),
        (false, 1844674407370955161)
    );
    assert_eq!(scn.cursor(), scn.buffer_len() - 1);
}

#[sky_macros::test]
fn read_i64_lf_underflow() {
    let buf = concat(i64::MIN, "1\n");
    let mut scn = s(&buf);
    assert_eq!(
        scn.try_next_ascii_i64_separated_by::<b'\n'>(),
        (false, -9223372036854775808)
    );
    assert_eq!(scn.cursor(), scn.buffer_len() - 1);
}

#[sky_macros::test]
fn rounding() {
    let mut scanner = s(b"123");
    for i in 1..=u8::MAX {
        match i {
            1..=3 => {
                assert_eq!(scanner.try_next_byte().unwrap(), (i + b'0'));
            }
            _ => {
                assert_eq!(scanner.rounded_cursor_value(), b'3');
            }
        }
    }
    assert_eq!(scanner.cursor(), scanner.buffer_len());
}
