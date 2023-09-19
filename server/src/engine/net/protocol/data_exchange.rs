/*
 * Created on Mon Sep 18 2023
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

/*
    Skyhash/2 Data Exchange Packets
    ---
    1. Client side packet:
    a. SQ
    S<len>\n<payload>
    b. PQ
    P<count>\n(<pld len>\n<pld>)*

    TODO(@ohsayan): Restore pipeline impl
*/

use crate::{engine::mem::BufferedScanner, util::compiler};
use std::slice;

#[derive(Debug, PartialEq)]
pub struct CSQuery<'a> {
    query: &'a [u8],
}

impl<'a> CSQuery<'a> {
    pub(super) const fn new(query: &'a [u8]) -> Self {
        Self { query }
    }
    pub const fn query(&self) -> &'a [u8] {
        self.query
    }
}

#[derive(Debug, PartialEq)]
pub enum CSQueryState {
    Initial,
    SizeSegmentPart(u64),
    WaitingForFullBlock(usize),
}

impl Default for CSQueryState {
    fn default() -> Self {
        Self::Initial
    }
}

#[derive(Debug, PartialEq)]
pub enum CSQueryExchangeResult<'a> {
    Completed(CSQuery<'a>),
    ChangeState(CSQueryState, usize),
    PacketError,
}

impl<'a> CSQuery<'a> {
    pub fn resume_with(
        scanner: &mut BufferedScanner<'a>,
        state: CSQueryState,
    ) -> CSQueryExchangeResult<'a> {
        match state {
            CSQueryState::Initial => Self::resume_initial(scanner),
            CSQueryState::SizeSegmentPart(part) => Self::resume_at_meta_segment(scanner, part),
            CSQueryState::WaitingForFullBlock(size) => Self::resume_at_data_segment(scanner, size),
        }
    }
}

enum LFTIntParseResult {
    Value(u64),
    Partial(u64),
    Error,
}

fn parse_lf_separated(
    scanner: &mut BufferedScanner,
    previously_buffered: u64,
) -> LFTIntParseResult {
    let mut ret = previously_buffered;
    let mut okay = true;
    while scanner.rounded_cursor_not_eof_matches(|b| *b != b'\n') & okay {
        let b = unsafe { scanner.next_byte() };
        okay &= b.is_ascii_digit();
        ret = match ret.checked_mul(10) {
            Some(r) => r,
            None => return LFTIntParseResult::Error,
        };
        ret = match ret.checked_add((b & 0x0F) as u64) {
            Some(r) => r,
            None => return LFTIntParseResult::Error,
        };
    }
    let payload_ok = okay;
    let lf_ok = scanner.rounded_cursor_not_eof_matches(|b| *b == b'\n');
    unsafe { scanner.incr_cursor_by(lf_ok as usize) }
    if payload_ok & lf_ok {
        LFTIntParseResult::Value(ret)
    } else {
        if payload_ok {
            LFTIntParseResult::Partial(ret)
        } else {
            LFTIntParseResult::Error
        }
    }
}

impl<'a> CSQuery<'a> {
    pub const PREEMPTIVE_READ: usize = 4;
    const FIRST_BYTE: u8 = b'S';
    fn resume_initial(scanner: &mut BufferedScanner<'a>) -> CSQueryExchangeResult<'a> {
        if cfg!(debug_assertions) {
            if scanner.remaining() < Self::PREEMPTIVE_READ {
                return CSQueryExchangeResult::ChangeState(
                    CSQueryState::Initial,
                    Self::PREEMPTIVE_READ,
                );
            }
        } else {
            assert!(scanner.remaining() >= Self::PREEMPTIVE_READ);
        }
        // get our block
        let first_byte = unsafe { scanner.next_byte() };
        // be optimistic and check first byte later
        let size_of_query = match parse_lf_separated(scanner, 0) {
            LFTIntParseResult::Value(v) => v as usize,
            LFTIntParseResult::Partial(v) => {
                if compiler::unlikely(first_byte != Self::FIRST_BYTE) {
                    return CSQueryExchangeResult::PacketError;
                } else {
                    // expect at least 1 LF and at least 1 query byte
                    return CSQueryExchangeResult::ChangeState(CSQueryState::SizeSegmentPart(v), 2);
                }
            }
            LFTIntParseResult::Error => {
                // that's pretty much over
                return CSQueryExchangeResult::PacketError;
            }
        };
        if compiler::unlikely(first_byte != Self::FIRST_BYTE) {
            return CSQueryExchangeResult::PacketError;
        }
        Self::resume_at_data_segment(scanner, size_of_query)
    }
    fn resume_at_meta_segment(
        scanner: &mut BufferedScanner<'a>,
        previous: u64,
    ) -> CSQueryExchangeResult<'a> {
        match parse_lf_separated(scanner, previous) {
            LFTIntParseResult::Value(v) => Self::resume_at_data_segment(scanner, v as usize),
            LFTIntParseResult::Partial(p) => {
                CSQueryExchangeResult::ChangeState(CSQueryState::SizeSegmentPart(p), 2)
            }
            LFTIntParseResult::Error => CSQueryExchangeResult::PacketError,
        }
    }
    fn resume_at_data_segment(
        scanner: &mut BufferedScanner<'a>,
        size: usize,
    ) -> CSQueryExchangeResult<'a> {
        if scanner.has_left(size) {
            let slice;
            unsafe {
                // UNSAFE(@ohsayan): checked len at branch
                slice = slice::from_raw_parts(scanner.current_buffer().as_ptr(), size);
                scanner.incr_cursor_by(size);
            }
            CSQueryExchangeResult::Completed(CSQuery::new(slice))
        } else {
            CSQueryExchangeResult::ChangeState(CSQueryState::WaitingForFullBlock(size), size)
        }
    }
}
