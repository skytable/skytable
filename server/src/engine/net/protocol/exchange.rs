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

use crate::engine::mem::BufferedScanner;

pub const EXCHANGE_MIN_SIZE: usize = b"S1\nh".len();
pub(super) const STATE_READ_INITIAL: QueryTimeExchangeResult<'static> =
    QueryTimeExchangeResult::ChangeState {
        new_state: QueryTimeExchangeState::Initial,
        expect_more: EXCHANGE_MIN_SIZE,
    };
pub(super) const STATE_ERROR: QueryTimeExchangeResult<'static> = QueryTimeExchangeResult::Error;

#[derive(Debug, PartialEq)]
/// State of a query time exchange
pub enum QueryTimeExchangeState {
    /// beginning of exchange
    Initial,
    /// SQ (part of packet size)
    SQ1Meta1Partial { packet_size_part: u64 },
    /// SQ (part of Q window)
    SQ2Meta2Partial {
        size_of_static_frame: usize,
        packet_size: usize,
        q_window_part: u64,
    },
    /// SQ waiting for block
    SQ3FinalizeWaitingForBlock {
        dataframe_size: usize,
        q_window: usize,
    },
}

impl Default for QueryTimeExchangeState {
    fn default() -> Self {
        Self::Initial
    }
}

#[derive(Debug, PartialEq)]
/// Result after attempting to complete (or terminate) a query time exchange
pub enum QueryTimeExchangeResult<'a> {
    /// We completed the exchange and yielded a [`SQuery`]
    SQCompleted(SQuery<'a>),
    /// We're changing states
    ChangeState {
        new_state: QueryTimeExchangeState,
        expect_more: usize,
    },
    /// We hit an error and need to terminate this exchange
    Error,
}

/// Resume a query time exchange
pub fn resume<'a>(
    scanner: &mut BufferedScanner<'a>,
    state: QueryTimeExchangeState,
) -> QueryTimeExchangeResult<'a> {
    match state {
        QueryTimeExchangeState::Initial => SQuery::resume_initial(scanner),
        QueryTimeExchangeState::SQ1Meta1Partial { packet_size_part } => {
            SQuery::resume_at_sq1_meta1_partial(scanner, packet_size_part)
        }
        QueryTimeExchangeState::SQ2Meta2Partial {
            packet_size,
            q_window_part,
            size_of_static_frame,
        } => SQuery::resume_at_sq2_meta2_partial(
            scanner,
            size_of_static_frame,
            packet_size,
            q_window_part,
        ),
        QueryTimeExchangeState::SQ3FinalizeWaitingForBlock {
            dataframe_size,
            q_window,
        } => SQuery::resume_at_final(scanner, q_window, dataframe_size),
    }
}

/*
    SQ
*/

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
    unsafe { scanner.incr_cursor_if(lf_ok) }
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

#[derive(Debug, PartialEq)]
pub struct SQuery<'a> {
    q: &'a [u8],
    q_window: usize,
}

impl<'a> SQuery<'a> {
    pub(super) fn new(q: &'a [u8], q_window: usize) -> Self {
        Self { q, q_window }
    }
    pub fn payload(&self) -> &'a [u8] {
        self.q
    }
    pub fn q_window(&self) -> usize {
        self.q_window
    }
    pub fn query(&self) -> &'a [u8] {
        &self.payload()[..self.q_window()]
    }
    pub fn params(&self) -> &'a [u8] {
        &self.payload()[self.q_window()..]
    }
    #[cfg(test)]
    pub fn query_str(&self) -> Option<&'a str> {
        core::str::from_utf8(self.query()).ok()
    }
    #[cfg(test)]
    pub fn params_str(&self) -> Option<&'a str> {
        core::str::from_utf8(self.params()).ok()
    }
}

impl<'a> SQuery<'a> {
    /// We're touching this packet for the first time
    fn resume_initial(scanner: &mut BufferedScanner<'a>) -> QueryTimeExchangeResult<'a> {
        if cfg!(debug_assertions) {
            if !scanner.has_left(EXCHANGE_MIN_SIZE) {
                return STATE_READ_INITIAL;
            }
        } else {
            assert!(scanner.has_left(EXCHANGE_MIN_SIZE));
        }
        // attempt to read atleast one byte
        if cfg!(debug_assertions) {
            match scanner.try_next_byte() {
                Some(b'S') => {}
                Some(_) => return STATE_ERROR,
                None => return STATE_READ_INITIAL,
            }
        } else {
            match unsafe { scanner.next_byte() } {
                b'S' => {}
                _ => return STATE_ERROR,
            }
        }
        Self::resume_at_sq1_meta1_partial(scanner, 0)
    }
    /// We found some part of meta1, and need to resume
    fn resume_at_sq1_meta1_partial(
        scanner: &mut BufferedScanner<'a>,
        prev: u64,
    ) -> QueryTimeExchangeResult<'a> {
        match parse_lf_separated(scanner, prev) {
            LFTIntParseResult::Value(packet_size) => {
                // we got the packet size; can we get the q window?
                Self::resume_at_sq2_meta2_partial(
                    scanner,
                    scanner.cursor(),
                    packet_size as usize,
                    0,
                )
            }
            LFTIntParseResult::Partial(partial_packet_size) => {
                // we couldn't get the packet size
                QueryTimeExchangeResult::ChangeState {
                    new_state: QueryTimeExchangeState::SQ1Meta1Partial {
                        packet_size_part: partial_packet_size,
                    },
                    expect_more: 3, // 1LF + 1ASCII + 1LF
                }
            }
            LFTIntParseResult::Error => STATE_ERROR,
        }
    }
    /// We found some part of meta2, and need to resume
    fn resume_at_sq2_meta2_partial(
        scanner: &mut BufferedScanner<'a>,
        static_size: usize,
        packet_size: usize,
        prev_qw_buffered: u64,
    ) -> QueryTimeExchangeResult<'a> {
        let start = scanner.cursor();
        match parse_lf_separated(scanner, prev_qw_buffered) {
            LFTIntParseResult::Value(q_window) => {
                // we got the q window; can we complete the exchange?
                let df_size = Self::compute_df_size(scanner, static_size, packet_size);
                if df_size == 0 {
                    return QueryTimeExchangeResult::Error;
                }
                Self::resume_at_final(scanner, q_window as usize, df_size)
            }
            LFTIntParseResult::Partial(q_window_partial) => {
                // not enough bytes for getting Q window
                QueryTimeExchangeResult::ChangeState {
                    new_state: QueryTimeExchangeState::SQ2Meta2Partial {
                        packet_size,
                        q_window_part: q_window_partial,
                        size_of_static_frame: static_size,
                    },
                    // we passed cursor - start bytes out of the packet, so expect this more
                    expect_more: packet_size - (scanner.cursor() - start),
                }
            }
            LFTIntParseResult::Error => STATE_ERROR,
        }
    }
    /// We got all our meta and need the dataframe
    fn resume_at_final(
        scanner: &mut BufferedScanner<'a>,
        q_window: usize,
        dataframe_size: usize,
    ) -> QueryTimeExchangeResult<'a> {
        if scanner.has_left(dataframe_size) {
            // we have sufficient bytes for the dataframe
            unsafe {
                // UNSAFE(@ohsayan): +lenck
                QueryTimeExchangeResult::SQCompleted(SQuery::new(
                    scanner.next_chunk_variable(dataframe_size),
                    q_window,
                ))
            }
        } else {
            // not enough bytes for the dataframe
            QueryTimeExchangeResult::ChangeState {
                new_state: QueryTimeExchangeState::SQ3FinalizeWaitingForBlock {
                    dataframe_size,
                    q_window,
                },
                expect_more: Self::compute_df_remaining(scanner, dataframe_size), // dataframe
            }
        }
    }
}

impl<'a> SQuery<'a> {
    fn compute_df_size(scanner: &BufferedScanner, static_size: usize, packet_size: usize) -> usize {
        (packet_size + static_size) - scanner.cursor()
    }
    fn compute_df_remaining(scanner: &BufferedScanner<'_>, df_size: usize) -> usize {
        (scanner.cursor() + df_size) - scanner.buffer_len()
    }
}

#[cfg(test)]
pub(super) fn create_simple_query<const N: usize>(query: &str, params: [&str; N]) -> Vec<u8> {
    let mut buf = vec![];
    let query_size_as_string = query.len().to_string();
    let size_of_variable_section = query.len()
        + params.iter().map(|l| l.len()).sum::<usize>()
        + query_size_as_string.len()
        + 1;
    // segment 1
    buf.push(b'S');
    buf.extend(size_of_variable_section.to_string().as_bytes());
    buf.push(b'\n');
    // segment
    buf.extend(query_size_as_string.as_bytes());
    buf.push(b'\n');
    // dataframe
    buf.extend(query.as_bytes());
    params
        .into_iter()
        .for_each(|param| buf.extend(param.as_bytes()));
    buf
}
