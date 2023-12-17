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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Resume(usize);
impl Resume {
    #[cfg(test)]
    pub(super) const fn test_new(v: usize) -> Self {
        Self(v)
    }
    #[cfg(test)]
    pub(super) const fn inner(&self) -> usize {
        self.0
    }
}
impl Default for Resume {
    fn default() -> Self {
        Self(0)
    }
}

pub(super) unsafe fn resume<'a>(
    buf: &'a [u8],
    last_cursor: Resume,
    last_state: QExchangeState,
) -> (Resume, QExchangeResult<'a>) {
    let mut scanner = unsafe {
        // UNSAFE(@ohsayan): we are the ones who generate the cursor and restore it
        BufferedScanner::new_with_cursor(buf, last_cursor.0)
    };
    let ret = last_state.resume(&mut scanner);
    (Resume(scanner.cursor()), ret)
}

/*
    SQ
*/

#[derive(Debug, PartialEq)]
pub(super) enum LFTIntParseResult {
    Value(u64),
    Partial(u64),
    Error,
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
    pub fn query_str(&self) -> &str {
        core::str::from_utf8(self.query()).unwrap()
    }
    #[cfg(test)]
    pub fn params_str(&self) -> &str {
        core::str::from_utf8(self.params()).unwrap()
    }
}

/*
    utils
*/

/// scan an integer:
/// - if just an LF:
///     - if disallowed single byte: return an error
///     - else, return value
/// - if no LF: return upto limit
/// - if LF: return value
pub(super) fn scanint(
    scanner: &mut BufferedScanner,
    first_run: bool,
    prev: u64,
) -> LFTIntParseResult {
    let mut current = prev;
    // guard a case where the buffer might be empty and can potentially have invalid chars
    let mut okay = !((scanner.rounded_cursor_value() == b'\n') & first_run);
    while scanner.rounded_cursor_not_eof_matches(|b| b'\n'.ne(b)) & okay {
        let byte = unsafe { scanner.next_byte() };
        okay &= byte.is_ascii_digit();
        match current
            .checked_mul(10)
            .map(|new| new.checked_add((byte & 0x0f) as u64))
        {
            Some(Some(int)) => {
                current = int;
            }
            _ => {
                okay = false;
            }
        }
    }
    let lf = scanner.rounded_cursor_not_eof_equals(b'\n');
    unsafe {
        // UNSAFE(@ohsayan): within buffer range
        scanner.incr_cursor_if(lf);
    }
    if lf & okay {
        LFTIntParseResult::Value(current)
    } else {
        if okay {
            LFTIntParseResult::Partial(current)
        } else {
            LFTIntParseResult::Error
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(super) enum QExchangeStateInternal {
    Initial,
    PendingMeta1,
    PendingMeta2,
    PendingData,
}

impl Default for QExchangeStateInternal {
    fn default() -> Self {
        Self::Initial
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct QExchangeState {
    state: QExchangeStateInternal,
    target: usize,
    md_packet_size: u64,
    md_q_window: u64,
}

impl Default for QExchangeState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq)]
/// Result after attempting to complete (or terminate) a query time exchange
pub(super) enum QExchangeResult<'a> {
    /// We completed the exchange and yielded a [`SQuery`]
    SQCompleted(SQuery<'a>),
    /// We're changing states
    ChangeState(QExchangeState),
    /// We hit an error and need to terminate this exchange
    Error,
}

impl QExchangeState {
    fn _new(
        state: QExchangeStateInternal,
        target: usize,
        md_packet_size: u64,
        md_q_window: u64,
    ) -> Self {
        Self {
            state,
            target,
            md_packet_size,
            md_q_window,
        }
    }
    #[cfg(test)]
    pub(super) fn new_test(
        state: QExchangeStateInternal,
        target: usize,
        md_packet_size: u64,
        md_q_window: u64,
    ) -> Self {
        Self::_new(state, target, md_packet_size, md_q_window)
    }
}

impl QExchangeState {
    pub const MIN_READ: usize = b"S\x00\n\x00\n".len();
    pub fn new() -> Self {
        Self::_new(QExchangeStateInternal::Initial, Self::MIN_READ, 0, 0)
    }
    pub fn has_reached_target(&self, new_buffer: &[u8]) -> bool {
        new_buffer.len() >= self.target
    }
    fn resume<'a>(self, scanner: &mut BufferedScanner<'a>) -> QExchangeResult<'a> {
        debug_assert!(scanner.has_left(Self::MIN_READ));
        match self.state {
            QExchangeStateInternal::Initial => self.start_initial(scanner),
            QExchangeStateInternal::PendingMeta1 => self.resume_at_md1(scanner, false),
            QExchangeStateInternal::PendingMeta2 => self.resume_at_md2(scanner, false),
            QExchangeStateInternal::PendingData => self.resume_data(scanner),
        }
    }
    fn start_initial<'a>(self, scanner: &mut BufferedScanner<'a>) -> QExchangeResult<'a> {
        if unsafe { scanner.next_byte() } != b'S' {
            // has to be a simple query!
            return QExchangeResult::Error;
        }
        self.resume_at_md1(scanner, true)
    }
    fn resume_at_md1<'a>(
        mut self,
        scanner: &mut BufferedScanner<'a>,
        first_run: bool,
    ) -> QExchangeResult<'a> {
        let packet_size = match scanint(scanner, first_run, self.md_packet_size) {
            LFTIntParseResult::Value(v) => v,
            LFTIntParseResult::Partial(p) => {
                // if this is the first run, we read 5 bytes and need atleast one more; if this is a resume we read one or more bytes and
                // need atleast one more
                self.target += 1;
                self.md_packet_size = p;
                self.state = QExchangeStateInternal::PendingMeta1;
                return QExchangeResult::ChangeState(self);
            }
            LFTIntParseResult::Error => return QExchangeResult::Error,
        };
        self.md_packet_size = packet_size;
        self.target = scanner.cursor() + packet_size as usize;
        // hand over control to md2
        self.resume_at_md2(scanner, true)
    }
    fn resume_at_md2<'a>(
        mut self,
        scanner: &mut BufferedScanner<'a>,
        first_run: bool,
    ) -> QExchangeResult<'a> {
        let q_window = match scanint(scanner, first_run, self.md_q_window) {
            LFTIntParseResult::Value(v) => v,
            LFTIntParseResult::Partial(p) => {
                self.md_q_window = p;
                self.state = QExchangeStateInternal::PendingMeta2;
                return QExchangeResult::ChangeState(self);
            }
            LFTIntParseResult::Error => return QExchangeResult::Error,
        };
        self.md_q_window = q_window;
        // hand over control to data
        self.resume_data(scanner)
    }
    fn resume_data<'a>(mut self, scanner: &mut BufferedScanner<'a>) -> QExchangeResult<'a> {
        let df_size = self.target - scanner.cursor();
        if scanner.remaining() == df_size {
            unsafe {
                QExchangeResult::SQCompleted(SQuery::new(
                    scanner.next_chunk_variable(df_size),
                    self.md_q_window as usize,
                ))
            }
        } else {
            self.state = QExchangeStateInternal::PendingData;
            QExchangeResult::ChangeState(self)
        }
    }
}
