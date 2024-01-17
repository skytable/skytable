/*
 * Created on Tue Sep 13 2022
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

mod raw;
#[cfg(test)]
pub use insecure_impl::InsecureLexer;
pub use raw::{Ident, Keyword, KeywordMisc, KeywordStmt, Symbol, Token};

use {
    crate::engine::{
        data::lit::Lit,
        error::{QueryError, QueryResult},
        mem::BufferedScanner,
    },
    core::slice,
};

/*
    basic lexer definition
*/

type Slice<'a> = &'a [u8];

#[derive(Debug, PartialEq)]
/// The internal lexer impl
pub struct Lexer<'a> {
    token_buffer: BufferedScanner<'a>,
    tokens: Vec<Token<'a>>,
    last_error: Option<QueryError>,
}

impl<'a> Lexer<'a> {
    /// Initialize a new lexer
    fn new(src: &'a [u8]) -> Self {
        Self {
            token_buffer: BufferedScanner::new(src),
            tokens: Vec::new(),
            last_error: None,
        }
    }
    /// set an error
    #[inline(never)]
    #[cold]
    fn set_error(&mut self, e: QueryError) {
        self.last_error = Some(e);
    }
    /// push in a new token
    fn push_token(&mut self, t: impl Into<Token<'a>>) {
        self.tokens.push(t.into())
    }
    fn no_error(&self) -> bool {
        self.last_error.is_none()
    }
}

impl<'a> Lexer<'a> {
    /// Scan an identifier
    fn scan_ident(&mut self) -> Slice<'a> {
        let s = self.token_buffer.cursor_ptr();
        unsafe {
            while self
                .token_buffer
                .rounded_cursor_not_eof_matches(|b| b.is_ascii_alphanumeric() || *b == b'_')
            {
                // UNSAFE(@ohsayan): increment cursor, this is valid
                self.token_buffer.incr_cursor();
            }
            // UNSAFE(@ohsayan): valid slice and ptrs
            slice::from_raw_parts(
                s,
                self.token_buffer.current_buffer().as_ptr().offset_from(s) as usize,
            )
        }
    }
    /// Scan an identifier or keyword
    fn scan_ident_or_keyword(&mut self) {
        let s = self.scan_ident();
        match Keyword::get(s) {
            Some(kw) => self.tokens.push(kw.into()),
            // FIXME(@ohsayan): Uh, mind fixing this? The only advantage is that I can keep the graph *memory* footprint small
            None if s.eq_ignore_ascii_case(b"true") || s.eq_ignore_ascii_case(b"false") => {
                self.push_token(Lit::new_bool(s.eq_ignore_ascii_case(b"true")))
            }
            None => self.tokens.push(unsafe {
                // UNSAFE(@ohsayan): scan_ident only returns a valid ident which is always a string
                Token::Ident(Ident::new(s))
            }),
        }
    }
    fn scan_byte(&mut self, byte: u8) {
        match Symbol::get(byte) {
            Some(tok) => self.push_token(tok),
            None => return self.set_error(QueryError::LexUnexpectedByte),
        }
        unsafe {
            // UNSAFE(@ohsayan): we are sent a byte, so fw cursor
            self.token_buffer.incr_cursor();
        }
    }
}

impl<'a> Lexer<'a> {
    fn trim_ahead(&mut self) {
        self.token_buffer
            .trim_ahead(|b| (b == b' ') | (b == b'\n') | (b == b'\t'))
    }
}

/*
    Insecure lexer
*/

mod insecure_impl {
    #![allow(unused)] // TODO(@ohsayan): yank this
    use {
        super::Lexer,
        crate::{
            engine::{
                data::lit::Lit,
                error::{QueryError, QueryResult},
                ql::lex::Token,
            },
            util::compiler,
        },
    };

    pub struct InsecureLexer<'a> {
        pub(crate) l: Lexer<'a>,
    }

    impl<'a> InsecureLexer<'a> {
        pub fn lex(src: &'a [u8]) -> QueryResult<Vec<Token<'a>>> {
            let slf = Self { l: Lexer::new(src) };
            slf._lex()
        }
        pub(crate) fn _lex(mut self) -> QueryResult<Vec<Token<'a>>> {
            while !self.l.token_buffer.eof() & self.l.no_error() {
                let byte = unsafe {
                    // UNSAFE(@ohsayan): loop invariant
                    self.l.token_buffer.deref_cursor()
                };
                match byte {
                    #[cfg(test)]
                    byte if byte == b'\x01' => {
                        self.l.push_token(Token::IgnorableComma);
                        unsafe {
                            // UNSAFE(@ohsayan): All good here. Already read the token
                            self.l.token_buffer.incr_cursor();
                        }
                    }
                    // ident
                    byte if byte.is_ascii_alphabetic() | (byte == b'_') => {
                        self.l.scan_ident_or_keyword()
                    }
                    // uint
                    byte if byte.is_ascii_digit() => self.scan_unsigned_integer(),
                    // sint
                    b'-' => {
                        unsafe {
                            // UNSAFE(@ohsayan): loop invariant
                            self.l.token_buffer.incr_cursor()
                        };
                        self.scan_signed_integer();
                    }
                    // binary
                    b'\r' => {
                        unsafe {
                            // UNSAFE(@ohsayan): loop invariant
                            self.l.token_buffer.incr_cursor()
                        }
                        self.scan_binary()
                    }
                    // string
                    quote_style @ (b'"' | b'\'') => {
                        unsafe {
                            // UNSAFE(@ohsayan): loop invariant
                            self.l.token_buffer.incr_cursor()
                        }
                        self.scan_quoted_string(quote_style)
                    }
                    // whitespace
                    b' ' | b'\n' | b'\t' => self.l.trim_ahead(),
                    // some random byte
                    byte => self.l.scan_byte(byte),
                }
            }
            match self.l.last_error {
                None => Ok(self.l.tokens),
                Some(e) => Err(e),
            }
        }
    }

    impl<'a> InsecureLexer<'a> {
        pub(crate) fn scan_binary(&mut self) {
            let Some(len) = self
                .l
                .token_buffer
                .try_next_ascii_u64_lf_separated_or_restore_cursor()
            else {
                self.l.set_error(QueryError::LexInvalidInput);
                return;
            };
            let len = len as usize;
            match self.l.token_buffer.try_next_variable_block(len) {
                Some(block) => self.l.push_token(Lit::new_bin(block)),
                None => self.l.set_error(QueryError::LexInvalidInput),
            }
        }
        pub(crate) fn scan_quoted_string(&mut self, quote_style: u8) {
            // cursor is at beginning of `"`; we need to scan until the end of quote or an escape
            let mut buf = Vec::new();
            while self
                .l
                .token_buffer
                .rounded_cursor_not_eof_matches(|b| *b != quote_style)
            {
                let byte = unsafe {
                    // UNSAFE(@ohsayan): loop invariant
                    self.l.token_buffer.next_byte()
                };
                match byte {
                    b'\\' => {
                        // hmm, this might be an escape (either `\\` or `\"`)
                        if self
                            .l
                            .token_buffer
                            .rounded_cursor_not_eof_matches(|b| *b == quote_style || *b == b'\\')
                        {
                            // ignore escaped byte
                            unsafe {
                                buf.push(self.l.token_buffer.next_byte());
                            }
                        } else {
                            // this is not allowed
                            unsafe {
                                // UNSAFE(@ohsayan): we move the cursor ahead, now we're moving it back
                                self.l.token_buffer.decr_cursor()
                            }
                            self.l.set_error(QueryError::LexInvalidInput);
                            return;
                        }
                    }
                    _ => buf.push(byte),
                }
            }
            let ended_with_quote = self
                .l
                .token_buffer
                .rounded_cursor_not_eof_equals(quote_style);
            // skip quote
            unsafe {
                // UNSAFE(@ohsayan): not eof
                self.l.token_buffer.incr_cursor_if(ended_with_quote)
            }
            match String::from_utf8(buf) {
                Ok(s) if ended_with_quote => self.l.push_token(Lit::new_string(s)),
                Err(_) | Ok(_) => self.l.set_error(QueryError::LexInvalidInput),
            }
        }
        pub(crate) fn scan_unsigned_integer(&mut self) {
            let mut okay = true;
            // extract integer
            let int = self
                .l
                .token_buffer
                .try_next_ascii_u64_stop_at::<false>(&mut okay, |b| b.is_ascii_digit());
            /*
                see if we ended at a correct byte:
                iff the integer has an alphanumeric byte at the end is the integer invalid
            */
            if compiler::unlikely(
                !okay
                    | self
                        .l
                        .token_buffer
                        .rounded_cursor_not_eof_matches(u8::is_ascii_alphanumeric),
            ) {
                self.l.set_error(QueryError::LexInvalidInput);
            } else {
                self.l.push_token(Lit::new_uint(int))
            }
        }
        pub(crate) fn scan_signed_integer(&mut self) {
            if self.l.token_buffer.rounded_cursor_value().is_ascii_digit() {
                unsafe {
                    // UNSAFE(@ohsayan): the cursor was moved ahead, now we're moving it back
                    self.l.token_buffer.decr_cursor()
                }
                let (okay, int) = self
                    .l
                    .token_buffer
                    .try_next_ascii_i64_stop_at(|b| !b.is_ascii_digit());
                if okay
                    & !self
                        .l
                        .token_buffer
                        .rounded_cursor_value()
                        .is_ascii_alphabetic()
                {
                    self.l.push_token(Lit::new_sint(int))
                } else {
                    self.l.set_error(QueryError::LexInvalidInput)
                }
            } else {
                self.l.push_token(Token![-]);
            }
        }
    }
}

/*
    secure
*/

#[derive(Debug)]
pub struct SecureLexer<'a> {
    l: Lexer<'a>,
    param_buffer: BufferedScanner<'a>,
}

impl<'a> SecureLexer<'a> {
    pub fn new_with_segments(q: &'a [u8], p: &'a [u8]) -> Self {
        Self {
            l: Lexer::new(q),
            param_buffer: BufferedScanner::new(p),
        }
    }
    pub fn lex(self) -> QueryResult<Vec<Token<'a>>> {
        self._lex()
    }
    #[cfg(test)]
    pub fn lex_with_window(src: &'a [u8], query_window: usize) -> QueryResult<Vec<Token<'a>>> {
        Self {
            l: Lexer::new(&src[..query_window]),
            param_buffer: BufferedScanner::new(&src[query_window..]),
        }
        .lex()
    }
}

impl<'a> SecureLexer<'a> {
    fn _lex(mut self) -> QueryResult<Vec<Token<'a>>> {
        while self.l.no_error() & !self.l.token_buffer.eof() {
            let b = unsafe {
                // UNSAFE(@ohsayan): loop invariant
                self.l.token_buffer.deref_cursor()
            };
            match b {
                b if b.is_ascii_alphabetic() | (b == b'_') => self.l.scan_ident_or_keyword(),
                b'?' if !self.param_buffer.eof() => {
                    // skip the param byte
                    unsafe {
                        // UNSAFE(@ohsayan): loop invariant
                        self.l.token_buffer.incr_cursor()
                    }
                    // find target
                    let ecc_code = SCAN_PARAM.len() - 1;
                    let target_code = self.param_buffer.rounded_cursor_value();
                    let target_fn = target_code.min(ecc_code as u8);
                    // forward if we have target
                    unsafe {
                        self.param_buffer
                            .incr_cursor_by((target_code == target_fn) as _)
                    }
                    // check requirements
                    let has_enough = self
                        .param_buffer
                        .has_left(SCAN_PARAM_EXPECT[target_fn as usize] as _);
                    let final_target =
                        (has_enough as u8 * target_fn) | (!has_enough as u8 * ecc_code as u8);
                    // exec
                    let final_target = final_target as usize;
                    unsafe {
                        if final_target >= SCAN_PARAM.len() {
                            impossible!()
                        }
                    }
                    unsafe {
                        // UNSAFE(@ohsayan): our computation above ensures that we're meeting the expected target
                        SCAN_PARAM[final_target](&mut self)
                    }
                }
                b' ' | b'\t' | b'\n' => self.l.trim_ahead(),
                sym => self.l.scan_byte(sym),
            }
        }
        match self.l.last_error {
            None => Ok(self.l.tokens),
            Some(e) => Err(e),
        }
    }
}

const SCAN_PARAM_EXPECT: [u8; 8] = [0, 1, 2, 2, 2, 2, 2, 0];
static SCAN_PARAM: [unsafe fn(&mut SecureLexer); 8] = unsafe {
    [
        // null
        |s| s.l.push_token(Token![null]),
        // bool
        |slf| {
            let nb = slf.param_buffer.next_byte();
            slf.l.push_token(Token::Lit(Lit::new_bool(nb == 1)));
            if nb > 1 {
                slf.l.set_error(QueryError::LexInvalidInput);
            }
        },
        // uint
        |slf| match slf
            .param_buffer
            .try_next_ascii_u64_lf_separated_or_restore_cursor()
        {
            Some(int) => slf.l.push_token(Lit::new_uint(int)),
            None => slf.l.set_error(QueryError::LexInvalidInput),
        },
        // sint
        |slf| {
            let (okay, int) = slf.param_buffer.try_next_ascii_i64_separated_by::<b'\n'>();
            if okay {
                slf.l.push_token(Lit::new_sint(int))
            } else {
                slf.l.set_error(QueryError::LexInvalidInput)
            }
        },
        // float
        |slf| {
            let start = slf.param_buffer.cursor();
            while !slf.param_buffer.eof() {
                let cursor = slf.param_buffer.cursor();
                let byte = slf.param_buffer.next_byte();
                if byte == b'\n' {
                    match core::str::from_utf8(&slf.param_buffer.inner_buffer()[start..cursor])
                        .map(core::str::FromStr::from_str)
                    {
                        Ok(Ok(f)) => slf.l.push_token(Lit::new_float(f)),
                        _ => slf.l.set_error(QueryError::LexInvalidInput),
                    }
                    return;
                }
            }
            slf.l.set_error(QueryError::LexInvalidInput)
        },
        // binary
        |slf| {
            let Some(size_of_body) = slf
                .param_buffer
                .try_next_ascii_u64_lf_separated_or_restore_cursor()
            else {
                slf.l.set_error(QueryError::LexInvalidInput);
                return;
            };
            match slf
                .param_buffer
                .try_next_variable_block(size_of_body as usize)
            {
                Some(block) => slf.l.push_token(Lit::new_bin(block)),
                None => slf.l.set_error(QueryError::LexInvalidInput),
            }
        },
        // string
        |slf| {
            let Some(size_of_body) = slf
                .param_buffer
                .try_next_ascii_u64_lf_separated_or_restore_cursor()
            else {
                slf.l.set_error(QueryError::LexInvalidInput);
                return;
            };
            match slf
                .param_buffer
                .try_next_variable_block(size_of_body as usize)
                .map(core::str::from_utf8)
            {
                // TODO(@ohsayan): obliterate this alloc
                Some(Ok(s)) => slf.l.push_token(Lit::new_string(s.to_owned())),
                _ => slf.l.set_error(QueryError::LexInvalidInput),
            }
        },
        // ecc
        |s| s.l.set_error(QueryError::LexInvalidInput),
    ]
};
