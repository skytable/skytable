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
pub use raw::{Ident, Keyword, KeywordMisc, KeywordStmt, Symbol, Token};
#[cfg(test)]
pub use {
    insecure_impl::InsecureLexer,
    scan_param::{PROTO_PARAM_SYM_LIST_CLOSE, PROTO_PARAM_SYM_LIST_OPEN},
};

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
            let lexer = Self { l: Lexer::new(src) };
            lexer._lex()
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
    fn _compute_param_parse_target(param_buffer: &mut BufferedScanner) -> usize {
        // find target
        let ecc_code = scan_param::SCAN_PARAM.len() - 1;
        let target_code = param_buffer.rounded_cursor_value();
        let target_fn = target_code.min(ecc_code as u8);
        // forward if we have target
        unsafe { param_buffer.incr_cursor_if(target_code == target_fn) }
        // check requirements
        let has_enough =
            param_buffer.has_left(scan_param::SCAN_PARAM_EXPECT[target_fn as usize] as _);
        let final_target = (has_enough as u8 * target_fn) | (!has_enough as u8 * ecc_code as u8);
        // exec
        let final_target = final_target as usize;
        unsafe {
            if final_target >= scan_param::SCAN_PARAM.len() {
                impossible!()
            }
        }
        final_target
    }
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
                    let final_target = Self::_compute_param_parse_target(&mut self.param_buffer);
                    unsafe {
                        // UNSAFE(@ohsayan): our computation above ensures that we're meeting the expected target
                        scan_param::SCAN_PARAM[final_target](&mut self)
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

mod scan_param {
    use crate::engine::{
        data::{cell::Datacell, lit::Lit},
        error::QueryError,
        ql::lex::{SecureLexer, Token},
    };
    pub const SCAN_PARAM_EXPECT: [u8; 9] = [0, 1, 2, 2, 2, 2, 2, 0, 1];
    pub static SCAN_PARAM: [unsafe fn(&mut SecureLexer); 9] = unsafe {
        [
            |l| l.l.tokens.push(Token![null]),
            |l| {
                scan_bool(l, |lexer, boolean| {
                    lexer.l.tokens.push(Token::from(Lit::new_bool(boolean)))
                })
            },
            |l| {
                scan_uint(l, |lexer, uint| {
                    lexer.l.tokens.push(Token::from(Lit::new_uint(uint)))
                })
            },
            |l| {
                scan_sint(l, |lexer, sint| {
                    lexer.l.tokens.push(Token::from(Lit::new_sint(sint)))
                })
            },
            |l| {
                scan_float(l, |lexer, float| {
                    lexer.l.tokens.push(Token::from(Lit::new_float(float)))
                })
            },
            |l| {
                scan_binary(l, |lexer, bin| {
                    lexer.l.tokens.push(Token::from(Lit::new_bin(bin)))
                })
            },
            |l| {
                scan_str(l, |lexer, string| {
                    lexer.l.tokens.push(Token::from(Lit::new_str(string)))
                })
            },
            scan_list,
            |l| l.l.set_error(QueryError::LexInvalidInput),
        ]
    };
    pub static SCAN_DC: [unsafe fn(&mut SecureLexer, &mut Vec<Datacell>); 8] = unsafe {
        [
            |_, lst| lst.push(Datacell::null()),
            |l, lst| scan_bool(l, |_, boolean| lst.push(Datacell::new_bool(boolean))),
            |l, lst| scan_uint(l, |_, uint| lst.push(Datacell::new_uint_default(uint))),
            |l, lst| scan_sint(l, |_, sint| lst.push(Datacell::new_sint_default(sint))),
            |l, lst| scan_float(l, |_, float| lst.push(Datacell::new_float_default(float))),
            |l, lst| {
                scan_binary(l, |_, bin| {
                    lst.push(Datacell::new_bin(bin.to_owned().into_boxed_slice()))
                })
            },
            |l, lst| {
                scan_str(l, |_, string| {
                    lst.push(Datacell::new_str(string.to_owned().into_boxed_str()))
                })
            },
            |l, _| l.l.set_error(QueryError::LexInvalidInput),
        ]
    };
    /*
        scan impls
    */
    #[inline(always)]
    unsafe fn scan_bool<'a>(
        lexer: &mut SecureLexer<'a>,
        callback: impl FnOnce(&mut SecureLexer<'a>, bool),
    ) {
        let nb = lexer.param_buffer.next_byte();
        callback(lexer, nb == 1);
        if nb > 1 {
            lexer.l.set_error(QueryError::LexInvalidInput);
        }
    }
    #[inline(always)]
    unsafe fn scan_uint<'a>(
        lexer: &mut SecureLexer<'a>,
        callback: impl FnOnce(&mut SecureLexer<'a>, u64),
    ) {
        match lexer
            .param_buffer
            .try_next_ascii_u64_lf_separated_or_restore_cursor()
        {
            Some(int) => callback(lexer, int),
            None => lexer.l.set_error(QueryError::LexInvalidInput),
        }
    }
    #[inline(always)]
    unsafe fn scan_sint<'a>(
        lexer: &mut SecureLexer<'a>,
        callback: impl FnOnce(&mut SecureLexer<'a>, i64),
    ) {
        let (okay, int) = lexer
            .param_buffer
            .try_next_ascii_i64_separated_by::<b'\n'>();
        if okay {
            callback(lexer, int)
        } else {
            lexer.l.set_error(QueryError::LexInvalidInput)
        }
    }
    #[inline(always)]
    unsafe fn scan_float<'a>(
        lexer: &mut SecureLexer<'a>,
        callback: impl FnOnce(&mut SecureLexer<'a>, f64),
    ) {
        let start = lexer.param_buffer.cursor();
        while !lexer.param_buffer.eof() {
            let cursor = lexer.param_buffer.cursor();
            let byte = lexer.param_buffer.next_byte();
            if byte == b'\n' {
                match core::str::from_utf8(&lexer.param_buffer.inner_buffer()[start..cursor])
                    .map(core::str::FromStr::from_str)
                {
                    Ok(Ok(f)) => callback(lexer, f),
                    _ => lexer.l.set_error(QueryError::LexInvalidInput),
                }
                return;
            }
        }
        lexer.l.set_error(QueryError::LexInvalidInput)
    }
    #[inline(always)]
    unsafe fn scan_binary<'a>(
        lexer: &mut SecureLexer<'a>,
        callback: impl FnOnce(&mut SecureLexer<'a>, &'a [u8]),
    ) {
        let Some(size_of_body) = lexer
            .param_buffer
            .try_next_ascii_u64_lf_separated_or_restore_cursor()
        else {
            lexer.l.set_error(QueryError::LexInvalidInput);
            return;
        };
        match lexer
            .param_buffer
            .try_next_variable_block(size_of_body as usize)
        {
            Some(block) => callback(lexer, block),
            None => lexer.l.set_error(QueryError::LexInvalidInput),
        }
    }
    #[inline(always)]
    unsafe fn scan_str<'a>(
        lexer: &mut SecureLexer<'a>,
        callback: impl FnOnce(&mut SecureLexer<'a>, &'a str),
    ) {
        let Some(size_of_body) = lexer
            .param_buffer
            .try_next_ascii_u64_lf_separated_or_restore_cursor()
        else {
            lexer.l.set_error(QueryError::LexInvalidInput);
            return;
        };
        match lexer
            .param_buffer
            .try_next_variable_block(size_of_body as usize)
            .map(core::str::from_utf8)
        {
            Some(Ok(s)) => callback(lexer, s),
            _ => lexer.l.set_error(QueryError::LexInvalidInput),
        }
    }
    /*
        list scan
    */
    pub const PROTO_PARAM_SYM_LIST_OPEN: u8 = b'\x07';
    pub const PROTO_PARAM_SYM_LIST_CLOSE: u8 = b']';
    pub fn scan_list(lx: &mut SecureLexer) {
        let mut pending_count = 1usize;
        let mut stack = vec![];
        let mut current_l = vec![];
        while pending_count != 0 && !lx.param_buffer.eof() && lx.l.no_error() {
            match unsafe {
                // UNSAFE(@ohsayan): we just verified that we haven't reached EOF at the loop invariant condition
                lx.param_buffer.deref_cursor()
            } {
                PROTO_PARAM_SYM_LIST_OPEN => {
                    // opening of a list; need to start processing this before continuing with current
                    stack.push(current_l);
                    current_l = vec![];
                    unsafe {
                        // UNSAFE(@ohsayan): we haven't forwarded the cursor yet and hence we're still not at EOF, so this is correct
                        lx.param_buffer.incr_cursor();
                    }
                    pending_count += 1;
                }
                PROTO_PARAM_SYM_LIST_CLOSE => {
                    // closing of a list; finish processing earlier list or finish
                    pending_count -= 1;
                    unsafe {
                        // UNSAFE(@ohsayan): we haven't forwarded the cursor yet and hence we're still not at EOF, so this is correct
                        lx.param_buffer.incr_cursor();
                    }
                    match stack.pop() {
                        None => break,
                        Some(mut parent) => {
                            parent.push(Datacell::new_list(current_l));
                            current_l = parent;
                        }
                    }
                }
                _ => {
                    // a data element
                    let final_target =
                        SecureLexer::_compute_param_parse_target(&mut lx.param_buffer);
                    unsafe {
                        // UNSAFE(@ohsayan): our computation above ensures that we're meeting the expected target
                        SCAN_DC[final_target](lx, &mut current_l)
                    }
                }
            }
        }
        if pending_count == 0 && lx.l.no_error() {
            lx.l.tokens.push(Token::dc_list(current_l));
        } else {
            lx.l.set_error(QueryError::LexInvalidInput)
        }
    }
}

#[sky_macros::test]
fn try_this_list() {
    use crate::engine::data::cell::Datacell;
    let params = format!(
        "{}\x00\x01\x01\x021234\n\x03-1234\n\x041234.5678\n{}\x0513\nbinarywithlf\n\x065\nsayan]{}]]",
        char::from(scan_param::PROTO_PARAM_SYM_LIST_OPEN),
        char::from(scan_param::PROTO_PARAM_SYM_LIST_OPEN),
        char::from(scan_param::PROTO_PARAM_SYM_LIST_OPEN),
    );
    let sec_lex = SecureLexer::new_with_segments(b"?", params.as_bytes());
    let tokens = sec_lex.lex().unwrap();
    assert_eq!(
        tokens[0],
        Token::dc_list(vec![
            Datacell::null(),
            Datacell::new_bool(true),
            Datacell::new_uint_default(1234),
            Datacell::new_sint_default(-1234),
            Datacell::new_float_default(1234.5678),
            Datacell::new_list(vec![
                Datacell::new_bin(b"binarywithlf\n".to_vec().into_boxed_slice()),
                Datacell::new_str("sayan".to_string().into_boxed_str())
            ]),
            Datacell::new_list(vec![]),
        ])
    )
}
