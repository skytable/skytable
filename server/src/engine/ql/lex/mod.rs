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

use {
    self::raw::RawLexer,
    crate::{
        engine::{
            data::{
                lit::{Lit, LitIR},
                spec::Dataspec1D,
            },
            error::{LexError, LexResult},
        },
        util::compiler,
    },
    core::{fmt, ops::BitOr, slice, str},
};

pub use self::raw::{Ident, Keyword, Symbol, Token};
pub type Slice<'a> = &'a [u8];

/*
    Lexer impls
*/

#[derive(Debug)]
/// This implements the `opmode-dev` for BlueQL
pub struct InsecureLexer<'a> {
    base: RawLexer<'a>,
}

impl<'a> InsecureLexer<'a> {
    #[inline(always)]
    pub const fn new(src: Slice<'a>) -> Self {
        Self {
            base: RawLexer::new(src),
        }
    }
    #[inline(always)]
    pub fn lex(src: Slice<'a>) -> LexResult<Vec<Token<'a>>> {
        let mut slf = Self::new(src);
        slf._lex();
        let RawLexer {
            tokens, last_error, ..
        } = slf.base;
        match last_error {
            None => Ok(tokens),
            Some(e) => Err(e),
        }
    }
    #[inline(always)]
    fn _lex(&mut self) {
        let slf = &mut self.base;
        while slf.not_exhausted() && slf.no_error() {
            match unsafe {
                // UNSAFE(@ohsayan): Verified non-null from pre
                slf.deref_cursor()
            } {
                byte if byte.is_ascii_alphabetic() => slf.scan_ident_or_keyword(),
                #[cfg(test)]
                byte if byte == b'\x01' => {
                    slf.push_token(Token::IgnorableComma);
                    unsafe {
                        // UNSAFE(@ohsayan): All good here. Already read the token
                        slf.incr_cursor();
                    }
                }
                byte if byte.is_ascii_digit() => Self::scan_unsigned_integer(slf),
                b'\r' => Self::scan_binary_literal(slf),
                b'-' => Self::scan_signed_integer(slf),
                qs @ (b'\'' | b'"') => Self::scan_quoted_string(slf, qs),
                // blank space or an arbitrary byte
                b' ' | b'\n' | b'\t' => slf.trim_ahead(),
                b => slf.scan_byte(b),
            }
        }
    }
}

// high-level methods
impl<'a> InsecureLexer<'a> {
    #[inline(always)]
    fn scan_signed_integer(slf: &mut RawLexer<'a>) {
        unsafe {
            // UNSAFE(@ohsayan): We hit an integer hence this was called
            slf.incr_cursor();
        }
        if slf.peek_is(|b| b.is_ascii_digit()) {
            // we have some digits
            let start = unsafe {
                // UNSAFE(@ohsayan): Take the (-) into the parse
                // TODO(@ohsayan): we can maybe look at a more efficient way later
                slf.cursor().sub(1)
            };
            while slf.peek_is_and_forward(|b| b.is_ascii_digit()) {}
            let wseof = slf.peek_is(|char| !char.is_ascii_alphabetic()) || slf.exhausted();
            match unsafe {
                // UNSAFE(@ohsayan): a sequence of ASCII bytes in the integer range will always be correct unicode
                str::from_utf8_unchecked(slice::from_raw_parts(
                    start,
                    // UNSAFE(@ohsayan): valid cursor and start pointers
                    slf.cursor().offset_from(start) as usize,
                ))
            }
            .parse::<i64>()
            {
                Ok(num) if compiler::likely(wseof) => {
                    slf.push_token(Lit::SignedInt(num));
                }
                _ => {
                    compiler::cold_call(|| slf.set_error(LexError::InvalidSignedNumericLit));
                }
            }
        } else {
            slf.push_token(Token![-]);
        }
    }
    #[inline(always)]
    fn scan_unsigned_integer(slf: &mut RawLexer<'a>) {
        let s = slf.cursor();

        while slf.peek_is(|b| b.is_ascii_digit()) {
            unsafe {
                // UNSAFE(@ohsayan): since we're going ahead, this is correct (until EOA)
                slf.incr_cursor();
            }
        }
        /*
            1234; // valid
            1234} // valid
            1234{ // invalid
            1234, // valid
            1234a // invalid
        */
        let wseof = slf.peek_is(|char| !char.is_ascii_alphabetic()) || slf.exhausted();
        match unsafe {
            /*
                UNSAFE(@ohsayan):
                (1) Valid cursor and start pointer (since we copy it from the cursor which is correct)
                (2) All ASCII alphabetic bytes are captured, hence this will always be a correct unicode string
            */
            str::from_utf8_unchecked(slice::from_raw_parts(
                s,
                slf.cursor().offset_from(s) as usize,
            ))
        }
        .parse()
        {
            Ok(num) if compiler::likely(wseof) => {
                slf.tokens.push(Token::Lit(Lit::UnsignedInt(num)))
            }
            _ => slf.set_error(LexError::InvalidUnsignedLiteral),
        }
    }

    #[inline(always)]
    fn scan_binary_literal(slf: &mut RawLexer<'a>) {
        unsafe {
            // UNSAFE(@ohsayan): cursor increment since we hit the marker byte (CR)
            slf.incr_cursor();
        }
        let mut size = 0usize;
        let mut okay = true;
        while slf.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): verified non-exhaustion
                slf.deref_cursor() != b'\n'
            }
            && okay
        {
            /*
                Don't ask me how stupid this is. Like, I was probably in some "mood" when I wrote this
                and it works duh, but isn't the most elegant of things (could I have just used a parse?
                nah, I'm just a hardcore numeric normie)
                -- Sayan
            */
            let byte = unsafe {
                // UNSAFE(@ohsayan): The pre invariant guarantees that this is correct
                slf.deref_cursor()
            };
            okay &= byte.is_ascii_digit();
            let (prod, of_flag) = size.overflowing_mul(10);
            okay &= !of_flag;
            let (sum, of_flag) = prod.overflowing_add((byte & 0x0F) as _);
            size = sum;
            okay &= !of_flag;
            unsafe {
                // UNSAFE(@ohsayan): We just read something, so this is fine (until EOA)
                slf.incr_cursor();
            }
        }
        okay &= slf.peek_eq_and_forward(b'\n');
        okay &= slf.remaining() >= size;
        if compiler::likely(okay) {
            unsafe {
                // UNSAFE(@ohsayan): Correct cursor and length (from above we know that we have enough bytes)
                slf.push_token(Lit::Bin(slice::from_raw_parts(slf.cursor(), size)));
                // UNSAFE(@ohsayan): Correct length increment
                slf.incr_cursor_by(size);
            }
        } else {
            slf.set_error(LexError::InvalidBinaryLiteral);
        }
    }
    #[inline(always)]
    fn scan_quoted_string(slf: &mut RawLexer<'a>, quote_style: u8) {
        debug_assert!(
            unsafe {
                // UNSAFE(@ohsayan): yessir, we just hit this byte. if called elsewhere, this function will crash and burn (or simply, segfault)
                slf.deref_cursor()
            } == quote_style,
            "illegal call to scan_quoted_string"
        );
        unsafe {
            // UNSAFE(@ohsayan): Increment this cursor (this is correct since we just hit the quote)
            slf.incr_cursor()
        }
        let mut buf = Vec::new();
        unsafe {
            while slf.peek_neq(quote_style) {
                // UNSAFE(@ohsayan): deref is good since peek passed
                match slf.deref_cursor() {
                    b if b != b'\\' => {
                        buf.push(b);
                    }
                    _ => {
                        // UNSAFE(@ohsayan): we read one byte, so this should work
                        slf.incr_cursor();
                        if slf.exhausted() {
                            break;
                        }
                        // UNSAFE(@ohsayan): correct because of the above branch
                        let b = slf.deref_cursor();
                        let quote = b == quote_style;
                        let bs = b == b'\\';
                        if quote | bs {
                            buf.push(b);
                        } else {
                            break; // what on good earth is that escape?
                        }
                    }
                }
                /*
                    UNSAFE(@ohsayan): This is correct because:
                    (a) If we are in arm 1: we move the cursor ahead from the `\` byte (the branch doesn't do it)
                    (b) If we are in arm 2: we don't skip the second quote byte in the branch, hence this is correct
                */
                slf.incr_cursor();
            }
            let terminated = slf.peek_eq_and_forward(quote_style);
            match String::from_utf8(buf) {
                Ok(st) if terminated => slf.tokens.push(Token::Lit(st.into_boxed_str().into())),
                _ => slf.set_error(LexError::InvalidStringLiteral),
            }
        }
    }
}

#[derive(Debug)]
/// This lexer implements the `opmod-safe` for BlueQL
pub struct SafeLexer<'a> {
    base: RawLexer<'a>,
}

impl<'a> SafeLexer<'a> {
    #[inline(always)]
    pub const fn new(src: Slice<'a>) -> Self {
        Self {
            base: RawLexer::new(src),
        }
    }
    #[inline(always)]
    pub fn lex(src: Slice<'a>) -> LexResult<Vec<Token>> {
        Self::new(src)._lex()
    }
    #[inline(always)]
    fn _lex(self) -> LexResult<Vec<Token<'a>>> {
        let Self { base: mut l } = self;
        while l.not_exhausted() && l.no_error() {
            let b = unsafe {
                // UNSAFE(@ohsayan): This is correct because of the pre invariant
                l.deref_cursor()
            };
            match b {
                // ident or kw
                b if b.is_ascii_alphabetic() => l.scan_ident_or_keyword(),
                // extra terminal chars
                b'\n' | b'\t' | b' ' => l.trim_ahead(),
                // arbitrary byte
                b => l.scan_byte(b),
            }
        }
        let RawLexer {
            last_error, tokens, ..
        } = l;
        match last_error {
            None => Ok(tokens),
            Some(e) => Err(e),
        }
    }
}

const ALLOW_UNSIGNED: bool = false;
const ALLOW_SIGNED: bool = true;

pub trait NumberDefinition: Sized + fmt::Debug + Copy + Clone + BitOr<Self, Output = Self> {
    const ALLOW_SIGNED: bool;
    fn mul_of(&self, v: u8) -> (Self, bool);
    fn add_of(&self, v: u8) -> (Self, bool);
    fn sub_of(&self, v: u8) -> (Self, bool);
    fn qualified_max_length() -> usize;
    fn zero() -> Self;
    fn b(self, b: bool) -> Self;
}

macro_rules! impl_number_def {
	($(
        $ty:ty {$supports_signed:ident, $qualified_max_length:expr}),* $(,)?
    ) => {
		$(impl NumberDefinition for $ty {
			const ALLOW_SIGNED: bool = $supports_signed;
            #[inline(always)] fn zero() -> Self { 0 }
            #[inline(always)] fn b(self, b: bool) -> Self { b as Self * self }
			#[inline(always)]
			fn mul_of(&self, v: u8) -> ($ty, bool) { <$ty>::overflowing_mul(*self, v as $ty) }
			#[inline(always)]
			fn add_of(&self, v: u8) -> ($ty, bool) { <$ty>::overflowing_add(*self, v as $ty) }
			#[inline(always)]
			fn sub_of(&self, v: u8) -> ($ty, bool) { <$ty>::overflowing_sub(*self, v as $ty) }
            #[inline(always)] fn qualified_max_length() -> usize { $qualified_max_length }
		})*
	}
}

#[cfg(target_pointer_width = "64")]
const SZ_USIZE: usize = 20;
#[cfg(target_pointer_width = "32")]
const SZ_USIZE: usize = 10;
#[cfg(target_pointer_width = "64")]
const SZ_ISIZE: usize = 20;
#[cfg(target_pointer_width = "32")]
const SZ_ISIZE: usize = 11;

impl_number_def! {
    usize {ALLOW_SIGNED, SZ_USIZE},
    // 255
    u8 {ALLOW_UNSIGNED, 3},
    // 65536
    u16 {ALLOW_UNSIGNED, 5},
    // 4294967296
    u32 {ALLOW_UNSIGNED, 10},
    // 18446744073709551616
    u64 {ALLOW_UNSIGNED, 20},
    // signed
    isize {ALLOW_SIGNED, SZ_ISIZE},
    // -128
    i8 {ALLOW_SIGNED, 4},
    // -32768
    i16 {ALLOW_SIGNED, 6},
    // -2147483648
    i32 {ALLOW_SIGNED, 11},
    // -9223372036854775808
    i64 {ALLOW_SIGNED, 20},
}

#[inline(always)]
pub(super) fn decode_num_ub<N>(src: &[u8], flag: &mut bool, cnt: &mut usize) -> N
where
    N: NumberDefinition,
{
    let l = src.len();
    let mut okay = !src.is_empty();
    let mut i = 0;
    let mut number = N::zero();
    let mut nx_stop = false;

    let is_signed = if N::ALLOW_SIGNED {
        let loc_s = i < l && src[i] == b'-';
        i += loc_s as usize;
        okay &= (i + 2) <= l; // [-][digit][LF]
        loc_s
    } else {
        false
    };

    while i < l && okay && !nx_stop {
        // potential exit
        nx_stop = src[i] == b'\n';
        // potential entry
        let mut local_ok = src[i].is_ascii_digit();
        let (p, p_of) = number.mul_of(10);
        local_ok &= !p_of;
        let lfret = if N::ALLOW_SIGNED && is_signed {
            let (d, d_of) = p.sub_of(src[i] & 0x0f);
            local_ok &= !d_of;
            d
        } else {
            let (s, s_of) = p.add_of(src[i] & 0x0f);
            local_ok &= !s_of;
            s
        };
        // reassign or assign
        let reassign = number.b(nx_stop);
        let assign = lfret.b(!nx_stop);
        number = reassign | assign;
        okay &= local_ok | nx_stop;
        i += okay as usize;
    }
    if N::ALLOW_SIGNED {
        number = number.b(okay);
    }
    okay &= nx_stop;
    *cnt += i;
    *flag &= okay;
    number
}

#[derive(Debug, PartialEq)]
/// Data constructed from `opmode-safe`
pub struct SafeQueryData<'a> {
    p: Box<[LitIR<'a>]>,
    t: Vec<Token<'a>>,
}

impl<'a> SafeQueryData<'a> {
    #[cfg(test)]
    pub fn new_test(p: Box<[LitIR<'a>]>, t: Vec<Token<'a>>) -> Self {
        Self { p, t }
    }
    #[inline(always)]
    pub fn parse_data(pf: Slice<'a>, pf_sz: usize) -> LexResult<Box<[LitIR<'a>]>> {
        Self::p_revloop(pf, pf_sz)
    }
    #[inline(always)]
    pub fn parse(qf: Slice<'a>, pf: Slice<'a>, pf_sz: usize) -> LexResult<Self> {
        let q = SafeLexer::lex(qf);
        let p = Self::p_revloop(pf, pf_sz);
        match (q, p) {
            (Ok(q), Ok(p)) => Ok(Self { t: q, p }),
            // first error
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }
    #[inline]
    pub(super) fn p_revloop(mut src: Slice<'a>, size: usize) -> LexResult<Box<[LitIR<'a>]>> {
        static LITIR_TF: [for<'a> fn(Slice<'a>, &mut usize, &mut Vec<LitIR<'a>>) -> bool; 7] = [
            SafeQueryData::uint,  // tc: 0
            SafeQueryData::sint,  // tc: 1
            SafeQueryData::bool,  // tc: 2
            SafeQueryData::float, // tc: 3
            SafeQueryData::bin,   // tc: 4
            SafeQueryData::str,   // tc: 5
            |_, _, _| false,      // ecc: 6
        ];
        let nonpadded_offset = (LITIR_TF.len() - 2) as u8;
        let ecc_offset = LITIR_TF.len() - 1;
        let mut okay = true;
        let mut data = Vec::with_capacity(size);
        while src.len() >= 3 && okay {
            let tc = src[0];
            okay &= tc <= nonpadded_offset;
            let mx = ecc_offset.min(tc as usize);
            let mut i_ = 1;
            okay &= LITIR_TF[mx](&src[1..], &mut i_, &mut data);
            src = &src[i_..];
        }
        okay &= src.is_empty() && data.len() == size;
        if compiler::likely(okay) {
            Ok(data.into_boxed_slice())
        } else {
            Err(LexError::BadPframe)
        }
    }
}

// low level methods
impl<'b> SafeQueryData<'b> {
    #[inline(always)]
    fn mxple<'a>(src: Slice<'a>, cnt: &mut usize, flag: &mut bool) -> Slice<'a> {
        // find payload length
        let mut i = 0;
        let payload_len = decode_num_ub::<usize>(src, flag, &mut i);
        let src = &src[i..];
        // find payload
        *flag &= src.len() >= payload_len;
        let mx_extract = payload_len.min(src.len());
        // incr cursor
        i += mx_extract;
        *cnt += i;
        unsafe {
            // UNSAFE(@ohsayan): src is correct (guaranteed). even if the decoded length returns an error we still remain within bounds of the EOA
            slice::from_raw_parts(src.as_ptr(), mx_extract)
        }
    }
    #[inline(always)]
    pub(super) fn uint<'a>(src: Slice<'a>, cnt: &mut usize, data: &mut Vec<LitIR<'a>>) -> bool {
        let mut b = true;
        let r = decode_num_ub(src, &mut b, cnt);
        data.push(LitIR::UnsignedInt(r));
        b
    }
    #[inline(always)]
    pub(super) fn sint<'a>(src: Slice<'a>, cnt: &mut usize, data: &mut Vec<LitIR<'a>>) -> bool {
        let mut b = true;
        let r = decode_num_ub(src, &mut b, cnt);
        data.push(LitIR::SignedInt(r));
        b
    }
    #[inline(always)]
    pub(super) fn bool<'a>(src: Slice<'a>, cnt: &mut usize, data: &mut Vec<LitIR<'a>>) -> bool {
        // `true\n` or `false\n`
        let mx = 6.min(src.len());
        let slice = &src[..mx];
        let v_true = slice.starts_with(b"true\n");
        let v_false = slice.starts_with(b"false\n");
        let incr = v_true as usize * 5 + v_false as usize * 6;
        data.push(LitIR::Bool(v_true));
        *cnt += incr;
        v_true | v_false
    }
    #[inline(always)]
    pub(super) fn float<'a>(src: Slice<'a>, cnt: &mut usize, data: &mut Vec<LitIR<'a>>) -> bool {
        let mut okay = true;
        let payload = Self::mxple(src, cnt, &mut okay);
        match String::from_utf8_lossy(payload).parse() {
            Ok(p) if compiler::likely(okay) => {
                data.push(LitIR::Float(p));
            }
            _ => {}
        }
        okay
    }
    #[inline(always)]
    pub(super) fn bin<'a>(src: Slice<'a>, cnt: &mut usize, data: &mut Vec<LitIR<'a>>) -> bool {
        let mut okay = true;
        let payload = Self::mxple(src, cnt, &mut okay);
        data.push(LitIR::Bin(payload));
        okay
    }
    #[inline(always)]
    pub(super) fn str<'a>(src: Slice<'a>, cnt: &mut usize, data: &mut Vec<LitIR<'a>>) -> bool {
        let mut okay = true;
        let payload = Self::mxple(src, cnt, &mut okay);
        match str::from_utf8(payload) {
            Ok(s) if compiler::likely(okay) => {
                data.push(LitIR::Str(s));
                true
            }
            _ => false,
        }
    }
}
