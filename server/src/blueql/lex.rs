/*
 * Created on Sat Jun 11 2022
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

/*
Minimal spec:
- Ident:
    - Starts with _ or alpha
    - Is ASCII
    - Subsequent chars contain alphanum
    - Cannot be empty
- Literals: TODO
- <punctuation> ::=
    <comma> | <open paren> | <close paren> | <open angle bracket> | <close angle bracket> | <colon>
- <comma> ::= ,
- <open paren> ::= (
- <close paren> ::= )
- <open angle bracket>: <
- <close angle bracket>: >
- <colon> ::= :
- <type> ::= "string" | "binary" | <type expression>
- <type expression> ::=
    <openparen> { [<field declaration>] <type> [comma] } <closeparen>
- <field declaration> ::= <ident> <colon>
*/

use {
    super::{find_ptr_distance, LangError, LangResult, QueryProcessor, Slice},
    core::{mem::transmute, slice, str},
};

pub trait LexItem: Sized {
    fn lex(qp: &mut QueryProcessor) -> LangResult<Self>;
}

pub struct Ident(Slice);

impl Ident {
    pub(super) unsafe fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl LexItem for Ident {
    #[inline(always)]
    fn lex(qp: &mut QueryProcessor) -> LangResult<Self> {
        let start_ptr = qp.cursor();
        // check the first byte
        let is_okay = qp.peek_is(|byte| byte.is_ascii_alphabetic() || byte == b'_');
        while is_okay
            && qp.peek_is(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            && qp.peek_neq(b' ')
        {
            unsafe {
                // UNSAFE(@ohsayan): The loop init invariant ensures this is correct
                qp.incr_cursor()
            };
        }
        if is_okay {
            let len = find_ptr_distance(start_ptr, qp.cursor());
            qp.skip_delimiter(); // skip whitespace (if any)
            unsafe {
                // UNSAFE(@ohsayan): The above procedure ensures validity
                Ok(Self(Slice::new(start_ptr, len)))
            }
        } else {
            Err(LangError::InvalidSyntax)
        }
    }
}

pub struct LitNum(pub u64);

impl LexItem for LitNum {
    #[inline(always)]
    fn lex(qp: &mut QueryProcessor) -> LangResult<Self> {
        let mut is_okay = true;
        let mut ret: u64 = 0;
        while is_okay && qp.peek_neq(b' ') {
            let cbyte = unsafe {
                // UNSAFE(@ohsayan): Loop invariant guarantees correctness
                qp.deref_cursor()
            };
            is_okay &= cbyte.is_ascii_digit();

            // multiply
            let (ret_on_mul, overflow_flag) = ret.overflowing_mul(10);
            ret = ret_on_mul;
            is_okay &= !overflow_flag;

            // add
            let (ret_on_add, overflow_flag) = ret.overflowing_add((cbyte & 0x0F) as u64);
            ret = ret_on_add;
            is_okay &= !overflow_flag;

            unsafe {
                // UNSAFE(@ohsayan): Loop invariant guarantees correctness. 1B past EOA is also
                // defined behavior in rust
                qp.incr_cursor()
            }
        }
        // IMPORTANT: By grammar rules, a number can only be followed by EOF or whitespace
        is_okay &= qp.did_skip_delimiter_or_eof();
        if is_okay {
            Ok(Self(ret))
        } else {
            Err(LangError::TypeParseFailure)
        }
    }
}

pub struct LitString<'a>(pub &'a str);

impl<'a> LexItem for LitString<'a> {
    #[inline(always)]
    fn lex(qp: &mut QueryProcessor) -> LangResult<Self> {
        // should start with '"'
        let mut is_okay = qp.peek_eq_and_forward(b'"');
        let start_ptr = qp.cursor();
        while is_okay && qp.peek_neq(b'"') {
            unsafe {
                // UNSAFE(@ohsayan): Loop invariant guarantees correctness. 1B past EOA is also
                // defined behavior in rust
                qp.incr_cursor()
            };
        }
        let stop_ptr = qp.cursor();
        // should be terminated by a '"'
        is_okay &= qp.peek_eq_and_forward(b'"');
        // IMPORTANT: By grammar rules, a literal can only be followed by EOF or whitespace
        is_okay &= qp.did_skip_delimiter_or_eof();
        if is_okay {
            let len = find_ptr_distance(start_ptr, stop_ptr);
            let string = str::from_utf8(unsafe { slice::from_raw_parts(start_ptr, len) })?;
            Ok(Self(string))
        } else {
            Err(LangError::TypeParseFailure)
        }
    }
}

#[inline(always)]
/// # Safety
/// - Ensure that the qp is not exhausted
unsafe fn check_escaped(qp: &mut QueryProcessor, escape_what: u8) -> bool {
    debug_assert!(qp.not_exhausted());
    qp.deref_cursor() == b'\\' && { qp.not_exhausted() && qp.deref_cursor() == escape_what }
}

pub struct LitStringEscaped(pub String);

impl LexItem for LitStringEscaped {
    #[inline(always)]
    fn lex(qp: &mut QueryProcessor) -> LangResult<Self> {
        let mut stringbuf = Vec::new();
        // should start with  '"'
        let mut is_okay = qp.peek_eq_and_forward(b'"');
        while is_okay && qp.peek_neq(b'"') {
            let is_escaped_backslash = unsafe {
                // UNSAFE(@ohsayan): The qp is not exhausted, so this is fine
                check_escaped(qp, b'\\')
            };
            let is_escaped_quote = unsafe {
                // UNSAFE(@ohsayan): The qp is not exhausted, so this is fine
                check_escaped(qp, b'"')
            };
            unsafe {
                // UNSAFE(@ohsayan): If either is true, then it is correct to do this
                qp.incr_cursor_if(is_escaped_backslash | is_escaped_quote)
            };
            unsafe {
                // UNSAFE(@ohsayan): if not escaped, this is fine. if escaped, this is still
                // fine because the escaped byte was checked
                stringbuf.push(qp.deref_cursor());
            }
            unsafe {
                // UNSAFE(@ohsayan): if escaped we have moved ahead by one but the escaped char
                // is still one more so we go ahead. if not, then business as usual
                qp.incr_cursor()
            };
        }
        // should be terminated by a '"'
        is_okay &= qp.peek_eq_and_forward(b'"');
        // IMPORTANT: By grammar rules, a literal can only be followed by EOF or whitespace
        is_okay &= qp.did_skip_delimiter_or_eof();
        match String::from_utf8(stringbuf) {
            Ok(s) if is_okay => Ok(Self(s)),
            _ => Err(LangError::TypeParseFailure),
        }
    }
}

macro_rules! impl_punctuation {
    ($($ty:ident: $byte:literal),*) => {
        $(
            pub struct $ty;
            impl $ty {
                pub(super) const fn get_byte() -> u8 { $byte }
            }
            impl LexItem for $ty {
                #[inline(always)]
                fn lex(qp: &mut QueryProcessor) -> LangResult<Self> {
                    if qp.peek_eq_and_forward($byte) {
                        qp.skip_delimiter();
                        Ok(Self)
                    } else {
                        Err(LangError::InvalidSyntax)
                    }
                }
            }
        )*
    };
}

impl_punctuation! {
    OpenParen: b'(',
    CloseParen: b')',
    OpenAngular: b'<',
    CloseAngular: b'>',
    Colon: b':',
    Semicolon: b';',
    SingleQuote: b'\'',
    DoubleQuote: b'"'
}

#[derive(Debug, PartialEq)]
#[repr(u8)]
pub enum Type {
    String,
    Binary,
    List,
}

impl Type {
    pub fn try_from_ident(id: &Ident) -> LangResult<Self> {
        let ret = match unsafe {
            // UNSAFE(@ohsayan): The lifetime of the `qp` ensures validity
            id.as_slice()
        } {
            b"string" => Self::String,
            b"binary" => Self::Binary,
            b"list" => Self::List,
            _ => return Err(LangError::UnknownType),
        };
        Ok(ret)
    }
}

impl LexItem for Type {
    #[inline(always)]
    fn lex(qp: &mut QueryProcessor) -> LangResult<Self> {
        qp.next::<Ident>().and_then(|id| Self::try_from_ident(&id))
    }
}

#[derive(PartialEq, Debug)]
pub struct TypeExpression(pub Vec<Type>);

impl LexItem for TypeExpression {
    fn lex(qp: &mut QueryProcessor) -> LangResult<Self> {
        /*
        A type expression looks like ty<ty<ty<...>>>
        */
        let mut type_expr = Vec::with_capacity(2);
        #[repr(u8)]
        #[derive(Clone, Copy)]
        enum Expect {
            Type = 0,
            Close = 1,
        }
        let mut expect = Expect::Type;
        let mut valid_expr = true;
        let mut open_c = 0;
        let mut close_c = 0;
        while qp.not_exhausted() && valid_expr {
            match expect {
                Expect::Close => {
                    valid_expr &=
                        unsafe { qp.deref_cursor_and_forward() } == CloseAngular::get_byte();
                    close_c += 1;
                    expect = Expect::Close;
                }
                Expect::Type => {
                    // we expect a type
                    match qp.next::<Type>() {
                        Ok(ty) => {
                            type_expr.push(ty);
                            // see if next is open '<'; if it is, then we expect a type, if it is a
                            // `>`, then we expect '>'
                            let next_is_open = qp.peek_eq(OpenAngular::get_byte());
                            let next_is_close = qp.peek_eq(CloseAngular::get_byte());
                            // this is important! if both of the above fail, then something is broken!
                            // this expression ensures that we catch the error
                            valid_expr &= next_is_open | next_is_close;
                            open_c += next_is_open as usize;
                            close_c += next_is_close as usize;
                            unsafe {
                                qp.incr_cursor_if(next_is_open | next_is_close);
                            }
                            expect = unsafe {
                                // UNSAFE(@ohsayan): This is all good! Atmost value is 1, which resolves
                                // to Expect::Close
                                transmute(next_is_close)
                            };
                        }
                        Err(_) => valid_expr = false,
                    }
                }
            }
        }
        valid_expr &= open_c == close_c;
        if valid_expr {
            qp.skip_delimiter();
            Ok(Self(type_expr))
        } else {
            Err(LangError::BadExpression)
        }
    }
}
