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
    super::{find_ptr_distance, LangError, LangResult, Scanner, Slice},
    core::{mem::transmute, slice, str},
};

pub trait LexItem: Sized {
    fn lex(scanner: &mut Scanner) -> LangResult<Self>;
}

pub struct Ident(Slice);

impl Ident {
    pub(super) unsafe fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl LexItem for Ident {
    #[inline(always)]
    fn lex(scanner: &mut Scanner) -> LangResult<Self> {
        let start_ptr = scanner.cursor(); // look at the current cursor
        let is_okay = {
            // check the first byte
            scanner.not_exhausted()
                && unsafe {
                    // UNSAFE(@ohsayan): The first operand guarantees correctness
                    let byte = scanner.deref_cursor();
                    byte.is_ascii_alphabetic() || byte == b'_'
                }
        };
        while scanner.not_exhausted()
            && is_okay
            && unsafe {
                // UNSAFE(@ohsayan): The first operand guarantees correctness
                let byte = scanner.deref_cursor();
                byte.is_ascii_alphanumeric() || byte == b'_'
            }
            && unsafe { scanner.deref_cursor() != b' ' }
        {
            unsafe {
                // UNSAFE(@ohsayan): The loop init invariant ensures this is correct
                scanner.incr_cursor()
            };
        }
        if is_okay {
            let len = find_ptr_distance(start_ptr, scanner.cursor());
            scanner.skip_separator(); // skip whitespace (if any)
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
    fn lex(scanner: &mut Scanner) -> LangResult<Self> {
        let mut is_okay = true;
        let mut ret: u64 = 0;
        while scanner.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first operand guarantees correctness
                scanner.deref_cursor() != b' '
            }
            && is_okay
        {
            let cbyte = unsafe {
                // UNSAFE(@ohsayan): Loop invariant guarantees correctness
                scanner.deref_cursor()
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
                scanner.incr_cursor()
            }
        }
        if is_okay {
            scanner.skip_separator();
            Ok(Self(ret))
        } else {
            Err(LangError::TypeParseFailure)
        }
    }
}

pub struct LitString<'a>(pub &'a str);

impl<'a> LexItem for LitString<'a> {
    #[inline(always)]
    fn lex(scanner: &mut Scanner) -> LangResult<Self> {
        // should start with '"'
        let mut is_okay = scanner.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first operand guarantees correctness
                let cond = scanner.deref_cursor() == b'"';
                scanner.incr_cursor();
                cond
            };
        let start_ptr = scanner.cursor();
        while is_okay
            && scanner.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first operand guarantees correctness
                scanner.deref_cursor() != b'"'
            }
        {
            unsafe {
                // UNSAFE(@ohsayan): Loop invariant guarantees correctness. 1B past EOA is also
                // defined behavior in rust
                scanner.incr_cursor()
            };
        }
        // should be terminated by a '"'
        is_okay &= scanner.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): First operand guarantees correctness
                scanner.deref_cursor() == b'"'
            };
        if is_okay {
            let len = find_ptr_distance(start_ptr, scanner.cursor());
            let string = str::from_utf8(unsafe { slice::from_raw_parts(start_ptr, len) })?;
            scanner.skip_separator();
            Ok(Self(string))
        } else {
            Err(LangError::TypeParseFailure)
        }
    }
}

#[inline(always)]
/// # Safety
/// - Ensure that the scanner is not exhausted
unsafe fn check_escaped(scanner: &mut Scanner, escape_what: u8) -> bool {
    debug_assert!(scanner.not_exhausted());
    scanner.deref_cursor() == b'\\' && {
        scanner.not_exhausted() && scanner.deref_cursor() == escape_what
    }
}

pub struct LitStringEscaped(pub String);

impl LexItem for LitStringEscaped {
    #[inline(always)]
    fn lex(scanner: &mut Scanner) -> LangResult<Self> {
        let mut stringbuf = Vec::new();
        // should start with  '"'
        let mut is_okay = scanner.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The first operand guarantees correctness
                let cond = scanner.deref_cursor() == b'"';
                scanner.incr_cursor();
                cond
            };
        while is_okay
            && scanner.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): The second operand guarantees correctness
                scanner.deref_cursor() != b'"'
            }
        {
            let is_escaped_backslash = unsafe {
                // UNSAFE(@ohsayan): The scanner is not exhausted, so this is fine
                check_escaped(scanner, b'\\')
            };
            let is_escaped_quote = unsafe {
                // UNSAFE(@ohsayan): The scanner is not exhausted, so this is fine
                check_escaped(scanner, b'"')
            };
            unsafe {
                // UNSAFE(@ohsayan): If either is true, then it is correct to do this
                scanner.incr_cursor_by((is_escaped_backslash | is_escaped_quote) as usize)
            };
            unsafe {
                // UNSAFE(@ohsayan): if not escaped, this is fine. if escaped, this is still
                // fine because the escaped byte was checked
                stringbuf.push(scanner.deref_cursor());
            }
            unsafe {
                // UNSAFE(@ohsayan): if escaped we have moved ahead by one but the escaped char
                // is still one more so we go ahead. if not, then business as usual
                scanner.incr_cursor()
            };
        }

        // should be terminated by a '"'
        is_okay &= scanner.not_exhausted()
            && unsafe {
                // UNSAFE(@ohsayan): First operand guarantees correctness
                scanner.deref_cursor() == b'"'
            };
        scanner.skip_separator();
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
                const fn get_byte() -> u8 { $byte }
            }
            impl LexItem for $ty {
                #[inline(always)]
                fn lex(scanner: &mut Scanner) -> LangResult<Self> {
                    if scanner.not_exhausted() && unsafe {
                        // UNSAFE(@ohsayan): The first operand ensures correctness
                        scanner.deref_cursor() == $byte
                    } {
                        unsafe {
                            // UNSAFE(@ohsayan): The above condition guarantees safety
                            scanner.incr_cursor()
                        };
                        scanner.skip_separator();
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

impl LexItem for Type {
    #[inline(always)]
    fn lex(scanner: &mut Scanner) -> LangResult<Self> {
        let ret = match Ident::lex(scanner) {
            Ok(ret) => {
                match unsafe {
                    // UNSAFE(@ohsayan): The lifetime of the `scanner` ensures validity
                    ret.as_slice()
                } {
                    b"string" => Self::String,
                    b"binary" => Self::Binary,
                    b"list" => Self::List,
                    _ => return Err(LangError::UnknownType),
                }
            }
            Err(_) => return Err(LangError::InvalidSyntax),
        };
        Ok(ret)
    }
}

#[derive(PartialEq)]
pub struct TypeExpression(pub Vec<Type>);

impl LexItem for TypeExpression {
    fn lex(scanner: &mut Scanner) -> LangResult<Self> {
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
        while scanner.not_exhausted() && valid_expr {
            match expect {
                Expect::Close => {
                    valid_expr &=
                        unsafe { scanner.deref_cursor_and_forward() } == CloseAngular::get_byte();
                    close_c += 1;
                    expect = Expect::Close;
                }
                Expect::Type => {
                    // we expect a type
                    match Type::lex(scanner) {
                        Ok(ty) => {
                            type_expr.push(ty);
                            // see if next is open '<'; if it is, then we expect a type, if it is a
                            // `>`, then we expect '>'
                            let next_is_open = scanner.peek_eq(OpenAngular::get_byte());
                            let next_is_close = scanner.peek_eq(CloseAngular::get_byte());
                            // this is important! if both of the above fail, then something is broken!
                            // this expression ensures that we catch the error
                            valid_expr &= next_is_open | next_is_close;
                            open_c += next_is_open as usize;
                            close_c += next_is_close as usize;
                            unsafe {
                                scanner.incr_cursor_by((next_is_open | next_is_close) as usize);
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
            scanner.skip_separator();
            Ok(Self(type_expr))
        } else {
            Err(LangError::BadExpression)
        }
    }
}
