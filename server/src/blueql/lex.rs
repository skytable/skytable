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

use super::{find_ptr_distance, LangError, LangResult, Scanner, Slice};

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
