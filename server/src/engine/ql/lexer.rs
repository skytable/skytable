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

use {
    super::{LangError, LangResult, RawSlice},
    crate::util::{compiler, Life},
    core::{marker::PhantomData, mem::size_of, slice, str},
};

/*
    Lex meta
*/

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Symbol(Symbol),
    Keyword(Keyword),
    UnsafeLit(RawSlice),
    Ident(RawSlice),
    #[cfg(test)]
    /// A comma that can be ignored (used for fuzzing)
    IgnorableComma,
    Lit(Lit), // literal
}

impl PartialEq<Symbol> for Token {
    fn eq(&self, other: &Symbol) -> bool {
        match self {
            Self::Symbol(s) => s == other,
            _ => false,
        }
    }
}

assertions! {
    size_of::<Token>() == 24, // FIXME(@ohsayan): Damn, what?
    size_of::<Symbol>() == 1,
    size_of::<Keyword>() == 2,
    size_of::<Lit>() == 24, // FIXME(@ohsayan): Ouch
}

enum_impls! {
    Token => {
        Keyword as Keyword,
        Symbol as Symbol,
        Lit as Lit,
    }
}

#[derive(Debug, PartialEq, Clone)]
#[repr(u8)]
pub enum Lit {
    Str(Box<str>),
    Bool(bool),
    Num(u64),
}

enum_impls! {
    Lit => {
        Box<str> as Str,
        bool as Bool,
        u64 as Num,
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Symbol {
    OpArithmeticAdd,  // +
    OpArithmeticSub,  // -
    OpArithmeticMul,  // *
    OpArithmeticDiv,  // /
    OpLogicalNot,     // !
    OpLogicalAnd,     // &
    OpLogicalXor,     // ^
    OpLogicalOr,      // |
    OpAssign,         // =
    TtOpenParen,      // (
    TtCloseParen,     // )
    TtOpenSqBracket,  // [
    TtCloseSqBracket, // ]
    TtOpenBrace,      // {
    TtCloseBrace,     // }
    OpComparatorLt,   // <
    OpComparatorGt,   // >
    QuoteS,           // '
    QuoteD,           // "
    SymAt,            // @
    SymHash,          // #
    SymDollar,        // $
    SymPercent,       // %
    SymUnderscore,    // _
    SymBackslash,     // \
    SymColon,         // :
    SymSemicolon,     // ;
    SymComma,         // ,
    SymPeriod,        // .
    SymQuestion,      // ?
    SymTilde,         // ~
    SymAccent,        // `
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum Keyword {
    Ddl(DdlKeyword),
    DdlMisc(DdlMiscKeyword),
    Dml(DmlKeyword),
    DmlMisc(DmlMiscKeyword),
    TypeId(Type),
    Misc(MiscKeyword),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum MiscKeyword {
    Null,
}

enum_impls! {
    Keyword => {
        DdlKeyword as Ddl,
        DdlMiscKeyword as DdlMisc,
        DmlKeyword as Dml,
        DmlMiscKeyword as DmlMisc,
        Type as TypeId,
        MiscKeyword as Misc,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DmlMiscKeyword {
    Limit,
    From,
    Into,
    Where,
    If,
    And,
    As,
    By,
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DmlKeyword {
    Insert,
    Select,
    Update,
    Delete,
    Exists,
    Truncate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DdlMiscKeyword {
    With,
    Add,
    Remove,
    Sort,
    Type,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DdlKeyword {
    Use,
    Create,
    Alter,
    Drop,
    Inspect,
    Model,
    Space,
    Primary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Type {
    String,
    Binary,
    List,
    Map,
    Bool,
    Int,
    Double,
    Float,
}

/*
    This section implements DAGs, as described by Czech et al in their paper. I wrote these pretty much by brute-force using
    a byte-level multiplicative function (inside a script). This unfortunately implies that every time we *do* need to add a
    new keyword, I will need to recompute and rewrite the vertices. I don't plan to use any codegen, so I think this is good
    as-is. The real challenge here is to keep the graph small, and I couldn't do that for the symbols table even with multiple
    trials. Please see if you can improve them.

    Also the functions are unique to every graph, and every input set, so BE WARNED!

    -- Sayan (@ohsayan)
    Sept. 18, 2022
*/

const SYM_MAGIC_A: u8 = b'w';
const SYM_MAGIC_B: u8 = b'E';

static SYM_GRAPH: [u8; 69] = [
    0, 0, 25, 0, 3, 0, 21, 0, 6, 13, 0, 0, 0, 0, 8, 0, 0, 0, 17, 0, 0, 30, 0, 28, 0, 20, 19, 12, 0,
    0, 2, 0, 0, 15, 0, 0, 0, 5, 0, 31, 14, 0, 1, 0, 18, 29, 24, 0, 0, 10, 0, 0, 26, 0, 0, 0, 22, 0,
    23, 7, 0, 27, 0, 4, 16, 11, 0, 0, 9,
];

static SYM_DATA: [(u8, Symbol); 32] = [
    (b'+', Symbol::OpArithmeticAdd),
    (b'-', Symbol::OpArithmeticSub),
    (b'*', Symbol::OpArithmeticMul),
    (b'/', Symbol::OpArithmeticDiv),
    (b'!', Symbol::OpLogicalNot),
    (b'&', Symbol::OpLogicalAnd),
    (b'^', Symbol::OpLogicalXor),
    (b'|', Symbol::OpLogicalOr),
    (b'=', Symbol::OpAssign),
    (b'(', Symbol::TtOpenParen),
    (b')', Symbol::TtCloseParen),
    (b'[', Symbol::TtOpenSqBracket),
    (b']', Symbol::TtCloseSqBracket),
    (b'{', Symbol::TtOpenBrace),
    (b'}', Symbol::TtCloseBrace),
    (b'<', Symbol::OpComparatorLt),
    (b'>', Symbol::OpComparatorGt),
    (b'\'', Symbol::QuoteS),
    (b'"', Symbol::QuoteD),
    (b'@', Symbol::SymAt),
    (b'#', Symbol::SymHash),
    (b'$', Symbol::SymDollar),
    (b'%', Symbol::SymPercent),
    (b'_', Symbol::SymUnderscore),
    (b'\\', Symbol::SymBackslash),
    (b':', Symbol::SymColon),
    (b';', Symbol::SymSemicolon),
    (b',', Symbol::SymComma),
    (b'.', Symbol::SymPeriod),
    (b'?', Symbol::SymQuestion),
    (b'~', Symbol::SymTilde),
    (b'`', Symbol::SymAccent),
];

#[inline(always)]
fn symfh(k: u8, magic: u8) -> u16 {
    (magic as u16 * k as u16) % SYM_GRAPH.len() as u16
}

#[inline(always)]
fn symph(k: u8) -> u8 {
    (SYM_GRAPH[symfh(k, SYM_MAGIC_A) as usize] + SYM_GRAPH[symfh(k, SYM_MAGIC_B) as usize])
        % SYM_GRAPH.len() as u8
}

#[inline(always)]
fn symof(sym: u8) -> Option<Symbol> {
    let hf = symph(sym);
    if hf < SYM_DATA.len() as u8 && SYM_DATA[hf as usize].0 == sym {
        Some(SYM_DATA[hf as usize].1)
    } else {
        None
    }
}

static KW_GRAPH: [u8; 40] = [
    0, 2, 32, 18, 4, 37, 11, 27, 34, 35, 26, 33, 0, 0, 10, 2, 22, 8, 5, 7, 16, 9, 8, 39, 21, 5, 0,
    22, 14, 19, 22, 31, 28, 38, 26, 21, 30, 24, 10, 18,
];

static KW_DATA: [(&str, Keyword); 38] = [
    ("use", Keyword::Ddl(DdlKeyword::Use)),
    ("create", Keyword::Ddl(DdlKeyword::Create)),
    ("alter", Keyword::Ddl(DdlKeyword::Alter)),
    ("drop", Keyword::Ddl(DdlKeyword::Drop)),
    ("inspect", Keyword::Ddl(DdlKeyword::Inspect)),
    ("model", Keyword::Ddl(DdlKeyword::Model)),
    ("space", Keyword::Ddl(DdlKeyword::Space)),
    ("primary", Keyword::Ddl(DdlKeyword::Primary)),
    ("with", Keyword::DdlMisc(DdlMiscKeyword::With)),
    ("add", Keyword::DdlMisc(DdlMiscKeyword::Add)),
    ("remove", Keyword::DdlMisc(DdlMiscKeyword::Remove)),
    ("sort", Keyword::DdlMisc(DdlMiscKeyword::Sort)),
    ("type", Keyword::DdlMisc(DdlMiscKeyword::Type)),
    ("insert", Keyword::Dml(DmlKeyword::Insert)),
    ("select", Keyword::Dml(DmlKeyword::Select)),
    ("update", Keyword::Dml(DmlKeyword::Update)),
    ("delete", Keyword::Dml(DmlKeyword::Delete)),
    ("exists", Keyword::Dml(DmlKeyword::Exists)),
    ("truncate", Keyword::Dml(DmlKeyword::Truncate)),
    ("limit", Keyword::DmlMisc(DmlMiscKeyword::Limit)),
    ("from", Keyword::DmlMisc(DmlMiscKeyword::From)),
    ("into", Keyword::DmlMisc(DmlMiscKeyword::Into)),
    ("where", Keyword::DmlMisc(DmlMiscKeyword::Where)),
    ("if", Keyword::DmlMisc(DmlMiscKeyword::If)),
    ("and", Keyword::DmlMisc(DmlMiscKeyword::And)),
    ("as", Keyword::DmlMisc(DmlMiscKeyword::As)),
    ("by", Keyword::DmlMisc(DmlMiscKeyword::By)),
    ("asc", Keyword::DmlMisc(DmlMiscKeyword::Asc)),
    ("desc", Keyword::DmlMisc(DmlMiscKeyword::Desc)),
    ("string", Keyword::TypeId(Type::String)),
    ("binary", Keyword::TypeId(Type::Binary)),
    ("list", Keyword::TypeId(Type::List)),
    ("map", Keyword::TypeId(Type::Map)),
    ("bool", Keyword::TypeId(Type::Bool)),
    ("int", Keyword::TypeId(Type::Int)),
    ("double", Keyword::TypeId(Type::Double)),
    ("float", Keyword::TypeId(Type::Float)),
    ("null", Keyword::Misc(MiscKeyword::Null)),
];

const KW_MAGIC_A: &[u8] = b"GSggb8qI";
const KW_MAGIC_B: &[u8] = b"ZaljIeOx";
const KW_MODULUS: usize = 8;

#[inline(always)]
fn kwfh(k: &[u8], magic: &[u8]) -> u32 {
    let mut i = 0;
    let mut s = 0;
    while i < k.len() {
        s += magic[(i % KW_MODULUS) as usize] as u32 * k[i] as u32;
        i += 1;
    }
    s % KW_GRAPH.len() as u32
}

#[inline(always)]
fn kwph(k: &[u8]) -> u8 {
    (KW_GRAPH[kwfh(k, KW_MAGIC_A) as usize] + KW_GRAPH[kwfh(k, KW_MAGIC_B) as usize])
        % KW_GRAPH.len() as u8
}

#[inline(always)]
fn kwof(key: &str) -> Option<Keyword> {
    let ph = kwph(key.as_bytes());
    if ph < KW_DATA.len() as u8 && KW_DATA[ph as usize].0 == key {
        Some(KW_DATA[ph as usize].1)
    } else {
        None
    }
}

/*
    Lexer impl
*/

pub struct Lexer<'a> {
    c: *const u8,
    e: *const u8,
    last_error: Option<LangError>,
    tokens: Vec<Token>,
    _lt: PhantomData<&'a [u8]>,
}

impl<'a> Lexer<'a> {
    pub const fn new(src: &'a [u8]) -> Self {
        unsafe {
            Self {
                c: src.as_ptr(),
                e: src.as_ptr().add(src.len()),
                last_error: None,
                tokens: Vec::new(),
                _lt: PhantomData,
            }
        }
    }
}

// meta
impl<'a> Lexer<'a> {
    #[inline(always)]
    const fn cursor(&self) -> *const u8 {
        self.c
    }
    #[inline(always)]
    const fn data_end_ptr(&self) -> *const u8 {
        self.e
    }
    #[inline(always)]
    fn not_exhausted(&self) -> bool {
        self.data_end_ptr() > self.cursor()
    }
    #[inline(always)]
    fn exhausted(&self) -> bool {
        self.cursor() == self.data_end_ptr()
    }
    #[inline(always)]
    fn remaining(&self) -> usize {
        unsafe { self.e.offset_from(self.c) as usize }
    }
    unsafe fn deref_cursor(&self) -> u8 {
        *self.cursor()
    }
    #[inline(always)]
    unsafe fn incr_cursor_by(&mut self, by: usize) {
        debug_assert!(self.remaining() >= by);
        self.c = self.cursor().add(by)
    }
    #[inline(always)]
    unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    #[inline(always)]
    unsafe fn incr_cursor_if(&mut self, iff: bool) {
        self.incr_cursor_by(iff as usize)
    }
    #[inline(always)]
    fn push_token(&mut self, token: impl Into<Token>) {
        self.tokens.push(token.into())
    }
    #[inline(always)]
    fn peek_is(&mut self, f: impl FnOnce(u8) -> bool) -> bool {
        self.not_exhausted() && unsafe { f(self.deref_cursor()) }
    }
    #[inline(always)]
    fn peek_is_and_forward(&mut self, f: impl FnOnce(u8) -> bool) -> bool {
        let did_fw = self.not_exhausted() && unsafe { f(self.deref_cursor()) };
        unsafe {
            self.incr_cursor_if(did_fw);
        }
        did_fw
    }
    #[inline(always)]
    fn peek_eq_and_forward_or_eof(&mut self, eq: u8) -> bool {
        unsafe {
            let eq = self.not_exhausted() && self.deref_cursor() == eq;
            self.incr_cursor_if(eq);
            eq | self.exhausted()
        }
    }
    #[inline(always)]
    fn peek_neq(&self, b: u8) -> bool {
        self.not_exhausted() && unsafe { self.deref_cursor() != b }
    }
    #[inline(always)]
    fn peek_eq_and_forward(&mut self, b: u8) -> bool {
        unsafe {
            let r = self.not_exhausted() && self.deref_cursor() == b;
            self.incr_cursor_if(r);
            r
        }
    }
    #[inline(always)]
    fn trim_ahead(&mut self) {
        while self.peek_is_and_forward(|b| b == b' ' || b == b'\t' || b == b'\n') {}
    }
}

impl<'a> Lexer<'a> {
    fn scan_ident(&mut self) -> RawSlice {
        let s = self.cursor();
        unsafe {
            while self.peek_is(|b| b.is_ascii_alphanumeric() || b == b'_') {
                self.incr_cursor();
            }
            RawSlice::new(s, self.cursor().offset_from(s) as usize)
        }
    }

    fn scan_ident_or_keyword(&mut self) {
        let s = self.scan_ident();
        let st = unsafe { s.as_str() };
        match kwof(st) {
            Some(kw) => self.tokens.push(kw.into()),
            // FIXME(@ohsayan): Uh, mind fixing this? The only advantage is that I can keep the graph *memory* footprint small
            None if st == "true" || st == "false" => self.push_token(Lit::Bool(st == "true")),
            None => self.tokens.push(Token::Ident(s)),
        }
    }

    fn scan_number(&mut self) {
        let s = self.cursor();
        unsafe {
            while self.peek_is(|b| b.is_ascii_digit()) {
                self.incr_cursor();
            }
            /*
                1234; // valid
                1234} // valid
                1234{ // invalid
                1234, // valid
                1234a // invalid
            */
            static TERMINAL_CHAR: [u8; 8] = [b';', b'}', b',', b' ', b'\n', b'\t', b',', b']'];
            let wseof = self.peek_is(|b| TERMINAL_CHAR.contains(&b)) || self.exhausted();
            match str::from_utf8_unchecked(slice::from_raw_parts(
                s,
                self.cursor().offset_from(s) as usize,
            ))
            .parse()
            {
                Ok(num) if compiler::likely(wseof) => self.tokens.push(Token::Lit(Lit::Num(num))),
                _ => self.last_error = Some(LangError::InvalidNumericLiteral),
            }
        }
    }

    fn scan_quoted_string(&mut self, quote_style: u8) {
        debug_assert!(
            unsafe { self.deref_cursor() } == quote_style,
            "illegal call to scan_quoted_string"
        );
        unsafe { self.incr_cursor() }
        let mut buf = Vec::new();
        unsafe {
            while self.peek_neq(quote_style) {
                match self.deref_cursor() {
                    b if b != b'\\' => {
                        buf.push(b);
                    }
                    _ => {
                        self.incr_cursor();
                        if self.exhausted() {
                            break;
                        }
                        let b = self.deref_cursor();
                        let quote = b == quote_style;
                        let bs = b == b'\\';
                        if quote | bs {
                            buf.push(b);
                        } else {
                            break; // what on good earth is that escape?
                        }
                    }
                }
                self.incr_cursor();
            }
            let terminated = self.peek_eq_and_forward(quote_style);
            match String::from_utf8(buf) {
                Ok(st) if terminated => self.tokens.push(Token::Lit(st.into_boxed_str().into())),
                _ => self.last_error = Some(LangError::InvalidStringLiteral),
            }
        }
    }
    fn scan_byte(&mut self, byte: u8) {
        match symof(byte) {
            Some(tok) => self.push_token(tok),
            None => {
                self.last_error = Some(LangError::UnexpectedChar);
                return;
            }
        }
        unsafe {
            self.incr_cursor();
        }
    }

    fn scan_unsafe_literal(&mut self) {
        unsafe {
            self.incr_cursor();
        }
        let mut size = 0usize;
        let mut okay = true;
        while self.not_exhausted() && unsafe { self.deref_cursor() != b'\n' } && okay {
            /*
                Don't ask me how stupid this is. Like, I was probably in some "mood" when I wrote this
                and it works duh, but isn't the most elegant of things (could I have just used a parse?
                nah, I'm just a hardcore numeric normie)
                -- Sayan
            */
            let byte = unsafe { self.deref_cursor() };
            okay &= byte.is_ascii_digit();
            let (prod, of_flag) = size.overflowing_mul(10);
            okay &= !of_flag;
            let (sum, of_flag) = prod.overflowing_add((byte & 0x0F) as _);
            size = sum;
            okay &= !of_flag;
            unsafe {
                self.incr_cursor();
            }
        }
        okay &= self.peek_eq_and_forward(b'\n');
        okay &= self.remaining() >= size;
        if compiler::likely(okay) {
            unsafe {
                self.push_token(Token::UnsafeLit(RawSlice::new(self.cursor(), size)));
                self.incr_cursor_by(size);
            }
        } else {
            self.last_error = Some(LangError::InvalidUnsafeLiteral);
        }
    }

    fn _lex(&mut self) {
        while self.not_exhausted() && self.last_error.is_none() {
            match unsafe { self.deref_cursor() } {
                byte if byte.is_ascii_alphabetic() => self.scan_ident_or_keyword(),
                #[cfg(test)]
                byte if byte == b'\r'
                    && self.remaining() > 1
                    && !(unsafe {
                        // UNSAFE(@ohsayan): The previous condition ensures that this doesn't segfault
                        *self.cursor().add(1)
                    })
                    .is_ascii_digit() =>
                {
                    /*
                        NOTE(@ohsayan): The above guard might look a little messy but is necessary to support raw
                        literals which will use the carriage return
                    */
                    self.push_token(Token::IgnorableComma);
                    unsafe {
                        // UNSAFE(@ohsayan): All good here. Already read the token
                        self.incr_cursor();
                    }
                }
                b'\r' => self.scan_unsafe_literal(),
                byte if byte.is_ascii_digit() => self.scan_number(),
                qs @ (b'\'' | b'"') => self.scan_quoted_string(qs),
                b' ' | b'\n' | b'\t' => self.trim_ahead(),
                b => self.scan_byte(b),
            }
        }
    }

    pub fn lex(src: &'a [u8]) -> LangResult<Life<'a, Vec<Token>>> {
        let mut slf = Self::new(src);
        slf._lex();
        match slf.last_error {
            None => Ok(Life::new(slf.tokens)),
            Some(e) => Err(e),
        }
    }
}

impl Token {
    #[inline(always)]
    pub(crate) fn is_ident(&self) -> bool {
        matches!(self, Token::Ident(_))
    }
    #[inline(always)]
    pub(crate) fn is_typeid(&self) -> bool {
        matches!(self, Token::Keyword(Keyword::TypeId(_)))
    }
    #[inline(always)]
    pub(crate) fn as_ident_eq_ignore_case(&self, arg: &[u8]) -> bool {
        self.is_ident()
            && unsafe {
                if let Self::Ident(id) = self {
                    id.as_slice().eq_ignore_ascii_case(arg)
                } else {
                    impossible!()
                }
            }
    }
    #[inline(always)]
    pub(super) unsafe fn ident_unchecked(&self) -> RawSlice {
        if let Self::Ident(id) = self {
            id.clone()
        } else {
            impossible!()
        }
    }
    #[inline(always)]
    pub(super) fn is_lit(&self) -> bool {
        matches!(self, Self::Lit(_))
    }
}

impl AsRef<Token> for Token {
    fn as_ref(&self) -> &Token {
        self
    }
}
