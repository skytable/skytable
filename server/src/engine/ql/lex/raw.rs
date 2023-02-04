/*
 * Created on Wed Feb 01 2023
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

use {
    super::Slice,
    crate::engine::ql::LangError,
    core::{borrow::Borrow, fmt, ops::Deref, slice, str},
};

#[repr(transparent)]
#[derive(PartialEq, Eq, Clone, Copy, Hash)]
pub struct Ident<'a>(&'a [u8]);
impl<'a> Ident<'a> {
    pub const unsafe fn new(v: &'a [u8]) -> Self {
        Self(v)
    }
    pub const fn new_str(v: &'a str) -> Self {
        Self(v.as_bytes())
    }
    pub fn as_slice(&self) -> &'a [u8] {
        self.0
    }
    pub fn as_str(&self) -> &'a str {
        unsafe {
            // UNSAFE(@ohsayan): it's the ctor
            str::from_utf8_unchecked(self.0)
        }
    }
}
impl<'a> fmt::Debug for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl<'a> Deref for Ident<'a> {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}
impl<'a> PartialEq<[u8]> for Ident<'a> {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}
impl<'a> PartialEq<Ident<'a>> for [u8] {
    fn eq(&self, other: &Ident<'a>) -> bool {
        self == other.as_bytes()
    }
}
impl<'a> PartialEq<str> for Ident<'a> {
    fn eq(&self, other: &str) -> bool {
        self.0 == other.as_bytes()
    }
}
impl<'a> PartialEq<Ident<'a>> for str {
    fn eq(&self, other: &Ident<'a>) -> bool {
        self == other.as_str()
    }
}
impl<'a> From<&'a str> for Ident<'a> {
    fn from(s: &'a str) -> Self {
        Self::new_str(s)
    }
}
impl<'a> AsRef<[u8]> for Ident<'a> {
    fn as_ref(&self) -> &'a [u8] {
        self.0
    }
}
impl<'a> AsRef<str> for Ident<'a> {
    fn as_ref(&self) -> &'a str {
        self.as_str()
    }
}
impl<'a> Default for Ident<'a> {
    fn default() -> Self {
        Self::new_str("")
    }
}
impl<'a> Borrow<[u8]> for Ident<'a> {
    fn borrow(&self) -> &[u8] {
        self.0
    }
}
impl<'a> Borrow<str> for Ident<'a> {
    fn borrow(&self) -> &'a str {
        self.as_str()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Token<'a> {
    Symbol(Symbol),
    Keyword(Keyword),
    Ident(Ident<'a>),
    #[cfg(test)]
    /// A comma that can be ignored (used for fuzzing)
    IgnorableComma,
    Lit(Lit<'a>), // literal
}

impl<'a> ToString for Token<'a> {
    fn to_string(&self) -> String {
        match self {
            Self::Symbol(s) => s.to_string(),
            Self::Keyword(k) => k.to_string(),
            Self::Ident(id) => id.to_string(),
            Self::Lit(l) => l.to_string(),
            #[cfg(test)]
            Self::IgnorableComma => "[IGNORE_COMMA]".to_owned(),
        }
    }
}

impl<'a> PartialEq<Symbol> for Token<'a> {
    fn eq(&self, other: &Symbol) -> bool {
        match self {
            Self::Symbol(s) => s == other,
            _ => false,
        }
    }
}

enum_impls! {
    Token<'a> => {
        Keyword as Keyword,
        Symbol as Symbol,
        Lit<'a> as Lit,
    }
}

#[derive(Debug, PartialEq, Clone)]
#[repr(u8)]
/// A [`Lit`] as represented by an insecure token stream
pub enum Lit<'a> {
    Str(Box<str>),
    Bool(bool),
    UnsignedInt(u64),
    SignedInt(i64),
    Bin(Slice<'a>),
}

impl<'a> ToString for Lit<'a> {
    fn to_string(&self) -> String {
        match self {
            Self::Str(s) => format!("{:?}", s),
            Self::Bool(b) => b.to_string(),
            Self::UnsignedInt(u) => u.to_string(),
            Self::SignedInt(s) => s.to_string(),
            Self::Bin(b) => format!("{:?}", b),
        }
    }
}

impl<'a> Lit<'a> {
    pub fn as_ir(&'a self) -> LitIR<'a> {
        match self {
            Self::Str(s) => LitIR::Str(s.as_ref()),
            Self::Bool(b) => LitIR::Bool(*b),
            Self::UnsignedInt(u) => LitIR::UInt(*u),
            Self::SignedInt(s) => LitIR::SInt(*s),
            Self::Bin(b) => LitIR::Bin(b),
        }
    }
}

impl<'a> From<&'static str> for Lit<'a> {
    fn from(s: &'static str) -> Self {
        Self::Str(s.into())
    }
}

enum_impls! {
    Lit<'a> => {
        Box<str> as Str,
        String as Str,
        bool as Bool,
        u64 as UnsignedInt,
    }
}

build_lut!(
    static KW_LUT in kwlut;
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    pub enum Keyword {
        Table = "table",
        Model = "model",
        Space = "space",
        Index = "index",
        Type = "type",
        Function = "function",
        Use = "use",
        Create = "create",
        Alter = "alter",
        Drop = "drop",
        Describe = "describe",
        Truncate = "truncate",
        Rename = "rename",
        Add = "add",
        Remove = "remove",
        Transform = "transform",
        Order = "order",
        By = "by",
        Primary = "primary",
        Key = "key",
        Value = "value",
        With = "with",
        On = "on",
        Lock = "lock",
        All = "all",
        Insert = "insert",
        Select = "select",
        Exists = "exists",
        Update = "update",
        Delete = "delete",
        Into = "into",
        From = "from",
        As = "as",
        Return = "return",
        Sort = "sort",
        Group = "group",
        Limit = "limit",
        Asc = "asc",
        Desc = "desc",
        To = "to",
        Set = "set",
        Auto = "auto",
        Default = "default",
        In = "in",
        Of = "of",
        Transaction = "transaction",
        Batch = "batch",
        Read = "read",
        Write = "write",
        Begin = "begin",
        End = "end",
        Where = "where",
        If = "if",
        And = "and",
        Or = "or",
        Not = "not",
        Null = "null",
    }
    |b: &str| -> &[u8] { b.as_bytes() },
    |b: &str| -> String { b.to_ascii_uppercase() }
);

build_lut!(
    static SYM_LUT in symlut;
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    #[repr(u8)]
    pub enum Symbol {
        OpArithmeticAdd = b'+',
        OpArithmeticSub = b'-',
        OpArithmeticMul = b'*',
        OpArithmeticDiv = b'/',
        OpLogicalNot = b'!',
        OpLogicalAnd = b'&',
        OpLogicalXor = b'^',
        OpLogicalOr = b'|',
        OpAssign = b'=',
        TtOpenParen = b'(',
        TtCloseParen = b')',
        TtOpenSqBracket = b'[',
        TtCloseSqBracket = b']',
        TtOpenBrace = b'{',
        TtCloseBrace = b'}',
        OpComparatorLt = b'<',
        OpComparatorGt = b'>',
        QuoteS = b'\'',
        QuoteD = b'"',
        SymAt = b'@',
        SymHash = b'#',
        SymDollar = b'$',
        SymPercent = b'%',
        SymUnderscore = b'_',
        SymBackslash = b'\\',
        SymColon = b':',
        SymSemicolon = b';',
        SymComma = b',',
        SymPeriod = b'.',
        SymQuestion = b'?',
        SymTilde = b'~',
        SymAccent = b'`',
    }
    |s: u8| -> u8 { s },
    |c: u8| -> String { char::from(c).to_string() }
);

/*
    This section implements LUTs constructed using DAGs, as described by Czech et al in their paper. I wrote these pretty much by
    brute-force using a byte-level multiplicative function (inside a script). This unfortunately implies that every time we *do*
    need to add a new keyword, I will need to recompute and rewrite the vertices. I don't plan to use any codegen, so I think
    this is good as-is. The real challenge here is to keep the graph small, and I couldn't do that for the symbols table even with
    multiple trials. Please see if you can improve them.

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
pub(super) fn symof(sym: u8) -> Option<Symbol> {
    let hf = symph(sym);
    if hf < SYM_LUT.len() as u8 && SYM_LUT[hf as usize].0 == sym {
        Some(SYM_LUT[hf as usize].1)
    } else {
        None
    }
}

static KWG: [u8; 63] = [
    0, 24, 15, 29, 51, 53, 44, 38, 43, 4, 27, 1, 37, 57, 32, 0, 46, 24, 59, 45, 32, 52, 8, 0, 23,
    19, 33, 48, 56, 60, 33, 53, 18, 47, 49, 53, 2, 19, 1, 34, 19, 58, 11, 5, 0, 41, 27, 24, 20, 2,
    0, 0, 48, 2, 42, 46, 43, 0, 18, 33, 21, 12, 41,
];

const KWMG_1: [u8; 11] = *b"MpVBwC1vsCy";
const KWMG_2: [u8; 11] = *b"m7sNd9mtGzC";
const KWMG_S: usize = KWMG_1.len();

#[inline(always)]
fn kwhf(k: &[u8], mg: &[u8]) -> u32 {
    let mut i = 0;
    let mut s = 0;
    while i < k.len() {
        s += mg[(i % KWMG_S) as usize] as u32 * k[i] as u32;
        i += 1;
    }
    s % KWG.len() as u32
}

#[inline(always)]
fn kwph(k: &[u8]) -> u8 {
    (KWG[kwhf(k, &KWMG_1) as usize] + KWG[kwhf(k, &KWMG_2) as usize]) % KWG.len() as u8
}

#[inline(always)]
pub(super) fn kwof(key: &[u8]) -> Option<Keyword> {
    let ph = kwph(key);
    if ph < KW_LUT.len() as u8 && KW_LUT[ph as usize].0 == key {
        Some(KW_LUT[ph as usize].1)
    } else {
        None
    }
}

impl<'a> Token<'a> {
    #[inline(always)]
    pub(crate) const fn is_ident(&self) -> bool {
        matches!(self, Token::Ident(_))
    }
    #[inline(always)]
    pub const fn is_lit(&self) -> bool {
        matches!(self, Self::Lit(_))
    }
}

impl<'a> AsRef<Token<'a>> for Token<'a> {
    #[inline(always)]
    fn as_ref(&self) -> &Token<'a> {
        self
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
/// Intermediate literal repr
pub enum LitIR<'a> {
    Str(&'a str),
    Bin(Slice<'a>),
    UInt(u64),
    SInt(i64),
    Bool(bool),
    Float(f64),
}

impl<'a> LitIR<'a> {
    pub fn to_litir_owned(&self) -> LitIROwned {
        match self {
            Self::Str(s) => LitIROwned::Str(s.to_string().into_boxed_str()),
            Self::Bin(b) => LitIROwned::Bin(b.to_vec().into_boxed_slice()),
            Self::UInt(u) => LitIROwned::UInt(*u),
            Self::SInt(s) => LitIROwned::SInt(*s),
            Self::Bool(b) => LitIROwned::Bool(*b),
            Self::Float(f) => LitIROwned::Float(*f),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum LitIROwned {
    Str(Box<str>),
    Bin(Box<[u8]>),
    UInt(u64),
    SInt(i64),
    Bool(bool),
    Float(f64),
}

#[derive(Debug)]
pub struct RawLexer<'a> {
    c: *const u8,
    e: *const u8,
    pub(super) tokens: Vec<Token<'a>>,
    pub(super) last_error: Option<LangError>,
}

// ctor
impl<'a> RawLexer<'a> {
    #[inline(always)]
    pub(super) const fn new(src: Slice<'a>) -> Self {
        Self {
            c: src.as_ptr(),
            e: unsafe {
                // UNSAFE(@ohsayan): Always safe (<= EOA)
                src.as_ptr().add(src.len())
            },
            last_error: None,
            tokens: Vec::new(),
        }
    }
}

// meta
impl<'a> RawLexer<'a> {
    #[inline(always)]
    pub(super) const fn cursor(&self) -> *const u8 {
        self.c
    }
    #[inline(always)]
    pub(super) const fn data_end_ptr(&self) -> *const u8 {
        self.e
    }
    #[inline(always)]
    pub(super) fn not_exhausted(&self) -> bool {
        self.data_end_ptr() > self.cursor()
    }
    #[inline(always)]
    pub(super) fn exhausted(&self) -> bool {
        self.cursor() == self.data_end_ptr()
    }
    #[inline(always)]
    pub(super) fn remaining(&self) -> usize {
        unsafe { self.e.offset_from(self.c) as usize }
    }
    #[inline(always)]
    pub(super) unsafe fn deref_cursor(&self) -> u8 {
        *self.cursor()
    }
    #[inline(always)]
    pub(super) unsafe fn incr_cursor_by(&mut self, by: usize) {
        debug_assert!(self.remaining() >= by);
        self.c = self.cursor().add(by)
    }
    #[inline(always)]
    pub(super) unsafe fn incr_cursor(&mut self) {
        self.incr_cursor_by(1)
    }
    #[inline(always)]
    unsafe fn incr_cursor_if(&mut self, iff: bool) {
        self.incr_cursor_by(iff as usize)
    }
    #[inline(always)]
    pub(super) fn push_token(&mut self, token: impl Into<Token<'a>>) {
        self.tokens.push(token.into())
    }
    #[inline(always)]
    pub(super) fn peek_is(&mut self, f: impl FnOnce(u8) -> bool) -> bool {
        self.not_exhausted() && unsafe { f(self.deref_cursor()) }
    }
    #[inline(always)]
    pub(super) fn peek_is_and_forward(&mut self, f: impl FnOnce(u8) -> bool) -> bool {
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
    pub(super) fn peek_neq(&self, b: u8) -> bool {
        self.not_exhausted() && unsafe { self.deref_cursor() != b }
    }
    #[inline(always)]
    pub(super) fn peek_eq_and_forward(&mut self, b: u8) -> bool {
        unsafe {
            let r = self.not_exhausted() && self.deref_cursor() == b;
            self.incr_cursor_if(r);
            r
        }
    }
    #[inline(always)]
    pub(super) fn trim_ahead(&mut self) {
        while self.peek_is_and_forward(|b| b == b' ' || b == b'\t' || b == b'\n') {}
    }
    #[inline(always)]
    pub(super) fn set_error(&mut self, e: LangError) {
        self.last_error = Some(e);
    }
    #[inline(always)]
    pub(super) fn no_error(&self) -> bool {
        self.last_error.is_none()
    }
}

// high level methods
impl<'a> RawLexer<'a> {
    #[inline(always)]
    pub(super) fn scan_ident(&mut self) -> Slice<'a> {
        let s = self.cursor();
        unsafe {
            while self.peek_is(|b| b.is_ascii_alphanumeric() || b == b'_') {
                self.incr_cursor();
            }
            slice::from_raw_parts(s, self.cursor().offset_from(s) as usize)
        }
    }
    #[inline(always)]
    pub(super) fn scan_ident_or_keyword(&mut self) {
        let s = self.scan_ident();
        let st = s.to_ascii_lowercase();
        match kwof(&st) {
            Some(kw) => self.tokens.push(kw.into()),
            // FIXME(@ohsayan): Uh, mind fixing this? The only advantage is that I can keep the graph *memory* footprint small
            None if st == b"true" || st == b"false" => self.push_token(Lit::Bool(st == b"true")),
            None => self.tokens.push(unsafe {
                // UNSAFE(@ohsayan): scan_ident only returns a valid ident which is always a string
                Token::Ident(Ident::new(s))
            }),
        }
    }
    #[inline(always)]
    pub(super) fn scan_byte(&mut self, byte: u8) {
        match symof(byte) {
            Some(tok) => self.push_token(tok),
            None => return self.set_error(LangError::UnexpectedChar),
        }
        unsafe {
            self.incr_cursor();
        }
    }
}
