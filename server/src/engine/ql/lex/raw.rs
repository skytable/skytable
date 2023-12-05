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
    crate::engine::data::lit::Lit,
    core::{borrow::Borrow, fmt, ops::Deref, str},
};

/*
    ident
*/

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
    pub fn boxed_str(&self) -> Box<str> {
        self.as_str().to_string().into_boxed_str()
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

/*
    token
*/

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

impl<'a> Token<'a> {
    pub unsafe fn uck_read_ident(&self) -> Ident<'a> {
        extract!(self, Self::Ident(id) => *id)
    }
    pub unsafe fn uck_read_lit(&self) -> &Lit<'a> {
        extract!(self, Self::Lit(l) => l)
    }
    pub fn ident_eq(&self, ident: &str) -> bool {
        matches!(self, Token::Ident(id) if id.eq_ignore_ascii_case(ident))
    }
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

direct_from! {
    Token<'a> => {
        Keyword as Keyword,
        Symbol as Symbol,
        Lit<'a> as Lit,
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

/*
    symbols
*/

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

impl Symbol {
    pub fn get(k: u8) -> Option<Self> {
        const SYM_MAGIC_A: u8 = b'w';
        const SYM_MAGIC_B: u8 = b'E';
        static G: [u8; 69] = [
            0, 0, 25, 0, 3, 0, 21, 0, 6, 13, 0, 0, 0, 0, 8, 0, 0, 0, 17, 0, 0, 30, 0, 28, 0, 20,
            19, 12, 0, 0, 2, 0, 0, 15, 0, 0, 0, 5, 0, 31, 14, 0, 1, 0, 18, 29, 24, 0, 0, 10, 0, 0,
            26, 0, 0, 0, 22, 0, 23, 7, 0, 27, 0, 4, 16, 11, 0, 0, 9,
        ];
        let symfh = |magic, k| (magic as u16 * k as u16) % G.len() as u16;
        let hf =
            (G[symfh(k, SYM_MAGIC_A) as usize] + G[symfh(k, SYM_MAGIC_B) as usize]) % G.len() as u8;
        if hf < SYM_LUT.len() as u8 && SYM_LUT[hf as usize].0 == k {
            Some(SYM_LUT[hf as usize].1)
        } else {
            None
        }
    }
}

/*
    keywords
*/

macro_rules! flattened_lut {
	(
        $staticvis:vis static $staticname:ident in $staticpriv:ident;
		$(#[$enumattr:meta])*
		$vis:vis enum $enum:ident {
			$($(#[$variant_attr:meta])* $variant:ident => {
                $(#[$nested_enum_attr:meta])*
                $nested_enum_vis:vis enum $nested_enum_name:ident {$($(#[$nested_variant_attr:meta])* $nested_enum_variant_name:ident $(= $nested_enum_variant_dscr:expr)?,)*}
            }),* $(,)?
		}
	) => {
		$(
			$(#[$nested_enum_attr])*
			$nested_enum_vis enum $nested_enum_name {$($(#[$nested_variant_attr])* $nested_enum_variant_name $(= $nested_enum_variant_dscr)*),*}
			impl $nested_enum_name {
                const __LEN: usize = {let mut i = 0; $( let _ = Self::$nested_enum_variant_name; i += 1; )*i};
                const __SL: [usize; 2] = {
                    let mut largest = 0;
                    let mut smallest = usize::MAX;
                    $(
                        let this = stringify!($nested_enum_variant_name).len();
                        if this > largest { largest = this } if this < smallest { smallest = this }
                    )*
                    [smallest, largest]
                };
                const __SMALLEST: usize = Self::__SL[0];
                const __LARGEST: usize = Self::__SL[1];
                const fn __max() -> usize { Self::__LEN }
				pub const fn as_str(&self) -> &'static str {match self {$(
                    Self::$nested_enum_variant_name => {
                        const NAME_STR: &'static str = stringify!($nested_enum_variant_name);
                        const NAME_BUF: [u8; { NAME_STR.len() }] = {
                            let mut buf = [0u8; { NAME_STR.len() }]; let name = NAME_STR.as_bytes();
                            buf[0] = name[0].to_ascii_lowercase(); let mut i = 1;
                            while i < NAME_STR.len() { buf[i] = name[i]; i += 1; }
                            buf
                        }; const NAME: &'static str = unsafe { core::str::from_utf8_unchecked(&NAME_BUF) }; NAME
                    }
				)*}}
			}
            impl ToString for $nested_enum_name { fn to_string(&self) -> String { self.as_str().to_owned() } }
		)*
        $(#[$enumattr])*
        $vis enum $enum {$($(#[$variant_attr])* $variant($nested_enum_name)),*}
        impl $enum { pub const fn as_str(&self) -> &'static str { match self {$(Self::$variant(v) => { $nested_enum_name::as_str(v) })*} } }
        impl $enum {
            const SL: [usize; 2] = {
                let mut largest = 0; let mut smallest = usize::MAX;
                $(
                    if $nested_enum_name::__LARGEST > largest { largest = $nested_enum_name::__LARGEST; }
                    if $nested_enum_name::__SMALLEST < smallest { smallest = $nested_enum_name::__SMALLEST; }
                )*
                [smallest, largest]
            };
            const SIZE_MIN: usize = Self::SL[0];
            const SIZE_MAX: usize = Self::SL[1];
        }
        impl ToString for $enum { fn to_string(&self) -> String { self.as_str().to_owned() } }
        mod $staticpriv { pub const LEN: usize = { let mut i = 0; $(i += super::$nested_enum_name::__max();)* i }; }
        $staticvis static $staticname: [(&'static [u8], $enum); { $staticpriv::LEN }] = [
            $($(($nested_enum_name::$nested_enum_variant_name.as_str().as_bytes() ,$enum::$variant($nested_enum_name::$nested_enum_variant_name)),)*)*
        ];
	}
}

flattened_lut! {
    static KW in kw;
    #[derive(Debug, PartialEq, Clone, Copy)]
    #[repr(u8)]
    pub enum Keyword {
        Statement => {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, sky_macros::EnumMethods)]
            #[repr(u8)]
            /// A statement keyword
            pub enum KeywordStmt {
                // system
                Sysctl = 0,
                // DDL
                Create = 1,
                Alter = 2,
                Drop = 3,
                // system/DDL misc
                Use = 4,
                Inspect = 5,
                Describe = 6,
                // DML
                Insert = 7,
                Select = 8,
                Update = 9,
                Delete = 10,
                Exists = 11,
            }
        },
        /// Hi
        Misc => {
            #[derive(Debug, PartialEq, Clone, Copy)]
            #[repr(u8)]
            /// Misc. keywords
            pub enum KeywordMisc {
                // item definitions
                Table,
                Model,
                Space,
                Index,
                Type,
                Function,
                // operations
                Rename,
                Add,
                Remove,
                Transform,
                Set,
                Return,
                // sort related
                Order,
                Sort,
                Group,
                Limit,
                Asc,
                Desc,
                All,
                // container relational specifier
                By,
                With,
                On,
                From,
                Into,
                As,
                To,
                In,
                Of,
                // logical
                And,
                Or,
                Not,
                // conditional
                If,
                Else,
                Where,
                When,
                Allow,
                // value
                Auto,
                Default,
                Null,
                // transaction related
                Transaction,
                Batch,
                Lock,
                Read,
                Write,
                Begin,
                End,
                // misc
                Key,
                Value,
                Primary,
                // temporarily reserved (will probably be removed in the future)
                Truncate, // TODO: decide what we want to do with this
            }
        }
    }
}

impl Keyword {
    #[inline(always)]
    pub fn get(k: &[u8]) -> Option<Self> {
        if (k.len() > Self::SIZE_MAX) | (k.len() < Self::SIZE_MIN) {
            None
        } else {
            Self::compute(k)
        }
    }
    fn compute(key: &[u8]) -> Option<Self> {
        static G: [u8; 69] = [
            0, 0, 9, 64, 16, 43, 7, 49, 24, 8, 41, 37, 19, 66, 18, 0, 17, 0, 12, 63, 34, 56, 3, 24,
            55, 14, 0, 67, 7, 0, 39, 60, 56, 0, 51, 23, 31, 19, 30, 12, 10, 58, 20, 39, 32, 0, 6,
            30, 26, 58, 52, 62, 39, 27, 24, 9, 4, 21, 24, 68, 10, 38, 40, 21, 62, 27, 53, 27, 44,
        ];
        static M1: [u8; 11] = *b"D8N5FwqrxdA";
        static M2: [u8; 11] = *b"FsIPJv9hsXx";
        let h1 = Self::_sum(key, M1) % G.len();
        let h2 = Self::_sum(key, M2) % G.len();
        let h = (G[h1] + G[h2]) as usize % G.len();
        if h < KW.len() && KW[h].0.eq_ignore_ascii_case(key) {
            Some(KW[h].1)
        } else {
            None
        }
    }
    #[inline(always)]
    fn _sum(key: &[u8], block: [u8; 11]) -> usize {
        let mut sum = 0;
        let mut i = 0;
        while i < key.len() {
            let char = block[i % 11];
            sum += char as usize * (key[i] | 0x20) as usize;
            i += 1;
        }
        sum
    }
}

impl KeywordStmt {
    pub const fn is_blocking(&self) -> bool {
        self.value_u8() <= Self::Drop.value_u8()
    }
}
