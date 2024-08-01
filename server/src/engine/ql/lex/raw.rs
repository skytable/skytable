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
    crate::engine::data::{cell::Datacell, lit::Lit},
    core::{borrow::Borrow, fmt, ops::Deref, str},
    std::cell::UnsafeCell,
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

#[derive(Debug)]
pub enum Token<'a> {
    Symbol(Symbol),
    Keyword(Keyword),
    Ident(Ident<'a>),
    #[cfg(test)]
    /// A comma that can be ignored (used for fuzzing)
    IgnorableComma,
    Lit(Lit<'a>), // literal
    DCList(UnsafeCell<Vec<Datacell>>),
}

impl<'a> PartialEq for Token<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Symbol(l0), Self::Symbol(r0)) => l0 == r0,
            (Self::Keyword(l0), Self::Keyword(r0)) => l0 == r0,
            (Self::Ident(l0), Self::Ident(r0)) => l0 == r0,
            (Self::Lit(l0), Self::Lit(r0)) => l0 == r0,
            (Self::DCList(l0), Self::DCList(r0)) => unsafe {
                // UNSAFE(@ohsayan): as a rule, no one ever leaves a dangling reference here
                l0.get().as_ref().unwrap() == r0.get().as_ref().unwrap()
            },
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

unsafe impl<'a> Send for Token<'a> {}
unsafe impl<'a> Sync for Token<'a> {}

#[cfg(test)]
impl<'a> Clone for Token<'a> {
    fn clone(&self) -> Self {
        match self {
            Self::Symbol(arg0) => Self::Symbol(arg0.clone()),
            Self::Keyword(arg0) => Self::Keyword(arg0.clone()),
            Self::Ident(arg0) => Self::Ident(arg0.clone()),
            Self::IgnorableComma => Self::IgnorableComma,
            Self::Lit(arg0) => Self::Lit(arg0.clone()),
            Self::DCList(arg0) => {
                Self::DCList(UnsafeCell::new(
                    unsafe {
                        // UNSAFE(@ohsayan): it's easy to see that we have an actual valid
                        arg0.get().as_ref()
                    }
                    .unwrap()
                    .clone(),
                ))
            }
        }
    }
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
    pub fn dc_list(dc_l: Vec<Datacell>) -> Self {
        Self::DCList(UnsafeCell::new(dc_l))
    }
    pub unsafe fn take_list(dcl: &UnsafeCell<Vec<Datacell>>) -> Vec<Datacell> {
        unsafe {
            /*
                UNSAFE(@ohsayan): nasty nasty stuff here because technically nothing here is guaranteeing that no
                two threads will be doing this in parallel. But the important bit is that two thrads are NEVER used
                to decode one token stream so doing crazy things to just enforce statical guarantee for a property
                we already know is guaranteed is inherently pointless. Hence, what we do is: swap the pointers!

                TODO(@ohsayan): BUT I MUST EMPHASIZE FOR GOODNESS SAKE IS THAT THIS IS JUST VERY AWFUL. IT'S PLAIN BUTCHERY
                OF BORROWCK'S RULES AND WE *MUST* DO SOMETHING TO `ast::State` to make this semantically better.

                But the way `State` works in a way does implicitly guarantee that this isn't easily breakable, but yes
                transmuting lifetimes are the way to completely break this.

                SCARY STUFF!
            */
            core::mem::take(&mut *dcl.get())
        }
    }
}

impl<'a> ToString for Token<'a> {
    fn to_string(&self) -> String {
        match self {
            Self::Symbol(s) => s.to_string(),
            Self::Keyword(k) => k.to_string(),
            Self::Ident(id) => id.to_string(),
            Self::Lit(l) => l.to_string(),
            Self::DCList(dc_lst) => {
                format!("{dc_lst:?}")
            }
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
                $nested_enum_vis:vis enum $nested_enum_name:ident {$($(#[$nested_variant_attr:meta])* $nested_enum_variant_name:ident $(: $($alternative_name:ident)|*)? $(= $nested_enum_variant_dscr:expr)?,)*}
            }),* $(,)?
		}
	) => {
		$(
			$(#[$nested_enum_attr])*
			$nested_enum_vis enum $nested_enum_name {$($(#[$nested_variant_attr])* $nested_enum_variant_name $(= $nested_enum_variant_dscr)*),*}
			impl $nested_enum_name {
                const __LEN: usize = {let mut i = 0; $( i += {let l = [$($(stringify!($alternative_name),)*)? stringify!($nested_enum_variant_name)].len(); if l == 1 { 1 } else { l - 1 }}; )*i};
                const __SL: [usize; 2] = {
                    let mut largest = 0; let mut smallest = usize::MAX;
                    $(
                        let alt_kw = [stringify!($nested_enum_variant_name), $($(stringify!($alternative_name),)*)?];
                        let mut alt_kw_i = 0;
                        while alt_kw_i < alt_kw.len() {
                            let this = alt_kw[alt_kw_i].len(); if this > largest { largest = this } if this < smallest { smallest = this } alt_kw_i += 1;
                        }
                    )*
                    [smallest, largest]
                };
                const __SMALLEST: usize = Self::__SL[0]; const __LARGEST: usize = Self::__SL[1];
                const fn __max() -> usize { Self::__LEN }
				pub const fn as_str(&self) -> &'static str {match self {$(
                    Self::$nested_enum_variant_name => {
                        const NAME_STR: &'static str = stringify!($nested_enum_variant_name);
                        const NAME_BUF: [u8; NAME_STR.len()] = {
                            let mut buf = [0u8; NAME_STR.len()]; let name = NAME_STR.as_bytes();
                            buf[0] = name[0].to_ascii_lowercase(); let mut i = 1;
                            while i < NAME_STR.len() { buf[i] = name[i]; i += 1; } buf
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
            const SIZE_MIN: usize = Self::SL[0]; const SIZE_MAX: usize = Self::SL[1];
        }
        impl ToString for $enum { fn to_string(&self) -> String { self.as_str().to_owned() } }
        mod $staticpriv { pub const LEN: usize = { let mut i = 0; $(i += super::$nested_enum_name::__max();)* i }; }
        $staticvis static $staticname: [(&'static [u8], $enum); { $staticpriv::LEN }] = {
            let mut ret = [(b"".as_slice(), Keyword::Misc(KeywordMisc::Auto)); { $staticpriv::LEN }];
            let mut i = 0;
            $($(
                let alt_kw = [stringify!($nested_enum_variant_name).as_bytes(), $($(stringify!($alternative_name).as_bytes(),)*)?];
                let mut j = 0; let k = if alt_kw.len() == 1 { 1 } else { alt_kw.len() - 1 };
                while j < k { ret[i] = (alt_kw[j] ,$enum::$variant($nested_enum_name::$nested_enum_variant_name)); i += 1; j += 1; }
            )*)*ret
        };
	}
}

macro_rules! hibit {
    ($e:expr) => {
        $e | (1 << (<u8>::BITS - 1))
    };
}

flattened_lut! {
    static KW in kw;
    #[derive(Debug, PartialEq, Clone, Copy)]
    #[repr(u8)]
    pub enum Keyword {
        Statement => {
            #[derive(
                Debug,
                PartialEq,
                Eq,
                PartialOrd,
                Ord,
                Clone,
                Copy,
                sky_macros::EnumMethods,
                sky_macros::TaggedEnum,
            )]
            #[repr(u8)]
            /// A statement keyword
            pub enum KeywordStmt {
                // blocking
                // system
                Sysctl = hibit!(0),
                // DDL
                Create = hibit!(1),
                Alter = hibit!(2),
                Drop = hibit!(3),
                // dml
                Truncate = hibit!(4),
                // nonblocking
                // system/DDL misc
                Use = 4,
                Inspect = 5,
                Describe = 6,
                // DML
                Insert: Ins | Insert = 7,
                Select: Sel | Select = 8,
                Update: Upd | Update  = 9,
                Delete: Del | Delete = 10,
                Upsert: Ups | Upsert = 11,
                Exists = 12,
            }
        },
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
        static G: [u8; 78] = [
            0, 0, 0, 23, 43, 74, 0, 50, 11, 26, 2, 0, 59, 4, 37, 24, 29, 33, 65, 50, 67, 53, 23,
            60, 7, 2, 52, 30, 46, 18, 28, 77, 76, 0, 36, 68, 61, 76, 28, 9, 67, 13, 69, 13, 15, 18,
            0, 36, 24, 31, 29, 63, 58, 10, 30, 55, 74, 0, 17, 40, 0, 63, 1, 20, 64, 39, 61, 25, 0,
            3, 17, 11, 67, 64, 23, 32, 15, 63,
        ];
        static M1: [u8; 11] = *b"XASUQtx6XDe";
        static M2: [u8; 11] = *b"xYcBjx55y9b";
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
    fn _sum<const N: usize>(key: &[u8], block: [u8; N]) -> usize {
        let mut sum = 0;
        let mut i = 0;
        while i < key.len() {
            let char = block[i % N];
            sum += char as usize * (key[i] | 0x20) as usize;
            i += 1;
        }
        sum
    }
}

impl KeywordStmt {
    pub const fn is_blocking(&self) -> bool {
        self.value_u8() & 0x80 != 0
    }
    pub const NONBLOCKING_COUNT: usize = Self::BLK_NBLK.1;
    const BLK_NBLK: (usize, usize) = {
        let mut i = 0usize;
        let mut nb = 0;
        let mut blk = 0;
        while i < Self::VARIANTS.len() {
            match Self::VARIANTS[i] {
                KeywordStmt::Create
                | KeywordStmt::Alter
                | KeywordStmt::Drop
                | KeywordStmt::Sysctl
                | KeywordStmt::Truncate => blk += 1,
                KeywordStmt::Use
                | KeywordStmt::Inspect
                | KeywordStmt::Describe
                | KeywordStmt::Insert
                | KeywordStmt::Select
                | KeywordStmt::Update
                | KeywordStmt::Delete
                | KeywordStmt::Exists
                | KeywordStmt::Upsert => nb += 1,
            }
            i += 1;
        }
        (blk, nb)
    };
    pub fn raw_code(&self) -> u8 {
        self.value_u8() & !0x80
    }
}

#[sky_macros::test]
fn blocking_capybara() {
    assert!(KeywordStmt::Truncate.is_blocking());
    assert!(!KeywordStmt::Insert.is_blocking());
}
