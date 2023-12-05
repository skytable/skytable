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

pub mod traits;

#[cfg(test)]
pub use traits::{
    parse_ast_node_full, parse_ast_node_full_with_space, parse_ast_node_multiple_full,
};

use {
    super::lex::{Keyword, KeywordStmt, Token},
    crate::{
        engine::{
            core::EntityIDRef,
            data::{cell::Datacell, lit::Lit},
            error::{QueryError, QueryResult},
        },
        util::{compiler, MaybeInit},
    },
};

#[derive(Debug, PartialEq)]
/// Query parse state
pub struct State<'a, Qd> {
    t: &'a [Token<'a>],
    d: Qd,
    i: usize,
    f: bool,
    cs: Option<&'static str>,
}

impl<'a> State<'a, InplaceData> {
    pub const fn new_inplace(tok: &'a [Token<'a>]) -> Self {
        Self::new(tok, InplaceData::new())
    }
}

impl<'a, Qd: QueryData<'a>> State<'a, Qd> {
    fn _entity_signature_match_self_full(a: &Token<'a>, b: &Token<'a>, c: &Token<'a>) -> bool {
        a.is_ident() & Token![.].eq(b) & c.is_ident()
    }
    fn _entity_signature_match_cs(&self, a: &Token<'a>) -> bool {
        a.is_ident() & self.cs.is_some()
    }
    unsafe fn _entity_new_from_tokens(&mut self) -> EntityIDRef<'a> {
        let space = self.fw_read().uck_read_ident();
        self.cursor_ahead();
        let entity = self.fw_read().uck_read_ident();
        EntityIDRef::new(space.as_str(), entity.as_str())
    }
    unsafe fn _entity_new_from_cs(&mut self) -> EntityIDRef<'a> {
        let entity = self.fw_read().uck_read_ident();
        EntityIDRef::new(self.cs.unwrap_unchecked(), entity.as_str())
    }
    pub fn set_space_maybe(&mut self, maybe: Option<&'static str>) {
        self.cs = maybe;
    }
    pub fn unset_space(&mut self) {
        self.set_space_maybe(None)
    }
    #[cfg(test)]
    pub fn set_space(&mut self, s: &'static str) {
        self.set_space_maybe(Some(s));
    }
    pub fn try_entity_buffered_into_state_uninit(&mut self) -> MaybeInit<EntityIDRef<'a>> {
        let mut ret = MaybeInit::uninit();
        let self_has_full = Self::_entity_signature_match_self_full(
            &self.t[self.cursor()],
            &self.t[self.cursor() + 1],
            &self.t[self.cursor() + 2],
        );
        let self_has_full_cs = self._entity_signature_match_cs(&self.t[self.cursor()]);
        unsafe {
            if self_has_full {
                ret = MaybeInit::new(self._entity_new_from_tokens());
            } else if self_has_full_cs {
                ret = MaybeInit::new(self._entity_new_from_cs());
            }
        }
        self.poison_if_not(self_has_full | self_has_full_cs);
        ret
    }
    pub fn try_entity_ref(&mut self) -> Option<EntityIDRef<'a>> {
        let self_has_full = Self::_entity_signature_match_self_full(
            self.offset_current_r(0),
            self.offset_current_r(1),
            self.offset_current_r(2),
        );
        let self_has_pre_full = self._entity_signature_match_cs(self.offset_current_r(0));
        if self_has_full {
            unsafe {
                // UNSAFE(@ohsayan): +branch condition
                Some(self._entity_new_from_tokens())
            }
        } else {
            if self_has_pre_full {
                unsafe {
                    // UNSAFE(@ohsayan): +branch condition
                    Some(self._entity_new_from_cs())
                }
            } else {
                None
            }
        }
    }
    pub fn try_entity_ref_result(&mut self) -> QueryResult<EntityIDRef<'a>> {
        self.try_entity_ref().ok_or(QueryError::QLExpectedEntity)
    }
}

impl<'a, Qd: QueryData<'a>> State<'a, Qd> {
    #[inline(always)]
    /// Create a new [`State`] instance using the given tokens and data
    pub const fn new(t: &'a [Token<'a>], d: Qd) -> Self {
        Self {
            i: 0,
            f: true,
            t,
            d,
            cs: None,
        }
    }
    #[inline(always)]
    /// Returns `true` if the state is okay
    pub const fn okay(&self) -> bool {
        self.f
    }
    #[inline(always)]
    /// Poison the state flag
    pub fn poison(&mut self) {
        self.f = false;
    }
    #[inline(always)]
    /// Poison the state flag if the expression is satisfied
    pub fn poison_if(&mut self, fuse: bool) {
        self.f &= !fuse;
    }
    #[inline(always)]
    /// Poison the state flag if the expression is not satisfied
    pub fn poison_if_not(&mut self, fuse: bool) {
        self.poison_if(!fuse);
    }
    #[inline(always)]
    /// Move the cursor ahead by 1
    pub fn cursor_ahead(&mut self) {
        self.cursor_ahead_by(1)
    }
    #[inline(always)]
    /// Move the cursor ahead by the given count
    pub fn cursor_ahead_by(&mut self, by: usize) {
        self.i += by;
    }
    #[inline(always)]
    /// Move the cursor ahead by 1 if the expression is satisfied
    pub fn cursor_ahead_if(&mut self, iff: bool) {
        self.cursor_ahead_by(iff as _);
    }
    #[inline(always)]
    /// Read the cursor
    pub fn read(&self) -> &'a Token<'a> {
        &self.t[self.i]
    }
    #[inline(always)]
    /// Return a subslice of the tokens using the current state
    pub fn current(&self) -> &'a [Token<'a>] {
        &self.t[self.i..]
    }
    #[inline(always)]
    pub fn offset_current_r(&self, offset: usize) -> &Token<'a> {
        &self.t[self.round_cursor_up(offset)]
    }
    #[inline(always)]
    /// Returns a count of the number of consumable tokens remaining
    pub fn remaining(&self) -> usize {
        self.t.len() - self.i
    }
    #[inline(always)]
    /// Read and forward the cursor
    pub fn fw_read(&mut self) -> &'a Token<'a> {
        let r = self.read();
        self.cursor_ahead();
        r
    }
    #[inline(always)]
    /// Check if the token stream has alteast `many` count of tokens
    pub fn has_remaining(&self, many: usize) -> bool {
        self.remaining() >= many
    }
    #[inline(always)]
    /// Returns true if the token stream has been exhausted
    pub fn exhausted(&self) -> bool {
        self.remaining() == 0
    }
    #[inline(always)]
    /// Returns true if the token stream has **not** been exhausted
    pub fn not_exhausted(&self) -> bool {
        self.remaining() != 0
    }
    #[inline(always)]
    /// Check if the current cursor can read a lit (with context from the data source); rounded
    pub fn can_read_lit_rounded(&self) -> bool {
        let mx = self.round_cursor();
        Qd::can_read_lit_from(&self.d, &self.t[mx]) & (mx == self.i)
    }
    #[inline(always)]
    /// Check if a lit can be read using the given token with context from the data source
    pub fn can_read_lit_from(&self, tok: &'a Token<'a>) -> bool {
        Qd::can_read_lit_from(&self.d, tok)
    }
    #[inline(always)]
    /// Read a lit from the cursor and data source
    ///
    /// ## Safety
    /// - Must ensure that `Self::can_read_lit_rounded` is true
    pub unsafe fn read_cursor_lit_unchecked(&mut self) -> Lit<'a> {
        let tok = self.read();
        Qd::read_lit(&mut self.d, tok)
    }
    #[inline(always)]
    /// Read a lit from the given token
    ///
    /// ## Safety
    /// - Must ensure that `Self::can_read_lit_from` is true for the token
    pub unsafe fn read_lit_unchecked_from(&mut self, tok: &'a Token<'a>) -> Lit<'a> {
        Qd::read_lit(&mut self.d, tok)
    }
    #[inline(always)]
    /// Check if the cursor equals the given token; rounded
    pub fn cursor_rounded_eq(&self, tok: Token<'a>) -> bool {
        let mx = self.round_cursor();
        (self.t[mx] == tok) & (mx == self.i)
    }
    #[inline(always)]
    /// Check if the cursor equals the given token
    pub(crate) fn cursor_eq(&self, token: Token) -> bool {
        self.t[self.i] == token
    }
    #[inline(always)]
    /// Move the cursor back by 1
    pub(crate) fn cursor_back(&mut self) {
        self.cursor_back_by(1);
    }
    #[inline(always)]
    /// Move the cursor back by the given count
    pub(crate) fn cursor_back_by(&mut self, by: usize) {
        self.i -= by;
    }
    #[inline(always)]
    pub(crate) fn cursor_has_ident_rounded(&self) -> bool {
        self.offset_current_r(0).is_ident() & self.not_exhausted()
    }
    #[inline(always)]
    /// Check if the current token stream matches the signature of an arity(0) fn; rounded
    ///
    /// NOTE: Consider using a direct comparison without rounding
    pub(crate) fn cursor_signature_match_fn_arity0_rounded(&self) -> bool {
        (self.offset_current_r(0).is_ident())
            & (Token![() open].eq(self.offset_current_r(1)))
            & (Token![() close].eq(self.offset_current_r(2)))
            & self.has_remaining(3)
    }
    #[inline(always)]
    /// Reads a lit using the given token and the internal data source and return a data type
    ///
    /// ## Safety
    ///
    /// Caller should have checked that the token matches a lit signature and that enough data is available
    /// in the data source. (ideally should run `can_read_lit_from` or `can_read_lit_rounded`)
    pub unsafe fn read_lit_into_data_type_unchecked_from(&mut self, tok: &'a Token) -> Datacell {
        self.d.read_data_type(tok)
    }
    #[inline(always)]
    /// Loop condition for tt and non-poisoned state only
    pub fn loop_tt(&self) -> bool {
        self.not_exhausted() & self.okay()
    }
    #[inline(always)]
    /// Returns the position of the cursor
    pub(crate) fn cursor(&self) -> usize {
        self.i
    }
    #[inline(always)]
    /// Returns true if the cursor is an ident
    pub(crate) fn cursor_is_ident(&self) -> bool {
        self.read().is_ident()
    }
    #[inline(always)]
    fn round_cursor_up(&self, up: usize) -> usize {
        core::cmp::min(self.t.len() - 1, self.i + up)
    }
    #[inline(always)]
    fn round_cursor(&self) -> usize {
        self.round_cursor_up(0)
    }
    pub fn try_statement(&mut self) -> QueryResult<KeywordStmt> {
        if compiler::unlikely(self.exhausted()) {
            compiler::cold_call(|| Err(QueryError::QLExpectedStatement))
        } else {
            match self.fw_read() {
                Token::Keyword(Keyword::Statement(stmt)) => Ok(*stmt),
                _ => Err(QueryError::QLExpectedStatement),
            }
        }
    }
    pub fn ensure_minimum_for_blocking_stmt(&self) -> QueryResult<()> {
        if self.remaining() < 2 {
            return Err(QueryError::QLExpectedStatement);
        } else {
            Ok(())
        }
    }
}

pub trait QueryData<'a> {
    /// Check if the given token is a lit, while also checking `self`'s data if necessary
    fn can_read_lit_from(&self, tok: &Token) -> bool;
    /// Read a lit using the given token, using `self`'s data as necessary
    ///
    /// ## Safety
    /// The current token **must match** the signature of a lit
    unsafe fn read_lit(&mut self, tok: &'a Token) -> Lit<'a>;
    /// Read a lit using the given token and then copy it into a [`DataType`]
    ///
    /// ## Safety
    /// The current token must match the signature of a lit
    unsafe fn read_data_type(&mut self, tok: &'a Token) -> Datacell;
    /// Returns true if the data source has enough data
    fn nonzero(&self) -> bool;
}

#[derive(Debug)]
pub struct InplaceData;
impl InplaceData {
    #[inline(always)]
    pub const fn new() -> Self {
        Self
    }
}

impl<'a> QueryData<'a> for InplaceData {
    #[inline(always)]
    fn can_read_lit_from(&self, tok: &Token) -> bool {
        tok.is_lit()
    }
    #[inline(always)]
    unsafe fn read_lit(&mut self, tok: &'a Token) -> Lit<'a> {
        tok.uck_read_lit().as_ir()
    }
    #[inline(always)]
    unsafe fn read_data_type(&mut self, tok: &'a Token) -> Datacell {
        Datacell::from(<Self as QueryData>::read_lit(self, tok))
    }
    #[inline(always)]
    fn nonzero(&self) -> bool {
        true
    }
}
