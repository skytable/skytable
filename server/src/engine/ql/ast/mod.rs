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
pub use traits::{parse_ast_node_full, parse_ast_node_multiple_full};
use {
    super::{
        ddl, dml,
        lex::{Ident, Token},
    },
    crate::{
        engine::{
            data::{cell::Datacell, lit::Lit},
            error::{QueryError, QueryResult},
        },
        util::{compiler, MaybeInit},
    },
};

#[inline(always)]
pub fn minidx<T>(src: &[T], index: usize) -> usize {
    (src.len() - 1).min(index)
}

#[derive(Debug, PartialEq)]
/// Query parse state
pub struct State<'a, Qd> {
    t: &'a [Token<'a>],
    d: Qd,
    i: usize,
    f: bool,
}

impl<'a> State<'a, InplaceData> {
    pub const fn new_inplace(tok: &'a [Token<'a>]) -> Self {
        Self::new(tok, InplaceData::new())
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
        let mx = minidx(self.t, self.i);
        Qd::can_read_lit_from(&self.d, &self.t[mx]) && mx == self.i
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
        let mx = minidx(self.t, self.i);
        self.t[mx] == tok && mx == self.i
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
        self.t[minidx(self.t, self.i)].is_ident() && self.not_exhausted()
    }
    #[inline(always)]
    /// Check if the current token stream matches the signature of an arity(0) fn; rounded
    ///
    /// NOTE: Consider using a direct comparison without rounding
    pub(crate) fn cursor_signature_match_fn_arity0_rounded(&self) -> bool {
        let rem = self.has_remaining(3);
        let idx_a = self.i * rem as usize;
        let idx_b = (self.i + 1) * rem as usize;
        let idx_c = (self.i + 2) * rem as usize;
        (self.t[idx_a].is_ident())
            & (self.t[idx_b] == Token![() open])
            & (self.t[idx_c] == Token![() close])
            & rem
    }
    #[inline(always)]
    /// Check if the current token stream matches the signature of a full entity; rounded
    ///
    /// NOTE: Consider using a direct comparison without rounding; rounding is always slower
    pub(crate) fn cursor_signature_match_entity_full_rounded(&self) -> bool {
        let rem = self.has_remaining(3);
        let rem_u = rem as usize;
        let idx_a = self.i * rem_u;
        let idx_b = (self.i + 1) * rem_u;
        let idx_c = (self.i + 2) * rem_u;
        (self.t[idx_a].is_ident()) & (self.t[idx_b] == Token![.]) & (self.t[idx_c].is_ident()) & rem
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
        self.not_exhausted() && self.okay()
    }
    #[inline(always)]
    #[cfg(test)]
    /// Returns the position of the cursor
    pub(crate) fn cursor(&self) -> usize {
        self.i
    }
    #[inline(always)]
    /// Returns true if the cursor is an ident
    pub(crate) fn cursor_is_ident(&self) -> bool {
        self.read().is_ident()
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

/*
    AST
*/

#[derive(Debug, PartialEq, Clone, Copy)]
/// An [`Entity`] represents the location for a specific structure, such as a model
pub enum Entity<'a> {
    /// A single entity is used when switching to a model wrt the currently set space (commonly used
    /// when running DML queries)
    ///
    /// syntax:
    /// ```sql
    /// model
    /// ```
    Single(Ident<'a>),
    /// A full entity is a complete definition to a model wrt to the given space (commonly used with
    /// DML queries)
    ///
    /// syntax:
    /// ```sql
    /// space.model
    /// ```
    Full(Ident<'a>, Ident<'a>),
}

impl<'a> Entity<'a> {
    pub fn into_full_result(self) -> QueryResult<(Ident<'a>, Ident<'a>)> {
        match self {
            Self::Full(a, b) => Ok((a, b)),
            _ => Err(QueryError::QLExpectedEntity),
        }
    }
}

impl<'a> From<(&'a str, &'a str)> for Entity<'a> {
    fn from((s, e): (&'a str, &'a str)) -> Self {
        Self::Full(s.into(), e.into())
    }
}

impl<'a> Entity<'a> {
    #[cfg(test)]
    pub fn into_full(self) -> Option<(Ident<'a>, Ident<'a>)> {
        if let Self::Full(a, b) = self {
            Some((a, b))
        } else {
            None
        }
    }
    #[inline(always)]
    /// Parse a full entity from the given slice
    ///
    /// ## Safety
    ///
    /// Caller guarantees that the token stream matches the exact stream of tokens
    /// expected for a full entity
    pub(super) unsafe fn parse_uck_tokens_full(sl: &'a [Token]) -> Self {
        Entity::Full(sl[0].uck_read_ident(), sl[2].uck_read_ident())
    }
    #[inline(always)]
    /// Parse a single entity from the given slice
    ///
    /// ## Safety
    ///
    /// Caller guarantees that the token stream matches the exact stream of tokens
    /// expected for a single entity
    pub(super) unsafe fn parse_uck_tokens_single(sl: &'a [Token]) -> Self {
        Entity::Single(sl[0].uck_read_ident())
    }
    #[inline(always)]
    #[cfg(test)]
    /// Returns true if the given token stream matches the signature of single entity syntax
    ///
    /// ⚠ WARNING: This will pass for full and single
    pub(super) fn signature_matches_single_len_checked(tok: &[Token]) -> bool {
        !tok.is_empty() && tok[0].is_ident()
    }
    #[inline(always)]
    #[cfg(test)]
    /// Returns true if the given token stream matches the signature of full entity syntax
    pub(super) fn signature_matches_full_len_checked(tok: &[Token]) -> bool {
        tok.len() > 2 && tok[0].is_ident() && tok[1] == Token![.] && tok[2].is_ident()
    }
    #[inline(always)]
    #[cfg(test)]
    /// Attempt to parse an entity using the given token stream. It also accepts a counter
    /// argument to forward the cursor
    pub fn parse_from_tokens_len_checked(tok: &'a [Token], c: &mut usize) -> QueryResult<Self> {
        let is_current = Self::signature_matches_single_len_checked(tok);
        let is_full = Self::signature_matches_full_len_checked(tok);
        let r = match () {
            _ if is_full => unsafe {
                // UNSAFE(@ohsayan): just verified signature
                *c += 3;
                Self::parse_uck_tokens_full(tok)
            },
            _ if is_current => unsafe {
                // UNSAFE(@ohsayan): just verified signature
                *c += 1;
                Self::parse_uck_tokens_single(tok)
            },
            _ => return Err(QueryError::QLExpectedEntity),
        };
        Ok(r)
    }
    #[inline(always)]
    pub fn parse_from_state_rounded_result<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
    ) -> QueryResult<Self> {
        let mut e = MaybeInit::uninit();
        Self::parse_from_state_rounded(state, &mut e);
        if compiler::likely(state.okay()) {
            unsafe {
                // UNSAFE(@ohsayan): just checked if okay
                Ok(e.assume_init())
            }
        } else {
            Err(QueryError::QLExpectedEntity)
        }
    }
    #[inline(always)]
    pub fn parse_from_state_rounded<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
        d: &mut MaybeInit<Entity<'a>>,
    ) {
        let tok = state.current();
        let is_full = state.cursor_signature_match_entity_full_rounded();
        let is_single = state.cursor_has_ident_rounded();
        unsafe {
            // UNSAFE(@ohsayan): verified signatures
            if is_full {
                state.cursor_ahead_by(3);
                *d = MaybeInit::new(Entity::parse_uck_tokens_full(tok));
            } else if is_single {
                state.cursor_ahead();
                *d = MaybeInit::new(Entity::parse_uck_tokens_single(tok));
            }
        }
        state.poison_if_not(is_full | is_single);
    }
    pub fn parse_from_state_len_unchecked<Qd: QueryData<'a>>(
        state: &mut State<'a, Qd>,
        d: &mut MaybeInit<Entity<'a>>,
    ) {
        let tok = state.current();
        let is_full = tok[0].is_ident() && tok[1] == Token![.] && tok[2].is_ident();
        let is_single = tok[0].is_ident();
        unsafe {
            // UNSAFE(@ohsayan): verified signatures
            if is_full {
                state.cursor_ahead_by(3);
                *d = MaybeInit::new(Entity::parse_uck_tokens_full(tok));
            } else if is_single {
                state.cursor_ahead();
                *d = MaybeInit::new(Entity::parse_uck_tokens_single(tok));
            }
        }
        state.poison_if_not(is_full | is_single);
    }
}

#[derive(Debug, PartialEq)]
#[allow(dead_code)] // TODO(@ohsayan): get rid of this
/// A [`Statement`] is a fully BlueQL statement that can be executed by the query engine
// TODO(@ohsayan): Determine whether we actually need this
pub enum Statement<'a> {
    /// DDL query to switch between spaces and models
    Use(Entity<'a>),
    /// DDL query to create a model
    CreateModel(ddl::crt::CreateModel<'a>),
    /// DDL query to create a space
    CreateSpace(ddl::crt::CreateSpace<'a>),
    /// DDL query to alter a space (properties)
    AlterSpace(ddl::alt::AlterSpace<'a>),
    /// DDL query to alter a model (properties, field types, etc)
    AlterModel(ddl::alt::AlterModel<'a>),
    /// DDL query to drop a model
    ///
    /// Conditions:
    /// - Model view is empty
    /// - Model is not in active use
    DropModel(ddl::drop::DropModel<'a>),
    /// DDL query to drop a space
    ///
    /// Conditions:
    /// - Space doesn't have any other structures
    /// - Space is not in active use
    DropSpace(ddl::drop::DropSpace<'a>),
    /// DDL query to inspect a space (returns a list of models in the space)
    InspectSpace(Ident<'a>),
    /// DDL query to inspect a model (returns the model definition)
    InspectModel(Entity<'a>),
    /// DDL query to inspect all spaces (returns a list of spaces in the database)
    InspectSpaces,
    /// DML insert
    Insert(dml::ins::InsertStatement<'a>),
    /// DML select
    Select(dml::sel::SelectStatement<'a>),
    /// DML update
    Update(dml::upd::UpdateStatement<'a>),
    /// DML delete
    Delete(dml::del::DeleteStatement<'a>),
}

#[inline(always)]
#[cfg(test)]
#[allow(dead_code)] // TODO(@ohsayan): get rid of this
pub fn compile<'a, Qd: QueryData<'a>>(tok: &'a [Token<'a>], d: Qd) -> QueryResult<Statement<'a>> {
    use self::traits::ASTNode;
    if compiler::unlikely(tok.len() < 2) {
        return Err(QueryError::QLUnexpectedEndOfStatement);
    }
    let mut state = State::new(tok, d);
    match state.fw_read() {
        // DDL
        Token![use] => Entity::parse_from_state_rounded_result(&mut state).map(Statement::Use),
        Token![create] => match state.fw_read() {
            Token![model] => ASTNode::test_parse_from_state(&mut state).map(Statement::CreateModel),
            Token![space] => ASTNode::test_parse_from_state(&mut state).map(Statement::CreateSpace),
            _ => compiler::cold_rerr(QueryError::QLUnknownStatement),
        },
        Token![alter] => match state.fw_read() {
            Token![model] => ASTNode::test_parse_from_state(&mut state).map(Statement::AlterModel),
            Token![space] => ASTNode::test_parse_from_state(&mut state).map(Statement::AlterSpace),
            _ => compiler::cold_rerr(QueryError::QLUnknownStatement),
        },
        Token![drop] if state.remaining() >= 2 => ddl::drop::parse_drop(&mut state),
        Token::Ident(id) if id.eq_ignore_ascii_case("inspect") => {
            ddl::ins::parse_inspect(&mut state)
        }
        // DML
        Token![insert] => ASTNode::test_parse_from_state(&mut state).map(Statement::Insert),
        Token![select] => ASTNode::test_parse_from_state(&mut state).map(Statement::Select),
        Token![update] => ASTNode::test_parse_from_state(&mut state).map(Statement::Update),
        Token![delete] => ASTNode::test_parse_from_state(&mut state).map(Statement::Delete),
        _ => compiler::cold_rerr(QueryError::QLUnknownStatement),
    }
}
