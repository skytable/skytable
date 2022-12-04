/*
 * Created on Fri Oct 14 2022
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
    TODO(@ohsayan): For now we've settled for an imprecise error site reporting for simplicity, which we
    should augment in future revisions of the QL engine
*/

use {
    super::{
        ast::Entity,
        lexer::{Lit, Symbol, Token},
        LangError, LangResult, RawSlice,
    },
    crate::{
        engine::memory::DataType,
        util::{compiler, MaybeInit},
    },
    std::{
        cmp,
        collections::HashMap,
        mem::{discriminant, Discriminant},
    },
};

/*
    Misc
*/

#[inline(always)]
fn process_entity(tok: &[Token], d: &mut MaybeInit<Entity>, i: &mut usize) -> bool {
    let is_full = Entity::tokens_with_full(tok);
    let is_single = Entity::tokens_with_single(tok);
    if is_full {
        *i += 3;
        *d = MaybeInit::new(unsafe {
            // UNSAFE(@ohsayan): Predicate ensures validity
            Entity::full_entity_from_slice(tok)
        })
    } else if is_single {
        *i += 1;
        *d = MaybeInit::new(unsafe {
            // UNSAFE(@ohsayan): Predicate ensures validity
            Entity::single_entity_from_slice(tok)
        });
    }
    is_full | is_single
}

/*
    Contexts
*/

#[derive(Debug, PartialEq)]
pub struct RelationalExpr<'a> {
    pub(super) lhs: &'a [u8],
    pub(super) rhs: &'a Lit,
    pub(super) opc: u8,
}

impl<'a> RelationalExpr<'a> {
    #[inline(always)]
    pub(super) fn new(lhs: &'a [u8], rhs: &'a Lit, opc: u8) -> Self {
        Self { lhs, rhs, opc }
    }
    pub(super) const OP_EQ: u8 = 1;
    pub(super) const OP_NE: u8 = 2;
    pub(super) const OP_GT: u8 = 3;
    pub(super) const OP_GE: u8 = 4;
    pub(super) const OP_LT: u8 = 5;
    pub(super) const OP_LE: u8 = 6;
    fn filter_hint_none(&self) -> bool {
        self.opc == Self::OP_EQ
    }
    #[inline(always)]
    fn parse_operator(tok: &[Token], i: &mut usize, okay: &mut bool) -> u8 {
        /*
            FIXME(@ohsayan): This is relatively messy right now, but does the job. Will
            re-implement later.
        */
        #[inline(always)]
        fn u(b: bool) -> u8 {
            b as _
        }
        let op_eq = u(tok[0] == Token![=]) * Self::OP_EQ;
        let op_ne = u(tok[0] == Token![!] && tok[1] == Token![=]) * Self::OP_NE;
        let op_ge = u(tok[0] == Token![>] && tok[1] == Token![=]) * Self::OP_GE;
        let op_gt = u(tok[0] == Token![>] && op_ge == 0) * Self::OP_GT;
        let op_le = u(tok[0] == Token![<] && tok[1] == Token![=]) * Self::OP_LE;
        let op_lt = u(tok[0] == Token![<] && op_le == 0) * Self::OP_LT;
        let opc = op_eq + op_ne + op_ge + op_gt + op_le + op_lt;
        *okay &= opc != 0;
        *i += 1 + (opc & 1 == 0) as usize;
        opc
    }
    #[inline(always)]
    fn try_parse(tok: &'a [Token], cnt: &mut usize) -> Option<Self> {
        /*
            Minimum length of an expression:
            [lhs] [operator] [rhs]
        */
        let mut okay = tok.len() >= 3;
        let mut i = 0_usize;
        if compiler::unlikely(!okay) {
            return None;
        }
        okay &= tok[0].is_ident();
        i += 1;
        // let's get ourselves the operator
        let operator = Self::parse_operator(&tok[i..], &mut i, &mut okay);
        okay &= i < tok.len();
        let lit_idx = cmp::min(i, tok.len() - 1);
        okay &= tok[lit_idx].is_lit(); // LOL, I really like saving cycles
        *cnt += i + okay as usize;
        if compiler::likely(okay) {
            Some(unsafe {
                // UNSAFE(@ohsayan): tok[0] is checked for being an ident, tok[lit_idx] also checked to be a lit
                Self {
                    lhs: extract!(tok[0], Token::Ident(ref id) => id.as_slice()),
                    rhs: extract!(tok[lit_idx], Token::Lit(ref l) => l),
                    opc: operator,
                }
            })
        } else {
            compiler::cold_err(None)
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct WhereClause<'a> {
    c: HashMap<&'a [u8], RelationalExpr<'a>>,
}

impl<'a> WhereClause<'a> {
    #[inline(always)]
    pub(super) fn new(c: HashMap<&'a [u8], RelationalExpr<'a>>) -> Self {
        Self { c }
    }
    #[inline(always)]
    /// Parse the expressions in a `where` context, appending it to the given map
    ///
    /// Notes:
    /// - Deny duplicate clauses
    /// - No enforcement on minimum number of clauses
    fn parse_where_and_append_to(
        tok: &'a [Token],
        cnt: &mut usize,
        c: &mut HashMap<&'a [u8], RelationalExpr<'a>>,
    ) -> bool {
        let l = tok.len();
        let mut okay = true;
        let mut i = 0;
        let mut has_more = true;
        while okay && i < l && has_more {
            okay &= RelationalExpr::try_parse(&tok[i..], &mut i)
                .map(|clause| c.insert(clause.lhs, clause).is_none())
                .unwrap_or(false);
            has_more = tok[cmp::min(i, l - 1)] == Token![and] && i < l;
            i += has_more as usize;
        }
        *cnt += i;
        okay
    }
    #[inline(always)]
    /// Parse a where context
    ///
    /// Notes:
    /// - Enforce a minimum of 1 clause
    pub(super) fn parse_where(tok: &'a [Token], flag: &mut bool, cnt: &mut usize) -> Self {
        let mut c = HashMap::with_capacity(2);
        *flag &= Self::parse_where_and_append_to(tok, cnt, &mut c);
        *flag &= !c.is_empty();
        Self { c }
    }
}

#[cfg(test)]
pub(super) fn parse_where_clause_full<'a>(tok: &'a [Token]) -> Option<WhereClause<'a>> {
    let mut flag = true;
    let mut i = 0;
    let ret = WhereClause::parse_where(tok, &mut flag, &mut i);
    assert_full_tt!(tok.len(), i);
    flag.then_some(ret)
}

#[cfg(test)]
#[inline(always)]
pub(super) fn parse_relexpr_full<'a>(tok: &'a [Token]) -> Option<RelationalExpr<'a>> {
    let mut i = 0;
    let okay = RelationalExpr::try_parse(tok, &mut i);
    assert_full_tt!(tok.len(), i);
    okay
}

/*
    Impls for insert
*/

/// Parse a list
///
/// **NOTE:** This function will error if the `[` token is passed. Make sure this is forwarded by the caller
pub(super) fn parse_list(
    tok: &[Token],
    list: &mut Vec<DataType>,
) -> (Option<Discriminant<DataType>>, usize, bool) {
    let l = tok.len();
    let mut okay = l != 0;
    let mut stop = okay && tok[0] == Symbol::TtCloseSqBracket;
    let mut i = stop as usize;
    let mut overall_dscr = None;
    let mut prev_nlist_dscr = None;
    while i < l && okay && !stop {
        let d = match &tok[i] {
            Token::Lit(l) => match l {
                Lit::Str(s) => DataType::String(s.to_string()),
                Lit::UnsignedInt(n) => DataType::UnsignedInt(*n),
                Lit::Bool(b) => DataType::Boolean(*b),
                Lit::UnsafeLit(l) => DataType::AnonymousTypeNeedsEval(l.clone()),
                Lit::SignedInt(uint) => DataType::SignedInt(*uint),
            },
            Token::Symbol(Symbol::TtOpenSqBracket) => {
                // a nested list
                let mut nested_list = Vec::new();
                let (nlist_dscr, nlist_i, nlist_okay) = parse_list(&tok[i + 1..], &mut nested_list);
                okay &= nlist_okay;
                i += nlist_i;
                // check type return
                okay &= {
                    prev_nlist_dscr.is_none()
                        || nlist_dscr.is_none()
                        || prev_nlist_dscr == nlist_dscr
                };
                if prev_nlist_dscr.is_none() && nlist_dscr.is_some() {
                    prev_nlist_dscr = nlist_dscr;
                }
                DataType::List(nested_list)
            }
            _ => {
                okay = false;
                break;
            }
        };
        i += 1;
        okay &= list.is_empty() || discriminant(&d) == discriminant(&list[0]);
        overall_dscr = Some(discriminant(&d));
        list.push(d);
        let nx_comma = i < l && tok[i] == Symbol::SymComma;
        let nx_csqrb = i < l && tok[i] == Symbol::TtCloseSqBracket;
        okay &= nx_comma | nx_csqrb;
        i += okay as usize;
        stop = nx_csqrb;
    }
    (overall_dscr, i, okay && stop)
}

#[cfg(test)]
pub(super) fn parse_list_full(tok: &[Token]) -> Option<Vec<DataType>> {
    let mut l = Vec::new();
    if matches!(parse_list(tok, &mut l), (_, i, true) if i == tok.len()) {
        Some(l)
    } else {
        None
    }
}

/// Parse the tuple data passed in with an insert query.
///
/// **Note:** Make sure you pass the `(` token
pub(super) fn parse_data_tuple_syntax(tok: &[Token]) -> (Vec<Option<DataType>>, usize, bool) {
    let l = tok.len();
    let mut okay = l != 0;
    let mut stop = okay && tok[0] == Token::Symbol(Symbol::TtCloseParen);
    let mut i = stop as usize;
    let mut data = Vec::new();
    while i < l && okay && !stop {
        match &tok[i] {
            Token::Lit(l) => match l {
                Lit::Str(s) => {
                    data.push(Some(s.to_string().into()));
                }
                Lit::UnsignedInt(n) => {
                    data.push(Some((*n).into()));
                }
                Lit::Bool(b) => {
                    data.push(Some((*b).into()));
                }
                Lit::UnsafeLit(r) => data.push(Some(DataType::AnonymousTypeNeedsEval(r.clone()))),
                Lit::SignedInt(int) => data.push(Some(DataType::SignedInt(*int))),
            },
            Token::Symbol(Symbol::TtOpenSqBracket) => {
                // ah, a list
                let mut l = Vec::new();
                let (_, lst_i, lst_okay) = parse_list(&tok[i + 1..], &mut l);
                data.push(Some(l.into()));
                i += lst_i;
                okay &= lst_okay;
            }
            Token![null] => {
                data.push(None);
            }
            _ => {
                okay = false;
                break;
            }
        }
        i += 1;
        let nx_comma = i < l && tok[i] == Symbol::SymComma;
        let nx_csprn = i < l && tok[i] == Symbol::TtCloseParen;
        okay &= nx_comma | nx_csprn;
        i += okay as usize;
        stop = nx_csprn;
    }
    (data, i, okay && stop)
}

#[cfg(test)]
pub(super) fn parse_data_tuple_syntax_full(tok: &[Token]) -> Option<Vec<Option<DataType>>> {
    let (ret, cnt, okay) = parse_data_tuple_syntax(tok);
    assert!(cnt == tok.len(), "didn't use full length");
    if okay {
        Some(ret)
    } else {
        None
    }
}

pub(super) fn parse_data_map_syntax<'a>(
    tok: &'a [Token],
) -> (HashMap<&'a [u8], Option<DataType>>, usize, bool) {
    let l = tok.len();
    let mut okay = l != 0;
    let mut stop = okay && tok[0] == Token::Symbol(Symbol::TtCloseBrace);
    let mut i = stop as usize;
    let mut data = HashMap::new();
    while i + 3 < l && okay && !stop {
        let (field, colon, expression) = (&tok[i], &tok[i + 1], &tok[i + 2]);
        okay &= colon == &Symbol::SymColon;
        match (field, expression) {
            (Token::Ident(id), Token::Lit(l)) => {
                let dt = match l {
                    Lit::Str(s) => s.to_string().into(),
                    Lit::Bool(b) => (*b).into(),
                    Lit::UnsignedInt(s) => (*s).into(),
                    Lit::UnsafeLit(l) => DataType::AnonymousTypeNeedsEval(l.clone()),
                    Lit::SignedInt(int) => DataType::SignedInt(*int),
                };
                okay &= data
                    .insert(
                        unsafe {
                            // UNSAFE(@ohsayan): Token lifetime ensures slice validity
                            id.as_slice()
                        },
                        Some(dt),
                    )
                    .is_none();
            }
            (Token::Ident(id), Token::Symbol(Symbol::TtOpenSqBracket)) => {
                // ooh a list
                let mut l = Vec::new();
                let (_, lst_i, lst_ok) = parse_list(&tok[i + 3..], &mut l);
                okay &= lst_ok;
                i += lst_i;
                okay &= data
                    .insert(
                        unsafe {
                            // UNSAFE(@ohsayan): Token lifetime ensures validity
                            id.as_slice()
                        },
                        Some(l.into()),
                    )
                    .is_none();
            }
            (Token::Ident(id), Token![null]) => {
                okay &= data
                    .insert(
                        unsafe {
                            // UNSAFE(@ohsayan): Token lifetime ensures validity
                            id.as_slice()
                        },
                        None,
                    )
                    .is_none();
            }
            _ => {
                okay = false;
                break;
            }
        }
        i += 3;
        let nx_comma = i < l && tok[i] == Symbol::SymComma;
        let nx_csbrc = i < l && tok[i] == Symbol::TtCloseBrace;
        okay &= nx_comma | nx_csbrc;
        i += okay as usize;
        stop = nx_csbrc;
    }
    (data, i, okay && stop)
}

#[cfg(test)]
pub(super) fn parse_data_map_syntax_full(
    tok: &[Token],
) -> Option<HashMap<Box<str>, Option<DataType>>> {
    let (dat, i, ok) = parse_data_map_syntax(tok);
    assert!(i == tok.len(), "didn't use full length");
    if ok {
        Some(
            dat.into_iter()
                .map(|(ident, val)| {
                    (
                        String::from_utf8_lossy(ident).to_string().into_boxed_str(),
                        val,
                    )
                })
                .collect(),
        )
    } else {
        None
    }
}

#[derive(Debug, PartialEq)]
pub enum InsertData<'a> {
    Ordered(Vec<Option<DataType>>),
    Map(HashMap<&'a [u8], Option<DataType>>),
}

impl<'a> From<Vec<Option<DataType>>> for InsertData<'a> {
    fn from(v: Vec<Option<DataType>>) -> Self {
        Self::Ordered(v)
    }
}

impl<'a> From<HashMap<&'static [u8], Option<DataType>>> for InsertData<'a> {
    fn from(m: HashMap<&'static [u8], Option<DataType>>) -> Self {
        Self::Map(m)
    }
}

#[derive(Debug, PartialEq)]
pub struct InsertStatement<'a> {
    pub(super) entity: Entity,
    pub(super) data: InsertData<'a>,
}

#[inline(always)]
fn parse_entity(tok: &[Token], entity: &mut MaybeInit<Entity>, i: &mut usize) -> bool {
    let is_full = tok[0].is_ident() && tok[1] == Token![.] && tok[2].is_ident();
    let is_half = tok[0].is_ident();
    unsafe {
        // UNSAFE(@ohsayan): The branch predicates assert their correctness
        if is_full {
            *i += 3;
            *entity = MaybeInit::new(Entity::full_entity_from_slice(&tok));
        } else if is_half {
            *i += 1;
            *entity = MaybeInit::new(Entity::single_entity_from_slice(&tok));
        }
    }
    is_full | is_half
}

pub(super) fn parse_insert<'a>(
    tok: &'a [Token],
    counter: &mut usize,
) -> LangResult<InsertStatement<'a>> {
    /*
        smallest:
        insert into model (primarykey)
               ^1    ^2   ^3      ^4 ^5
    */
    let l = tok.len();
    if compiler::unlikely(l < 5) {
        return compiler::cold_err(Err(LangError::UnexpectedEndofStatement));
    }
    let mut okay = tok[0] == Token![into];
    let mut i = okay as usize;
    let mut entity = MaybeInit::uninit();
    okay &= parse_entity(&tok[i..], &mut entity, &mut i);
    let mut data = None;
    if !(i < l) {
        unsafe {
            // UNSAFE(@ohsayan): ALWAYS true because 1 + 3 for entity; early exit if smaller
            impossible!();
        }
    }
    match tok[i] {
        Token![() open] => {
            let (this_data, incr, ok) = parse_data_tuple_syntax(&tok[i + 1..]);
            okay &= ok;
            i += incr + 1;
            data = Some(InsertData::Ordered(this_data));
        }
        Token![open {}] => {
            let (this_data, incr, ok) = parse_data_map_syntax(&tok[i + 1..]);
            okay &= ok;
            i += incr + 1;
            data = Some(InsertData::Map(this_data));
        }
        _ => okay = false,
    }
    *counter += i;
    if okay {
        let data = unsafe {
            // UNSAFE(@ohsayan): Will be safe because of `okay` since it ensures that entity has been initialized
            data.unwrap_unchecked()
        };
        Ok(InsertStatement {
            entity: unsafe {
                // UNSAFE(@ohsayan): Will be safe because of `okay` since it ensures that entity has been initialized
                entity.assume_init()
            },
            data,
        })
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[cfg(test)]
pub(super) fn parse_insert_full<'a>(tok: &'a [Token]) -> Option<InsertStatement<'a>> {
    let mut z = 0;
    let s = self::parse_insert(tok, &mut z);
    assert!(z == tok.len(), "didn't use full length");
    s.ok()
}

/*
    Impls for select
*/

#[derive(Debug, PartialEq)]
pub(super) struct SelectStatement<'a> {
    /// the entity
    pub(super) entity: Entity,
    /// fields in order of querying. will be zero when wildcard is set
    pub(super) fields: Vec<RawSlice>,
    /// whether a wildcard was passed
    pub(super) wildcard: bool,
    /// where clause
    pub(super) clause: WhereClause<'a>,
}
impl<'a> SelectStatement<'a> {
    #[inline(always)]
    pub(crate) fn new_test(
        entity: Entity,
        fields: Vec<RawSlice>,
        wildcard: bool,
        clauses: HashMap<&'a [u8], RelationalExpr<'a>>,
    ) -> SelectStatement<'a> {
        Self::new(entity, fields, wildcard, clauses)
    }
    #[inline(always)]
    fn new(
        entity: Entity,
        fields: Vec<RawSlice>,
        wildcard: bool,
        clauses: HashMap<&'a [u8], RelationalExpr<'a>>,
    ) -> SelectStatement<'a> {
        Self {
            entity,
            fields,
            wildcard,
            clause: WhereClause::new(clauses),
        }
    }
}

/// Parse a `select` query. The cursor should have already passed the `select` token when this
/// function is called.
pub(super) fn parse_select<'a>(
    tok: &'a [Token],
    counter: &mut usize,
) -> LangResult<SelectStatement<'a>> {
    /*
        Smallest query:
        select * from model
               ^ ^    ^
               1 2    3
    */
    let l = tok.len();
    if compiler::unlikely(l < 3) {
        return compiler::cold_err(Err(LangError::UnexpectedEndofStatement));
    }
    let mut i = 0;
    let mut okay = true;
    let mut select_fields = Vec::new();
    let is_wildcard = tok[0] == Token![*];
    i += is_wildcard as usize;
    while i < l && okay && !is_wildcard {
        match tok[i] {
            Token::Ident(ref id) => select_fields.push(id.clone()),
            _ => {
                break;
            }
        }
        i += 1;
        let nx_idx = cmp::min(i, l);
        let nx_comma = tok[nx_idx] == Token![,] && i < l;
        let nx_from = tok[nx_idx] == Token![from];
        okay &= nx_comma | nx_from;
        i += nx_comma as usize;
    }
    okay &= is_wildcard | !select_fields.is_empty();
    okay &= (i + 2) <= l;
    if compiler::unlikely(!okay) {
        return compiler::cold_err(Err(LangError::UnexpectedToken));
    }
    okay &= tok[i] == Token![from];
    i += okay as usize;
    // now process entity
    let mut entity = MaybeInit::uninit();
    okay &= process_entity(&tok[i..], &mut entity, &mut i);
    let has_where = tok[cmp::min(i, l)] == Token![where];
    i += has_where as usize;
    let mut clauses = <_ as Default>::default();
    if has_where {
        okay &= WhereClause::parse_where_and_append_to(&tok[i..], &mut i, &mut clauses);
        okay &= !clauses.is_empty(); // append doesn't enforce clause arity
    }
    *counter += i;
    if okay {
        Ok(SelectStatement {
            entity: unsafe {
                // UNSAFE(@ohsayan): `process_entity` and `okay` assert correctness
                entity.assume_init()
            },
            fields: select_fields,
            wildcard: is_wildcard,
            clause: WhereClause::new(clauses),
        })
    } else {
        Err(LangError::UnexpectedToken)
    }
}

#[cfg(test)]
/// **test-mode only** parse for a `select` where the full token stream is exhausted
pub(super) fn parse_select_full<'a>(tok: &'a [Token]) -> Option<SelectStatement<'a>> {
    let mut i = 0;
    let r = self::parse_select(tok, &mut i);
    assert_full_tt!(i, tok.len());
    r.ok()
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// TODO(@ohsayan): This only helps with the parser test for now. Replace this with actual operator expressions
pub(super) enum Operator {
    Assign,
    AddAssign,
    SubAssign,
    MulAssign,
    DivAssign,
}

static OPERATOR: [Operator; 6] = [
    Operator::Assign,
    Operator::Assign,
    Operator::AddAssign,
    Operator::SubAssign,
    Operator::MulAssign,
    Operator::DivAssign,
];

#[derive(Debug, PartialEq)]
pub struct AssignmentExpression<'a> {
    /// the LHS ident
    pub(super) lhs: RawSlice,
    /// the RHS lit
    pub(super) rhs: &'a Lit,
    /// operator
    pub(super) operator_fn: Operator,
}

impl<'a> AssignmentExpression<'a> {
    pub(super) fn new(lhs: RawSlice, rhs: &'a Lit, operator_fn: Operator) -> Self {
        Self {
            lhs,
            rhs,
            operator_fn,
        }
    }
    /// Attempt to parse an expression and then append it to the given vector of expressions. This will return `true`
    /// if the expression was parsed correctly, otherwise `false` is returned
    #[inline(always)]
    fn parse_and_append_expression(
        tok: &'a [Token],
        expressions: &mut Vec<Self>,
        counter: &mut usize,
    ) -> bool {
        /*
            smallest expression:
            <ident> <operator> <lit>
        */
        let l = tok.len();
        let mut i = 0;
        let mut okay = tok.len() > 2 && tok[0].is_ident();
        i += okay as usize;

        let op_assign = (i < l && tok[i] == Token![=]) as usize * 1;
        let op_add = (i < l && tok[i] == Token![+]) as usize * 2;
        let op_sub = (i < l && tok[i] == Token![-]) as usize * 3;
        let op_mul = (i < l && tok[i] == Token![*]) as usize * 4;
        let op_div = (i < l && tok[i] == Token![/]) as usize * 5;

        let operator_code = op_assign + op_add + op_sub + op_mul + op_div;
        unsafe {
            // UNSAFE(@ohsayan): Inherently obvious, just a hint
            if operator_code > 5 {
                impossible!()
            }
        }
        okay &= operator_code != 0;
        i += okay as usize;

        let has_double_assign = i < l && tok[i] == Token![=];
        let double_assign_okay = operator_code != 1 && has_double_assign;
        let single_assign_okay = operator_code == 1 && !double_assign_okay;
        okay &= single_assign_okay | double_assign_okay;
        i += double_assign_okay as usize; // skip on <op>assign

        let has_rhs = i < l && tok[i].is_lit();
        okay &= has_rhs;
        *counter += i + has_rhs as usize;

        if okay {
            let expression = unsafe {
                /*
                   UNSAFE(@ohsayan): tok[0] is checked for being an ident early on; second, tok[i]
                   is also checked for being a lit and then `okay` ensures correctness
                */
                AssignmentExpression {
                    lhs: extract!(tok[0], Token::Ident(ref r) => r.clone()),
                    rhs: extract!(tok[i], Token::Lit(ref l) => l),
                    operator_fn: OPERATOR[operator_code as usize],
                }
            };
            expressions.push(expression);
        }

        okay
    }
}

#[cfg(test)]
pub(super) fn parse_expression_full<'a>(tok: &'a [Token]) -> Option<AssignmentExpression<'a>> {
    let mut i = 0;
    let mut exprs = Vec::new();
    if AssignmentExpression::parse_and_append_expression(tok, &mut exprs, &mut i) {
        assert_full_tt!(i, tok.len());
        Some(exprs.remove(0))
    } else {
        None
    }
}

/*
    Impls for update
*/

#[derive(Debug, PartialEq)]
pub struct UpdateStatement<'a> {
    pub(super) entity: Entity,
    pub(super) expressions: Vec<AssignmentExpression<'a>>,
    pub(super) wc: WhereClause<'a>,
}

impl<'a> UpdateStatement<'a> {
    #[inline(always)]
    #[cfg(test)]
    pub fn new_test(
        entity: Entity,
        expressions: Vec<AssignmentExpression<'a>>,
        wc: HashMap<&'a [u8], RelationalExpr<'a>>,
    ) -> Self {
        Self::new(entity, expressions, WhereClause::new(wc))
    }
    #[inline(always)]
    pub fn new(
        entity: Entity,
        expressions: Vec<AssignmentExpression<'a>>,
        wc: WhereClause<'a>,
    ) -> Self {
        Self {
            entity,
            expressions,
            wc,
        }
    }
    #[inline(always)]
    pub(super) fn parse_update(tok: &'a [Token], counter: &mut usize) -> LangResult<Self> {
        /*
            TODO(@ohsayan): Allow volcanoes
            smallest tt:
            update model SET x  =  1 where x = 1
                   ^1    ^2  ^3 ^4 ^5^6    ^7^8^9
        */
        let l = tok.len();
        if compiler::unlikely(l < 9) {
            return compiler::cold_err(Err(LangError::UnexpectedEndofStatement));
        }
        let mut i = 0;
        let mut entity = MaybeInit::uninit();
        let mut okay = parse_entity(&tok[i..], &mut entity, &mut i);
        if !((i + 6) <= l) {
            unsafe {
                // UNSAFE(@ohsayan): Obvious, just a hint; entity can fw by 3 max
                impossible!();
            }
        }
        okay &= tok[i] == Token![set];
        i += 1; // ignore whatever we have here, even if it's broken
        let mut nx_where = false;
        let mut expressions = Vec::new();
        while i < l && okay && !nx_where {
            okay &= AssignmentExpression::parse_and_append_expression(
                &tok[i..],
                &mut expressions,
                &mut i,
            );
            let nx_idx = cmp::min(i, l);
            let nx_comma = tok[nx_idx] == Token![,] && i < l;
            // NOTE: volcano
            nx_where = tok[nx_idx] == Token![where] && i < l;
            okay &= nx_comma | nx_where; // NOTE: volcano
            i += nx_comma as usize;
        }
        okay &= nx_where;
        i += okay as usize;
        // now process expressions
        let mut clauses = <_ as Default>::default();
        okay &= WhereClause::parse_where_and_append_to(&tok[i..], &mut i, &mut clauses);
        okay &= !clauses.is_empty(); // NOTE: volcano
        *counter += i;
        if okay {
            Ok(Self {
                entity: unsafe {
                    // UNSAFE(@ohsayan): This is safe because of `parse_entity` and `okay`
                    entity.assume_init()
                },
                expressions,
                wc: WhereClause::new(clauses),
            })
        } else {
            Err(LangError::UnexpectedToken)
        }
    }
}

#[cfg(test)]
pub(super) fn parse_update_full<'a>(tok: &'a [Token]) -> LangResult<UpdateStatement<'a>> {
    let mut i = 0;
    let r = UpdateStatement::parse_update(tok, &mut i);
    assert_full_tt!(i, tok.len());
    r
}

/*
    Impls for delete
    ---
    Smallest statement:
    delete model:primary_key
*/

#[derive(Debug, PartialEq)]
pub(super) struct DeleteStatement<'a> {
    pub(super) entity: Entity,
    pub(super) wc: WhereClause<'a>,
}

impl<'a> DeleteStatement<'a> {
    #[inline(always)]
    pub(super) fn new(entity: Entity, wc: WhereClause<'a>) -> Self {
        Self { entity, wc }
    }
    #[inline(always)]
    #[cfg(test)]
    pub(super) fn new_test(entity: Entity, wc: HashMap<&'a [u8], RelationalExpr<'a>>) -> Self {
        Self::new(entity, WhereClause::new(wc))
    }
    pub(super) fn parse_delete(tok: &'a [Token], counter: &mut usize) -> LangResult<Self> {
        /*
            TODO(@ohsayan): Volcano
            smallest tt:
            delete from model where x = 1
                   ^1   ^2    ^3    ^4  ^5
        */
        let l = tok.len();
        if compiler::unlikely(l < 5) {
            return compiler::cold_err(Err(LangError::UnexpectedEndofStatement));
        }
        let mut i = 0;
        let mut okay = tok[i] == Token![from];
        i += 1; // skip even if incorrect
        let mut entity = MaybeInit::uninit();
        okay &= parse_entity(&tok[i..], &mut entity, &mut i);
        if !(i < l) {
            unsafe {
                // UNSAFE(@ohsayan): Obvious, we have atleast 5, used max 4
                impossible!();
            }
        }
        okay &= tok[i] == Token![where]; // NOTE: volcano
        i += 1; // skip even if incorrect
        let mut clauses = <_ as Default>::default();
        okay &= WhereClause::parse_where_and_append_to(&tok[i..], &mut i, &mut clauses);
        okay &= !clauses.is_empty();
        *counter += i;
        if okay {
            Ok(Self {
                entity: unsafe {
                    // UNSAFE(@ohsayan): obvious due to `okay` and `parse_entity`
                    entity.assume_init()
                },
                wc: WhereClause::new(clauses),
            })
        } else {
            Err(LangError::UnexpectedToken)
        }
    }
}

#[cfg(test)]
pub(super) fn parse_delete_full<'a>(tok: &'a [Token]) -> LangResult<DeleteStatement<'a>> {
    let mut i = 0_usize;
    let r = DeleteStatement::parse_delete(tok, &mut i);
    assert_full_tt!(i, tok.len());
    r
}
