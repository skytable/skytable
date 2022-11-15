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
    crate::{engine::memory::DataType, util::MaybeInit},
    std::{
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
        *d = MaybeInit::new(unsafe { Entity::full_entity_from_slice(tok) })
    } else if is_single {
        *i += 1;
        *d = MaybeInit::new(unsafe { Entity::single_entity_from_slice(tok) });
    }
    is_full | is_single
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
            Token::Lit(Lit::Str(s)) => DataType::String(s.to_string()),
            Token::Lit(Lit::Num(n)) => DataType::Number(*n),
            Token::Lit(Lit::Bool(b)) => DataType::Boolean(*b),
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
            Token::Lit(Lit::Str(s)) => {
                data.push(Some(s.to_string().into()));
            }
            Token::Lit(Lit::Num(n)) => {
                data.push(Some((*n).into()));
            }
            Token::Lit(Lit::Bool(b)) => {
                data.push(Some((*b).into()));
            }
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
            (Token::Ident(id), Token::Lit(Lit::Str(s))) => {
                okay &= data
                    .insert(unsafe { id.as_slice() }, Some(s.to_string().into()))
                    .is_none();
            }
            (Token::Ident(id), Token::Lit(Lit::Num(n))) => {
                okay &= data
                    .insert(unsafe { id.as_slice() }, Some((*n).into()))
                    .is_none();
            }
            (Token::Ident(id), Token::Lit(Lit::Bool(b))) => {
                okay &= data
                    .insert(unsafe { id.as_slice() }, Some((*b).into()))
                    .is_none();
            }
            (Token::Ident(id), Token::Symbol(Symbol::TtOpenSqBracket)) => {
                // ooh a list
                let mut l = Vec::new();
                let (_, lst_i, lst_ok) = parse_list(&tok[i + 3..], &mut l);
                okay &= lst_ok;
                i += lst_i;
                okay &= data
                    .insert(unsafe { id.as_slice() }, Some(l.into()))
                    .is_none();
            }
            (Token::Ident(id), Token![null]) => {
                okay &= data.insert(unsafe { id.as_slice() }, None).is_none();
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
    pub(super) primary_key: &'a Lit,
    pub(super) entity: Entity,
    pub(super) data: InsertData<'a>,
}

pub(super) fn parse_insert<'a>(
    src: &'a [Token],
    counter: &mut usize,
) -> LangResult<InsertStatement<'a>> {
    /*
        smallest:
        insert space:primary_key ()
        ^1     ^2   ^3^4         ^^5,6
    */
    let l = src.len();
    let is_full = Entity::tokens_with_full(src);
    let is_half = Entity::tokens_with_single(src);

    let mut okay = is_full | is_half;
    let mut i = 0;
    let mut entity = MaybeInit::uninit();

    okay &= process_entity(&src[i..], &mut entity, &mut i);

    // primary key is a lit; atleast lit + (<oparen><cparen>) | (<obrace><cbrace>)
    okay &= l >= (i + 4);
    // colon, lit
    okay &= src[i] == Token![:] && src[i + 1].is_lit();
    // check data
    let is_map = okay && src[i + 2] == Token![open {}];
    let is_tuple = okay && src[i + 2] == Token![() open];
    okay &= is_map | is_tuple;

    if !okay {
        return Err(LangError::UnexpectedToken);
    }

    let primary_key = unsafe { extract!(&src[i+1], Token::Lit(l) => l) };
    i += 3; // skip col, lit + op/ob

    let data;
    if is_tuple {
        let (ord, cnt, ok) = parse_data_tuple_syntax(&src[i..]);
        okay &= ok;
        i += cnt;
        data = InsertData::Ordered(ord);
    } else {
        let (map, cnt, ok) = parse_data_map_syntax(&src[i..]);
        okay &= ok;
        i += cnt;
        data = InsertData::Map(map);
    }

    *counter += i;

    if okay {
        Ok(InsertStatement {
            primary_key,
            entity: unsafe { entity.assume_init() },
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
    /// the primary key
    pub(super) primary_key: &'a Lit,
    /// the entity
    pub(super) entity: Entity,
    /// fields in order of querying. will be zero when wildcard is set
    pub(super) fields: Vec<RawSlice>,
    /// whether a wildcard was passed
    pub(super) wildcard: bool,
}

/// Parse a `select` query. The cursor should have already passed the `select` token when this
/// function is called.
pub(super) fn parse_select<'a>(
    tok: &'a [Token],
    counter: &mut usize,
) -> LangResult<SelectStatement<'a>> {
    let l = tok.len();

    let mut i = 0_usize;
    let mut okay = l > 4;
    let mut fields = Vec::new();
    let is_wildcard = i < l && tok[i] == Token![*];
    i += is_wildcard as usize;

    while okay && i < l && tok[i].is_ident() && !is_wildcard {
        unsafe {
            fields.push(extract!(&tok[i], Token::Ident(id) => id.clone()));
        }
        i += 1;
        // skip comma
        let nx_comma = i < l && tok[i] == Token![,];
        let nx_from = i < l && tok[i] == Token![from];
        okay &= nx_comma | nx_from;
        i += nx_comma as usize;
    }

    okay &= i < l && tok[i] == Token![from];
    i += okay as usize;

    // parsed upto select a, b, c from ...; now parse entity and select
    let mut entity = MaybeInit::uninit();
    okay &= process_entity(&tok[i..], &mut entity, &mut i);

    // now primary key
    okay &= i < l && tok[i] == Token![:];
    i += okay as usize;
    okay &= i < l && tok[i].is_lit();

    *counter += i + okay as usize;

    if okay {
        let primary_key = unsafe { extract!(tok[i], Token::Lit(ref l) => l) };
        Ok(SelectStatement {
            primary_key,
            entity: unsafe { entity.assume_init() },
            fields,
            wildcard: is_wildcard,
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
    assert!(i == tok.len(), "didn't use full length");
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
        assert_eq!(i, tok.len(), "full token stream not utilized");
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
    pub(super) primary_key: &'a Lit,
    pub(super) entity: Entity,
    pub(super) expressions: Vec<AssignmentExpression<'a>>,
}

impl<'a> UpdateStatement<'a> {
    pub(super) fn parse_update(tok: &'a [Token], counter: &mut usize) -> LangResult<Self> {
        let l = tok.len();
        // TODO(@ohsayan): This would become 8 when we add `SET`. It isn't exactly needed but is for purely aesthetic purposes
        let mut okay = l > 7;
        let mut i = 0_usize;

        // parse entity
        let mut entity = MaybeInit::uninit();
        okay &= process_entity(tok, &mut entity, &mut i);

        // check if we have our primary key
        okay &= i < l && tok[i] == Token![:];
        i += okay as usize;
        okay &= i < l && tok[i].is_lit();
        let primary_key_location = i;
        i += okay as usize;

        // now parse expressions that we have to update
        let mut expressions = Vec::new();
        while i < l && okay {
            okay &= AssignmentExpression::parse_and_append_expression(
                &tok[i..],
                &mut expressions,
                &mut i,
            );
            let nx_comma = i < l && tok[i] == Token![,];
            // TODO(@ohsayan): Define the need for a semicolon; remember, no SQL unsafety!
            let nx_over = i == l;
            okay &= nx_comma | nx_over;
            i += nx_comma as usize;
        }
        *counter += i;

        if okay {
            let primary_key =
                unsafe { extract!(tok[primary_key_location], Token::Lit(ref pk) => pk) };
            Ok(Self {
                primary_key,
                entity: unsafe { entity.assume_init() },
                expressions,
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
    assert_eq!(i, tok.len(), "full token stream not utilized");
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
    pub(super) primary_key: &'a Lit,
    pub(super) entity: Entity,
}

impl<'a> DeleteStatement<'a> {
    #[inline(always)]
    pub(super) fn new(primary_key: &'a Lit, entity: Entity) -> Self {
        Self {
            primary_key,
            entity,
        }
    }
    pub(super) fn parse_delete(tok: &'a [Token], counter: &mut usize) -> LangResult<Self> {
        let l = tok.len();
        let mut okay = l > 2;
        let mut i = 0_usize;

        // parse entity
        let mut entity = MaybeInit::uninit();
        okay &= process_entity(tok, &mut entity, &mut i);

        // find primary key
        okay &= i < l && tok[i] == Token![:];
        i += okay as usize;
        okay &= i < l && tok[i].is_lit();
        let primary_key_idx = i;
        i += okay as usize;

        *counter += i;

        if okay {
            unsafe {
                Ok(Self {
                    primary_key: extract!(tok[primary_key_idx], Token::Lit(ref l) => l),
                    entity: entity.assume_init(),
                })
            }
        } else {
            Err(LangError::UnexpectedToken)
        }
    }
}

#[cfg(test)]
pub(super) fn parse_delete_full<'a>(tok: &'a [Token]) -> LangResult<DeleteStatement<'a>> {
    let mut i = 0_usize;
    let r = DeleteStatement::parse_delete(tok, &mut i);
    assert_eq!(i, tok.len());
    r
}
