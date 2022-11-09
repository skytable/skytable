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

use std::mem::MaybeUninit;

use {
    super::{
        ast::Entity,
        lexer::{Lit, Symbol, Token},
        LangError, LangResult,
    },
    crate::engine::memory::DataType,
    std::{
        collections::HashMap,
        mem::{discriminant, Discriminant},
    },
};

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
    if cnt == tok.len() && okay {
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
    if i == tok.len() && ok {
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
    let mut entity = MaybeUninit::uninit();

    if is_full {
        i += 3;
        entity = MaybeUninit::new(unsafe { Entity::full_entity_from_slice(src) });
    } else if is_half {
        i += 1;
        entity = MaybeUninit::new(unsafe { Entity::single_entity_from_slice(src) });
    }

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
    if z == tok.len() {
        s.ok()
    } else {
        None
    }
}
