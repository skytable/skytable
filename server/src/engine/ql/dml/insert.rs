/*
 * Created on Fri Jan 06 2023
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

#[cfg(test)]
use crate::engine::ql::ast::InplaceData;
use {
    super::parse_entity,
    crate::{
        engine::{
            memory::DataType,
            ql::{
                ast::{Entity, QueryData, State},
                lexer::Token,
                LangError, LangResult,
            },
        },
        util::{compiler, MaybeInit},
    },
    core::mem::{discriminant, Discriminant},
    std::collections::HashMap,
};

/*
    Impls for insert
*/

/// ## Panics
/// - If tt length is less than 1
pub(super) fn parse_list<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
    list: &mut Vec<DataType>,
) -> Option<Discriminant<DataType>> {
    let mut stop = state.cursor_eq(Token![close []]);
    state.cursor_ahead_if(stop);
    let mut overall_dscr = None;
    let mut prev_nlist_dscr = None;
    while state.not_exhausted() && state.okay() && !stop {
        let d = match state.read() {
            tok if state.can_read_lit_from(tok) => {
                let r = unsafe {
                    // UNSAFE(@ohsayan): the if guard guarantees correctness
                    DataType::clone_from_litir(state.read_cursor_lit_unchecked())
                };
                state.cursor_ahead();
                r
            }
            Token![open []] => {
                state.cursor_ahead();
                // a nested list
                let mut nested_list = Vec::new();
                let nlist_dscr = parse_list(state, &mut nested_list);
                // check type return
                state.poison_if_not(
                    prev_nlist_dscr.is_none()
                        || nlist_dscr.is_none()
                        || prev_nlist_dscr == nlist_dscr,
                );
                if prev_nlist_dscr.is_none() && nlist_dscr.is_some() {
                    prev_nlist_dscr = nlist_dscr;
                }
                DataType::List(nested_list)
            }
            _ => {
                state.poison();
                break;
            }
        };
        state.poison_if_not(list.is_empty() || discriminant(&d) == discriminant(&list[0]));
        overall_dscr = Some(discriminant(&d));
        list.push(d);
        let nx_comma = state.cursor_rounded_eq(Token![,]);
        let nx_csqrb = state.cursor_rounded_eq(Token![close []]);
        state.poison_if_not(nx_comma | nx_csqrb);
        state.cursor_ahead_if(state.okay());
        stop = nx_csqrb;
    }
    overall_dscr
}

#[cfg(test)]
pub fn parse_list_full<'a>(tok: &'a [Token], qd: impl QueryData<'a>) -> Option<Vec<DataType>> {
    let mut l = Vec::new();
    let mut state = State::new(tok, qd);
    parse_list(&mut state, &mut l);
    assert_full_tt!(state);
    state.okay().then_some(l)
}

/// ## Panics
/// - If tt is empty
pub(super) fn parse_data_tuple_syntax<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> Vec<Option<DataType>> {
    let mut stop = state.cursor_eq(Token![() close]);
    state.cursor_ahead_if(stop);
    let mut data = Vec::new();
    while state.not_exhausted() && state.okay() && !stop {
        match state.read() {
            tok if state.can_read_lit_from(tok) => {
                unsafe {
                    // UNSAFE(@ohsayan): if guard guarantees correctness
                    data.push(Some(DataType::clone_from_litir(
                        state.read_cursor_lit_unchecked(),
                    )))
                }
                state.cursor_ahead();
            }
            Token![open []] if state.remaining() >= 2 => {
                state.cursor_ahead();
                let mut l = Vec::new();
                let _ = parse_list(state, &mut l);
                data.push(Some(l.into()));
            }
            Token![null] => {
                state.cursor_ahead();
                data.push(None);
            }
            _ => {
                state.poison();
                break;
            }
        }
        let nx_comma = state.cursor_rounded_eq(Token![,]);
        let nx_csprn = state.cursor_rounded_eq(Token![() close]);
        state.poison_if_not(nx_comma | nx_csprn);
        state.cursor_ahead_if(state.okay());
        stop = nx_csprn;
    }
    data
}

#[cfg(test)]
pub fn parse_data_tuple_syntax_full(tok: &[Token]) -> Option<Vec<Option<DataType>>> {
    let mut state = State::new(tok, InplaceData::new());
    let ret = parse_data_tuple_syntax(&mut state);
    assert_full_tt!(state);
    state.okay().then_some(ret)
}

/// ## Panics
/// Panics if tt is empty
pub(super) fn parse_data_map_syntax<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> HashMap<&'a [u8], Option<DataType>> {
    let mut stop = state.cursor_eq(Token![close {}]);
    state.cursor_ahead_if(stop);
    let mut data = HashMap::with_capacity(2);
    while state.has_remaining(3) && state.okay() && !stop {
        let field = state.read();
        let colon = state.read_ahead(1);
        let expr = state.read_ahead(2);
        state.poison_if_not(Token![:].eq(colon));
        match (field, expr) {
            (Token::Ident(id), tok) if state.can_read_lit_from(tok) => {
                state.cursor_ahead_by(2); // ident + colon
                let ldata = Some(DataType::clone_from_litir(unsafe {
                    // UNSAFE(@ohsayan): The if guard guarantees correctness
                    state.read_cursor_lit_unchecked()
                }));
                state.cursor_ahead();
                state.poison_if_not(data.insert(*id, ldata).is_none());
            }
            (Token::Ident(id), Token![null]) => {
                state.cursor_ahead_by(3);
                state.poison_if_not(data.insert(*id, None).is_none());
            }
            (Token::Ident(id), Token![open []]) if state.remaining() >= 4 => {
                state.cursor_ahead_by(3);
                let mut l = Vec::new();
                let _ = parse_list(state, &mut l);
                state.poison_if_not(data.insert(*id, Some(l.into())).is_none());
            }
            _ => {
                state.poison();
                break;
            }
        }
        let nx_comma = state.cursor_rounded_eq(Token![,]);
        let nx_csbrc = state.cursor_rounded_eq(Token![close {}]);
        state.poison_if_not(nx_comma | nx_csbrc);
        state.cursor_ahead_if(state.okay());
        stop = nx_csbrc;
    }
    data
}

#[cfg(test)]
pub fn parse_data_map_syntax_full(tok: &[Token]) -> Option<HashMap<Box<str>, Option<DataType>>> {
    let mut state = State::new(tok, InplaceData::new());
    let r = parse_data_map_syntax(&mut state);
    assert_full_tt!(state);
    state.okay().then_some(
        r.into_iter()
            .map(|(ident, val)| {
                (
                    String::from_utf8_lossy(ident).to_string().into_boxed_str(),
                    val,
                )
            })
            .collect(),
    )
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
    pub(super) entity: Entity<'a>,
    pub(super) data: InsertData<'a>,
}

impl<'a> InsertStatement<'a> {
    #[inline(always)]
    pub fn new(entity: Entity<'a>, data: InsertData<'a>) -> Self {
        Self { entity, data }
    }
}

impl<'a> InsertStatement<'a> {
    pub fn parse_insert<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
        /*
            smallest:
            insert into model (primarykey)
                   ^1    ^2   ^3      ^4 ^5
        */
        if compiler::unlikely(state.remaining() < 5) {
            return compiler::cold_rerr(LangError::UnexpectedEndofStatement);
        }
        state.poison_if_not(state.cursor_eq(Token![into]));
        state.cursor_ahead(); // ignore errors

        // entity
        let mut entity = MaybeInit::uninit();
        parse_entity(state, &mut entity);
        let what_data = state.read();
        state.cursor_ahead(); // ignore errors for now
        let mut data = None;
        match what_data {
            Token![() open] if state.not_exhausted() => {
                let this_data = parse_data_tuple_syntax(state);
                data = Some(InsertData::Ordered(this_data));
            }
            Token![open {}] if state.not_exhausted() => {
                let this_data = parse_data_map_syntax(state);
                data = Some(InsertData::Map(this_data));
            }
            _ => {
                state.poison();
            }
        }
        if state.okay() {
            let data = unsafe {
                // UNSAFE(@ohsayan): state's flag guarantees correctness
                data.unwrap_unchecked()
            };
            Ok(InsertStatement {
                entity: unsafe {
                    // UNSAFE(@ohsayan): state's flag ensures correctness
                    entity.assume_init()
                },
                data,
            })
        } else {
            compiler::cold_rerr(LangError::UnexpectedToken)
        }
    }
}

#[cfg(test)]
pub fn parse_insert_full<'a>(tok: &'a [Token]) -> Option<InsertStatement<'a>> {
    let mut state = State::new(tok, InplaceData::new());
    let ret = InsertStatement::parse_insert(&mut state);
    assert_full_tt!(state);
    ret.ok()
}
