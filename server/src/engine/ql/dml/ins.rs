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

use {
    super::read_ident,
    crate::{
        engine::{
            core::Datacell,
            error::{LangError, LangResult},
            ql::{
                ast::{Entity, QueryData, State},
                lex::{Ident, Token},
            },
        },
        util::{compiler, MaybeInit},
    },
    core::cmp,
    std::{
        collections::HashMap,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    uuid::Uuid,
};

/*
    Impls for insert
*/

pub const T_UUIDSTR: &str = "4593264b-0231-43e9-b0aa-50784f14e204";
pub const T_UUIDBIN: &[u8] = T_UUIDSTR.as_bytes();
pub const T_TIMESEC: u64 = 1673187839_u64;

type ProducerFn = fn() -> Datacell;

// base
#[inline(always)]
fn pfnbase_time() -> Duration {
    if cfg!(debug_assertions) {
        Duration::from_secs(T_TIMESEC)
    } else {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
    }
}
#[inline(always)]
fn pfnbase_uuid() -> Uuid {
    if cfg!(debug_assertions) {
        Uuid::parse_str(T_UUIDSTR).unwrap()
    } else {
        Uuid::new_v4()
    }
}
// impl
#[inline(always)]
fn pfn_timesec() -> Datacell {
    Datacell::new_uint(pfnbase_time().as_secs())
}
#[inline(always)]
fn pfn_uuidstr() -> Datacell {
    Datacell::new_str(pfnbase_uuid().to_string().into_boxed_str())
}
#[inline(always)]
fn pfn_uuidbin() -> Datacell {
    Datacell::new_bin(pfnbase_uuid().as_bytes().to_vec().into_boxed_slice())
}

static PRODUCER_G: [u8; 4] = [0, 2, 3, 0];
static PRODUCER_F: [(&[u8], ProducerFn); 3] = [
    (b"uuidstr", pfn_uuidstr),
    (b"uuidbin", pfn_uuidbin),
    (b"timesec", pfn_timesec),
];
const MAGIC_1: [u8; 7] = *b"cp21rLd";
const MAGIC_2: [u8; 7] = *b"zS8zgaK";
const MAGIC_L: usize = MAGIC_1.len();

#[inline(always)]
fn hashf(key: &[u8], m: &[u8]) -> u32 {
    let mut i = 0;
    let mut s = 0;
    while i < key.len() {
        s += m[(i % MAGIC_L) as usize] as u32 * key[i] as u32;
        i += 1;
    }
    s % PRODUCER_G.len() as u32
}
#[inline(always)]
fn hashp(key: &[u8]) -> u32 {
    (PRODUCER_G[hashf(key, &MAGIC_1) as usize] + PRODUCER_G[hashf(key, &MAGIC_2) as usize]) as u32
        % PRODUCER_G.len() as u32
}
#[inline(always)]
fn ldfunc(func: Ident<'_>) -> Option<ProducerFn> {
    let func = func.as_bytes();
    let ph = hashp(func) as usize;
    let min = cmp::min(ph, PRODUCER_F.len() - 1);
    let data = PRODUCER_F[min as usize];
    if data.0 == func {
        Some(data.1)
    } else {
        None
    }
}
#[inline(always)]
fn ldfunc_exists(func: Ident<'_>) -> bool {
    ldfunc(func).is_some()
}
#[inline(always)]
unsafe fn ldfunc_unchecked(func: &[u8]) -> ProducerFn {
    let ph = hashp(func) as usize;
    debug_assert_eq!(PRODUCER_F[ph as usize].0, func);
    PRODUCER_F[ph as usize].1
}

/// ## Panics
/// - If tt length is less than 1
pub(super) fn parse_list<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
    list: &mut Vec<Datacell>,
) -> Option<TagClass> {
    let mut stop = state.cursor_eq(Token![close []]);
    state.cursor_ahead_if(stop);
    let mut overall_dscr = None;
    let mut prev_nlist_dscr = None;
    while state.not_exhausted() && state.okay() && !stop {
        let d = match state.fw_read() {
            tok if state.can_read_lit_from(tok) => {
                let r = unsafe {
                    // UNSAFE(@ohsayan): the if guard guarantees correctness
                    state.read_lit_into_data_type_unchecked_from(tok)
                };
                r
            }
            Token![open []] => {
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
                Datacell::new_list(nested_list)
            }
            Token![@] if state.cursor_signature_match_fn_arity0_rounded() => match unsafe {
                // UNSAFE(@ohsayan): Just verified at guard
                handle_func_sub(state)
            } {
                Some(value) => value,
                None => {
                    state.poison();
                    break;
                }
            },
            _ => {
                state.cursor_back();
                state.poison();
                break;
            }
        };
        state.poison_if_not(list.is_empty() || d.kind() == list[0].kind());
        overall_dscr = Some(d.kind());
        list.push(d);
        let nx_comma = state.cursor_rounded_eq(Token![,]);
        let nx_csqrb = state.cursor_rounded_eq(Token![close []]);
        state.poison_if_not(nx_comma | nx_csqrb);
        state.cursor_ahead_if(state.okay());
        stop = nx_csqrb;
    }
    overall_dscr
}

#[inline(always)]
/// ## Safety
/// - Cursor must match arity(0) function signature
unsafe fn handle_func_sub<'a, Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> Option<Datacell> {
    let func = read_ident(state.fw_read());
    state.cursor_ahead_by(2); // skip tt:paren
    ldfunc(func).map(move |f| f())
}

/// ## Panics
/// - If tt is empty
pub(super) fn parse_data_tuple_syntax<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> Vec<Datacell> {
    let mut stop = state.cursor_eq(Token![() close]);
    state.cursor_ahead_if(stop);
    let mut data = Vec::new();
    while state.not_exhausted() && state.okay() && !stop {
        match state.fw_read() {
            tok if state.can_read_lit_from(tok) => unsafe {
                // UNSAFE(@ohsayan): if guard guarantees correctness
                data.push(state.read_lit_into_data_type_unchecked_from(tok));
            },
            Token![open []] if state.not_exhausted() => {
                let mut l = Vec::new();
                let _ = parse_list(state, &mut l);
                data.push(l.into());
            }
            Token![null] => data.push(Datacell::null()),
            Token![@] if state.cursor_signature_match_fn_arity0_rounded() => match unsafe {
                // UNSAFE(@ohsayan): Just verified at guard
                handle_func_sub(state)
            } {
                Some(value) => data.push(value),
                None => {
                    state.poison();
                    break;
                }
            },
            _ => {
                state.cursor_back();
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

/// ## Panics
/// Panics if tt is empty
pub(super) fn parse_data_map_syntax<'a, Qd: QueryData<'a>>(
    state: &mut State<'a, Qd>,
) -> HashMap<Ident<'a>, Datacell> {
    let mut stop = state.cursor_eq(Token![close {}]);
    state.cursor_ahead_if(stop);
    let mut data = HashMap::with_capacity(2);
    while state.has_remaining(3) && state.okay() && !stop {
        let field = state.fw_read();
        let colon = state.fw_read();
        let expr = state.fw_read();
        state.poison_if_not(Token![:].eq(colon));
        match (field, expr) {
            (Token::Ident(id), tok) if state.can_read_lit_from(tok) => {
                let ldata = unsafe {
                    // UNSAFE(@ohsayan): The if guard guarantees correctness
                    state.read_lit_into_data_type_unchecked_from(tok)
                };
                state.poison_if_not(data.insert(*id, ldata).is_none());
            }
            (Token::Ident(id), Token![null]) => {
                state.poison_if_not(data.insert(*id, Datacell::null()).is_none());
            }
            (Token::Ident(id), Token![open []]) if state.not_exhausted() => {
                let mut l = Vec::new();
                let _ = parse_list(state, &mut l);
                state.poison_if_not(data.insert(*id, l.into()).is_none());
            }
            (Token::Ident(id), Token![@]) if state.cursor_signature_match_fn_arity0_rounded() => {
                match unsafe {
                    // UNSAFE(@ohsayan): Just verified at guard
                    handle_func_sub(state)
                } {
                    Some(value) => state.poison_if_not(data.insert(*id, value).is_none()),
                    None => {
                        state.poison();
                        break;
                    }
                }
            }
            _ => {
                state.cursor_back_by(3);
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

#[derive(Debug, PartialEq)]
pub enum InsertData<'a> {
    Ordered(Vec<Datacell>),
    Map(HashMap<Ident<'a>, Datacell>),
}

impl<'a> From<Vec<Datacell>> for InsertData<'a> {
    fn from(v: Vec<Datacell>) -> Self {
        Self::Ordered(v)
    }
}

impl<'a> From<HashMap<Ident<'static>, Datacell>> for InsertData<'a> {
    fn from(m: HashMap<Ident<'static>, Datacell>) -> Self {
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
            return compiler::cold_rerr(LangError::UnexpectedEOS);
        }
        state.poison_if_not(state.cursor_eq(Token![into]));
        state.cursor_ahead(); // ignore errors

        // entity
        let mut entity = MaybeInit::uninit();
        Entity::parse_entity(state, &mut entity);
        let mut data = None;
        match state.fw_read() {
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
            compiler::cold_rerr(LangError::BadSyntax)
        }
    }
}

#[cfg(test)]
pub use impls::test::{DataMap, DataTuple, List};

use crate::engine::data::tag::TagClass;
mod impls {
    use {
        super::InsertStatement,
        crate::engine::{
            error::LangResult,
            ql::ast::{traits::ASTNode, QueryData, State},
        },
    };
    impl<'a> ASTNode<'a> for InsertStatement<'a> {
        fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
            Self::parse_insert(state)
        }
    }
    #[cfg(test)]
    pub mod test {
        use {
            super::super::{
                parse_data_map_syntax, parse_data_tuple_syntax, parse_list, Datacell, HashMap,
            },
            crate::engine::{
                error::LangResult,
                ql::ast::{traits::ASTNode, QueryData, State},
            },
        };
        #[derive(sky_macros::Wrapper, Debug)]
        pub struct List(Vec<Datacell>);
        impl<'a> ASTNode<'a> for List {
            // important: upstream must verify this
            const VERIFY: bool = true;
            fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
                let mut l = Vec::new();
                parse_list(state, &mut l);
                Ok(List(l))
            }
        }
        #[derive(sky_macros::Wrapper, Debug)]
        pub struct DataTuple(Vec<Datacell>);
        impl<'a> ASTNode<'a> for DataTuple {
            // important: upstream must verify this
            const VERIFY: bool = true;
            fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
                let r = parse_data_tuple_syntax(state);
                Ok(Self(r))
            }
        }
        #[derive(sky_macros::Wrapper, Debug)]
        pub struct DataMap(HashMap<Box<str>, Datacell>);
        impl<'a> ASTNode<'a> for DataMap {
            // important: upstream must verify this
            const VERIFY: bool = true;
            fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
                let r = parse_data_map_syntax(state);
                Ok(Self(
                    r.into_iter()
                        .map(|(ident, val)| (ident.to_string().into_boxed_str(), val))
                        .collect(),
                ))
            }
        }
    }
}