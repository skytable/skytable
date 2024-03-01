/*
 * Created on Thu Oct 05 2023
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

use crate::engine::{
    core::{ddl_misc, dml, model::ModelData, space::Space},
    error::{QueryError, QueryResult},
    fractal::{Global, GlobalInstanceLike},
    net::protocol::{ClientLocalState, Response, ResponseType, SQuery},
    ql::{
        ast::{traits::ASTNode, InplaceData, State},
        ddl::Use,
        lex::KeywordStmt,
    },
};

/*
    ---
    trigger warning: disgusting hacks below owing to token lifetimes
*/

pub async fn dispatch_to_executor<'a>(
    global: &Global,
    cstate: &mut ClientLocalState,
    query: SQuery<'a>,
) -> QueryResult<Response> {
    let tokens =
        crate::engine::ql::lex::SecureLexer::new_with_segments(query.query(), query.params())
            .lex()?;
    let mut state = State::new_inplace(&tokens);
    state.set_space_maybe(unsafe {
        // UNSAFE(@ohsayan): exclusively used within this scope
        core::mem::transmute(cstate.get_cs())
    });
    let stmt = state.try_statement()?;
    if stmt.is_blocking() {
        run_blocking_stmt(global, cstate, state, stmt).await
    } else {
        run_nb(global, cstate, state, stmt)
    }
}

fn _callgs_map<A: ASTNode<'static> + core::fmt::Debug, T>(
    g: &Global,
    state: &mut State<'static, InplaceData>,
    f: impl FnOnce(&Global, A) -> Result<T, QueryError>,
    map: impl FnOnce(T) -> Response,
) -> QueryResult<Response> {
    let cs = ASTNode::parse_from_state_hardened(state)?;
    Ok(map(f(&g, cs)?))
}

#[inline(always)]
fn _callgs<A: ASTNode<'static> + core::fmt::Debug, T>(
    g: &Global,
    state: &mut State<'static, InplaceData>,
    f: impl FnOnce(&Global, A) -> Result<T, QueryError>,
) -> QueryResult<T> {
    let cs = ASTNode::parse_from_state_hardened(state)?;
    f(&g, cs)
}

#[inline(always)]
fn _callgcs<A: ASTNode<'static> + core::fmt::Debug, T>(
    g: &Global,
    cstate: &ClientLocalState,
    state: &mut State<'static, InplaceData>,
    f: impl FnOnce(&Global, &ClientLocalState, A) -> Result<T, QueryError>,
) -> QueryResult<T> {
    let a = ASTNode::parse_from_state_hardened(state)?;
    f(&g, cstate, a)
}

#[inline(always)]
fn translate_ddl_result(x: Option<bool>) -> Response {
    match x {
        Some(b) => Response::Bool(b),
        None => Response::Empty,
    }
}

async fn run_blocking_stmt(
    global: &Global,
    cstate: &mut ClientLocalState,
    mut state: State<'_, InplaceData>,
    stmt: KeywordStmt,
) -> Result<Response, QueryError> {
    if !(cstate.is_root() | (stmt == KeywordStmt::Sysctl)) {
        // all the actions here need root permission (but we do an exception for sysctl which allows status to be called by anyone)
        return Err(QueryError::SysPermissionDenied);
    }
    state.ensure_minimum_for_blocking_stmt()?;
    /*
        IMPORTANT: DDL queries will NOT pick up the currently set space. instead EVERY DDL query must manually fully specify the entity that
        they want to manipulate. this prevents a whole set of exciting errors like dropping a model with the same model name from another space
    */
    state.unset_space();
    let (a, b) = (&state.current()[0], &state.current()[1]);
    let sysctl = stmt == KeywordStmt::Sysctl;
    let create = stmt == KeywordStmt::Create;
    let alter = stmt == KeywordStmt::Alter;
    let drop = stmt == KeywordStmt::Drop;
    let last_id = b.is_ident();
    let last_allow = Token![allow].eq(b);
    let last_if = Token![if].eq(b);
    let c_s = (create & Token![space].eq(a) & (last_id | last_if)) as u8 * 2;
    let c_m = (create & Token![model].eq(a) & (last_id | last_if)) as u8 * 3;
    let a_s = (alter & Token![space].eq(a) & last_id) as u8 * 4;
    let a_m = (alter & Token![model].eq(a) & last_id) as u8 * 5;
    let d_s = (drop & Token![space].eq(a) & (last_id | last_allow | last_if)) as u8 * 6;
    let d_m = (drop & Token![model].eq(a) & (last_id | last_allow | last_if)) as u8 * 7;
    let fc = sysctl as u8 | c_s | c_m | a_s | a_m | d_s | d_m;
    state.cursor_ahead_if(!sysctl);
    static BLK_EXEC: [fn(
        Global,
        &ClientLocalState,
        &mut State<'static, InplaceData>,
    ) -> QueryResult<Response>; 8] = [
        |_, _, _| Err(QueryError::QLUnknownStatement),
        blocking_exec_sysctl,
        |g, _, t| {
            _callgs_map(
                &g,
                t,
                Space::transactional_exec_create,
                translate_ddl_result,
            )
        },
        |g, _, t| {
            _callgs_map(
                &g,
                t,
                ModelData::transactional_exec_create,
                translate_ddl_result,
            )
        },
        |g, _, t| _callgs_map(&g, t, Space::transactional_exec_alter, |_| Response::Empty),
        |g, _, t| {
            _callgs_map(&g, t, ModelData::transactional_exec_alter, |_| {
                Response::Empty
            })
        },
        |g, _, t| _callgs_map(&g, t, Space::transactional_exec_drop, translate_ddl_result),
        |g, _, t| {
            _callgs_map(
                &g,
                t,
                ModelData::transactional_exec_drop,
                translate_ddl_result,
            )
        },
    ];
    let r = unsafe {
        // UNSAFE(@ohsayan): the only await is within this block
        let c_glob = global.clone();
        let static_cstate: &'static ClientLocalState = core::mem::transmute(cstate);
        let static_state: &'static mut State<'static, InplaceData> =
            core::mem::transmute(&mut state);
        tokio::task::spawn_blocking(move || {
            BLK_EXEC[fc as usize](c_glob, static_cstate, static_state)
        })
        .await
    };
    r.unwrap()
}

fn blocking_exec_sysctl(
    g: Global,
    cstate: &ClientLocalState,
    state: &mut State<'static, InplaceData>,
) -> QueryResult<Response> {
    let r = ASTNode::parse_from_state_hardened(state)?;
    super::dcl::exec(g, cstate, r).map(|_| Response::Empty)
}

/*
    nb exec
*/

fn cstate_use(
    global: &Global,
    cstate: &mut ClientLocalState,
    state: &mut State<'static, InplaceData>,
) -> QueryResult<Response> {
    let use_c = Use::parse_from_state_hardened(state)?;
    match use_c {
        Use::Null => cstate.unset_cs(),
        Use::Space(new_space) => {
            /*
                NB: just like SQL, we don't really care about what this is set to as it's basically a shorthand.
                so we do a simple vanity check
            */
            if !global
                .state()
                .namespace()
                .contains_space(new_space.as_str())
            {
                return Err(QueryError::QExecObjectNotFound);
            }
            cstate.set_cs(new_space.boxed_str());
        }
        Use::RefreshCurrent => match cstate.get_cs() {
            None => return Ok(Response::Null),
            Some(space) => {
                if !global.state().namespace().contains_space(space) {
                    cstate.unset_cs();
                    return Err(QueryError::QExecObjectNotFound);
                }
                return Ok(Response::Serialized {
                    ty: ResponseType::String,
                    size: space.len(),
                    data: space.to_owned().into_bytes(),
                });
            }
        },
    }
    Ok(Response::Empty)
}

fn run_nb(
    global: &Global,
    cstate: &mut ClientLocalState,
    mut state: State<'_, InplaceData>,
    stmt: KeywordStmt,
) -> QueryResult<Response> {
    let stmt_c = stmt.value_u8() - KeywordStmt::Use.value_u8();
    static F: [fn(
        &Global,
        &mut ClientLocalState,
        &mut State<'static, InplaceData>,
    ) -> QueryResult<Response>; 9] = [
        cstate_use, // use
        |g, c, s| _callgcs(g, c, s, ddl_misc::inspect),
        |_, _, _| Err(QueryError::QLUnknownStatement), // describe
        |g, _, s| _callgs(g, s, dml::insert_resp),
        |g, _, s| _callgs(g, s, dml::select_resp),
        |g, _, s| _callgs(g, s, dml::update_resp),
        |g, _, s| _callgs(g, s, dml::delete_resp),
        |_, _, _| Err(QueryError::QLUnknownStatement), // exists
        |g, _, s| _callgs(g, s, dml::select_all_resp),
    ];
    {
        let n_offset_adjust = (stmt == KeywordStmt::Select) & state.cursor_rounded_eq(Token![all]);
        state.cursor_ahead_if(n_offset_adjust);
        let corrected_offset = (n_offset_adjust as u8 * 8) | (stmt_c * (!n_offset_adjust as u8));
        let mut state = unsafe {
            // UNSAFE(@ohsayan): this is a lifetime issue with the token handle
            core::mem::transmute(state)
        };
        F[corrected_offset as usize](global, cstate, &mut state)
    }
}
