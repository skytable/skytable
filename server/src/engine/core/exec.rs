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
    core::{dml, model::Model, space::Space},
    error::{QueryError, QueryResult},
    fractal::{Global, GlobalInstanceLike},
    net::protocol::{ClientLocalState, Response, SQuery},
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

#[inline(always)]
fn _call<A: ASTNode<'static> + core::fmt::Debug, T>(
    g: &Global,
    state: &mut State<'static, InplaceData>,
    f: impl FnOnce(&Global, A) -> Result<T, QueryError>,
) -> QueryResult<T> {
    let cs = ASTNode::parse_from_state_hardened(state)?;
    f(&g, cs)
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
    let c_s = (create & Token![space].eq(a) & last_id) as u8 * 2;
    let c_m = (create & Token![model].eq(a) & last_id) as u8 * 3;
    let a_s = (alter & Token![space].eq(a) & last_id) as u8 * 4;
    let a_m = (alter & Token![model].eq(a) & last_id) as u8 * 5;
    let d_s = (drop & Token![space].eq(a) & last_id) as u8 * 6;
    let d_m = (drop & Token![model].eq(a) & last_id) as u8 * 7;
    let fc = sysctl as u8 | c_s | c_m | a_s | a_m | d_s | d_m;
    state.cursor_ahead_if(!sysctl);
    static BLK_EXEC: [fn(
        Global,
        &ClientLocalState,
        &mut State<'static, InplaceData>,
    ) -> QueryResult<()>; 8] = [
        |_, _, _| Err(QueryError::QLUnknownStatement),
        blocking_exec_sysctl,
        |g, _, t| _call(&g, t, Space::transactional_exec_create),
        |g, _, t| _call(&g, t, Model::transactional_exec_create),
        |g, _, t| _call(&g, t, Space::transactional_exec_alter),
        |g, _, t| _call(&g, t, Model::transactional_exec_alter),
        |g, _, t| _call(&g, t, Space::transactional_exec_drop),
        |g, _, t| _call(&g, t, Model::transactional_exec_drop),
    ];
    let r = unsafe {
        // UNSAFE(@ohsayan): the only await is within this block
        let c_glob = global.clone();
        let static_cstate: &'static ClientLocalState = core::mem::transmute(cstate);
        let static_state: &'static mut State<'static, InplaceData> =
            core::mem::transmute(&mut state);
        tokio::task::spawn_blocking(move || {
            BLK_EXEC[fc as usize](c_glob, static_cstate, static_state)?;
            Ok(Response::Empty)
        })
        .await
    };
    r.unwrap()
}

fn blocking_exec_sysctl(
    g: Global,
    cstate: &ClientLocalState,
    state: &mut State<'static, InplaceData>,
) -> QueryResult<()> {
    let r = ASTNode::parse_from_state_hardened(state)?;
    super::dcl::exec(g, cstate, r)
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
            if !global.namespace().contains_space(new_space.as_str()) {
                return Err(QueryError::QExecObjectNotFound);
            }
            cstate.set_cs(new_space.boxed_str());
        }
    }
    Ok(Response::Empty)
}

fn run_nb(
    global: &Global,
    cstate: &mut ClientLocalState,
    state: State<'_, InplaceData>,
    stmt: KeywordStmt,
) -> QueryResult<Response> {
    let stmt = stmt.value_u8() - KeywordStmt::Use.value_u8();
    static F: [fn(
        &Global,
        &mut ClientLocalState,
        &mut State<'static, InplaceData>,
    ) -> QueryResult<Response>; 8] = [
        cstate_use,                                    // use
        |_, _, _| Err(QueryError::QLUnknownStatement), // inspect
        |_, _, _| Err(QueryError::QLUnknownStatement), // describe
        |g, _, s| _call(g, s, dml::insert_resp),
        |g, _, s| _call(g, s, dml::select_resp),
        |g, _, s| _call(g, s, dml::update_resp),
        |g, _, s| _call(g, s, dml::delete_resp),
        |_, _, _| Err(QueryError::QLUnknownStatement), // exists
    ];
    {
        let mut state = unsafe {
            // UNSAFE(@ohsayan): this is a lifetime issue with the token handle
            core::mem::transmute(state)
        };
        F[stmt as usize](global, cstate, &mut state)
    }
}
