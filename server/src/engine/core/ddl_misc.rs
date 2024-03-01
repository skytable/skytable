/*
 * Created on Thu Nov 30 2023
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
    error::{QueryError, QueryResult},
    fractal::GlobalInstanceLike,
    net::protocol::{ClientLocalState, Response, ResponseType},
    ql::ddl::Inspect,
};

pub fn inspect(
    g: &impl GlobalInstanceLike,
    c: &ClientLocalState,
    stmt: Inspect,
) -> QueryResult<Response> {
    let ret = match stmt {
        Inspect::Global => {
            // collect spaces
            let spaces = g.state().namespace().idx().read();
            let mut spaces_iter = spaces.iter().peekable();
            let mut ret = format!("{{\"spaces\":[");
            while let Some((space, _)) = spaces_iter.next() {
                ret.push('"');
                ret.push_str(&space);
                ret.push('"');
                if spaces_iter.peek().is_some() {
                    ret.push(',');
                }
            }
            if c.is_root() {
                // iff the user is root, show information about other users. if not, just show models and settings
                ret.push_str("],\"users\":[");
                drop(spaces_iter);
                drop(spaces);
                // collect users
                let users = g.state().namespace().sys_db().users().read();
                let mut users_iter = users.iter().peekable();
                while let Some((user, _)) = users_iter.next() {
                    ret.push('"');
                    ret.push_str(&user);
                    ret.push('"');
                    if users_iter.peek().is_some() {
                        ret.push(',');
                    }
                }
            }
            ret.push_str("],\"settings\":{}}");
            ret
        }
        Inspect::Model(m) => match g.state().namespace().idx_models().read().get(&m) {
            Some(m) => {
                let m = m.data();
                format!(
                    "{{\"decl\":\"{}\",\"rows\":{},\"properties\":{{}}}}",
                    m.describe(),
                    m.primary_index().count()
                )
            }
            None => return Err(QueryError::QExecObjectNotFound),
        },
        Inspect::Space(s) => match g.state().namespace().idx().read().get(s.as_str()) {
            Some(s) => {
                let mut ret = format!("{{\"models\":[");
                let mut models_iter = s.models().iter().peekable();
                while let Some(mdl) = models_iter.next() {
                    ret.push('\"');
                    ret.push_str(&mdl);
                    ret.push('\"');
                    if models_iter.peek().is_some() {
                        ret.push(',');
                    }
                }
                ret.push_str("]}}");
                ret
            }
            None => return Err(QueryError::QExecObjectNotFound),
        },
    };
    Ok(Response::Serialized {
        ty: ResponseType::String,
        size: ret.len(),
        data: ret.into_bytes(),
    })
}
