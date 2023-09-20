/*
 * Created on Fri Sep 15 2023
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

mod exchange;
mod handshake;
#[cfg(test)]
mod tests;

use {
    self::handshake::{CHandshake, HandshakeResult, HandshakeState},
    super::{IoResult, QLoopReturn, Socket},
    crate::engine::mem::BufferedScanner,
    bytes::{Buf, BytesMut},
    tokio::io::{AsyncReadExt, BufWriter},
};

pub async fn query_loop<S: Socket>(
    con: &mut BufWriter<S>,
    buf: &mut BytesMut,
) -> IoResult<QLoopReturn> {
    // handshake
    match do_handshake(con, buf).await? {
        Some(ret) => return Ok(ret),
        None => {}
    }
    // done handshaking
    loop {
        let read_many = con.read_buf(buf).await?;
        if let Some(t) = see_if_connection_terminates(read_many, buf) {
            return Ok(t);
        }
        todo!()
    }
}

fn see_if_connection_terminates(read_many: usize, buf: &[u8]) -> Option<QLoopReturn> {
    if read_many == 0 {
        // that's a connection termination
        if buf.is_empty() {
            // nice termination
            return Some(QLoopReturn::Fin);
        } else {
            return Some(QLoopReturn::ConnectionRst);
        }
    }
    None
}

async fn do_handshake<S: Socket>(
    con: &mut BufWriter<S>,
    buf: &mut BytesMut,
) -> IoResult<Option<QLoopReturn>> {
    let mut expected = CHandshake::INITIAL_READ;
    let mut state = HandshakeState::default();
    let mut cursor = 0;
    let handshake;
    loop {
        let read_many = con.read_buf(buf).await?;
        if let Some(t) = see_if_connection_terminates(read_many, buf) {
            return Ok(Some(t));
        }
        if buf.len() < expected {
            continue;
        }
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match handshake::CHandshake::resume_with(&mut scanner, state) {
            HandshakeResult::Completed(hs) => {
                handshake = hs;
                break;
            }
            HandshakeResult::ChangeState { new_state, expect } => {
                expected = expect;
                state = new_state;
                cursor = scanner.cursor();
            }
            HandshakeResult::Error(_) => todo!(),
        }
    }
    dbg!(handshake);
    buf.advance(cursor);
    Ok(None)
}
