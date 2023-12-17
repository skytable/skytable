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

// re-export
pub use exchange::SQuery;

use {
    self::{
        exchange::{QExchangeResult, QExchangeState},
        handshake::{
            AuthMode, CHandshake, DataExchangeMode, HandshakeResult, HandshakeState,
            HandshakeVersion, ProtocolError, ProtocolVersion, QueryMode,
        },
    },
    super::{IoResult, QueryLoopResult, Socket},
    crate::engine::{
        self,
        error::QueryError,
        fractal::{Global, GlobalInstanceLike},
        mem::{BufferedScanner, IntegerRepr},
    },
    bytes::{Buf, BytesMut},
    tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter},
};

#[repr(u8)]
#[derive(sky_macros::EnumMethods, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(unused)]
pub enum ResponseType {
    Null = 0x00,
    Bool = 0x01,
    UInt8 = 0x02,
    UInt16 = 0x03,
    UInt32 = 0x04,
    UInt64 = 0x05,
    SInt8 = 0x06,
    SInt16 = 0x07,
    SInt32 = 0x08,
    SInt64 = 0x09,
    Float32 = 0x0A,
    Float64 = 0x0B,
    Binary = 0x0C,
    String = 0x0D,
    List = 0x0E,
    Dict = 0x0F,
    Error = 0x10,
    Row = 0x11,
    Empty = 0x12,
    MultiRow = 0x13,
}

#[derive(Debug, PartialEq)]
pub struct ClientLocalState {
    username: Box<str>,
    root: bool,
    hs: handshake::CHandshakeStatic,
    cs: Option<Box<str>>,
}

impl ClientLocalState {
    pub fn new(username: Box<str>, root: bool, hs: handshake::CHandshakeStatic) -> Self {
        Self {
            username,
            root,
            hs,
            cs: None,
        }
    }
    pub fn is_root(&self) -> bool {
        self.root
    }
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn set_cs(&mut self, new: Box<str>) {
        self.cs = Some(new);
    }
    pub fn unset_cs(&mut self) {
        self.cs = None;
    }
    pub fn get_cs(&self) -> Option<&str> {
        self.cs.as_deref()
    }
}

#[derive(Debug, PartialEq)]
pub enum Response {
    Empty,
    Null,
    Serialized {
        ty: ResponseType,
        size: usize,
        data: Vec<u8>,
    },
    Bool(bool),
}

pub(super) async fn query_loop<S: Socket>(
    con: &mut BufWriter<S>,
    buf: &mut BytesMut,
    global: &Global,
) -> IoResult<QueryLoopResult> {
    // handshake
    let mut client_state = match do_handshake(con, buf, global).await? {
        PostHandshake::Okay(hs) => hs,
        PostHandshake::ConnectionClosedFin => return Ok(QueryLoopResult::Fin),
        PostHandshake::ConnectionClosedRst => return Ok(QueryLoopResult::Rst),
        PostHandshake::Error(e) => {
            // failed to handshake; we'll close the connection
            let hs_err_packet = [b'H', 0, 1, e.value_u8()];
            con.write_all(&hs_err_packet).await?;
            return Ok(QueryLoopResult::HSFailed);
        }
    };
    // done handshaking
    con.write_all(b"H\x00\x00\x00").await?;
    con.flush().await?;
    let mut state = QExchangeState::default();
    let mut cursor = Default::default();
    loop {
        if con.read_buf(buf).await? == 0 {
            if buf.is_empty() {
                return Ok(QueryLoopResult::Fin);
            } else {
                return Ok(QueryLoopResult::Rst);
            }
        }
        if !state.has_reached_target(buf) {
            // we haven't buffered sufficient bytes; keep working
            continue;
        }
        let sq = match unsafe {
            // UNSAFE(@ohsayan): as the resume cursor is private, we can't access this anyways
            exchange::resume(buf, cursor, state)
        } {
            (_, QExchangeResult::SQCompleted(sq)) => sq,
            (new_cursor, QExchangeResult::ChangeState(new_state)) => {
                cursor = new_cursor;
                state = new_state;
                continue;
            }
            (_, QExchangeResult::Error) => {
                // respond with error
                let [a, b] = (QueryError::SysNetworkSystemIllegalClientPacket.value_u8() as u16)
                    .to_le_bytes();
                con.write_all(&[ResponseType::Error.value_u8(), a, b])
                    .await?;
                con.flush().await?;
                // reset buffer, cursor and state
                buf.clear();
                cursor = Default::default();
                state = QExchangeState::default();
                continue;
            }
        };
        // now execute query
        match engine::core::exec::dispatch_to_executor(global, &mut client_state, sq).await {
            Ok(Response::Empty) => {
                con.write_all(&[ResponseType::Empty.value_u8()]).await?;
            }
            Ok(Response::Serialized { ty, size, data }) => {
                con.write_u8(ty.value_u8()).await?;
                let mut irep = IntegerRepr::new();
                con.write_all(irep.as_bytes(size as u64)).await?;
                con.write_u8(b'\n').await?;
                con.write_all(&data).await?;
            }
            Ok(Response::Bool(b)) => {
                con.write_all(&[ResponseType::Bool.value_u8(), b as u8])
                    .await?
            }
            Ok(Response::Null) => con.write_u8(ResponseType::Null.value_u8()).await?,
            Err(e) => {
                let [a, b] = (e.value_u8() as u16).to_le_bytes();
                con.write_all(&[ResponseType::Error.value_u8(), a, b])
                    .await?;
            }
        }
        con.flush().await?;
        // reset buffer, cursor and state
        buf.clear();
        cursor = Default::default();
        state = QExchangeState::default();
    }
}

#[derive(Debug, PartialEq)]
enum PostHandshake {
    Okay(ClientLocalState),
    Error(ProtocolError),
    ConnectionClosedFin,
    ConnectionClosedRst,
}

async fn do_handshake<S: Socket>(
    con: &mut BufWriter<S>,
    buf: &mut BytesMut,
    global: &Global,
) -> IoResult<PostHandshake> {
    let mut expected = CHandshake::INITIAL_READ;
    let mut state = HandshakeState::default();
    let mut cursor = 0;
    let handshake;
    loop {
        let read_many = con.read_buf(buf).await?;
        if read_many == 0 {
            if buf.is_empty() {
                return Ok(PostHandshake::ConnectionClosedFin);
            } else {
                return Ok(PostHandshake::ConnectionClosedRst);
            }
        }
        if buf.len() < expected {
            continue;
        }
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match handshake::CHandshake::resume_with(&mut scanner, state) {
            HandshakeResult::Completed(hs) => {
                handshake = hs;
                cursor = scanner.cursor();
                break;
            }
            HandshakeResult::ChangeState { new_state, expect } => {
                expected = expect;
                state = new_state;
                cursor = scanner.cursor();
            }
            HandshakeResult::Error(e) => {
                return Ok(PostHandshake::Error(e));
            }
        }
    }
    // check handshake
    if cfg!(debug_assertions) {
        assert_eq!(
            handshake.hs_static().hs_version(),
            HandshakeVersion::Original
        );
        assert_eq!(handshake.hs_static().protocol(), ProtocolVersion::Original);
        assert_eq!(
            handshake.hs_static().exchange_mode(),
            DataExchangeMode::QueryTime
        );
        assert_eq!(handshake.hs_static().query_mode(), QueryMode::Bql1);
        assert_eq!(handshake.hs_static().auth_mode(), AuthMode::Password);
    }
    match core::str::from_utf8(handshake.hs_auth().username()) {
        Ok(uname) => {
            let auth = global.sys_store().system_store().auth_data().read();
            let r = auth.verify_user_check_root(uname, handshake.hs_auth().password());
            match r {
                Ok(is_root) => {
                    let hs = handshake.hs_static();
                    let ret = Ok(PostHandshake::Okay(ClientLocalState::new(
                        uname.into(),
                        is_root,
                        hs,
                    )));
                    buf.advance(cursor);
                    return ret;
                }
                Err(_) => {}
            }
        }
        Err(_) => {}
    };
    Ok(PostHandshake::Error(ProtocolError::RejectAuth))
}
