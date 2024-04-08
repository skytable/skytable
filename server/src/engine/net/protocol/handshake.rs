/*
 * Created on Mon Sep 18 2023
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

use crate::{
    engine::mem::scanner::{BufferedScanner, ScannerDecodeResult},
    util::compiler::{self, TaggedEnum},
};

#[derive(Debug, PartialEq, Eq, Clone, Copy, sky_macros::EnumMethods)]
#[repr(u8)]
/// Low-level protocol errors
pub enum ProtocolError {
    /// packet has incorrect structure
    CorruptedHSPacket = 0,
    /// incorrect handshake version
    RejectHSVersion = 1,
    /// invalid protocol version
    RejectProtocol = 2,
    /// invalid exchange mode
    RejectExchangeMode = 3,
    /// invalid query mode
    RejectQueryMode = 4,
    /// invalid auth details
    ///
    /// **NB**: this can be due to either an incorrect auth flag, or incorrect auth data or disallowed auth mode. we keep it
    /// in one error for purposes of security
    RejectAuth = 5,
}

impl ProtocolError {
    #[cold]
    unsafe fn get_error(
        invalid_first_byte: bool,
        invalid_hs_version: bool,
        invalid_proto_version: bool,
        invalid_exchange_mode: bool,
        invalid_query_mode: bool,
        invalid_auth_mode: bool,
    ) -> ProtocolError {
        if invalid_first_byte {
            ProtocolError::CorruptedHSPacket
        } else if invalid_hs_version {
            ProtocolError::RejectHSVersion
        } else if invalid_proto_version {
            ProtocolError::RejectProtocol
        } else if invalid_exchange_mode {
            ProtocolError::RejectExchangeMode
        } else if invalid_query_mode {
            ProtocolError::RejectQueryMode
        } else if invalid_auth_mode {
            ProtocolError::RejectAuth
        } else {
            impossible!()
        }
    }
}

/*
    handshake meta
*/

#[derive(Debug, PartialEq, Eq, Clone, Copy, sky_macros::EnumMethods, sky_macros::TaggedEnum)]
#[repr(u8)]
/// the handshake version
pub enum HandshakeVersion {
    /// Skyhash/2.0 HS
    Original = 0,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, sky_macros::EnumMethods, sky_macros::TaggedEnum)]
#[repr(u8)]
/// the skyhash protocol version
pub enum ProtocolVersion {
    /// Skyhash/2.0 protocol
    Original = 0,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, sky_macros::EnumMethods, sky_macros::TaggedEnum)]
#[repr(u8)]
/// the data exchange mode
pub enum DataExchangeMode {
    /// query-time data exchange mode
    QueryTime = 0,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, sky_macros::EnumMethods, sky_macros::TaggedEnum)]
#[repr(u8)]
/// the query mode
pub enum QueryMode {
    /// BQL-1 query mode
    Bql1 = 0,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, sky_macros::EnumMethods, sky_macros::TaggedEnum)]
#[repr(u8)]
/// the authentication mode
pub enum AuthMode {
    Password = 0,
}

impl AuthMode {
    /// returns the minimum number of metadata bytes need to parse the payload for this auth mode
    const fn min_payload_bytes(&self) -> usize {
        match self {
            Self::Password => 4,
        }
    }
}

/*
    client handshake
*/

/// The handshake state
#[derive(Debug, PartialEq, Clone)]
pub enum HandshakeState {
    /// we just began the handshake
    Initial,
    /// we just processed the static block
    StaticBlock(CHandshakeStatic),
    /// Expecting some more auth meta
    ExpectingMetaForVariableBlock {
        /// static block
        static_hs: CHandshakeStatic,
        /// uname len
        uname_l: usize,
    },
    /// we're expecting to finish the handshake
    ExpectingVariableBlock {
        /// static block
        static_hs: CHandshakeStatic,
        /// uname len
        uname_l: usize,
        /// pwd len
        pwd_l: usize,
    },
}

impl Default for HandshakeState {
    fn default() -> Self {
        Self::Initial
    }
}

/// The static segment of the handshake
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CHandshakeStatic {
    /// the handshake version
    hs_version: HandshakeVersion,
    /// protocol version
    protocol: ProtocolVersion,
    /// exchange mode
    exchange_mode: DataExchangeMode,
    /// query mode
    query_mode: QueryMode,
    /// authentication mode
    auth_mode: AuthMode,
}

impl CHandshakeStatic {
    pub const fn new(
        hs_version: HandshakeVersion,
        protocol: ProtocolVersion,
        exchange_mode: DataExchangeMode,
        query_mode: QueryMode,
        auth_mode: AuthMode,
    ) -> Self {
        Self {
            hs_version,
            protocol,
            exchange_mode,
            query_mode,
            auth_mode,
        }
    }
    pub fn hs_version(&self) -> HandshakeVersion {
        self.hs_version
    }
    pub fn protocol(&self) -> ProtocolVersion {
        self.protocol
    }
    pub fn exchange_mode(&self) -> DataExchangeMode {
        self.exchange_mode
    }
    pub fn query_mode(&self) -> QueryMode {
        self.query_mode
    }
    pub fn auth_mode(&self) -> AuthMode {
        self.auth_mode
    }
}

/// handshake authentication
// TODO(@ohsayan): enum?
#[derive(Debug, PartialEq)]
pub struct CHandshakeAuth<'a> {
    username: &'a [u8],
    password: &'a [u8],
}

impl<'a> CHandshakeAuth<'a> {
    pub fn new(username: &'a [u8], password: &'a [u8]) -> Self {
        Self { username, password }
    }
    pub fn username(&self) -> &[u8] {
        self.username
    }
    pub fn password(&self) -> &[u8] {
        self.password
    }
}

#[derive(Debug, PartialEq)]
pub enum HandshakeResult<'a> {
    /// Finished handshake
    Completed(CHandshake<'a>),
    /// Update handshake state
    ChangeState { new_state: HandshakeState },
    /// An error occurred
    Error(ProtocolError),
}

/// The client's handshake record
#[derive(Debug, PartialEq)]
pub struct CHandshake<'a> {
    /// the static segment of the handshake
    hs_static: CHandshakeStatic,
    /// the auth section of the dynamic segment of the handshake
    hs_auth: CHandshakeAuth<'a>,
}

impl<'a> CHandshake<'a> {
    pub const INITIAL_READ: usize = 6;
    const CLIENT_HELLO: u8 = b'H';
    pub fn new(hs_static: CHandshakeStatic, hs_auth: CHandshakeAuth<'a>) -> Self {
        Self { hs_static, hs_auth }
    }
    /// Resume handshake with the given state and buffer
    pub fn resume_with(
        scanner: &mut BufferedScanner<'a>,
        state: HandshakeState,
    ) -> HandshakeResult<'a> {
        match state {
            // nothing buffered yet
            HandshakeState::Initial => Self::resume_initial(scanner),
            // buffered static block
            HandshakeState::StaticBlock(static_block) => {
                Self::resume_at_auth_metadata1(scanner, static_block)
            }
            // buffered some auth meta
            HandshakeState::ExpectingMetaForVariableBlock { static_hs, uname_l } => {
                Self::resume_at_auth_metadata2(scanner, static_hs, uname_l)
            }
            // buffered full auth meta
            HandshakeState::ExpectingVariableBlock {
                static_hs,
                uname_l,
                pwd_l,
            } => Self::resume_at_variable_block_payload(scanner, static_hs, uname_l, pwd_l),
        }
    }
    pub fn hs_static(&self) -> CHandshakeStatic {
        self.hs_static
    }
    pub fn hs_auth(&self) -> &CHandshakeAuth<'a> {
        &self.hs_auth
    }
}

impl<'a> CHandshake<'a> {
    /// Resume from the initial state (nothing buffered yet)
    fn resume_initial(scanner: &mut BufferedScanner<'a>) -> HandshakeResult<'a> {
        // get our block
        if scanner.remaining() < Self::INITIAL_READ {
            return HandshakeResult::ChangeState {
                new_state: HandshakeState::Initial,
            };
        }
        let buf: [u8; CHandshake::INITIAL_READ] = unsafe {
            // UNSAFE(@ohsayan): validated in earlier branch
            scanner.next_chunk()
        };
        let invalid_first_byte = buf[0] != Self::CLIENT_HELLO;
        let invalid_hs_version = buf[1] > HandshakeVersion::MAX_DSCR;
        let invalid_proto_version = buf[2] > ProtocolVersion::MAX_DSCR;
        let invalid_exchange_mode = buf[3] > DataExchangeMode::MAX_DSCR;
        let invalid_query_mode = buf[4] > QueryMode::MAX_DSCR;
        let invalid_auth_mode = buf[5] > AuthMode::MAX_DSCR;
        // check block
        if compiler::unlikely(
            invalid_first_byte
                | invalid_hs_version
                | invalid_proto_version
                | invalid_exchange_mode
                | invalid_query_mode
                | invalid_auth_mode,
        ) {
            return HandshakeResult::Error(unsafe {
                // UNSAFE(@ohsayan): it is guaranteed by the branch that one or more of these booleans are true
                ProtocolError::get_error(
                    invalid_first_byte,
                    invalid_hs_version,
                    invalid_proto_version,
                    invalid_exchange_mode,
                    invalid_query_mode,
                    invalid_auth_mode,
                )
            });
        }
        // init header
        let static_header = CHandshakeStatic::new(
            HandshakeVersion::Original,
            ProtocolVersion::Original,
            DataExchangeMode::QueryTime,
            QueryMode::Bql1,
            unsafe {
                // UNSAFE(@ohsayan): already checked
                AuthMode::from_raw(buf[5])
            },
        );
        // check if we have auth data
        Self::resume_at_auth_metadata1(scanner, static_header)
    }
    fn resume_at_variable_block_payload(
        scanner: &mut BufferedScanner<'a>,
        static_hs: CHandshakeStatic,
        uname_l: usize,
        pwd_l: usize,
    ) -> HandshakeResult<'a> {
        if scanner.has_left(uname_l + pwd_l) {
            // we're done here
            return unsafe {
                // UNSAFE(@ohsayan): we just checked buffered size
                let uname = scanner.next_chunk_variable(uname_l);
                let pwd = scanner.next_chunk_variable(pwd_l);
                HandshakeResult::Completed(Self::new(static_hs, CHandshakeAuth::new(uname, pwd)))
            };
        }
        HandshakeResult::ChangeState {
            new_state: HandshakeState::ExpectingVariableBlock {
                static_hs,
                uname_l,
                pwd_l,
            },
        }
    }
}

impl<'a> CHandshake<'a> {
    /// Resume parsing at the first part of the auth metadata
    fn resume_at_auth_metadata1(
        scanner: &mut BufferedScanner<'a>,
        static_header: CHandshakeStatic,
    ) -> HandshakeResult<'a> {
        // now let's see if we have buffered enough data for auth
        if scanner.remaining() < static_header.auth_mode.min_payload_bytes() {
            // we need more data
            return HandshakeResult::ChangeState {
                new_state: HandshakeState::StaticBlock(static_header),
            };
        }
        // we seem to have enough data for this auth mode
        match static_header.auth_mode {
            AuthMode::Password => {}
        }
        // let us see if we can parse the username length
        let uname_l = match scanner.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor()
        {
            ScannerDecodeResult::NeedMore => {
                return HandshakeResult::ChangeState {
                    new_state: HandshakeState::StaticBlock(static_header),
                };
            }
            ScannerDecodeResult::Value(v) => v as usize,
            ScannerDecodeResult::Error => {
                return HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
            }
        };
        Self::resume_at_auth_metadata2(scanner, static_header, uname_l)
    }
    /// Resume at trying to get the final part of the auth metadata
    fn resume_at_auth_metadata2(
        scanner: &mut BufferedScanner<'a>,
        static_hs: CHandshakeStatic,
        uname_l: usize,
    ) -> HandshakeResult<'a> {
        // we just have to get the password len
        let pwd_l = match scanner.try_next_ascii_u64_lf_separated_with_result_or_restore_cursor() {
            ScannerDecodeResult::Value(v) => v as usize,
            ScannerDecodeResult::NeedMore => {
                // newline missing (or maybe there's more?)
                return HandshakeResult::ChangeState {
                    new_state: HandshakeState::ExpectingMetaForVariableBlock { static_hs, uname_l },
                };
            }
            ScannerDecodeResult::Error => {
                return HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
            }
        };
        Self::resume_at_variable_block_payload(scanner, static_hs, uname_l, pwd_l)
    }
}
