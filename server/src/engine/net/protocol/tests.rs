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

use crate::engine::mem::BufferedScanner;
use crate::engine::net::protocol::handshake::{
    AuthMode, CHandshake, CHandshakeAuth, CHandshakeStatic, DataExchangeMode, HandshakeResult,
    HandshakeState, HandshakeVersion, ProtocolVersion, QueryMode,
};

const FULL_HANDSHAKE_NO_AUTH: [u8; 7] = [b'H', 0, 0, 0, 0, 0, 0];
const FULL_HANDSHAKE_WITH_AUTH: [u8; 23] = *b"H\0\0\0\0\x015\n8\nsayanpass1234";

const STATIC_HANDSHAKE_NO_AUTH: CHandshakeStatic = CHandshakeStatic::new(
    HandshakeVersion::Original,
    ProtocolVersion::Original,
    DataExchangeMode::QueryTime,
    QueryMode::Bql1,
    AuthMode::Anonymous,
);

const STATIC_HANDSHAKE_WITH_AUTH: CHandshakeStatic = CHandshakeStatic::new(
    HandshakeVersion::Original,
    ProtocolVersion::Original,
    DataExchangeMode::QueryTime,
    QueryMode::Bql1,
    AuthMode::Password,
);

/*
    handshake with no state changes
*/

#[test]
fn parse_staged_no_auth() {
    for i in 0..FULL_HANDSHAKE_NO_AUTH.len() {
        let buf = &FULL_HANDSHAKE_NO_AUTH[..i + 1];
        let mut scanner = BufferedScanner::new(buf);
        let result = CHandshake::resume_with(&mut scanner, HandshakeState::Initial);
        match buf.len() {
            1..=5 => {
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::Initial,
                        expect: CHandshake::INITIAL_READ,
                    }
                );
            }
            6 => {
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::StaticBlock(STATIC_HANDSHAKE_NO_AUTH),
                        expect: 1,
                    }
                );
            }
            7 => {
                assert_eq!(
                    result,
                    HandshakeResult::Completed(CHandshake::new(STATIC_HANDSHAKE_NO_AUTH, None))
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn parse_staged_with_auth() {
    for i in 0..FULL_HANDSHAKE_WITH_AUTH.len() {
        let buf = &FULL_HANDSHAKE_WITH_AUTH[..i + 1];
        let mut s = BufferedScanner::new(buf);
        let ref mut scanner = s;
        let result = CHandshake::resume_with(scanner, HandshakeState::Initial);
        match buf.len() {
            1..=5 => {
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::Initial,
                        expect: CHandshake::INITIAL_READ
                    }
                );
            }
            6..=9 => {
                // might seem funny that we don't parse the second integer at all, but it's because
                // of the relatively small size of the integers
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::StaticBlock(STATIC_HANDSHAKE_WITH_AUTH),
                        expect: 4
                    }
                );
            }
            10..=22 => {
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::ExpectingVariableBlock {
                            static_hs: STATIC_HANDSHAKE_WITH_AUTH,
                            uname_l: 5,
                            pwd_l: 8
                        },
                        expect: 13,
                    }
                );
            }
            23 => {
                assert_eq!(
                    result,
                    HandshakeResult::Completed(CHandshake::new(
                        STATIC_HANDSHAKE_WITH_AUTH,
                        Some(CHandshakeAuth::new(b"sayan", b"pass1234"))
                    ))
                );
            }
            _ => unreachable!(),
        }
    }
}

/*
    handshake with state changes
*/

fn run_state_changes_return_rounds(src: &[u8], expected_final_handshake: CHandshake) -> usize {
    let mut rounds = 0;
    let hs;
    let mut state = HandshakeState::default();
    let mut cursor = 0;
    let mut expect_many = CHandshake::INITIAL_READ;
    loop {
        rounds += 1;
        let buf = &src[..cursor + expect_many];
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match CHandshake::resume_with(&mut scanner, state) {
            HandshakeResult::ChangeState { new_state, expect } => {
                state = new_state;
                expect_many = expect;
                cursor = scanner.cursor();
            }
            HandshakeResult::Completed(c) => {
                hs = c;
                assert_eq!(hs, expected_final_handshake);
                break;
            }
            HandshakeResult::Error(e) => panic!("unexpected handshake error: {:?}", e),
        }
    }
    rounds
}

#[test]
fn parse_no_auth_with_state_updates() {
    let rounds = run_state_changes_return_rounds(
        &FULL_HANDSHAKE_NO_AUTH,
        CHandshake::new(STATIC_HANDSHAKE_NO_AUTH, None),
    );
    assert_eq!(rounds, 2); // r1 = initial, r2 = auth NUL
}

#[test]
fn parse_auth_with_state_updates() {
    let rounds = run_state_changes_return_rounds(
        &FULL_HANDSHAKE_WITH_AUTH,
        CHandshake::new(
            STATIC_HANDSHAKE_WITH_AUTH,
            Some(CHandshakeAuth::new(b"sayan", b"pass1234")),
        ),
    );
    assert_eq!(rounds, 3); // r1 = initial read, r2 = lengths, r3 = items
}
