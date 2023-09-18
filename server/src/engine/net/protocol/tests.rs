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

use crate::engine::{
    mem::BufferedScanner,
    net::protocol::{
        data_exchange::{CSQuery, CSQueryExchangeResult, CSQueryState},
        handshake::{
            AuthMode, CHandshake, CHandshakeAuth, CHandshakeStatic, DataExchangeMode,
            HandshakeResult, HandshakeState, HandshakeVersion, ProtocolVersion, QueryMode,
        },
    },
};

/*
    client handshake
*/

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
            HandshakeResult::Completed(hs) => {
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

/*
    QT-DEX/SQ
*/

const FULL_SQ: [u8; 116] = *b"S111\nSELECT username, email, bio, profile_pic, following, followers, FROM mysocialapp.users WHERE username = 'sayan'";
const SQ_FULL: CSQuery<'static> = CSQuery::new(
    b"SELECT username, email, bio, profile_pic, following, followers, FROM mysocialapp.users WHERE username = 'sayan'"
);

#[test]
fn staged_qt_dex_sq() {
    for i in 0..FULL_SQ.len() {
        let buf = &FULL_SQ[..i + 1];
        let mut scanner = BufferedScanner::new(buf);
        let result = CSQuery::resume_with(&mut scanner, CSQueryState::default());
        match buf.len() {
            1..=3 => assert_eq!(
                result,
                CSQueryExchangeResult::ChangeState(CSQueryState::Initial, CSQuery::PREEMPTIVE_READ)
            ),
            4 => assert_eq!(
                result,
                CSQueryExchangeResult::ChangeState(CSQueryState::SizeSegmentPart(111), 2)
            ),
            5..=115 => assert_eq!(
                result,
                CSQueryExchangeResult::ChangeState(CSQueryState::WaitingForFullBlock(111), 111),
            ),
            116 => assert_eq!(result, CSQueryExchangeResult::Completed(SQ_FULL)),
            _ => unreachable!(),
        }
    }
}

#[test]
fn staged_with_status_switch_qt_dex_sq() {
    let mut cursor = 0;
    let mut expect = CSQuery::PREEMPTIVE_READ;
    let mut state = CSQueryState::default();
    let mut rounds = 0;
    loop {
        rounds += 1;
        let buf = &FULL_SQ[..cursor + expect];
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match CSQuery::resume_with(&mut scanner, state) {
            CSQueryExchangeResult::Completed(c) => {
                assert_eq!(c, SQ_FULL);
                break;
            }
            CSQueryExchangeResult::ChangeState(new_state, _expect) => {
                state = new_state;
                expect = _expect;
                cursor = scanner.cursor();
            }
            CSQueryExchangeResult::PacketError => panic!("packet error"),
        }
    }
    assert_eq!(rounds, 3);
}
