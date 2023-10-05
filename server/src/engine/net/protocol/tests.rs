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
        exchange::{self, create_simple_query, QueryTimeExchangeResult, QueryTimeExchangeState},
        handshake::{
            AuthMode, CHandshake, CHandshakeAuth, CHandshakeStatic, DataExchangeMode,
            HandshakeResult, HandshakeState, HandshakeVersion, ProtocolVersion, QueryMode,
        },
    },
};

/*
    client handshake
*/

const FULL_HANDSHAKE_WITH_AUTH: [u8; 23] = *b"H\0\0\0\0\x015\n8\nsayanpass1234";

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
                        CHandshakeAuth::new(b"sayan", b"pass1234")
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
fn parse_auth_with_state_updates() {
    let rounds = run_state_changes_return_rounds(
        &FULL_HANDSHAKE_WITH_AUTH,
        CHandshake::new(
            STATIC_HANDSHAKE_WITH_AUTH,
            CHandshakeAuth::new(b"sayan", b"pass1234"),
        ),
    );
    assert_eq!(rounds, 3); // r1 = initial read, r2 = lengths, r3 = items
}

/*
    QT-DEX/SQ
*/

const SQ: &str = "select * from myspace.mymodel where username = ?";

#[test]
fn qtdex_simple_query() {
    let query = create_simple_query(SQ, ["sayan"]);
    let mut fin = 52;
    for i in 0..query.len() {
        let mut scanner = BufferedScanner::new(&query[..i + 1]);
        let result = exchange::resume(&mut scanner, Default::default());
        match scanner.buffer_len() {
            1..=3 => assert_eq!(result, exchange::STATE_READ_INITIAL),
            4 => assert_eq!(
                result,
                QueryTimeExchangeResult::ChangeState {
                    new_state: QueryTimeExchangeState::SQ2Meta2Partial {
                        size_of_static_frame: 4,
                        packet_size: 56,
                        q_window_part: 0,
                    },
                    expect_more: 56,
                }
            ),
            5 => assert_eq!(
                result,
                QueryTimeExchangeResult::ChangeState {
                    new_state: QueryTimeExchangeState::SQ2Meta2Partial {
                        size_of_static_frame: 4,
                        packet_size: 56,
                        q_window_part: 4,
                    },
                    expect_more: 55,
                }
            ),
            6 => assert_eq!(
                result,
                QueryTimeExchangeResult::ChangeState {
                    new_state: QueryTimeExchangeState::SQ2Meta2Partial {
                        size_of_static_frame: 4,
                        packet_size: 56,
                        q_window_part: 48,
                    },
                    expect_more: 54,
                }
            ),
            7 => assert_eq!(
                result,
                QueryTimeExchangeResult::ChangeState {
                    new_state: QueryTimeExchangeState::SQ3FinalizeWaitingForBlock {
                        dataframe_size: 53,
                        q_window: 48,
                    },
                    expect_more: 53,
                }
            ),
            8..=59 => {
                assert_eq!(
                    result,
                    QueryTimeExchangeResult::ChangeState {
                        new_state: QueryTimeExchangeState::SQ3FinalizeWaitingForBlock {
                            dataframe_size: 53,
                            q_window: 48
                        },
                        expect_more: fin,
                    }
                );
                fin -= 1;
            }
            60 => match result {
                QueryTimeExchangeResult::SQCompleted(sq) => {
                    assert_eq!(sq.query_str().unwrap(), SQ);
                    assert_eq!(sq.params_str().unwrap(), "sayan");
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }
}

#[test]
fn qtdex_simple_query_update_state() {
    let query = create_simple_query(SQ, ["sayan"]);
    let mut state = QueryTimeExchangeState::default();
    let mut cursor = 0;
    let mut expected = 0;
    let mut rounds = 0;
    loop {
        rounds += 1;
        let buf = &query[..expected + cursor];
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match exchange::resume(&mut scanner, state) {
            QueryTimeExchangeResult::SQCompleted(sq) => {
                assert_eq!(sq.query_str().unwrap(), SQ);
                assert_eq!(sq.params_str().unwrap(), "sayan");
                break;
            }
            QueryTimeExchangeResult::ChangeState {
                new_state,
                expect_more,
            } => {
                expected = expect_more;
                state = new_state;
            }
            QueryTimeExchangeResult::Error => panic!("hit error!"),
        }
        cursor = scanner.cursor();
    }
    assert_eq!(rounds, 3);
}
