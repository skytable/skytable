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

use {
    super::{
        exchange::{Exchange, ExchangeError, ExchangeResult, ExchangeState},
        handshake::ProtocolError,
    },
    crate::engine::{
        mem::BufferedScanner,
        net::protocol::{
            exchange::{PipeState, SQState, Usize},
            handshake::{
                AuthMode, CHandshake, CHandshakeAuth, CHandshakeStatic, DataExchangeMode,
                HandshakeResult, HandshakeState, HandshakeVersion, ProtocolVersion, QueryMode,
            },
            SQuery,
        },
    },
    std::ops::Range,
};

/*
    client handshake
*/

const FULL_HANDSHAKE_WITH_AUTH: [u8; 23] = *b"H\0\0\0\0\05\n8\nsayanpass1234";

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
fn handshake_with_multiple_errors() {
    for (bad_hs, error) in [
        // all incorrect
        (
            b"H\xFF\xFF\xFF\xFF\xFF5\n8\nsayanpass1234",
            ProtocolError::RejectHSVersion,
        ),
        // protocol and continuing bytes
        (
            b"H\x00\xFF\xFF\xFF\xFF5\n8\nsayanpass1234",
            ProtocolError::RejectProtocol,
        ),
        // xchg and continuing bytes
        (
            b"H\x00\x00\xFF\xFF\xFF5\n8\nsayanpass1234",
            ProtocolError::RejectExchangeMode,
        ),
        // qmode and continuing bytes
        (
            b"H\x00\x00\x00\xFF\xFF5\n8\nsayanpass1234",
            ProtocolError::RejectQueryMode,
        ),
        // auth
        (
            b"H\x00\x00\x00\x00\xFF5\n8\nsayanpass1234",
            ProtocolError::RejectAuth,
        ),
    ] {
        assert_eq!(
            CHandshake::resume_with(&mut BufferedScanner::new(bad_hs), HandshakeState::Initial),
            HandshakeResult::Error(error)
        );
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
    loop {
        rounds += 1;
        let buf = &src[..rounds];
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match CHandshake::resume_with(&mut scanner, state) {
            HandshakeResult::ChangeState { new_state } => {
                state = new_state;
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
    assert_eq!(rounds, FULL_HANDSHAKE_WITH_AUTH.len())
}

const HS_BAD_PACKET: [u8; 6] = *b"I\x00\0\0\0\0";
const HS_BAD_VERSION_HS: [u8; 6] = *b"H\x01\0\0\0\0";
const HS_BAD_VERSION_PROTO: [u8; 6] = *b"H\0\x01\0\0\0";
const HS_BAD_MODE_XCHG: [u8; 6] = *b"H\0\0\x01\0\0";
const HS_BAD_MODE_QUERY: [u8; 6] = *b"H\0\0\0\x01\0";
const HS_BAD_MODE_AUTH: [u8; 6] = *b"H\0\0\0\0\x01";

fn scan_hs(hs: impl AsRef<[u8]>, f: impl Fn(HandshakeResult)) {
    let mut scanner = BufferedScanner::new(hs.as_ref());
    let hs = CHandshake::resume_with(&mut scanner, Default::default());
    f(hs)
}

#[test]
fn hs_bad_packet_illegal_username_length() {
    scan_hs(b"H\0\0\0\0\0A\n8\nsayanpass1234", |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_packet_illegal_password_length() {
    scan_hs(b"H\0\0\0\0\05\nA\nsayanpass1234", |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_packet_illegal_pwd_uname_length() {
    scan_hs(b"H\0\0\0\0\0A\nA\nsayanpass1234", |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_packet_first_byte() {
    scan_hs(HS_BAD_PACKET, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_version_hs() {
    scan_hs(HS_BAD_VERSION_HS, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectHSVersion)
        )
    })
}

#[test]
fn hs_bad_version_proto() {
    scan_hs(HS_BAD_VERSION_PROTO, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectProtocol)
        )
    })
}

#[test]
fn hs_bad_exchange_mode() {
    scan_hs(HS_BAD_MODE_XCHG, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectExchangeMode)
        )
    })
}

#[test]
fn hs_bad_query_mode() {
    scan_hs(HS_BAD_MODE_QUERY, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectQueryMode)
        )
    })
}

#[test]
fn hs_bad_auth_mode() {
    scan_hs(HS_BAD_MODE_AUTH, |hs_result| {
        assert_eq!(hs_result, HandshakeResult::Error(ProtocolError::RejectAuth))
    })
}

/*
    QT-DEX
*/

fn iterate_payload(payload: impl AsRef<[u8]>, start: usize, f: impl Fn(usize, &[u8])) {
    let payload = payload.as_ref();
    for i in start..=payload.len() {
        f(i, &payload[..i])
    }
}

fn iterate_exchange_payload(
    payload: impl AsRef<[u8]>,
    start: usize,
    f: impl Fn(usize, Result<(ExchangeResult, usize), ExchangeError>),
) {
    iterate_payload(payload, start, |i, bytes| {
        let scanner = BufferedScanner::new(bytes);
        f(i, Exchange::try_complete(scanner, ExchangeState::default()))
    })
}

fn iterate_exchange_payload_from_zero(
    payload: impl AsRef<[u8]>,
    f: impl Fn(usize, Result<(ExchangeResult, usize), ExchangeError>),
) {
    iterate_exchange_payload(payload, 0, f)
}

/*
    corner cases
*/

#[test]
fn zero_sized_packet() {
    for payload in [
        "S0\n",    // zero packet
        "S2\n0\n", // zero query
        "S1\n\n",  // zero query
    ] {
        iterate_exchange_payload_from_zero(payload, |size, result| {
            if size == payload.len() {
                // we got the full payload
                if payload.len() == 3 {
                    assert_eq!(result, Err(ExchangeError::UnterminatedInteger))
                } else {
                    assert_eq!(result, Err(ExchangeError::IncorrectQuerySizeOrMoreBytes))
                }
            } else {
                // we don't have the full payload
                if size < 3 {
                    assert_eq!(
                        result,
                        Ok((ExchangeResult::NewState(ExchangeState::Initial), 0))
                    )
                } else {
                    assert!(
                        matches!(
                            result,
                            Ok((ExchangeResult::NewState(ExchangeState::Simple(_)), _))
                        ),
                        "failed for {:?}, result is {:?}",
                        &payload[..size],
                        result,
                    );
                }
            }
        });
    }
}

#[test]
fn invalid_first_byte() {
    for payload in ["A1\n\n", "B7\n5\nsayan"] {
        iterate_exchange_payload(payload, 1, |size, result| {
            if size >= 3 {
                assert_eq!(result, Err(ExchangeError::UnknownFirstByte))
            } else {
                assert_eq!(
                    result,
                    Ok((ExchangeResult::NewState(ExchangeState::Initial), 0))
                )
            }
        })
    }
}

pub struct EQuery {
    // payload
    payload: String,
    variable_range: Range<usize>,
    // query
    query: String,
    query_range: Range<usize>,
    // params
    params: &'static [&'static str],
    param_range: Range<usize>,
    param_indices: Vec<Range<usize>>,
}

impl EQuery {
    fn new(query: String, params: &'static [&'static str]) -> Self {
        var!(let variable_start, variable_end, query_start, query_end, param_start);
        /*
            prepare the "back" of the payload
        */
        let total_size = query.len() + params.iter().map(|p| p.len()).sum::<usize>();
        let total_size_string = format!("{total_size}\n");

        /*
            compute offsets
        */

        let packet_size = total_size_string.len() + total_size;
        let mut buffer = String::new();
        buffer.push('S');
        buffer.push_str(&format!("{packet_size}\n"));

        // record start of variable block
        variable_start = buffer.len();

        buffer.push_str(&query.len().to_string());
        buffer.push('\n');

        // record start of query
        query_start = buffer.len();
        buffer.push_str(&query);
        query_end = buffer.len();

        // record start of params
        param_start = buffer.len();
        let mut param_indices = Vec::new();
        for param in params {
            let start = buffer.len();
            buffer.push_str(param);
            param_indices.push(start..buffer.len());
        }

        variable_end = buffer.len();
        Self {
            payload: buffer,
            variable_range: variable_start..variable_end,
            query,
            query_range: query_start..query_end,
            params,
            param_range: param_start..variable_end,
            param_indices,
        }
    }
}

#[test]
fn ext_query() {
    let ext_query = EQuery::new("create space myspace".to_owned(), &["sayan", "pass", ""]);
    let query_starts_at = ext_query.payload[ext_query.variable_range.clone()]
        .find('\n')
        .unwrap()
        + 1;
    assert_eq!(
        &ext_query.payload[ext_query.variable_range.clone()]
            [query_starts_at..query_starts_at + ext_query.query.len()],
        ext_query.query
    );
    assert_eq!(ext_query.query, &ext_query.payload[ext_query.query_range]);
    assert_eq!("sayanpass", &ext_query.payload[ext_query.param_range]);
    for (param_indices, real_param) in ext_query.param_indices.iter().zip(ext_query.params) {
        assert_eq!(*real_param, &ext_query.payload[param_indices.clone()]);
    }
}

/*
    simple queries
*/

const fn dig_count(real: usize) -> usize {
    // count the number of digits
    let mut dig_count = 0;
    let mut real_ = real;
    while real_ != 0 {
        dig_count += 1;
        real_ /= 10;
    }
    // account for a `0`
    dig_count += (real == 0) as usize;
    dig_count
}

const fn nth_position_value(mut real: usize, mut pos: usize) -> usize {
    let digits = dig_count(real);
    while digits != pos {
        real /= 10;
        pos += 1;
    }
    real
}

#[test]
fn simple_query() {
    for query in [
        // small query without params
        EQuery::new("small query".to_owned(), &[]),
        // small query with params
        EQuery::new("small query".to_owned(), &["hello", "world"]),
        // giant query without params
        EQuery::new(
            "abcdefghijklmnopqrstuvwxyz 123456789 ABCDEFGHIJKLMNOPQRSTUVWXYZ".repeat(1000),
            &[],
        ),
        // giant query with params
        EQuery::new(
            "abcdefghijklmnopqrstuvwxyz 123456789 ABCDEFGHIJKLMNOPQRSTUVWXYZ".repeat(1000),
            &["hello", "world"],
        ),
    ] {
        iterate_exchange_payload_from_zero(query.payload.as_bytes(), |read_position, result| {
            /*
                S<packet size>\n<query window>\n<query><param>
                                ^ variable      ^query ^param
                                range start     start   start

                - if before (variable range start - 1) then depending on the position from the first byte we will have, say the query size is 123
                then we will have wrt distance from first byte (i.e position - 1) [1], [12], [123]
                - if at (variable range start - 1) then we will have the exact size at [123] and in completed state
                - if >= query start, then we will continue to issue changes of state until we have the full size which will be caught in a  different branch
            */
            if read_position < 3 {
                // didn't reach minimum threshold
                assert_eq!(
                    result,
                    Ok((ExchangeResult::NewState(ExchangeState::Initial), 0))
                )
            } else if read_position <= query.variable_range.start - 1 {
                let index = read_position - 1;
                assert_eq!(
                    result,
                    Ok((
                        ExchangeResult::NewState(ExchangeState::Simple(SQState::_new(
                            Usize::new_unflagged(nth_position_value(
                                query.variable_range.len(),
                                index
                            ))
                        ))),
                        read_position
                    ))
                )
            } else if read_position >= query.variable_range.start {
                if read_position == query.payload.len() {
                    let (result, cursor) = result.unwrap();
                    assert_eq!(cursor, query.payload.len());
                    assert_eq!(
                        result,
                        ExchangeResult::Simple(SQuery::_new(
                            query.payload[query.query_range.start..].as_bytes(),
                            query.query_range.len()
                        ))
                    );
                } else {
                    assert_eq!(
                        result,
                        Ok((
                            ExchangeResult::NewState(ExchangeState::Simple(SQState::_new(
                                Usize::new_flagged(query.variable_range.len())
                            ))),
                            query.variable_range.start // the cursor will not go ahead until the full query is read
                        ))
                    )
                }
            } else {
                unreachable!()
            }
        })
    }
}

/*
    pipeline
*/

struct EPipeQuery {
    payload: String,
    q: String,
    p: &'static [&'static str],
    q_range: Range<usize>,
    p_range: Range<usize>,
}

impl EPipeQuery {
    fn new_string(q: &str, p: &'static [&'static str]) -> String {
        Self::new(q.to_owned(), p).payload
    }
    fn new(q: String, p: &'static [&'static str]) -> Self {
        let mut buffer = String::new();
        buffer.push_str(&q.len().to_string());
        buffer.push('\n');
        buffer.push_str(&p.iter().map(|s| s.len()).sum::<usize>().to_string());
        buffer.push('\n');
        let q_start = buffer.len();
        buffer.push_str(q.as_str());
        let q_stop = buffer.len();
        p.iter().for_each(|p_| buffer.push_str(p_));
        let p_stop = buffer.len();
        Self {
            payload: buffer,
            q,
            p,
            q_range: q_start..q_stop,
            p_range: q_stop..p_stop,
        }
    }
}

fn pipe<const N: usize>(queries: [String; N]) -> String {
    let packed_queries = queries.concat();
    format!("P{}\n{packed_queries}", packed_queries.len())
}

fn get_pipeline_from_result(pipeline: ExchangeResult) -> Vec<SQuery> {
    let pipeline: Vec<SQuery<'_>> = match pipeline {
        ExchangeResult::Pipeline(p) => p.into_iter().map(Result::unwrap).collect(),
        _ => panic!("expected pipeline got: {:?}", pipeline),
    };
    pipeline
}

#[test]
fn full_pipe_scan() {
    let pipeline_buffer = pipe([
        EPipeQuery::new_string("create space myspace", &[]),
        EPipeQuery::new_string(
            "create model myspace.mymodel(username: string, password: string)",
            &[],
        ),
        EPipeQuery::new_string("insert into myspace.mymodel(?, ?)", &["sayan", "cake"]),
    ]);
    let (pipeline, cursor) = Exchange::try_complete(
        BufferedScanner::new(pipeline_buffer.as_bytes()),
        ExchangeState::default(),
    )
    .unwrap();
    assert_eq!(cursor, pipeline_buffer.len());
    let pipeline = get_pipeline_from_result(pipeline);
    assert_eq!(
        pipeline,
        vec![
            SQuery::_new(b"create space myspace", "create space myspace".len()),
            SQuery::_new(
                b"create model myspace.mymodel(username: string, password: string)",
                "create model myspace.mymodel(username: string, password: string)".len()
            ),
            SQuery::_new(
                b"insert into myspace.mymodel(?, ?)sayancake",
                "insert into myspace.mymodel(?, ?)".len()
            )
        ]
    );
}

struct EPipe {
    payload: String,
    variable_range: Range<usize>,
    queries: Vec<EPipeQuery>,
}

impl EPipe {
    fn new<const N: usize>(queries: [EPipeQuery; N]) -> Self {
        let packed_queries_len = queries.iter().map(|epq| epq.payload.len()).sum::<usize>();
        let mut buffer = format!("P{packed_queries_len}\n");
        let variable_start = buffer.len();
        for query in queries.iter() {
            buffer.push_str(query.payload.as_str());
        }
        let variable_end = buffer.len();
        Self {
            payload: buffer,
            variable_range: variable_start..variable_end,
            queries: queries.into_iter().collect(),
        }
    }
}

#[test]
fn pipeline() {
    for pipe in [
        EPipe::new([
            // small query with no params
            EPipeQuery::new("create space myspace".to_owned(), &[]),
            // small query with params
            EPipeQuery::new(
                "insert into myspace.mymodel(?, ?)".to_owned(),
                &["sayan", "elx"],
            ),
            // giant query with no params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &[],
            ),
            // giant query with params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &["hello", "world"],
            ),
        ]),
        EPipe::new([
            // giant query with no params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &[],
            ),
            // giant query with params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &["hello", "world"],
            ),
            // small query with no params
            EPipeQuery::new("create space myspace".to_owned(), &[]),
            // small query with params
            EPipeQuery::new(
                "insert into myspace.mymodel(?, ?)".to_owned(),
                &["sayan", "elx"],
            ),
        ]),
    ] {
        iterate_exchange_payload_from_zero(pipe.payload.as_bytes(), |buffer_position, result| {
            if buffer_position < 3 {
                // we didn't read enough to pass the initial bounds check
                assert_eq!(
                    result,
                    Ok((ExchangeResult::NewState(ExchangeState::Initial), 0))
                )
            } else {
                if buffer_position <= pipe.variable_range.start - 1 {
                    // in this case we're before completion of the packet size. we succesively read [1], [12], [123]
                    assert_eq!(
                        result,
                        Ok((
                            ExchangeResult::NewState(ExchangeState::Pipeline(PipeState::_new(
                                Usize::new_unflagged(nth_position_value(
                                    pipe.variable_range.len(),
                                    buffer_position - 1
                                ))
                            ))),
                            buffer_position
                        ))
                    )
                } else if buffer_position == pipe.variable_range.start {
                    // in this case we passed the newline for the packet size so we have everything in order
                    assert_eq!(
                        result,
                        Ok((
                            ExchangeResult::NewState(ExchangeState::Pipeline(PipeState::_new(
                                Usize::new_flagged(pipe.variable_range.len())
                            ))),
                            pipe.variable_range.start
                        ))
                    )
                } else if buffer_position >= pipe.variable_range.start {
                    // in this case we have more bytes than the variable range start
                    if buffer_position == pipe.payload.len() {
                        // if we're here, we've read the full packet
                        let (status, cursor) = result.unwrap();
                        assert_eq!(cursor, pipe.variable_range.end); // the whole chunk is obtained
                        let pipeline = get_pipeline_from_result(status);
                        pipeline
                            .into_iter()
                            .zip(pipe.queries.iter())
                            .for_each(|(spq, epq)| {
                                let decoded_query = core::str::from_utf8(spq.query()).unwrap();
                                let decoded_params = core::str::from_utf8(spq.params()).unwrap();
                                assert_eq!(decoded_query, epq.q);
                                assert_eq!(decoded_params, epq.p.concat());
                                assert_eq!(decoded_query, &epq.payload[epq.q_range.clone()]);
                                assert_eq!(decoded_params, &epq.payload[epq.p_range.clone()]);
                            })
                    } else {
                        // if we're here we haven't read the full packet because we're waiting for it to complete
                        let (status, cursor) = result.unwrap();
                        assert_eq!(cursor, pipe.variable_range.start); // the cursor won't move until
                        assert_eq!(
                            status,
                            ExchangeResult::NewState(ExchangeState::Pipeline(PipeState::_new(
                                Usize::new_flagged(pipe.variable_range.len())
                            )))
                        )
                    }
                } else {
                    unreachable!()
                }
            }
        })
    }
}

fn run_staged(full_payload: &[u8], f: impl Fn(ExchangeResult)) {
    let mut read_amount = 0;
    let mut state = ExchangeState::default();
    let mut cursor = 0;
    loop {
        let buffer = &full_payload[..read_amount];
        let scanner = unsafe { BufferedScanner::new_with_cursor(buffer, cursor) };
        match Exchange::try_complete(scanner, state).unwrap() {
            (result, new_cursor) => match result {
                ExchangeResult::NewState(new_state) => {
                    state = new_state;
                    cursor = new_cursor;
                }
                state => {
                    f(state);
                    break;
                }
            },
        }
        read_amount += 1;
    }
}

#[test]
fn staged_simple_query() {
    for eq in [
        EQuery::new(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
            &[],
        ),
        EQuery::new(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
            &["hello", "world"],
        ),
    ] {
        run_staged(eq.payload.as_bytes(), |state| match state {
            ExchangeResult::NewState(_) => unreachable!(),
            ExchangeResult::Simple(s) => {
                assert_eq!(s.query(), eq.query.as_bytes());
                assert_eq!(s.params(), eq.payload[eq.param_range.clone()].as_bytes());
            }
            ExchangeResult::Pipeline(_) => unreachable!(),
        });
    }
}

#[test]
fn staged_pipeline() {
    for epipe in [
        EPipe::new([
            // small query with no params
            EPipeQuery::new("create space myspace".to_owned(), &[]),
            // small query with params
            EPipeQuery::new(
                "insert into myspace.mymodel(?, ?)".to_owned(),
                &["sayan", "elx"],
            ),
            // giant query with no params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &[],
            ),
            // giant query with params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &["hello", "world"],
            ),
        ]),
        EPipe::new([
            // giant query with no params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &[],
            ),
            // giant query with params
            EPipeQuery::new(
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789".repeat(1000),
                &["hello", "world"],
            ),
            // small query with no params
            EPipeQuery::new("create space myspace".to_owned(), &[]),
            // small query with params
            EPipeQuery::new(
                "insert into myspace.mymodel(?, ?)".to_owned(),
                &["sayan", "elx"],
            ),
        ]),
    ] {
        run_staged(epipe.payload.as_bytes(), |result| match result {
            ExchangeResult::NewState(_) => unreachable!(),
            ExchangeResult::Simple(_) => unreachable!(),
            ExchangeResult::Pipeline(p) => {
                p.into_iter()
                    .zip(epipe.queries.iter())
                    .for_each(|(spq, epq)| {
                        let spq = spq.unwrap();
                        assert_eq!(spq.query(), epq.q.as_bytes());
                        assert_eq!(spq.params(), epq.payload[epq.p_range.clone()].as_bytes());
                    })
            }
        })
    }
}
