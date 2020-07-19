/*
 * Created on Sat Jul 18 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use corelib::responses;
use corelib::ActionType;
use std::panic;

#[derive(Debug, PartialEq)]
pub struct PreQMF {
    action_type: ActionType,
    content_size: usize,
    metaline_size: usize,
}

impl PreQMF {
    pub fn from_buffer(buf: String) -> Result<Self, Vec<u8>> {
        let buf: Vec<&str> = buf.split('!').collect();
        if let (Some(atype), Some(csize), Some(metaline_size)) =
            (buf.get(0), buf.get(1), buf.get(2))
        {
            if let Some(atype) = atype.chars().next() {
                let atype = match atype {
                    '+' => ActionType::Simple,
                    '$' => ActionType::Pipeline,
                    _ => return Err(responses::RESP_INVALID_MF.to_owned()),
                };
                let (csize, metaline_size) =
                    match (csize.parse::<usize>(), metaline_size.parse::<usize>()) {
                        (Ok(x), Ok(y)) => (x, y),
                        _ => return Err(responses::RESP_INVALID_MF.to_owned()),
                    };
                return Ok(PreQMF {
                    action_type: atype,
                    content_size: csize,
                    metaline_size,
                });
            }
        }
        Err(responses::RESP_INVALID_MF.to_owned())
    }
}

#[cfg(test)]
#[test]
fn test_preqmf() {
    let read_what = "+!12!4".to_owned();
    let preqmf = PreQMF::from_buffer(read_what).unwrap();
    let pqmf_should_be = PreQMF {
        action_type: ActionType::Simple,
        content_size: 12,
        metaline_size: 4,
    };
    assert_eq!(pqmf_should_be, preqmf);
    let a_pipe = "$!12!4".to_owned();
    let preqmf = PreQMF::from_buffer(a_pipe).unwrap();
    let pqmf_should_be = PreQMF {
        action_type: ActionType::Pipeline,
        content_size: 12,
        metaline_size: 4,
    };
    assert_eq!(preqmf, pqmf_should_be);
}

pub fn get_sizes(stream: String) -> Result<Vec<usize>, Vec<u8>> {
    let sstr: Vec<&str> = stream.split('#').collect();
    let mut sstr_iter = sstr.into_iter().peekable();
    let mut sizes = Vec::with_capacity(sstr_iter.len());
    while let Some(size) = sstr_iter.next() {
        if sstr_iter.peek().is_some() {
            // Skip the last element
            if let Ok(val) = size.parse::<usize>() {
                sizes.push(val);
            } else {
                return Err(responses::RESP_INVALID_MF.to_owned());
            }
        } else {
            break;
        }
    }
    Ok(sizes)
}

#[cfg(test)]
#[test]
fn test_get_sizes() {
    let retbuf = "10#20#30#".to_owned();
    let sizes = get_sizes(retbuf).unwrap();
    assert_eq!(sizes, vec![10usize, 20usize, 30usize]);
}

pub fn extract_idents(buf: Vec<u8>, skip_sequence: Vec<usize>) -> Vec<String> {
    skip_sequence
        .into_iter()
        .scan(buf.into_iter(), |databuf, size| {
            let tok: Vec<u8> = databuf.take(size).collect();
            let _ = databuf.next();
            Some(String::from_utf8_lossy(&tok).to_string())
        })
        .collect()
}

#[cfg(test)]
#[test]
fn test_extract_idents() {
    let testbuf = "set\nsayan\n17\n".as_bytes().to_vec();
    let skip_sequence: Vec<usize> = vec![3, 5, 2];
    let res = extract_idents(testbuf, skip_sequence);
    assert_eq!(
        vec!["set".to_owned(), "sayan".to_owned(), "17".to_owned()],
        res
    );
    let badbuf = vec![0, 0, 159, 146, 150];
    let skip_sequence: Vec<usize> = vec![1, 2];
    let res = extract_idents(badbuf, skip_sequence);
    assert_eq!(res[1], "��");
}
