/*
 * Created on Fri Oct 14 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

use std::mem::{discriminant, Discriminant};

use super::lexer::{Lit, Symbol};

use {super::lexer::Token, crate::engine::memory::DataType};

pub(super) fn parse_list(
    tok: &[Token],
    list: &mut Vec<DataType>,
) -> (Option<Discriminant<DataType>>, usize, bool) {
    let l = tok.len();
    let mut okay = l != 0;
    let mut stop = okay && tok[0] == Symbol::TtCloseSqBracket;
    let mut i = stop as usize;
    let mut overall_dscr = None;
    let mut prev_nlist_dscr = None;
    while i < l && okay && !stop {
        let d = match &tok[i] {
            Token::Lit(Lit::Str(s)) => DataType::String(s.to_string()),
            Token::Lit(Lit::Num(n)) => DataType::Number(*n),
            Token::Lit(Lit::Bool(b)) => DataType::Boolean(*b),
            Token::Symbol(Symbol::TtOpenSqBracket) => {
                // a nested list
                let mut nested_list = Vec::new();
                let (nlist_dscr, nlist_i, nlist_okay) = parse_list(&tok[i + 1..], &mut nested_list);
                okay &= nlist_okay;
                i += nlist_i;
                // check type return
                okay &= {
                    prev_nlist_dscr.is_none()
                        || nlist_dscr.is_none()
                        || prev_nlist_dscr == nlist_dscr
                };
                if prev_nlist_dscr.is_none() && nlist_dscr.is_some() {
                    prev_nlist_dscr = nlist_dscr;
                }
                DataType::List(nested_list)
            }
            _ => {
                okay = false;
                break;
            }
        };
        i += 1;
        okay &= list.is_empty() || discriminant(&d) == discriminant(&list[0]);
        overall_dscr = Some(discriminant(&d));
        list.push(d);
        let nx_comma = i < l && tok[i] == Symbol::SymComma;
        let nx_csqrb = i < l && tok[i] == Symbol::TtCloseSqBracket;
        okay &= nx_comma | nx_csqrb;
        i += okay as usize;
        stop = nx_csqrb;
    }
    (overall_dscr, i, okay && stop)
}

#[cfg(test)]
pub(super) fn parse_list_full(tok: &[Token]) -> Option<Vec<DataType>> {
    let mut l = Vec::new();
    if let (_, _, true) = parse_list(tok, &mut l) {
        Some(l)
    } else {
        None
    }
}
