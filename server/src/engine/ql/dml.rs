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

use {
    super::lexer::{Lit, Symbol, Token},
    crate::engine::memory::DataType,
    std::mem::{discriminant, Discriminant},
};

/// Parse a list
///
/// **NOTE:** This function will error if the `[` token is passed. Make sure this is forwarded by the caller
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
    if matches!(parse_list(tok, &mut l), (_, i, true) if i == tok.len()) {
        Some(l)
    } else {
        None
    }
}

#[cfg(test)]
/// Parse the tuple data passed in with an insert query.
///
/// **Note:** Make sure you pass the `(` token
pub(super) fn parse_data_tuple_syntax(tok: &[Token]) -> (Vec<DataType>, usize, bool) {
    let l = tok.len();
    let mut okay = l != 0;
    let mut stop = okay && tok[0] == Token::Symbol(Symbol::TtCloseParen);
    let mut i = stop as usize;
    let mut data = Vec::new();
    while i < l && okay && !stop {
        match &tok[i] {
            Token::Lit(Lit::Str(s)) => {
                data.push(s.to_string().into());
            }
            Token::Lit(Lit::Num(n)) => {
                data.push((*n).into());
            }
            Token::Lit(Lit::Bool(b)) => {
                data.push((*b).into());
            }
            Token::Symbol(Symbol::TtOpenSqBracket) => {
                // ah, a list
                let mut l = Vec::new();
                let (_, lst_i, lst_okay) = parse_list(&tok[i + 1..], &mut l);
                data.push(l.into());
                i += lst_i;
                okay &= lst_okay;
            }
            _ => {
                okay = false;
                break;
            }
        }
        i += 1;
        let nx_comma = i < l && tok[i] == Symbol::SymComma;
        let nx_csprn = i < l && tok[i] == Symbol::TtCloseParen;
        okay &= nx_comma | nx_csprn;
        i += okay as usize;
        stop = nx_csprn;
    }
    (data, i, okay && stop)
}

#[cfg(test)]
pub(super) fn parse_data_tuple_syntax_full(tok: &[Token]) -> Option<Vec<DataType>> {
    let (ret, cnt, okay) = parse_data_tuple_syntax(tok);
    if cnt == tok.len() && okay {
        Some(ret)
    } else {
        None
    }
}
