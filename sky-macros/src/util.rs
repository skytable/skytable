/*
 * Created on Wed Mar 09 2022
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

use core::fmt::Display;
use core::str::FromStr;
use proc_macro2::Span;
use rand::Rng;
use syn::{Lit, MetaNameValue};

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub fn get_rand_string(rng: &mut impl Rng) -> String {
    (0..64)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

pub fn parse_string(int: &Lit, span: Span, field: &str) -> Result<String, syn::Error> {
    match int {
        syn::Lit::Str(s) => Ok(s.value()),
        syn::Lit::Verbatim(s) => Ok(s.to_string()),
        _ => Err(syn::Error::new(
            span,
            format!("Failed to parse {} into a string.", field),
        )),
    }
}

pub fn parse_number<T: FromStr<Err = E>, E: Display>(
    int: &Lit,
    span: Span,
    field: &str,
) -> Result<T, syn::Error> {
    match int {
        syn::Lit::Int(int) => int.base10_parse::<T>(),
        _ => Err(syn::Error::new(
            span,
            format!("Failed to parse {} into an int.", field),
        )),
    }
}

pub fn parse_bool(boolean: &Lit, span: Span, field: &str) -> Result<bool, syn::Error> {
    match boolean {
        Lit::Bool(boolean) => Ok(boolean.value),
        _ => Err(syn::Error::new(
            span,
            format!("Failed to parse {} into a boolean.", field),
        )),
    }
}

pub fn get_metanamevalue_data(namevalue: &MetaNameValue) -> (String, &Lit, Span) {
    match namevalue
        .path
        .get_ident()
        .map(|ident| ident.to_string().to_lowercase())
    {
        None => panic!("Must have specified ident!"),
        Some(ident) => (ident, &namevalue.lit, namevalue.lit.span()),
    }
}
