/*
 * Created on Wed Nov 29 2023
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
    proc_macro2::Ident,
    syn::{Lit, Meta, MetaNameValue, NestedMeta, Path},
};

pub enum AttributeKind {
    Lit(Lit),
    NestedAttrs { name: Ident, attrs: Vec<Self> },
    Pair(Ident, Lit),
    Path(Path),
}

impl AttributeKind {
    pub fn into_pair(self) -> (Ident, Lit) {
        match self {
            Self::Pair(i, l) => (i, l),
            _ => panic!("expected attribute name pair"),
        }
    }
}

pub fn extract_attribute(attr: &NestedMeta) -> AttributeKind {
    match attr {
        NestedMeta::Lit(l) => AttributeKind::Lit(l.clone()),
        NestedMeta::Meta(m) => match m {
            Meta::List(l) => AttributeKind::NestedAttrs {
                name: l.path.get_ident().unwrap().clone(),
                attrs: l.nested.iter().map(extract_attribute).collect(),
            },
            Meta::NameValue(MetaNameValue { path, lit, .. }) => {
                AttributeKind::Pair(path.get_ident().unwrap().clone(), lit.clone())
            }
            Meta::Path(p) => AttributeKind::Path(p.clone()),
        },
    }
}

pub fn extract_str_from_lit(l: &Lit) -> Option<String> {
    match l {
        Lit::Str(s) => Some(s.value()),
        _ => None,
    }
}

pub fn extract_int_from_lit<I>(l: &Lit) -> Option<I>
where
    I: std::str::FromStr,
    I::Err: std::fmt::Display,
{
    match l {
        Lit::Int(i) => i.base10_parse::<I>().ok(),
        _ => None,
    }
}
