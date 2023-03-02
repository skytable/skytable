/*
 * Created on Mon Feb 06 2023
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

pub mod cell;

use crate::engine::{
    data::tag::{DataTag, FullTag, TagSelector},
    error::{DatabaseError, DatabaseResult},
    ql::ddl::syn::LayerSpec,
};

// FIXME(@ohsayan): update this!

#[derive(Debug)]
pub struct ModelView {}

#[cfg(test)]
impl PartialEq for ModelView {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

/*
    Layer
*/

static G: [u8; 15] = [0, 13, 12, 5, 6, 4, 3, 6, 1, 10, 4, 5, 7, 5, 5];
static S1: [u8; 7] = [13, 9, 4, 14, 2, 4, 7];
static S2: [u8; 7] = [12, 8, 2, 6, 4, 9, 9];

static LUT: [(&str, FullTag); 14] = [
    ("bool", FullTag::BOOL),
    ("uint8", FullTag::new_uint(TagSelector::UInt8)),
    ("uint16", FullTag::new_uint(TagSelector::UInt16)),
    ("uint32", FullTag::new_uint(TagSelector::UInt32)),
    ("uint64", FullTag::new_uint(TagSelector::UInt64)),
    ("sint8", FullTag::new_sint(TagSelector::SInt8)),
    ("sint16", FullTag::new_sint(TagSelector::SInt16)),
    ("sint32", FullTag::new_sint(TagSelector::SInt32)),
    ("sint64", FullTag::new_sint(TagSelector::SInt64)),
    ("float32", FullTag::new_float(TagSelector::Float32)),
    ("float64", FullTag::new_float(TagSelector::Float64)),
    ("binary", FullTag::BIN),
    ("string", FullTag::STR),
    ("list", FullTag::LIST),
];

#[derive(Debug, PartialEq, Clone)]
pub struct LayerView(Box<[Layer]>);

impl LayerView {
    pub fn layers(&self) -> &[Layer] {
        &self.0
    }
    pub fn parse_layers(spec: Vec<LayerSpec>) -> DatabaseResult<Self> {
        let mut layers = spec.into_iter().rev();
        let mut okay = true;
        let mut fin = false;
        let mut layerview = Vec::with_capacity(layers.len());
        while (layers.len() != 0) & okay & !fin {
            let LayerSpec { ty, props } = layers.next().unwrap();
            okay &= props.is_empty(); // FIXME(@ohsayan): you know what to do here
            match Layer::get_layer(&ty) {
                Some(l) => {
                    fin = l.tag.tag_selector() != TagSelector::List;
                    layerview.push(l);
                }
                None => okay = false,
            }
        }
        okay &= fin & (layers.len() == 0);
        if okay {
            Ok(Self(layerview.into_boxed_slice()))
        } else {
            Err(DatabaseError::DdlModelInvalidTypeDefinition)
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct Layer {
    tag: FullTag,
    config: [usize; 2],
}

impl Layer {
    pub const fn bool() -> Self {
        Self::empty(FullTag::BOOL)
    }
    pub const fn uint8() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt8))
    }
    pub const fn uint16() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt16))
    }
    pub const fn uint32() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt32))
    }
    pub const fn uint64() -> Self {
        Self::empty(FullTag::new_uint(TagSelector::UInt64))
    }
    pub const fn sint8() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt8))
    }
    pub const fn sint16() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt16))
    }
    pub const fn sint32() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt32))
    }
    pub const fn sint64() -> Self {
        Self::empty(FullTag::new_sint(TagSelector::SInt64))
    }
    pub const fn float32() -> Self {
        Self::empty(FullTag::new_float(TagSelector::Float32))
    }
    pub const fn float64() -> Self {
        Self::empty(FullTag::new_float(TagSelector::Float64))
    }
    pub const fn bin() -> Self {
        Self::empty(FullTag::BIN)
    }
    pub const fn str() -> Self {
        Self::empty(FullTag::STR)
    }
    pub const fn list() -> Self {
        Self::empty(FullTag::LIST)
    }
}

impl Layer {
    const fn new(tag: FullTag, config: [usize; 2]) -> Self {
        Self { tag, config }
    }
    const fn empty(tag: FullTag) -> Self {
        Self::new(tag, [0; 2])
    }
    fn hf(key: &[u8], v: [u8; 7]) -> u16 {
        let mut tot = 0;
        let mut i = 0;
        while i < key.len() {
            tot += v[i % v.len()] as u16 * key[i] as u16;
            i += 1;
        }
        tot % 15
    }
    fn pf(key: &[u8]) -> u16 {
        (G[Self::hf(key, S1) as usize] as u16 + G[Self::hf(key, S2) as usize] as u16) % 15
    }
    fn get_layer(ident: &str) -> Option<Self> {
        let idx = Self::pf(ident.as_bytes()) as usize;
        if idx < LUT.len() && LUT[idx].0 == ident {
            Some(Self::empty(LUT[idx].1))
        } else {
            None
        }
    }
}
