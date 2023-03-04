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
    core::model::cell::Datacell,
    data::tag::{DataTag, FullTag, TagClass, TagSelector},
    error::{DatabaseError, DatabaseResult},
    mem::VInline,
    ql::ddl::syn::LayerSpec,
};
#[cfg(test)]
use std::cell::RefCell;

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
pub struct LayerView {
    layers: VInline<1, Layer>,
    nullable: bool,
}

impl LayerView {
    pub fn layers(&self) -> &[Layer] {
        &self.layers
    }
    pub fn parse_layers(spec: Vec<LayerSpec>, nullable: bool) -> DatabaseResult<Self> {
        let mut layers = spec.into_iter().rev();
        let mut okay = true;
        let mut fin = false;
        let mut layerview = VInline::new();
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
            Ok(Self {
                layers: layerview,
                nullable,
            })
        } else {
            Err(DatabaseError::DdlModelInvalidTypeDefinition)
        }
    }
    #[inline(always)]
    fn single_pass_for(&self, dc: &Datacell) -> bool {
        ((self.layers().len() == 1) & (self.layers()[0].tag.tag_class() == dc.kind()))
            | (self.nullable & dc.is_null())
    }
    #[inline(always)]
    fn compute_index(&self, dc: &Datacell) -> usize {
        // escape check if it makes sense to
        !(self.nullable & dc.is_null()) as usize * self.layers()[0].tag.tag_class().word()
    }
    pub fn validate_data_fpath(&self, data: &Datacell) -> bool {
        // if someone sends a PR with an added check, I'll personally come to your house and throw a brick on your head
        if self.single_pass_for(data) {
            layertrace("fpath");
            unsafe { LVERIFY[self.compute_index(data)](self.layers()[0], data) }
        } else {
            Self::rverify_layers(self.layers(), data)
        }
    }
    // TODO(@ohsayan): improve algo with dfs
    fn rverify_layers(layers: &[Layer], data: &Datacell) -> bool {
        let layer = layers[0];
        let layers = &layers[1..];
        match (layer.tag.tag_class(), data.kind()) {
            (layer_tag, data_tag) if (layer_tag == data_tag) & (layer_tag < TagClass::List) => {
                // time to go home
                (unsafe { LVERIFY[layer.tag.tag_class().word()](layer, data) } & layers.is_empty())
            }
            (TagClass::List, TagClass::List) => unsafe {
                let mut okay = !layers.is_empty() & LVERIFY[TagClass::List.word()](layer, data);
                let list = data.read_list().read();
                let mut it = list.iter();
                while (it.len() != 0) & okay {
                    okay &= Self::rverify_layers(layers, it.next().unwrap());
                }
                okay
            },
            _ => false,
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
    #[inline(always)]
    fn compute_index(&self, dc: &Datacell) -> usize {
        self.tag.tag_class().word() * (dc.is_null() as usize)
    }
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

static LVERIFY: [unsafe fn(Layer, &Datacell) -> bool; 7] = [
    lverify_bool,
    lverify_uint,
    lverify_sint,
    lverify_float,
    lverify_bin,
    lverify_str,
    lverify_list,
];

#[cfg(test)]
thread_local! {
    static LAYER_TRACE: RefCell<Vec<Box<str>>> = RefCell::new(Vec::new());
}

#[inline(always)]
fn layertrace(_layer: impl ToString) {
    #[cfg(test)]
    {
        LAYER_TRACE.with(|v| v.borrow_mut().push(_layer.to_string().into()));
    }
}

#[cfg(test)]
/// Obtain a layer trace and clear older traces
pub(super) fn layer_traces() -> Box<[Box<str>]> {
    LAYER_TRACE.with(|x| {
        let ret = x.borrow().iter().cloned().collect();
        x.borrow_mut().clear();
        ret
    })
}

unsafe fn lverify_bool(_: Layer, _: &Datacell) -> bool {
    layertrace("bool");
    true
}
unsafe fn lverify_uint(l: Layer, d: &Datacell) -> bool {
    layertrace("uint");
    const MX: [u64; 4] = [u8::MAX as _, u16::MAX as _, u32::MAX as _, u64::MAX];
    d.read_uint() <= MX[l.tag.tag_selector().word() - 1]
}
unsafe fn lverify_sint(l: Layer, d: &Datacell) -> bool {
    layertrace("sint");
    const MN_MX: [(i64, i64); 4] = [
        (i8::MIN as _, i8::MAX as _),
        (i16::MIN as _, i16::MAX as _),
        (i32::MIN as _, i32::MAX as _),
        (i64::MIN, i64::MAX),
    ];
    let (mn, mx) = MN_MX[l.tag.tag_selector().word() - 5];
    (d.read_sint() >= mn) & (d.read_sint() <= mx)
}
unsafe fn lverify_float(l: Layer, d: &Datacell) -> bool {
    layertrace("float");
    const MN_MX: [(f64, f64); 2] = [(f32::MIN as _, f32::MAX as _), (f64::MIN, f64::MAX)];
    let (mn, mx) = MN_MX[l.tag.tag_selector().word() - 9];
    (d.read_float() >= mn) & (d.read_float() <= mx)
}
unsafe fn lverify_bin(_: Layer, _: &Datacell) -> bool {
    layertrace("binary");
    true
}
unsafe fn lverify_str(_: Layer, _: &Datacell) -> bool {
    layertrace("string");
    true
}
unsafe fn lverify_list(_: Layer, _: &Datacell) -> bool {
    layertrace("list");
    true
}
