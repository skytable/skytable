/*
 * Created on Wed Aug 16 2023
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
    super::{dec, PersistObject, VecU8},
    crate::{
        engine::{
            core::{
                model::{Field, Layer, ModelData},
                space::Space,
            },
            data::{
                tag::{DataTag, TagClass, TagSelector},
                uuid::Uuid,
                DictGeneric,
            },
            error::{RuntimeResult, StorageError},
            idx::IndexSTSeqCns,
            mem::{unsafe_apis::BoxStr, BufferedScanner, VInline},
        },
        util::{compiler::TaggedEnum, EndianQW},
    },
    std::marker::PhantomData,
};

/*
    generic cells
*/

pub mod cell {
    use {
        super::super::{DataSource, VecU8},
        crate::{
            engine::data::{
                cell::Datacell,
                tag::{DataTag, TagClass, TagSelector},
            },
            util::{compiler::TaggedEnum, EndianQW},
        },
    };
    #[derive(
        Debug,
        PartialEq,
        Eq,
        Clone,
        Copy,
        PartialOrd,
        Ord,
        Hash,
        sky_macros::EnumMethods,
        sky_macros::TaggedEnum,
    )]
    #[repr(u8)]
    #[allow(dead_code)]
    pub enum StorageCellTypeID {
        Null = 0x00,
        Bool = 0x01,
        UInt8 = 0x02,
        UInt16 = 0x03,
        UInt32 = 0x04,
        UInt64 = 0x05,
        SInt8 = 0x06,
        SInt16 = 0x07,
        SInt32 = 0x08,
        SInt64 = 0x09,
        Float32 = 0x0A,
        Float64 = 0x0B,
        Bin = 0x0C,
        Str = 0x0D,
        List = 0x0E,
        Dict = 0x0F,
    }
    impl StorageCellTypeID {
        #[inline(always)]
        pub const fn is_valid(d: u8) -> bool {
            d <= Self::MAX_DSCR
        }
        unsafe fn into_selector(self) -> TagSelector {
            debug_assert!(self.value_u8() != Self::Null.value_u8());
            TagSelector::from_raw(self.value_u8() - 1)
        }
        #[inline(always)]
        pub fn expect_atleast(d: u8) -> usize {
            [0u8, 1, 8, 8][d.min(3) as usize] as usize
        }
    }
    pub fn encode(buf: &mut VecU8, dc: &Datacell) {
        buf.push(encode_tag(dc));
        encode_cell(buf, dc)
    }
    pub fn encode_tag(dc: &Datacell) -> u8 {
        (dc.tag().tag_selector().value_u8() + 1) * (dc.is_init() as u8)
    }
    pub fn encode_cell(buf: &mut VecU8, dc: &Datacell) {
        if dc.is_null() {
            return;
        }
        unsafe {
            use TagClass::*;
            match dc.tag().tag_class() {
                Bool if dc.is_init() => buf.push(dc.read_bool() as u8),
                Bool => {}
                UnsignedInt | SignedInt | Float => buf.extend(dc.read_uint().to_le_bytes()),
                Str | Bin => {
                    let slc = dc.read_bin();
                    buf.extend(slc.len().u64_bytes_le());
                    buf.extend(slc);
                }
                List => {
                    let lst = dc.read_list().read();
                    buf.extend(lst.len().u64_bytes_le());
                    for item in lst.iter() {
                        encode(buf, item);
                    }
                }
            }
        }
    }
    pub trait ElementYield {
        type Yield;
        type Error;
        const CAN_YIELD_DICT: bool = false;
        fn yield_data(dc: Datacell) -> Result<Self::Yield, Self::Error>;
        fn yield_dict() -> Result<Self::Yield, Self::Error> {
            panic!()
        }
        fn error() -> Result<Self::Yield, Self::Error>;
    }
    impl ElementYield for Datacell {
        type Yield = Self;
        type Error = ();
        fn yield_data(dc: Datacell) -> Result<Self::Yield, Self::Error> {
            Ok(dc)
        }
        fn error() -> Result<Self::Yield, Self::Error> {
            Err(())
        }
    }
    #[derive(Debug, PartialEq)]
    pub enum CanYieldDict {
        Data(Datacell),
        Dict,
    }
    impl ElementYield for CanYieldDict {
        type Yield = Self;
        type Error = ();
        const CAN_YIELD_DICT: bool = true;
        fn error() -> Result<Self::Yield, Self::Error> {
            Err(())
        }
        fn yield_data(dc: Datacell) -> Result<Self::Yield, Self::Error> {
            Ok(CanYieldDict::Data(dc))
        }
        fn yield_dict() -> Result<Self::Yield, Self::Error> {
            Ok(CanYieldDict::Dict)
        }
    }
    pub unsafe fn decode_element<EY: ElementYield, DS: DataSource>(
        s: &mut DS,
        dscr: StorageCellTypeID,
    ) -> Result<EY::Yield, DS::Error>
    where
        DS::Error: From<EY::Error>,
        DS::Error: From<()>,
    {
        if dscr == StorageCellTypeID::Dict {
            if EY::CAN_YIELD_DICT {
                return Ok(EY::yield_dict()?);
            } else {
                return Ok(EY::error()?);
            }
        }
        if dscr == StorageCellTypeID::Null {
            return Ok(EY::yield_data(Datacell::null())?);
        }
        let tag = dscr.into_selector().into_full();
        let d = match tag.tag_class() {
            TagClass::Bool => {
                let nx = s.read_next_byte()?;
                if nx > 1 {
                    return Ok(EY::error()?);
                }
                Datacell::new_bool(nx == 1)
            }
            TagClass::UnsignedInt | TagClass::SignedInt | TagClass::Float => {
                let nx = s.read_next_u64_le()?;
                Datacell::new_qw(nx, tag)
            }
            TagClass::Bin | TagClass::Str => {
                let len = s.read_next_u64_le()? as usize;
                let block = s.read_next_variable_block(len)?;
                if tag.tag_class() == TagClass::Str {
                    match String::from_utf8(block).map(|s| Datacell::new_str(s.into_boxed_str())) {
                        Ok(s) => s,
                        Err(_) => return Ok(EY::error()?),
                    }
                } else {
                    Datacell::new_bin(block.into())
                }
            }
            TagClass::List => {
                let len = s.read_next_u64_le()? as usize;
                let mut l = vec![];
                while (l.len() != len) & s.has_remaining(1) {
                    let Some(dscr) = StorageCellTypeID::try_from_raw(s.read_next_byte()?) else {
                        return Ok(EY::error()?);
                    };
                    // FIXME(@ohsayan): right now, a list cannot contain a dict!
                    if !s.has_remaining(StorageCellTypeID::expect_atleast(dscr.value_u8())) {
                        return Ok(EY::error()?);
                    }
                    l.push(self::decode_element::<Datacell, DS>(s, dscr)?);
                }
                if l.len() != len {
                    return Ok(EY::error()?);
                }
                Datacell::new_list(l)
            }
        };
        Ok(EY::yield_data(d)?)
    }
}

/*
    layer
*/

#[derive(Debug)]
pub struct LayerMD {
    type_selector: u64,
    prop_set_arity: u64,
}

impl LayerMD {
    const fn new(type_selector: u64, prop_set_arity: u64) -> Self {
        Self {
            type_selector,
            prop_set_arity,
        }
    }
}

#[derive(Clone, Copy)]
pub struct LayerRef<'a>(pub &'a Layer);
impl<'a> From<&'a Layer> for LayerRef<'a> {
    fn from(value: &'a Layer) -> Self {
        Self(value)
    }
}
impl<'a> PersistObject for LayerRef<'a> {
    const METADATA_SIZE: usize = sizeof!(u64, 2);
    type InputType = LayerRef<'a>;
    type OutputType = Layer;
    type Metadata = LayerMD;
    fn pretest_can_dec_object(_: &BufferedScanner, _: &Self::Metadata) -> bool {
        true
    }
    fn meta_enc(buf: &mut VecU8, LayerRef(layer): Self::InputType) {
        buf.extend(layer.tag().tag_selector().value_qword().to_le_bytes());
        buf.extend(0u64.to_le_bytes());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(LayerMD::new(scanner.next_u64_le(), scanner.next_u64_le()))
    }
    fn obj_enc(_: &mut VecU8, _: Self::InputType) {}
    unsafe fn obj_dec(
        _: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        if (md.type_selector > TagSelector::List.value_qword()) | (md.prop_set_arity != 0) {
            return Err(StorageError::InternalDecodeStructureCorruptedPayload.into());
        }
        Ok(Layer::new_empty_props(
            TagSelector::from_raw(md.type_selector as u8).into_full(),
        ))
    }
}

/*
    field
*/

pub struct FieldMD {
    prop_c: u64,
    layer_c: u64,
    null: u8,
}

impl FieldMD {
    pub(super) const fn new(prop_c: u64, layer_c: u64, null: u8) -> Self {
        Self {
            prop_c,
            layer_c,
            null,
        }
    }
}

pub struct FieldRef<'a>(PhantomData<&'a Field>);
impl<'a> From<&'a Field> for FieldRef<'a> {
    fn from(_: &'a Field) -> Self {
        Self(PhantomData)
    }
}
impl<'a> PersistObject for FieldRef<'a> {
    const METADATA_SIZE: usize = sizeof!(u64, 2) + 1;
    type InputType = &'a Field;
    type OutputType = Field;
    type Metadata = FieldMD;
    fn pretest_can_dec_object(_: &BufferedScanner, _: &Self::Metadata) -> bool {
        true
    }
    fn meta_enc(buf: &mut VecU8, slf: Self::InputType) {
        // [prop_c][layer_c][null]
        buf.extend(0u64.to_le_bytes());
        buf.extend(slf.layers().len().u64_bytes_le());
        buf.push(slf.is_nullable() as u8);
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(FieldMD::new(
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_byte(),
        ))
    }
    fn obj_enc(buf: &mut VecU8, slf: Self::InputType) {
        for layer in slf.layers() {
            LayerRef::default_full_enc(buf, LayerRef(layer));
        }
    }
    unsafe fn obj_dec(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let mut layers = VInline::new();
        let mut fin = false;
        while (!scanner.eof())
            & (layers.len() as u64 != md.layer_c)
            & (LayerRef::pretest_can_dec_metadata(scanner))
            & !fin
        {
            let layer_md = unsafe {
                // UNSAFE(@ohsayan): pretest
                LayerRef::meta_dec(scanner)?
            };
            let l = LayerRef::obj_dec(scanner, layer_md)?;
            fin = l.tag().tag_class() != TagClass::List;
            layers.push(l);
        }
        let field = Field::new(layers, md.null == 1);
        if (field.layers().len() as u64 == md.layer_c) & (md.null <= 1) & (md.prop_c == 0) & fin {
            Ok(field)
        } else {
            Err(StorageError::InternalDecodeStructureCorrupted.into())
        }
    }
}

#[derive(Debug)]
pub struct ModelLayoutMD {
    model_uuid: Uuid,
    p_key_len: u64,
    p_key_tag: u64,
    field_c: u64,
}

impl ModelLayoutMD {
    pub(super) const fn new(
        model_uuid: Uuid,
        p_key_len: u64,
        p_key_tag: u64,
        field_c: u64,
    ) -> Self {
        Self {
            model_uuid,
            p_key_len,
            p_key_tag,
            field_c,
        }
    }
    pub fn p_key_len(&self) -> u64 {
        self.p_key_len
    }
}

#[derive(Clone, Copy)]
pub struct ModelLayoutRef<'a>(pub(super) &'a ModelData);
impl<'a> From<&'a ModelData> for ModelLayoutRef<'a> {
    fn from(mdl: &'a ModelData) -> Self {
        Self(mdl)
    }
}
impl<'a> PersistObject for ModelLayoutRef<'a> {
    const METADATA_SIZE: usize = sizeof!(u128) + sizeof!(u64, 3);
    type InputType = ModelLayoutRef<'a>;
    type OutputType = ModelData;
    type Metadata = ModelLayoutMD;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left(md.p_key_len as usize)
    }
    fn meta_enc(buf: &mut VecU8, ModelLayoutRef(model_def): Self::InputType) {
        buf.extend(model_def.get_uuid().to_le_bytes());
        buf.extend(model_def.p_key().len().u64_bytes_le());
        buf.extend(model_def.p_tag().tag_selector().value_qword().to_le_bytes());
        buf.extend(model_def.fields().len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(ModelLayoutMD::new(
            Uuid::from_bytes(scanner.next_chunk()),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
        ))
    }
    fn obj_enc(buf: &mut VecU8, ModelLayoutRef(model_definition): Self::InputType) {
        buf.extend(model_definition.p_key().as_bytes());
        <super::map::PersistMapImpl<super::map::FieldMapSpec<_>> as PersistObject>::obj_enc(
            buf,
            model_definition.fields(),
        )
    }
    unsafe fn obj_dec(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let key = dec::utils::decode_string_into(scanner, md.p_key_len as usize, BoxStr::new)?;
        let fieldmap = <super::map::PersistMapImpl<
            super::map::FieldMapSpec<IndexSTSeqCns<BoxStr, _>>,
        > as PersistObject>::obj_dec(
            scanner, super::map::MapIndexSizeMD(md.field_c as usize)
        )?;
        let ptag = if md.p_key_tag > TagSelector::MAX_DSCR as u64 {
            return Err(StorageError::InternalDecodeStructureCorruptedPayload.into());
        } else {
            TagSelector::from_raw(md.p_key_tag as u8)
        };
        Ok(ModelData::new_restore(
            md.model_uuid,
            key,
            ptag.into_full(),
            fieldmap,
        ))
    }
}

#[derive(Debug)]
pub struct SpaceLayoutMD {
    uuid: Uuid,
    prop_c: usize,
}

impl SpaceLayoutMD {
    pub fn new(uuid: Uuid, prop_c: usize) -> Self {
        Self { uuid, prop_c }
    }
}

#[derive(Clone, Copy)]
pub struct SpaceLayoutRef<'a>(&'a Space, &'a DictGeneric);
impl<'a> From<(&'a Space, &'a DictGeneric)> for SpaceLayoutRef<'a> {
    fn from((spc, spc_meta): (&'a Space, &'a DictGeneric)) -> Self {
        Self(spc, spc_meta)
    }
}
impl<'a> PersistObject for SpaceLayoutRef<'a> {
    const METADATA_SIZE: usize = sizeof!(u128) + sizeof!(u64);
    type InputType = SpaceLayoutRef<'a>;
    type OutputType = Space;
    type Metadata = SpaceLayoutMD;
    fn pretest_can_dec_object(_: &BufferedScanner, _: &Self::Metadata) -> bool {
        true
    }
    fn meta_enc(buf: &mut VecU8, SpaceLayoutRef(space, space_meta): Self::InputType) {
        buf.extend(space.get_uuid().to_le_bytes());
        buf.extend(space_meta.len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(SpaceLayoutMD::new(
            Uuid::from_bytes(scanner.next_chunk()),
            scanner.next_u64_le() as usize,
        ))
    }
    fn obj_enc(buf: &mut VecU8, SpaceLayoutRef(_, space_meta): Self::InputType) {
        <super::map::PersistMapImpl<super::map::GenericDictSpec> as PersistObject>::obj_enc(
            buf, space_meta,
        )
    }
    unsafe fn obj_dec(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let space_meta =
            <super::map::PersistMapImpl<super::map::GenericDictSpec> as PersistObject>::obj_dec(
                scanner,
                super::map::MapIndexSizeMD(md.prop_c),
            )?;
        Ok(Space::new_restore_empty(md.uuid, space_meta))
    }
}
