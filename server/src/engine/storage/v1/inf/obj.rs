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

use crate::engine::{core::model::delta::IRModel, data::DictGeneric};

use {
    super::{PersistTypeDscr, PersistObject, VecU8},
    crate::{
        engine::{
            core::{
                model::{Field, Layer, Model},
                space::{Space, SpaceMeta},
            },
            data::{
                cell::Datacell,
                tag::{DataTag, TagClass, TagSelector},
                uuid::Uuid,
            },
            mem::VInline,
            storage::v1::{inf, rw::BufferedScanner, SDSSError, SDSSResult},
        },
        util::EndianQW,
    },
};

pub fn encode_element(buf: &mut VecU8, dc: &Datacell) {
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
                    encode_element(buf, item);
                }
            }
        }
    }
}

pub fn encode_datacell_tag(buf: &mut VecU8, dc: &Datacell) {
    buf.push(
        PersistTypeDscr::translate_from_class(dc.tag().tag_class()).value_u8()
            * (!dc.is_null() as u8),
    )
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
        buf.extend(layer.tag().tag_selector().d().u64_bytes_le());
        buf.extend(0u64.to_le_bytes());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata> {
        Ok(LayerMD::new(scanner.next_u64_le(), scanner.next_u64_le()))
    }
    fn obj_enc(_: &mut VecU8, _: Self::InputType) {}
    unsafe fn obj_dec(_: &mut BufferedScanner, md: Self::Metadata) -> SDSSResult<Self::OutputType> {
        if (md.type_selector > TagSelector::List.d() as u64) | (md.prop_set_arity != 0) {
            return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
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

pub struct FieldRef<'a>(&'a Field);
impl<'a> From<&'a Field> for FieldRef<'a> {
    fn from(f: &'a Field) -> Self {
        Self(f)
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
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata> {
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
    ) -> SDSSResult<Self::OutputType> {
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
            Err(SDSSError::InternalDecodeStructureCorrupted)
        }
    }
}

pub struct ModelLayout;
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
pub struct ModelLayoutRef<'a>(pub(super) &'a Model, pub(super) &'a IRModel<'a>);
impl<'a> From<(&'a Model, &'a IRModel<'a>)> for ModelLayoutRef<'a> {
    fn from((mdl, irm): (&'a Model, &'a IRModel<'a>)) -> Self {
        Self(mdl, irm)
    }
}
impl<'a> PersistObject for ModelLayoutRef<'a> {
    const METADATA_SIZE: usize = sizeof!(u128) + sizeof!(u64, 3);
    type InputType = ModelLayoutRef<'a>;
    type OutputType = Model;
    type Metadata = ModelLayoutMD;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left(md.p_key_len as usize)
    }
    fn meta_enc(buf: &mut VecU8, ModelLayoutRef(v, irm): Self::InputType) {
        buf.extend(v.get_uuid().to_le_bytes());
        buf.extend(v.p_key().len().u64_bytes_le());
        buf.extend(v.p_tag().tag_selector().d().u64_bytes_le());
        buf.extend(irm.fields().len().u64_bytes_le());
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata> {
        Ok(ModelLayoutMD::new(
            Uuid::from_bytes(scanner.next_chunk()),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
        ))
    }
    fn obj_enc(buf: &mut VecU8, ModelLayoutRef(mdl, irm): Self::InputType) {
        buf.extend(mdl.p_key().as_bytes());
        <super::map::PersistMapImpl<super::map::FieldMapSpec> as PersistObject>::obj_enc(
            buf,
            irm.fields(),
        )
    }
    unsafe fn obj_dec(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> SDSSResult<Self::OutputType> {
        let key = inf::dec::utils::decode_string(scanner, md.p_key_len as usize)?;
        let fieldmap =
            <super::map::PersistMapImpl<super::map::FieldMapSpec> as PersistObject>::obj_dec(
                scanner,
                super::map::MapIndexSizeMD(md.field_c as usize),
            )?;
        let ptag = if md.p_key_tag > TagSelector::max_dscr() as u64 {
            return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
        } else {
            TagSelector::from_raw(md.p_key_tag as u8)
        };
        Ok(Model::new_restore(
            md.model_uuid,
            key.into_boxed_str(),
            ptag.into_full(),
            fieldmap,
        ))
    }
}

pub struct SpaceLayout;
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
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> SDSSResult<Self::Metadata> {
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
    ) -> SDSSResult<Self::OutputType> {
        let space_meta =
            <super::map::PersistMapImpl<super::map::GenericDictSpec> as PersistObject>::obj_dec(
                scanner,
                super::map::MapIndexSizeMD(md.prop_c),
            )?;
        Ok(Space::new_restore_empty(
            SpaceMeta::new_with_meta(space_meta),
            md.uuid,
        ))
    }
}
