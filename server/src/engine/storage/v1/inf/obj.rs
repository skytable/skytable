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
    super::{dec_md, map::FieldMapSpec, PersistObjectHlIO, PersistObjectMD, SimpleSizeMD, VecU8},
    crate::{
        engine::{
            core::model::{Field, Layer, Model},
            data::{
                tag::{DataTag, FullTag, TagClass, TagSelector},
                uuid::Uuid,
            },
            mem::VInline,
            storage::v1::{rw::BufferedScanner, SDSSError, SDSSResult},
        },
        util::EndianQW,
    },
};

/*
    Full 8B tag block. Notes:
    1. 7B at this moment is currently unused but there's a lot of additional flags that we might want to store here
    2. If we end up deciding that this is indeed a waste of space, version this out and get rid of the 7B (or whatever we determine
    to be the correct size.)
*/

struct POByteBlockFullTag(FullTag);

impl PersistObjectHlIO for POByteBlockFullTag {
    const ALWAYS_VERIFY_PAYLOAD_USING_MD: bool = false;
    type Type = FullTag;
    type Metadata = SimpleSizeMD<{ sizeof!(u64) }>;
    fn pe_obj_hlio_enc(buf: &mut VecU8, slf: &Self::Type) {
        buf.extend(slf.tag_selector().d().u64_bytes_le())
    }
    unsafe fn pe_obj_hlio_dec(
        scanner: &mut BufferedScanner,
        _: Self::Metadata,
    ) -> SDSSResult<FullTag> {
        let dscr = scanner.next_u64_le();
        if dscr > TagSelector::max_dscr() as u64 {
            return Err(SDSSError::InternalDecodeStructureCorruptedPayload);
        }
        Ok(TagSelector::from_raw(dscr as u8).into_full())
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

impl PersistObjectMD for LayerMD {
    const MD_DEC_INFALLIBLE: bool = true;
    fn pretest_src_for_metadata_dec(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64, 2))
    }
    fn pretest_src_for_object_dec(&self, _: &BufferedScanner) -> bool {
        true
    }
    unsafe fn dec_md_payload(scanner: &mut BufferedScanner) -> Option<Self> {
        Some(Self::new(
            u64::from_le_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
        ))
    }
}

impl PersistObjectHlIO for Layer {
    const ALWAYS_VERIFY_PAYLOAD_USING_MD: bool = false;
    type Type = Layer;
    type Metadata = LayerMD;
    fn pe_obj_hlio_enc(buf: &mut VecU8, slf: &Self::Type) {
        // [8B: type sig][8B: empty property set]
        POByteBlockFullTag::pe_obj_hlio_enc(buf, &slf.tag());
        buf.extend(0u64.to_le_bytes());
    }
    unsafe fn pe_obj_hlio_dec(
        _: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> SDSSResult<Self::Type> {
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

impl PersistObjectMD for FieldMD {
    const MD_DEC_INFALLIBLE: bool = true;
    fn pretest_src_for_metadata_dec(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64, 2) + 1)
    }
    fn pretest_src_for_object_dec(&self, _: &BufferedScanner) -> bool {
        // nothing here really; we can't help much with the stuff ahead
        true
    }
    unsafe fn dec_md_payload(scanner: &mut BufferedScanner) -> Option<Self> {
        Some(Self::new(
            u64::from_le_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
            scanner.next_byte(),
        ))
    }
}

impl PersistObjectHlIO for Field {
    const ALWAYS_VERIFY_PAYLOAD_USING_MD: bool = false;
    type Type = Self;
    type Metadata = FieldMD;
    fn pe_obj_hlio_enc(buf: &mut VecU8, slf: &Self::Type) {
        // [prop_c][layer_c][null]
        buf.extend(0u64.to_le_bytes());
        buf.extend(slf.layers().len().u64_bytes_le());
        buf.push(slf.is_nullable() as u8);
        for layer in slf.layers() {
            Layer::pe_obj_hlio_enc(buf, layer);
        }
    }
    unsafe fn pe_obj_hlio_dec(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> SDSSResult<Self::Type> {
        let mut layers = VInline::new();
        let mut fin = false;
        while (!scanner.eof())
            & (layers.len() as u64 != md.layer_c)
            & (<Layer as PersistObjectHlIO>::Metadata::pretest_src_for_metadata_dec(scanner))
            & !fin
        {
            let layer_md = unsafe {
                // UNSAFE(@ohsayan): pretest
                dec_md::<_, true>(scanner)?
            };
            let l = Layer::pe_obj_hlio_dec(scanner, layer_md)?;
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
}

impl ModelLayoutMD {
    pub(super) const fn new(model_uuid: Uuid, p_key_len: u64, p_key_tag: u64) -> Self {
        Self {
            model_uuid,
            p_key_len,
            p_key_tag,
        }
    }
}

impl PersistObjectMD for ModelLayoutMD {
    const MD_DEC_INFALLIBLE: bool = true;
    fn pretest_src_for_metadata_dec(scanner: &BufferedScanner) -> bool {
        scanner.has_left(sizeof!(u64, 3) + sizeof!(u128)) // u64,3 since the fieldmap len is also there, but we don't handle it directly
    }
    fn pretest_src_for_object_dec(&self, scanner: &BufferedScanner) -> bool {
        scanner.has_left(self.p_key_len as usize)
    }
    unsafe fn dec_md_payload(scanner: &mut BufferedScanner) -> Option<Self> {
        Some(Self::new(
            Uuid::from_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
            u64::from_le_bytes(scanner.next_chunk()),
        ))
    }
}

impl PersistObjectHlIO for ModelLayout {
    const ALWAYS_VERIFY_PAYLOAD_USING_MD: bool = true;
    type Type = Model;
    type Metadata = ModelLayoutMD;
    fn pe_obj_hlio_enc(buf: &mut VecU8, v: &Self::Type) {
        let irm = v.intent_read_model();
        buf.extend(v.get_uuid().to_le_bytes());
        buf.extend(v.p_key().len().u64_bytes_le());
        buf.extend(v.p_tag().tag_selector().d().u64_bytes_le());
        buf.extend(v.p_key().as_bytes());
        super::map::enc_dict_into_buffer::<FieldMapSpec>(buf, irm.fields())
    }
    unsafe fn pe_obj_hlio_dec(
        scanner: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> SDSSResult<Self::Type> {
        let key = String::from_utf8(
            scanner
                .next_chunk_variable(md.p_key_len as usize)
                .to_owned(),
        )
        .map_err(|_| SDSSError::InternalDecodeStructureCorruptedPayload)?;
        let fieldmap = super::map::dec_dict::<FieldMapSpec>(scanner)?;
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
