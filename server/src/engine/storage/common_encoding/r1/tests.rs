/*
 * Created on Sun Aug 13 2023
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
    super::obj,
    crate::{
        engine::{
            core::{
                model::{Field, Layer, ModelData},
                space::Space,
            },
            data::{
                cell::Datacell,
                dict::{DictEntryGeneric, DictGeneric},
                tag::{FloatSpec, SIntSpec, TagSelector, UIntSpec},
                uuid::Uuid,
            },
            idx::{IndexBaseSpec, IndexSTSeqCns, STIndex, STIndexSeq},
            mem::BufferedScanner,
            storage::common_encoding::r1::obj::cell::StorageCellTypeID,
        },
        util::compiler::TaggedEnum,
    },
};

#[sky_macros::test]
fn dict() {
    let dict: DictGeneric = into_dict! {
        "hello" => Datacell::new_str("world".into()),
        "omg a null?" => Datacell::null(),
        "a big fat dict" => DictEntryGeneric::Map(into_dict!(
            "with a value" => Datacell::new_uint_default(1002),
            "and a null" => Datacell::null(),
        ))
    };
    let encoded = super::enc::full_dict::<super::map::GenericDictSpec>(&dict);
    let decoded = super::dec::dict_full::<super::map::GenericDictSpec>(&encoded).unwrap();
    assert_eq!(dict, decoded);
}

#[sky_macros::test]
fn layer() {
    let layer = Layer::list();
    let encoded = super::enc::full::<obj::LayerRef>(obj::LayerRef(&layer));
    let dec = super::dec::full::<obj::LayerRef>(&encoded).unwrap();
    assert_eq!(layer, dec);
}

#[sky_macros::test]
fn field() {
    let field = Field::new([Layer::list(), Layer::uint64()].into(), true);
    let encoded = super::enc::full::<obj::FieldRef>((&field).into());
    let dec = super::dec::full::<obj::FieldRef>(&encoded).unwrap();
    assert_eq!(field, dec);
}

#[sky_macros::test]
fn fieldmap() {
    use crate::engine::mem::unsafe_apis::BoxStr;

    let mut fields = IndexSTSeqCns::<BoxStr, Field>::idx_init();
    fields.st_insert("password".into(), Field::new([Layer::bin()].into(), false));
    fields.st_insert(
        "profile_pic".into(),
        Field::new([Layer::bin()].into(), true),
    );
    let enc = super::enc::full_dict::<super::map::FieldMapSpec<_>>(&fields);
    let dec = super::dec::dict_full::<
        super::map::FieldMapSpec<crate::engine::idx::IndexSTSeqCns<BoxStr, _>>,
    >(&enc)
    .unwrap();
    for ((orig_field_id, orig_field), (restored_field_id, restored_field)) in
        fields.stseq_ord_kv().zip(dec.stseq_ord_kv())
    {
        assert_eq!(orig_field_id, restored_field_id);
        assert_eq!(orig_field, restored_field);
    }
}

#[sky_macros::test]
fn model() {
    let uuid = Uuid::new();
    let model = ModelData::new_restore(
        uuid,
        "username".into(),
        TagSelector::String.into_full(),
        into_dict! {
            "password" => Field::new([Layer::bin()].into(), false),
            "profile_pic" => Field::new([Layer::bin()].into(), true),
        },
    );
    let enc = super::enc::full::<obj::ModelLayoutRef>(obj::ModelLayoutRef(&model));
    let dec = super::dec::full::<obj::ModelLayoutRef>(&enc).unwrap();
    assert_eq!(model, dec);
}

#[sky_macros::test]
fn space() {
    let uuid = Uuid::new();
    let space = Space::new_restore_empty(uuid, Default::default());
    let enc =
        super::enc::full::<obj::SpaceLayoutRef>(obj::SpaceLayoutRef::from((&space, space.props())));
    let dec = super::dec::full::<obj::SpaceLayoutRef>(&enc).unwrap();
    assert_eq!(space, dec);
}

#[sky_macros::non_miri_test] // FIXME(@ohsayan): Yet another miri slowdown. Not sure why
fn dc_encode_decode() {
    fn enc_dec(dc: &Datacell) {
        let mut encoded = vec![];
        super::obj::cell::encode(&mut encoded, &dc);
        let mut scanner = BufferedScanner::new(&encoded);
        let tag = scanner
            .try_next_byte()
            .map(StorageCellTypeID::try_from_raw)
            .unwrap()
            .unwrap();
        let dc_restored: Datacell = unsafe {
            super::obj::cell::decode_element::<Datacell, BufferedScanner>(&mut scanner, tag)
                .unwrap()
        };
        assert_eq!(dc, &dc_restored);
        assert_eq!(dc.tag(), dc_restored.tag());
    }
    let dc_tests = [
        // null
        Datacell::null(),
        // bool
        Datacell::new_bool(true),
        Datacell::new_bool(false),
        // uint
        // uint (8)
        Datacell::new_uint(u8::MIN as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt8.into_full())
        }),
        Datacell::new_uint(u8::MAX as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt8.into_full())
        }),
        // uint (16)
        Datacell::new_uint(u16::MIN as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt16.into_full())
        }),
        Datacell::new_uint(u16::MAX as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt16.into_full())
        }),
        // uint (32)
        Datacell::new_uint(u32::MIN as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt32.into_full())
        }),
        Datacell::new_uint(u32::MAX as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt32.into_full())
        }),
        // uint (64)
        Datacell::new_uint(u64::MIN as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt64.into_full())
        }),
        Datacell::new_uint(u64::MAX as _, unsafe {
            UIntSpec::from_full(TagSelector::UInt64.into_full())
        }),
        // sint
        // sint (8)
        Datacell::new_sint(i8::MIN as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt8.into_full())
        }),
        Datacell::new_sint(i8::MAX as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt8.into_full())
        }),
        // sint (16)
        Datacell::new_sint(i16::MIN as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt16.into_full())
        }),
        Datacell::new_sint(i16::MAX as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt16.into_full())
        }),
        // sint (32)
        Datacell::new_sint(i32::MIN as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt32.into_full())
        }),
        Datacell::new_sint(i32::MAX as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt32.into_full())
        }),
        // sint (64)
        Datacell::new_sint(i64::MIN as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt64.into_full())
        }),
        Datacell::new_sint(i64::MAX as _, unsafe {
            SIntSpec::from_full(TagSelector::SInt64.into_full())
        }),
        // float
        // float (32)
        Datacell::new_float(f32::MIN as _, unsafe {
            FloatSpec::from_full(TagSelector::Float32.into_full())
        }),
        Datacell::new_float(f32::MAX as _, unsafe {
            FloatSpec::from_full(TagSelector::Float32.into_full())
        }),
        // float (64)
        Datacell::new_float(f64::MIN as _, unsafe {
            FloatSpec::from_full(TagSelector::Float64.into_full())
        }),
        Datacell::new_float(f64::MAX as _, unsafe {
            FloatSpec::from_full(TagSelector::Float64.into_full())
        }),
        // bin
        Datacell::new_bin(b"".to_vec().into_boxed_slice()),
        Datacell::new_bin(b"abcdefghijkl".to_vec().into_boxed_slice()),
        // str
        Datacell::new_str("".to_owned().into_boxed_str()),
        Datacell::new_str("abcdefghijkl".to_owned().into_boxed_str()),
        // list
        Datacell::new_list(vec![]),
    ];
    for value in dc_tests {
        enc_dec(&value)
    }
    let mut dc = Datacell::new_list(vec![]);
    for _ in 0..100 {
        enc_dec(&dc);
        dc = Datacell::new_list(vec![dc.clone()]);
    }
}
