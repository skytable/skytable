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
    crate::engine::{
        core::{
            model::{Field, Layer, Model},
            space::{Space, SpaceMeta},
        },
        data::{
            cell::Datacell,
            dict::{DictEntryGeneric, DictGeneric},
            tag::TagSelector,
            uuid::Uuid,
        },
        idx::{IndexBaseSpec, IndexSTSeqCns, STIndex, STIndexSeq},
    },
};

#[test]
fn dict() {
    let dict: DictGeneric = into_dict! {
        "hello" => Datacell::new_str("world".into()),
        "omg a null?" => Datacell::null(),
        "a big fat dict" => DictEntryGeneric::Map(into_dict!(
            "with a value" => Datacell::new_uint_default(1002),
            "and a null" => Datacell::null(),
        ))
    };
    let encoded = super::enc::enc_dict_full::<super::map::GenericDictSpec>(&dict);
    let decoded = super::dec::dec_dict_full::<super::map::GenericDictSpec>(&encoded).unwrap();
    assert_eq!(dict, decoded);
}

#[test]
fn layer() {
    let layer = Layer::list();
    let encoded = super::enc::enc_full::<obj::LayerRef>(obj::LayerRef(&layer));
    let dec = super::dec::dec_full::<obj::LayerRef>(&encoded).unwrap();
    assert_eq!(layer, dec);
}

#[test]
fn field() {
    let field = Field::new([Layer::list(), Layer::uint64()].into(), true);
    let encoded = super::enc::enc_full::<obj::FieldRef>((&field).into());
    let dec = super::dec::dec_full::<obj::FieldRef>(&encoded).unwrap();
    assert_eq!(field, dec);
}

#[test]
fn fieldmap() {
    let mut fields = IndexSTSeqCns::<Box<str>, Field>::idx_init();
    fields.st_insert("password".into(), Field::new([Layer::bin()].into(), false));
    fields.st_insert(
        "profile_pic".into(),
        Field::new([Layer::bin()].into(), true),
    );
    let enc = super::enc::enc_dict_full::<super::map::FieldMapSpec>(&fields);
    let dec = super::dec::dec_dict_full::<super::map::FieldMapSpec>(&enc).unwrap();
    for ((orig_field_id, orig_field), (restored_field_id, restored_field)) in
        fields.stseq_ord_kv().zip(dec.stseq_ord_kv())
    {
        assert_eq!(orig_field_id, restored_field_id);
        assert_eq!(orig_field, restored_field);
    }
}

#[test]
fn model() {
    let uuid = Uuid::new();
    let model = Model::new_restore(
        uuid,
        "username".into(),
        TagSelector::Str.into_full(),
        into_dict! {
            "password" => Field::new([Layer::bin()].into(), false),
            "profile_pic" => Field::new([Layer::bin()].into(), true),
        },
    );
    let model_irm = model.intent_read_model();
    let enc = super::enc::enc_full::<obj::ModelLayoutRef>(obj::ModelLayoutRef(&model, &model_irm));
    let dec = super::dec::dec_full::<obj::ModelLayoutRef>(&enc).unwrap();
    assert_eq!(model, dec);
}

#[test]
fn space() {
    let uuid = Uuid::new();
    let space = Space::new_with_uuid(Default::default(), SpaceMeta::default(), uuid);
    let space_meta_read = space.metadata().dict().read();
    let enc = super::enc::enc_full::<obj::SpaceLayoutRef>(obj::SpaceLayoutRef::from((
        &space,
        &*space_meta_read,
    )));
    let dec = super::dec::dec_full::<obj::SpaceLayoutRef>(&enc).unwrap();
    assert_eq!(space, dec);
}
