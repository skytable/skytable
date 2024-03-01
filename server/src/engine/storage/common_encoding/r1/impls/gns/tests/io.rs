/*
 * Created on Thu Aug 24 2023
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
    super::super::{
        model::{self, ModelIDRes},
        space, SpaceIDRef, SpaceIDRes,
    },
    crate::engine::{
        core::{model::ModelData, space::Space},
        storage::common_encoding::r1::{dec, enc},
        txn::ModelIDRef,
    },
};

mod space_tests {
    use {
        super::{
            dec, enc,
            space::{AlterSpaceTxnRestorePL, CreateSpaceTxnRestorePL},
            Space, SpaceIDRef,
        },
        crate::engine::txn::gns::space::{AlterSpaceTxn, CreateSpaceTxn, DropSpaceTxn},
    };
    #[test]
    fn create() {
        let orig_space = Space::new_auto_all();
        let space_r = orig_space.props();
        let txn = CreateSpaceTxn::new(&space_r, "myspace", &orig_space);
        let encoded = enc::full_self(txn);
        let decoded = dec::full::<CreateSpaceTxn>(&encoded).unwrap();
        assert_eq!(
            CreateSpaceTxnRestorePL {
                space_name: "myspace".into(),
                space: Space::new_restore_empty(orig_space.get_uuid(), Default::default())
            },
            decoded
        );
    }
    #[test]
    fn alter() {
        let space = Space::new_auto_all();
        let space_r = space.props();
        let txn = AlterSpaceTxn::new(SpaceIDRef::new("myspace", &space), &space_r);
        let encoded = enc::full_self(txn);
        let decoded = dec::full::<AlterSpaceTxn>(&encoded).unwrap();
        assert_eq!(
            AlterSpaceTxnRestorePL {
                space_id: super::SpaceIDRes::new(space.get_uuid(), "myspace".into()),
                space_meta: space_r.clone()
            },
            decoded
        );
    }
    #[test]
    fn drop() {
        let space = Space::new_auto_all();
        let txn = DropSpaceTxn::new(super::SpaceIDRef::new("myspace", &space));
        let encoded = enc::full_self(txn);
        let decoded = dec::full::<DropSpaceTxn>(&encoded).unwrap();
        assert_eq!(
            super::SpaceIDRes::new(space.get_uuid(), "myspace".into()),
            decoded
        );
    }
}

mod model_tests {
    use {
        super::{
            model::{
                AlterModelAddTxnRestorePL, AlterModelRemoveTxnRestorePL,
                AlterModelUpdateTxnRestorePL, CreateModelTxnRestorePL,
            },
            ModelData, Space,
        },
        crate::engine::{
            core::model::{Field, Layer},
            data::{tag::TagSelector, uuid::Uuid},
            txn::gns::model::{
                AlterModelAddTxn, AlterModelRemoveTxn, AlterModelUpdateTxn, CreateModelTxn,
                DropModelTxn,
            },
        },
    };
    fn default_space_model() -> (Space, ModelData) {
        let space = Space::new_auto_all();
        let model = ModelData::new_restore(
            Uuid::new(),
            "username".into(),
            TagSelector::String.into_full(),
            into_dict!(
                "password" => Field::new([Layer::bin()].into(), false),
                "profile_pic" => Field::new([Layer::bin()].into(), true),
            ),
        );
        (space, model)
    }
    #[test]
    fn create() {
        let (space, model) = default_space_model();
        let txn = CreateModelTxn::new(super::SpaceIDRef::new("myspace", &space), "mymodel", &model);
        let encoded = super::enc::full_self(txn);
        let decoded = super::dec::full::<CreateModelTxn>(&encoded).unwrap();
        assert_eq!(
            CreateModelTxnRestorePL {
                space_id: super::SpaceIDRes::new(space.get_uuid(), "myspace".into()),
                model_name: "mymodel".into(),
                model,
            },
            decoded
        )
    }
    #[test]
    fn alter_add() {
        let (space, model) = default_space_model();
        let new_fields = into_dict! {
            "auth_2fa" => Field::new([Layer::bool()].into(), true),
        };
        let txn = AlterModelAddTxn::new(
            super::ModelIDRef::new(
                super::SpaceIDRef::new("myspace", &space),
                "mymodel",
                model.get_uuid(),
                model.delta_state().schema_current_version().value_u64(),
            ),
            &new_fields,
        );
        let encoded = super::enc::full_self(txn);
        let decoded = super::dec::full::<AlterModelAddTxn>(&encoded).unwrap();
        assert_eq!(
            AlterModelAddTxnRestorePL {
                model_id: super::ModelIDRes::new(
                    super::SpaceIDRes::new(space.get_uuid(), "myspace".into()),
                    "mymodel".into(),
                    model.get_uuid(),
                    model.delta_state().schema_current_version().value_u64()
                ),
                new_fields: into_dict! {
                    "auth_2fa" => Field::new([Layer::bool()].into(), true),
                }
            },
            decoded
        );
    }
    #[test]
    fn alter_remove() {
        let (space, model) = default_space_model();
        let removed_fields = ["profile_pic".into()];
        let txn = AlterModelRemoveTxn::new(
            super::ModelIDRef::new(
                super::SpaceIDRef::new("myspace", &space),
                "mymodel",
                model.get_uuid(),
                model.delta_state().schema_current_version().value_u64(),
            ),
            &removed_fields,
        );
        let encoded = super::enc::full_self(txn);
        let decoded = super::dec::full::<AlterModelRemoveTxn>(&encoded).unwrap();
        assert_eq!(
            AlterModelRemoveTxnRestorePL {
                model_id: super::ModelIDRes::new(
                    super::SpaceIDRes::new(space.get_uuid(), "myspace".into()),
                    "mymodel".into(),
                    model.get_uuid(),
                    model.delta_state().schema_current_version().value_u64()
                ),
                removed_fields: ["profile_pic".into()].into()
            },
            decoded
        );
    }
    #[test]
    fn alter_update() {
        let (space, model) = default_space_model();
        let updated_fields_copy = into_dict! {
            // people of your social app will hate this, but hehe
            "profile_pic" => Field::new([Layer::bin()].into(), false)
        };
        let updated_fields = into_dict! {
            // people of your social app will hate this, but hehe
            "profile_pic" => Field::new([Layer::bin()].into(), false)
        };
        let txn = AlterModelUpdateTxn::new(
            super::ModelIDRef::new(
                super::SpaceIDRef::new("myspace", &space),
                "mymodel".into(),
                model.get_uuid(),
                model.delta_state().schema_current_version().value_u64(),
            ),
            &updated_fields,
        );
        let encoded = super::enc::full_self(txn);
        let decoded = super::dec::full::<AlterModelUpdateTxn>(&encoded).unwrap();
        assert_eq!(
            AlterModelUpdateTxnRestorePL {
                model_id: super::ModelIDRes::new(
                    super::SpaceIDRes::new(space.get_uuid(), "myspace".into()),
                    "mymodel".into(),
                    model.get_uuid(),
                    model.delta_state().schema_current_version().value_u64()
                ),
                updated_fields: updated_fields_copy
            },
            decoded
        );
    }
    #[test]
    fn drop() {
        let (space, model) = default_space_model();
        let txn = DropModelTxn::new(super::ModelIDRef::new(
            super::SpaceIDRef::new("myspace", &space),
            "mymodel",
            model.get_uuid(),
            model.delta_state().schema_current_version().value_u64(),
        ));
        let encoded = super::enc::full_self(txn);
        let decoded = super::dec::full::<DropModelTxn>(&encoded).unwrap();
        assert_eq!(
            super::ModelIDRes::new(
                super::SpaceIDRes::new(space.get_uuid(), "myspace".into()),
                "mymodel".into(),
                model.get_uuid(),
                model.delta_state().schema_current_version().value_u64()
            ),
            decoded
        );
    }
}
