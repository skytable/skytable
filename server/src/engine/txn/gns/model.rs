/*
 * Created on Sat Feb 10 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use crate::engine::{
    core::model::{Field, ModelData},
    idx::{IndexST, IndexSTSeqCns},
    mem::unsafe_apis::BoxStr,
    ql::lex::Ident,
    txn::{ModelIDRef, SpaceIDRef},
};

impl_gns_event!(
    CreateModelTxn<'_> = CreateModel,
    AlterModelAddTxn<'_> = AlterModelAdd,
    AlterModelRemoveTxn<'_> = AlterModelRemove,
    AlterModelUpdateTxn<'_> = AlterModelUpdate,
    DropModelTxn<'_> = DropModel
);

#[derive(Debug, Clone, Copy)]
/// The commit payload for a `create model ... (...) with {...}` txn
pub struct CreateModelTxn<'a> {
    space_id: SpaceIDRef<'a>,
    model_name: &'a str,
    model: &'a ModelData,
}

impl<'a> CreateModelTxn<'a> {
    pub const fn new(space_id: SpaceIDRef<'a>, model_name: &'a str, model: &'a ModelData) -> Self {
        Self {
            space_id,
            model_name,
            model,
        }
    }
    pub fn space_id(&self) -> SpaceIDRef<'_> {
        self.space_id
    }
    pub fn model_name(&self) -> &str {
        self.model_name
    }
    pub fn model(&self) -> &ModelData {
        self.model
    }
}

#[derive(Debug, Clone, Copy)]
/// Transaction commit payload for an `alter model add ...` query
pub struct AlterModelAddTxn<'a> {
    model_id: ModelIDRef<'a>,
    new_fields: &'a IndexSTSeqCns<BoxStr, Field>,
}

impl<'a> AlterModelAddTxn<'a> {
    pub const fn new(
        model_id: ModelIDRef<'a>,
        new_fields: &'a IndexSTSeqCns<BoxStr, Field>,
    ) -> Self {
        Self {
            model_id,
            new_fields,
        }
    }
    pub fn model_id(&self) -> ModelIDRef<'_> {
        self.model_id
    }
    pub fn new_fields(&self) -> &IndexSTSeqCns<BoxStr, Field> {
        self.new_fields
    }
}

#[derive(Debug, Clone, Copy)]
/// Transaction commit payload for an `alter model remove` transaction
pub struct AlterModelRemoveTxn<'a> {
    model_id: ModelIDRef<'a>,
    removed_fields: &'a [Ident<'a>],
}
impl<'a> AlterModelRemoveTxn<'a> {
    pub const fn new(model_id: ModelIDRef<'a>, removed_fields: &'a [Ident<'a>]) -> Self {
        Self {
            model_id,
            removed_fields,
        }
    }
    pub fn model_id(&self) -> ModelIDRef<'_> {
        self.model_id
    }
    pub fn removed_fields(&self) -> &[Ident<'_>] {
        self.removed_fields
    }
}

#[derive(Debug, Clone, Copy)]
/// Transaction commit payload for an `alter model update ...` query
pub struct AlterModelUpdateTxn<'a> {
    model_id: ModelIDRef<'a>,
    updated_fields: &'a IndexST<BoxStr, Field>,
}

impl<'a> AlterModelUpdateTxn<'a> {
    pub const fn new(model_id: ModelIDRef<'a>, updated_fields: &'a IndexST<BoxStr, Field>) -> Self {
        Self {
            model_id,
            updated_fields,
        }
    }

    pub fn model_id(&self) -> ModelIDRef<'_> {
        self.model_id
    }

    pub fn updated_fields(&self) -> &IndexST<BoxStr, Field> {
        self.updated_fields
    }
}

#[derive(Debug, Clone, Copy)]
/// Transaction commit payload for a `drop model ...` query
pub struct DropModelTxn<'a> {
    model_id: ModelIDRef<'a>,
}

impl<'a> DropModelTxn<'a> {
    pub const fn new(model_id: ModelIDRef<'a>) -> Self {
        Self { model_id }
    }
    pub fn model_id(&self) -> ModelIDRef<'_> {
        self.model_id
    }
}
