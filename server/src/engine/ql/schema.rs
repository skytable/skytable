/*
 * Created on Tue Sep 13 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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
    super::{
        ast::{Compiler, Statement},
        lexer::{Lit, Ty},
        LangResult, RawSlice,
    },
    std::collections::HashMap,
};

macro_rules! boxed {
    ([] $ty:ty) => {
        ::std::boxed::Box::<[$ty]>
    };
}

/*
    Meta
*/

struct FieldMeta {
    field_name: Option<RawSlice>,
    unprocessed_layers: boxed![[] TypeConfig],
}

type Dictionary = HashMap<String, Lit>;

struct TypeConfig {
    ty: Ty,
    dict: Dictionary,
}

struct CreateStatement {
    entity: RawSlice,
}

/*
    Validation
*/

fn parse_dictionary(_c: &mut Compiler) -> LangResult<Dictionary> {
    todo!()
}

fn parse_field(_c: &mut Compiler) -> LangResult<FieldMeta> {
    todo!()
}

fn parse_type_definition(_c: &mut Compiler) -> LangResult<boxed![[] TypeConfig]> {
    todo!()
}

pub(super) fn parse_schema(_c: &mut Compiler, _model: RawSlice) -> LangResult<Statement> {
    todo!()
}
