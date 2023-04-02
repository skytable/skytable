/*
 * Created on Thu Feb 02 2023
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
    super::syn::{self, DictFoldState, FieldSpec},
    crate::{
        engine::{
            data::DictGeneric,
            error::{LangError, LangResult},
            ql::{
                ast::{Entity, QueryData, State},
                lex::Ident,
            },
        },
        util::{compiler, MaybeInit},
    },
};

#[derive(Debug, PartialEq)]
/// A space
pub struct CreateSpace<'a> {
    /// the space name
    pub space_name: Ident<'a>,
    /// properties
    pub props: DictGeneric,
}

impl<'a> CreateSpace<'a> {
    #[inline(always)]
    /// Parse space data from the given tokens
    fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
        // smallest declaration: `create space myspace` -> >= 1 token
        if compiler::unlikely(state.remaining() < 1) {
            return compiler::cold_rerr(LangError::UnexpectedEOS);
        }
        let space_name = state.fw_read();
        state.poison_if_not(space_name.is_ident());
        // either we have `with` or nothing. don't be stupid
        let has_more_properties = state.cursor_rounded_eq(Token![with]);
        state.poison_if_not(has_more_properties | state.exhausted());
        state.cursor_ahead_if(has_more_properties); // +WITH
        let mut d = DictGeneric::new();
        // properties
        if has_more_properties && state.okay() {
            syn::rfold_dict(DictFoldState::OB, state, &mut d);
        }
        if state.okay() {
            Ok(CreateSpace {
                space_name: unsafe {
                    // UNSAFE(@ohsayan): we checked if `space_name` with `is_ident` above
                    space_name.uck_read_ident()
                },
                props: d,
            })
        } else {
            Err(LangError::BadSyntax)
        }
    }
}

#[derive(Debug, PartialEq)]
/// A model definition
pub struct CreateModel<'a> {
    /// the model name
    pub(in crate::engine) model_name: Entity<'a>,
    /// the fields
    pub(in crate::engine) fields: Vec<FieldSpec<'a>>,
    /// properties
    pub(in crate::engine) props: DictGeneric,
}

/*
    model definition:
    create model mymodel(
        [primary|null] ident: type,
    )
*/

impl<'a> CreateModel<'a> {
    pub fn new(model_name: Entity<'a>, fields: Vec<FieldSpec<'a>>, props: DictGeneric) -> Self {
        Self {
            model_name,
            fields,
            props,
        }
    }

    fn parse<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
        if compiler::unlikely(state.remaining() < 10) {
            return compiler::cold_rerr(LangError::UnexpectedEOS);
        }
        // model name; ignore errors
        let mut model_uninit = MaybeInit::uninit();
        Entity::parse_from_state_len_unchecked(state, &mut model_uninit);
        state.poison_if_not(state.cursor_eq(Token![() open]));
        state.cursor_ahead();
        // fields
        let mut stop = false;
        let mut fields = Vec::with_capacity(2);
        while state.loop_tt() && !stop {
            fields.push(FieldSpec::parse(state)?);
            let nx_close = state.cursor_rounded_eq(Token![() close]);
            let nx_comma = state.cursor_rounded_eq(Token![,]);
            state.poison_if_not(nx_close | nx_comma);
            state.cursor_ahead_if(nx_close | nx_comma);
            stop = nx_close;
        }
        state.poison_if_not(stop);
        // check props
        let mut props = DictGeneric::new();
        if state.cursor_rounded_eq(Token![with]) {
            state.cursor_ahead();
            // parse props
            syn::rfold_dict(DictFoldState::OB, state, &mut props);
        }
        // we're done
        if state.okay() {
            Ok(Self {
                model_name: unsafe {
                    // UNSAFE(@ohsayan): we verified if `model_name` is initialized through the state
                    model_uninit.assume_init()
                },
                fields,
                props,
            })
        } else {
            Err(LangError::BadSyntax)
        }
    }
}

mod impls {
    use {
        super::{CreateModel, CreateSpace},
        crate::engine::{
            error::LangResult,
            ql::ast::{traits::ASTNode, QueryData, State},
        },
    };
    impl<'a> ASTNode<'a> for CreateSpace<'a> {
        fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
            Self::parse(state)
        }
    }
    impl<'a> ASTNode<'a> for CreateModel<'a> {
        fn _from_state<Qd: QueryData<'a>>(state: &mut State<'a, Qd>) -> LangResult<Self> {
            Self::parse(state)
        }
    }
}
