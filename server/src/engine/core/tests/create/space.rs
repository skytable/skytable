/*
 * Created on Wed Feb 08 2023
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

use crate::engine::{
    core::{
        space::{Space, SpaceMeta},
        GlobalNS, HSData,
    },
    error::DatabaseError,
    idx::STIndex,
    ql::{
        ast::{compile_test, Statement},
        tests::lex_insecure as lex,
    },
};

#[test]
fn exec_create_space_simple() {
    let gns = GlobalNS::empty();
    let tok = lex("create space myspace".as_bytes()).unwrap();
    let space = extract_safe!(compile_test(&tok).unwrap(), Statement::CreateSpace(s) => s);
    Space::validate_apply(&gns, space).unwrap();
    assert_eq!(
        gns._spaces()
            .read()
            .st_get_cloned("myspace".as_bytes())
            .unwrap()
            .as_ref(),
        &Space::new(Default::default(), SpaceMeta::with_env(into_dict! {}))
    );
}

#[test]
fn exec_create_space_with_env() {
    let gns = GlobalNS::empty();
    let tok = lex(br#"
        create space myspace with {
            env: {
                MAX_MODELS: 100
            }
        }
    "#)
    .unwrap();
    let space = extract_safe!(compile_test(&tok).unwrap(), Statement::CreateSpace(s) => s);
    Space::validate_apply(&gns, space).unwrap();
    assert_eq!(
        gns._spaces()
            .read()
            .st_get_cloned("myspace".as_bytes())
            .unwrap()
            .as_ref(),
        &Space::new(
            into_dict! {},
            SpaceMeta::with_env(into_dict! {
                "MAX_MODELS" => HSData::UnsignedInt(100)
            })
        )
    );
}

#[test]
fn exec_create_space_with_bad_env_type() {
    let gns = GlobalNS::empty();
    let tok = lex(br#"create space myspace with { env: 100 }"#).unwrap();
    let space = extract_safe!(compile_test(&tok).unwrap(), Statement::CreateSpace(s) => s);
    assert_eq!(
        Space::validate_apply(&gns, space).unwrap_err(),
        DatabaseError::DdlCreateSpaceBadProperty
    );
}

#[test]
fn exec_create_space_with_random_property() {
    let gns = GlobalNS::empty();
    let random_property = "i_am_blue_da_ba_dee";
    let query = format!("create space myspace with {{ {random_property}: 100 }}").into_bytes();
    let tok = lex(&query).unwrap();
    let space = extract_safe!(compile_test(&tok).unwrap(), Statement::CreateSpace(s) => s);
    assert_eq!(
        Space::validate_apply(&gns, space).unwrap_err(),
        DatabaseError::DdlCreateSpaceBadProperty
    );
}
