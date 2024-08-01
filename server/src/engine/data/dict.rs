/*
 * Created on Thu Feb 09 2023
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
    crate::engine::{
        data::{cell::Datacell, lit::Lit},
        idx::STIndex,
        mem::unsafe_apis::BoxStr,
    },
    std::collections::HashMap,
};

/// A generic dictionary built from scratch from syntactical elements
pub type DictGeneric = HashMap<BoxStr, DictEntryGeneric>;

#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(Clone))]
/// A generic dict entry: either a literal or a recursive dictionary
pub enum DictEntryGeneric {
    /// A literal
    Data(Datacell),
    /// A map
    Map(DictGeneric),
}

impl DictEntryGeneric {
    pub fn into_dict(self) -> Option<DictGeneric> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }
    pub fn into_data(self) -> Option<Datacell> {
        match self {
            Self::Data(d) => Some(d),
            _ => None,
        }
    }
}

/*
    patchsets
*/

/// Accepts a dict with possible null values, and removes those null values
pub fn rflatten_metadata(mut new: DictGeneric) -> DictGeneric {
    _rflatten_metadata(&mut new);
    new
}

fn _rflatten_metadata(new: &mut DictGeneric) {
    new.retain(|_, v| match v {
        DictEntryGeneric::Data(d) => d.is_init(),
        DictEntryGeneric::Map(m) => {
            _rflatten_metadata(m);
            true
        }
    });
}

/// Recursively merge a [`DictGeneric`] into a [`DictGeneric`] with the use of an intermediary
/// patchset to avoid inconsistent states
pub fn rmerge_metadata(current: &mut DictGeneric, new: DictGeneric) -> bool {
    match rprepare_metadata_patch(current as &_, new) {
        Some(patch) => {
            rmerge_data_with_patch(current, patch);
            true
        }
        None => false,
    }
}

pub fn rprepare_metadata_patch(current: &DictGeneric, new: DictGeneric) -> Option<DictGeneric> {
    let mut patch = Default::default();
    if rmerge_metadata_prepare_patch(current, new, &mut patch) {
        Some(patch)
    } else {
        None
    }
}

pub fn rmerge_data_with_patch(current: &mut DictGeneric, patch: DictGeneric) {
    for (key, patch) in patch {
        match patch {
            DictEntryGeneric::Data(d) if d.is_init() => {
                current.st_upsert(key, DictEntryGeneric::Data(d));
            }
            DictEntryGeneric::Data(_) => {
                // null
                let _ = current.remove(&key);
            }
            DictEntryGeneric::Map(m) => match current.get_mut(&key) {
                Some(current_recursive) => match current_recursive {
                    DictEntryGeneric::Map(current_m) => {
                        rmerge_data_with_patch(current_m, m);
                    }
                    _ => {
                        // can never reach here since the patch is always correct
                        unreachable!()
                    }
                },
                None => {
                    let mut new = DictGeneric::new();
                    rmerge_data_with_patch(&mut new, m);
                }
            },
        }
    }
}

fn rmerge_metadata_prepare_patch(
    current: &DictGeneric,
    new: DictGeneric,
    patch: &mut DictGeneric,
) -> bool {
    let mut new = new.into_iter();
    let mut okay = true;
    while new.len() != 0 && okay {
        let (key, new_entry) = new.next().unwrap();
        match (current.get(&key), new_entry) {
            // non-null -> non-null: merge flatten update
            (Some(DictEntryGeneric::Data(this_data)), DictEntryGeneric::Data(new_data))
                if new_data.is_init() =>
            {
                if this_data.kind() == new_data.kind() {
                    patch.insert(key, DictEntryGeneric::Data(new_data));
                } else {
                    okay = false;
                }
            }
            (Some(DictEntryGeneric::Data(_)), DictEntryGeneric::Map(_)) => {
                okay = false;
            }
            (
                Some(DictEntryGeneric::Map(this_recursive_data)),
                DictEntryGeneric::Map(new_recursive_data),
            ) => {
                let mut this_patch = DictGeneric::new();
                let _okay = rmerge_metadata_prepare_patch(
                    this_recursive_data,
                    new_recursive_data,
                    &mut this_patch,
                );
                patch.insert(key, DictEntryGeneric::Map(this_patch));
                okay &= _okay;
            }
            // null -> non-null: flatten insert
            (None, DictEntryGeneric::Data(l)) if l.is_init() => {
                let _ = patch.insert(key, DictEntryGeneric::Data(l));
            }
            (None, DictEntryGeneric::Map(m)) => {
                let mut this_patch = DictGeneric::new();
                okay &= rmerge_metadata_prepare_patch(&into_dict!(), m, &mut this_patch);
                let _ = patch.insert(key, DictEntryGeneric::Map(this_patch));
            }
            // non-null -> null: remove
            (Some(_), DictEntryGeneric::Data(l)) => {
                debug_assert!(l.is_null());
                patch.insert(key, DictEntryGeneric::Data(Datacell::null()));
            }
            (None, DictEntryGeneric::Data(l)) => {
                debug_assert!(l.is_null());
                // ignore
            }
        }
    }
    okay
}

/*
    impls
*/

impl<'a> From<Lit<'a>> for DictEntryGeneric {
    fn from(l: Lit<'a>) -> Self {
        Self::Data(Datacell::from(l))
    }
}

direct_from! {
    DictEntryGeneric => {
        Datacell as Data,
        DictGeneric as Map,
    }
}
