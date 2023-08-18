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
        data::{
            cell::Datacell,
            lit::{Lit, LitIR},
        },
        idx::STIndex,
    },
    std::collections::HashMap,
};

/// A generic dictionary built from scratch from syntactical elements
pub type DictGeneric = HashMap<Box<str>, DictEntryGeneric>;

#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(Clone))]
/// A generic dict entry: either a literal or a recursive dictionary
pub enum DictEntryGeneric {
    /// A literal
    Data(Datacell),
    /// A map
    Map(DictGeneric),
}

/*
    patchsets
*/

#[derive(Debug, PartialEq, Default)]
struct DictGenericPatch(HashMap<Box<str>, Option<DictGenericPatchEntry>>);
#[derive(Debug, PartialEq)]
enum DictGenericPatchEntry {
    Data(Datacell),
    Map(DictGenericPatch),
}

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
    let mut patch = DictGenericPatch::default();
    let current_ref = current as &_;
    let r = rmerge_metadata_prepare_patch(current_ref, new, &mut patch);
    if r {
        merge_data_with_patch(current, patch);
    }
    r
}

fn merge_data_with_patch(current: &mut DictGeneric, patch: DictGenericPatch) {
    for (key, patch) in patch.0 {
        match patch {
            Some(DictGenericPatchEntry::Data(d)) => {
                current.st_upsert(key, DictEntryGeneric::Data(d));
            }
            Some(DictGenericPatchEntry::Map(m)) => match current.get_mut(&key) {
                Some(current_recursive) => match current_recursive {
                    DictEntryGeneric::Map(current_m) => {
                        merge_data_with_patch(current_m, m);
                    }
                    _ => {
                        // can never reach here since the patch is always correct
                        unreachable!()
                    }
                },
                None => {
                    let mut new = DictGeneric::new();
                    merge_data_with_patch(&mut new, m);
                }
            },
            None => {
                let _ = current.remove(&key);
            }
        }
    }
}

fn rmerge_metadata_prepare_patch(
    current: &DictGeneric,
    new: DictGeneric,
    patch: &mut DictGenericPatch,
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
                    patch
                        .0
                        .insert(key, Some(DictGenericPatchEntry::Data(new_data)));
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
                let mut this_patch = DictGenericPatch::default();
                let _okay = rmerge_metadata_prepare_patch(
                    this_recursive_data,
                    new_recursive_data,
                    &mut this_patch,
                );
                patch
                    .0
                    .insert(key, Some(DictGenericPatchEntry::Map(this_patch)));
                okay &= _okay;
            }
            // null -> non-null: flatten insert
            (None, DictEntryGeneric::Data(l)) if l.is_init() => {
                let _ = patch.0.insert(key, Some(DictGenericPatchEntry::Data(l)));
            }
            (None, DictEntryGeneric::Map(m)) => {
                let mut this_patch = DictGenericPatch::default();
                okay &= rmerge_metadata_prepare_patch(&into_dict!(), m, &mut this_patch);
                let _ = patch
                    .0
                    .insert(key, Some(DictGenericPatchEntry::Map(this_patch)));
            }
            // non-null -> null: remove
            (Some(_), DictEntryGeneric::Data(l)) => {
                debug_assert!(l.is_null());
                patch.0.insert(key, None);
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

impl<'a> From<LitIR<'a>> for DictEntryGeneric {
    fn from(l: LitIR<'a>) -> Self {
        Self::Data(Datacell::from(l))
    }
}

impl<'a> From<Lit<'a>> for DictEntryGeneric {
    fn from(value: Lit<'a>) -> Self {
        Self::Data(Datacell::from(value))
    }
}

direct_from! {
    DictEntryGeneric => {
        Datacell as Data,
        DictGeneric as Map,
    }
}
