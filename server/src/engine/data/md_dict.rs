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
        core::Datacell,
        data::lit::{Lit, LitIR},
        idx::STIndex,
    },
    std::collections::HashMap,
};

/*
    dict kinds: one is from a generic parse while the other one is the stored metadata
*/

/// A metadata dictionary
pub type MetaDict = HashMap<Box<str>, MetaDictEntry>;
/// A generic dictionary built from scratch from syntactical elements
pub type DictGeneric = HashMap<Box<str>, Option<DictEntryGeneric>>;

#[derive(Debug, PartialEq)]
/// A generic dict entry: either a literal or a recursive dictionary
pub enum DictEntryGeneric {
    Lit(Datacell),
    Map(DictGeneric),
}

#[derive(Debug, PartialEq)]
#[cfg_attr(test, derive(Clone))]
/// A metadata dictionary
pub enum MetaDictEntry {
    Data(Datacell),
    Map(MetaDict),
}

/*
    patchsets
*/

#[derive(Debug, PartialEq, Default)]
struct MetaDictPatch(HashMap<Box<str>, Option<MetaDictPatchEntry>>);
#[derive(Debug, PartialEq)]
enum MetaDictPatchEntry {
    Data(Datacell),
    Map(MetaDictPatch),
}

/// Recursively flatten a [`DictGeneric`] into a [`MetaDict`]
pub fn rflatten_metadata(new: DictGeneric) -> MetaDict {
    let mut empty = MetaDict::new();
    _rflatten_metadata(new, &mut empty);
    empty
}

fn _rflatten_metadata(new: DictGeneric, empty: &mut MetaDict) {
    for (key, val) in new {
        if let Some(v) = val {
            match v {
                DictEntryGeneric::Lit(l) => {
                    empty.insert(key, MetaDictEntry::Data(l));
                }
                DictEntryGeneric::Map(m) => {
                    let mut rnew = MetaDict::new();
                    _rflatten_metadata(m, &mut rnew);
                    empty.insert(key, MetaDictEntry::Map(rnew));
                }
            }
        }
    }
}

/// Recursively merge a [`DictGeneric`] into a [`MetaDict`] with the use of an intermediary
/// patchset to avoid inconsistent states
pub fn rmerge_metadata(current: &mut MetaDict, new: DictGeneric) -> bool {
    let mut patch = MetaDictPatch::default();
    let current_ref = current as &_;
    let r = rmerge_metadata_prepare_patch(current_ref, new, &mut patch);
    if r {
        merge_data_with_patch(current, patch);
    }
    r
}

fn merge_data_with_patch(current: &mut MetaDict, patch: MetaDictPatch) {
    for (key, patch) in patch.0 {
        match patch {
            Some(MetaDictPatchEntry::Data(d)) => {
                current.st_upsert(key, MetaDictEntry::Data(d));
            }
            Some(MetaDictPatchEntry::Map(m)) => match current.get_mut(&key) {
                Some(current_recursive) => match current_recursive {
                    MetaDictEntry::Map(current_m) => {
                        merge_data_with_patch(current_m, m);
                    }
                    _ => {
                        // can never reach here since the patch is always correct
                        unreachable!()
                    }
                },
                None => {
                    let mut new = MetaDict::new();
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
    current: &MetaDict,
    new: DictGeneric,
    patch: &mut MetaDictPatch,
) -> bool {
    let mut new = new.into_iter();
    let mut okay = true;
    while new.len() != 0 && okay {
        let (key, new_entry) = new.next().unwrap();
        match (current.get(&key), new_entry) {
            // non-null -> non-null: merge flatten update
            (Some(this_current), Some(new_entry)) => {
                okay &= {
                    match (this_current, new_entry) {
                        (MetaDictEntry::Data(this_data), DictEntryGeneric::Lit(new_data))
                            if this_data.kind() == new_data.kind() =>
                        {
                            patch
                                .0
                                .insert(key, Some(MetaDictPatchEntry::Data(new_data)));
                            true
                        }
                        (
                            MetaDictEntry::Map(this_recursive_data),
                            DictEntryGeneric::Map(new_recursive_data),
                        ) => {
                            let mut this_patch = MetaDictPatch::default();
                            let okay = rmerge_metadata_prepare_patch(
                                this_recursive_data,
                                new_recursive_data,
                                &mut this_patch,
                            );
                            patch
                                .0
                                .insert(key, Some(MetaDictPatchEntry::Map(this_patch)));
                            okay
                        }
                        _ => false,
                    }
                };
            }
            // null -> non-null: flatten insert
            (None, Some(new_entry)) => match new_entry {
                DictEntryGeneric::Lit(d) => {
                    let _ = patch.0.insert(key, Some(MetaDictPatchEntry::Data(d)));
                }
                DictEntryGeneric::Map(m) => {
                    let mut this_patch = MetaDictPatch::default();
                    okay &= rmerge_metadata_prepare_patch(&into_dict!(), m, &mut this_patch);
                    let _ = patch
                        .0
                        .insert(key, Some(MetaDictPatchEntry::Map(this_patch)));
                }
            },
            // non-null -> null: remove
            (Some(_), None) => {
                patch.0.insert(key, None);
            }
            (None, None) => {
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
        Self::Lit(Datacell::from(l))
    }
}

impl<'a> From<Lit<'a>> for DictEntryGeneric {
    fn from(value: Lit<'a>) -> Self {
        Self::Lit(Datacell::from(value))
    }
}

direct_from! {
    DictEntryGeneric => {
        Datacell as Lit,
        DictGeneric as Map,
    }
}

direct_from! {
    MetaDictEntry => {
        Datacell as Data,
        MetaDict as Map,
    }
}
