/*
 * Created on Mon May 08 2023
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

#[cfg(debug_assertions)]
use super::IndexSTSeqDllMetrics;

use {
    super::{config::Config, IndexSTSeqDll},
    crate::engine::{
        idx::{AsKey, AsValue, IndexBaseSpec},
        sync::smart::EArc,
    },
    std::{
        mem::ManuallyDrop,
        ops::{Deref, DerefMut},
    },
};

#[derive(Debug)]
pub struct OrderedIdxRC<K, V, C: Config<K, V>> {
    base: ManuallyDrop<IndexSTSeqDll<K, V, C>>,
    rc: EArc,
}

impl<K, V, C: Config<K, V>> OrderedIdxRC<K, V, C> {
    fn new() -> Self {
        Self::new_with(IndexSTSeqDll::new())
    }
    fn new_with(idx: IndexSTSeqDll<K, V, C>) -> Self {
        Self {
            base: ManuallyDrop::new(idx),
            rc: unsafe {
                // UNSAFE(@ohsayan): we'll clean this up
                EArc::new()
            },
        }
    }
}

impl<K, V, C: Config<K, V>> Deref for OrderedIdxRC<K, V, C> {
    type Target = IndexSTSeqDll<K, V, C>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<K, V, C: Config<K, V>> DerefMut for OrderedIdxRC<K, V, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<K, V, C: Config<K, V>> Clone for OrderedIdxRC<K, V, C> {
    fn clone(&self) -> Self {
        let rc = unsafe {
            // UNSAFE(@ohsayan): called at clone position
            self.rc.rc_clone()
        };
        Self {
            base: unsafe {
                // UNSAFE(@ohsayan): just a raw clone. no big deal since this is an RC
                core::mem::transmute_copy(&self.base)
            },
            rc,
        }
    }
}

impl<K, V, C: Config<K, V>> Drop for OrderedIdxRC<K, V, C> {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): this is the dtor
            self.rc.rc_drop(|| ManuallyDrop::drop(&mut self.base))
        }
    }
}

impl<K, V, C: Config<K, V>> IndexBaseSpec for OrderedIdxRC<K, V, C> {
    const PREALLOC: bool = true;
    #[cfg(debug_assertions)]
    type Metrics = IndexSTSeqDllMetrics;

    fn idx_init_cap(cap: usize) -> Self {
        Self::new_with(IndexSTSeqDll::with_capacity(cap))
    }

    fn idx_init() -> Self {
        Self::new()
    }

    fn idx_init_with(s: Self) -> Self {
        s
    }

    #[cfg(debug_assertions)]
    fn idx_metrics(&self) -> &Self::Metrics {
        self.base.idx_metrics()
    }
}

impl<K: AsKey, V: AsValue + PartialEq, C: Config<K, V>> PartialEq for OrderedIdxRC<K, V, C> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                ._iter_unord_kv()
                .all(|(k, v)| other._get(k).unwrap().eq(v))
    }
}
