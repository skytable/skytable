/*
 * Created on Sun Jan 29 2023
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
    super::{IndexSTSeqDllNode, IndexSTSeqDllNodePtr},
    crate::engine::idx::meta::AsHasher,
    std::{collections::hash_map::RandomState, marker::PhantomData, ptr},
};

#[derive(Debug)]
pub struct LiberalStrategy<K, V> {
    f: *mut IndexSTSeqDllNode<K, V>,
}

impl<K, V> AllocStrategy<K, V> for LiberalStrategy<K, V> {
    const NEW: Self = Self { f: ptr::null_mut() };
    const METRIC_REFRESH: bool = true;
    #[inline(always)]
    unsafe fn free(&mut self, n: *mut IndexSTSeqDllNode<K, V>) {
        (*n).n = self.f;
        self.f = n;
    }
    #[inline(always)]
    fn alloc(
        &mut self,
        node: IndexSTSeqDllNode<K, V>,
        refresh_metric: &mut bool,
    ) -> IndexSTSeqDllNodePtr<K, V> {
        if self.f.is_null() {
            IndexSTSeqDllNode::alloc_box(node)
        } else {
            *refresh_metric = true;
            unsafe {
                // UNSAFE(@ohsayan): Safe because we already did a nullptr check
                let f = self.f;
                self.f = (*self.f).n;
                ptr::write(f, node);
                IndexSTSeqDllNodePtr::new_unchecked(f)
            }
        }
    }
    #[inline(always)]
    fn cleanup(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): All nullck
            let mut c = self.f;
            while !c.is_null() {
                let nx = (*c).n;
                IndexSTSeqDllNode::dealloc_headless(c);
                c = nx;
            }
        }
        self.f = ptr::null_mut();
    }
}

#[derive(Debug)]
pub struct ConservativeStrategy<K, V> {
    _d: PhantomData<IndexSTSeqDllNodePtr<K, V>>,
}

impl<K, V> AllocStrategy<K, V> for ConservativeStrategy<K, V> {
    const NEW: Self = Self { _d: PhantomData };
    const METRIC_REFRESH: bool = false;
    #[inline(always)]
    unsafe fn free(&mut self, n: *mut IndexSTSeqDllNode<K, V>) {
        IndexSTSeqDllNode::dealloc_headless(n)
    }
    #[inline(always)]
    fn alloc(&mut self, node: IndexSTSeqDllNode<K, V>, _: &mut bool) -> IndexSTSeqDllNodePtr<K, V> {
        IndexSTSeqDllNode::alloc_box(node)
    }
    #[inline(always)]
    fn cleanup(&mut self) {}
}

pub trait AllocStrategy<K, V>: Sized {
    // HACK(@ohsayan): I trust the optimizer, but not so much
    const METRIC_REFRESH: bool;
    const NEW: Self;
    fn alloc(
        &mut self,
        node: IndexSTSeqDllNode<K, V>,
        refresh_metric: &mut bool,
    ) -> IndexSTSeqDllNodePtr<K, V>;
    unsafe fn free(&mut self, f: *mut IndexSTSeqDllNode<K, V>);
    fn cleanup(&mut self);
}

pub trait Config<K, V> {
    type Hasher: AsHasher;
    type AllocStrategy: AllocStrategy<K, V>;
}

#[derive(Debug, Default)]
pub struct ConservativeConfig<K, V>(PhantomData<super::IndexSTSeqDll<K, V, Self>>);

impl<K, V> Config<K, V> for ConservativeConfig<K, V> {
    type Hasher = RandomState;
    type AllocStrategy = ConservativeStrategy<K, V>;
}

#[derive(Debug, Default)]
pub struct LiberalConfig<K, V>(PhantomData<super::IndexSTSeqDll<K, V, Self>>);

impl<K, V> Config<K, V> for LiberalConfig<K, V> {
    type Hasher = RandomState;
    type AllocStrategy = LiberalStrategy<K, V>;
}
