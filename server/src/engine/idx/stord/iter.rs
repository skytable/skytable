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
    super::{
        config::Config, IndexSTSeqDll, IndexSTSeqDllKeyptr, IndexSTSeqDllNode, IndexSTSeqDllNodePtr,
    },
    crate::engine::idx::{AsKey, AsValue},
    std::{
        collections::{
            hash_map::{Iter as StdMapIter, Keys as StdMapIterKey, Values as StdMapIterVal},
            HashMap as StdMap,
        },
        fmt::{self, Debug},
        iter::FusedIterator,
        marker::PhantomData,
        mem::ManuallyDrop,
        ptr::{self, NonNull},
    },
};

macro_rules! unsafe_marker_impl {
    (unsafe impl for $ty:ty) => {
        unsafe impl<'a, K: Send, V: Send> Send for $ty {}
        unsafe impl<'a, K: Sync, V: Sync> Sync for $ty {}
    };
}

pub struct IndexSTSeqDllIterUnordKV<'a, K: 'a, V: 'a> {
    i: StdMapIter<'a, IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>>,
}

// UNSAFE(@ohsayan): aliasing guarantees correctness
unsafe_marker_impl!(unsafe impl for IndexSTSeqDllIterUnordKV<'a, K, V>);

impl<'a, K: 'a, V: 'a> IndexSTSeqDllIterUnordKV<'a, K, V> {
    #[inline(always)]
    #[allow(dead_code)]
    pub(super) fn new<S>(
        m: &'a StdMap<IndexSTSeqDllKeyptr<K>, NonNull<IndexSTSeqDllNode<K, V>>, S>,
    ) -> Self {
        Self { i: m.iter() }
    }
}

impl<'a, K, V> Clone for IndexSTSeqDllIterUnordKV<'a, K, V> {
    fn clone(&self) -> Self {
        Self { i: self.i.clone() }
    }
}

impl<'a, K, V> Iterator for IndexSTSeqDllIterUnordKV<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.i.next().map(|(_, n)| {
            let n = n.as_ptr();
            unsafe {
                // UNSAFE(@ohsayan): nullck
                (&(*n).k, &(*n).v)
            }
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        <_ as Iterator>::size_hint(&self.i)
    }
}

impl<'a, K, V> ExactSizeIterator for IndexSTSeqDllIterUnordKV<'a, K, V> {
    fn len(&self) -> usize {
        self.i.len()
    }
}

impl<'a, K, V> FusedIterator for IndexSTSeqDllIterUnordKV<'a, K, V> {}

impl<'a, K: 'a + Debug, V: 'a + Debug> Debug for IndexSTSeqDllIterUnordKV<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

pub struct IndexSTSeqDllIterUnordKey<'a, K: 'a, V: 'a> {
    k: StdMapIterKey<'a, IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>>,
}

// UNSAFE(@ohsayan): aliasing guarantees correctness
unsafe_marker_impl!(unsafe impl for IndexSTSeqDllIterUnordKey<'a, K, V>);

impl<'a, K: 'a, V: 'a> IndexSTSeqDllIterUnordKey<'a, K, V> {
    #[inline(always)]
    #[allow(dead_code)]
    pub(super) fn new<S>(
        m: &'a StdMap<IndexSTSeqDllKeyptr<K>, NonNull<IndexSTSeqDllNode<K, V>>, S>,
    ) -> Self {
        Self { k: m.keys() }
    }
}

impl<'a, K, V> Clone for IndexSTSeqDllIterUnordKey<'a, K, V> {
    fn clone(&self) -> Self {
        Self { k: self.k.clone() }
    }
}

impl<'a, K, V> Iterator for IndexSTSeqDllIterUnordKey<'a, K, V> {
    type Item = &'a K;
    fn next(&mut self) -> Option<Self::Item> {
        self.k.next().map(|k| {
            unsafe {
                // UNSAFE(@ohsayan): nullck
                &*k.p
            }
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        <_ as Iterator>::size_hint(&self.k)
    }
}

impl<'a, K, V> ExactSizeIterator for IndexSTSeqDllIterUnordKey<'a, K, V> {
    fn len(&self) -> usize {
        self.k.len()
    }
}

impl<'a, K, V> FusedIterator for IndexSTSeqDllIterUnordKey<'a, K, V> {}

impl<'a, K: Debug, V> Debug for IndexSTSeqDllIterUnordKey<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

pub struct IndexSTSeqDllIterUnordValue<'a, K: 'a, V: 'a> {
    v: StdMapIterVal<'a, IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>>,
}

// UNSAFE(@ohsayan): aliasing guarantees correctness
unsafe_marker_impl!(unsafe impl for IndexSTSeqDllIterUnordValue<'a, K, V>);

impl<'a, K: 'a, V: 'a> IndexSTSeqDllIterUnordValue<'a, K, V> {
    #[inline(always)]
    #[allow(dead_code)]
    pub(super) fn new<S>(
        m: &'a StdMap<IndexSTSeqDllKeyptr<K>, NonNull<IndexSTSeqDllNode<K, V>>, S>,
    ) -> Self {
        Self { v: m.values() }
    }
}

impl<'a, K, V> Clone for IndexSTSeqDllIterUnordValue<'a, K, V> {
    fn clone(&self) -> Self {
        Self { v: self.v.clone() }
    }
}

impl<'a, K, V> Iterator for IndexSTSeqDllIterUnordValue<'a, K, V> {
    type Item = &'a V;
    fn next(&mut self) -> Option<Self::Item> {
        self.v.next().map(|n| {
            unsafe {
                // UNSAFE(@ohsayan): nullck
                &(*n.as_ptr()).v
            }
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        <_ as Iterator>::size_hint(&self.v)
    }
}

impl<'a, K, V> ExactSizeIterator for IndexSTSeqDllIterUnordValue<'a, K, V> {
    fn len(&self) -> usize {
        self.v.len()
    }
}

impl<'a, K, V> FusedIterator for IndexSTSeqDllIterUnordValue<'a, K, V> {}

impl<'a, K, V: Debug> Debug for IndexSTSeqDllIterUnordValue<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

trait IndexSTSeqDllIterOrdConfig<K, V> {
    type Ret<'a>
    where
        K: 'a,
        V: 'a;
    /// ## Safety
    /// Ptr must be non-null
    unsafe fn read_ret<'a>(ptr: *const IndexSTSeqDllNode<K, V>) -> Option<Self::Ret<'a>>
    where
        K: 'a,
        V: 'a;
}

struct IndexSTSeqDllIterOrdConfigFull;

impl<K, V> IndexSTSeqDllIterOrdConfig<K, V> for IndexSTSeqDllIterOrdConfigFull {
    type Ret<'a> = (&'a K, &'a V) where K: 'a, V: 'a;
    #[inline(always)]
    unsafe fn read_ret<'a>(ptr: *const IndexSTSeqDllNode<K, V>) -> Option<Self::Ret<'a>>
    where
        K: 'a,
        V: 'a,
    {
        Some((&(*ptr).k, &(*ptr).v))
    }
}

struct IndexSTSeqDllIterOrdConfigKey;

impl<K, V> IndexSTSeqDllIterOrdConfig<K, V> for IndexSTSeqDllIterOrdConfigKey {
    type Ret<'a> = &'a K
    where
        K: 'a,
        V: 'a;
    #[inline(always)]
    unsafe fn read_ret<'a>(ptr: *const IndexSTSeqDllNode<K, V>) -> Option<&'a K>
    where
        K: 'a,
        V: 'a,
    {
        Some(&(*ptr).k)
    }
}

struct IndexSTSeqDllIterOrdConfigValue;

impl<K, V> IndexSTSeqDllIterOrdConfig<K, V> for IndexSTSeqDllIterOrdConfigValue {
    type Ret<'a> = &'a V
    where
        K: 'a,
        V: 'a;
    #[inline(always)]
    unsafe fn read_ret<'a>(ptr: *const IndexSTSeqDllNode<K, V>) -> Option<&'a V>
    where
        K: 'a,
        V: 'a,
    {
        Some(&(*ptr).v)
    }
}

pub(super) struct OrderedOwnedIteratorRaw<K, V> {
    h: *mut IndexSTSeqDllNode<K, V>,
    t: *mut IndexSTSeqDllNode<K, V>,
    r: usize,
}

impl<K: AsKey, V: AsValue> OrderedOwnedIteratorRaw<K, V> {
    pub(super) fn new<Mc: Config<K, V>>(mut idx: IndexSTSeqDll<K, V, Mc>) -> Self {
        let (h, t) = if idx.h.is_null() {
            (ptr::null_mut(), ptr::null_mut())
        } else {
            unsafe {
                // UNSAFE(@ohsayan): just verified that head is non null
                let r = ((*idx.h).p, (*idx.h).n);
                // dealloc sentinel
                IndexSTSeqDllNode::dealloc_headless(idx.h);
                r
            }
        };
        // clean up if needed
        idx.vacuum_full();
        let mut idx = ManuallyDrop::new(idx);
        // chuck the map
        drop(unsafe { ptr::read((&mut idx.m) as *mut _) });
        // we own everything now
        Self { h, t, r: idx.len() }
    }
}

impl<K, V> OrderedOwnedIteratorRaw<K, V> {
    #[inline(always)]
    fn _next(&mut self) -> Option<(K, V)> {
        // don't use head tail comparison! if there is just one item in the list, and since we don't have the sentinel
        // it will just point to itself!
        if self.r == 0 {
            None
        } else {
            self.r -= 1;
            unsafe {
                // UNSAFE(@ohsayan): +nullck
                let this = ptr::read(self.h);
                // ptr of next node
                let ptr = (*self.h).p;
                // destroy this node
                IndexSTSeqDllNode::dealloc_headless(self.h);
                // update head ptr
                self.h = ptr;
                Some((this.k, this.v))
            }
        }
    }
    #[inline(always)]
    fn _next_back(&mut self) -> Option<(K, V)> {
        if self.r == 0 {
            None
        } else {
            self.r -= 1;
            unsafe {
                // UNSAFE(@ohsayan): +nullck
                self.t = (*self.t).n;
                let this = ptr::read(self.t);
                IndexSTSeqDllNode::dealloc_headless(self.t);
                Some((this.k, this.v))
            }
        }
    }
}

impl<K, V> Drop for OrderedOwnedIteratorRaw<K, V> {
    fn drop(&mut self) {
        // clean up what's left
        while let Some(_) = self._next() {}
    }
}

pub struct OrderedOwnedIteratorKV<K, V>(pub(super) OrderedOwnedIteratorRaw<K, V>);

impl<K: AsKey, V: AsValue> Iterator for OrderedOwnedIteratorKV<K, V> {
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.0._next()
    }
}

impl<K: AsKey, V: AsValue> DoubleEndedIterator for OrderedOwnedIteratorKV<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0._next_back()
    }
}

pub struct OrderedOwnedIteratorKey<K, V>(pub(super) OrderedOwnedIteratorRaw<K, V>);

impl<K: AsKey, V: AsValue> Iterator for OrderedOwnedIteratorKey<K, V> {
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        self.0._next().map(|(k, _)| k)
    }
}
impl<K: AsKey, V: AsValue> DoubleEndedIterator for OrderedOwnedIteratorKey<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0._next_back().map(|(k, _)| k)
    }
}

pub struct OrderedOwnedIteratorValue<K, V>(pub(super) OrderedOwnedIteratorRaw<K, V>);

impl<K: AsKey, V: AsValue> Iterator for OrderedOwnedIteratorValue<K, V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.0._next().map(|(_, v)| v)
    }
}
impl<K: AsKey, V: AsValue> DoubleEndedIterator for OrderedOwnedIteratorValue<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0._next_back().map(|(_, v)| v)
    }
}

struct IndexSTSeqDllIterOrdBase<'a, K: 'a, V: 'a, C: IndexSTSeqDllIterOrdConfig<K, V>> {
    h: *const IndexSTSeqDllNode<K, V>,
    t: *const IndexSTSeqDllNode<K, V>,
    r: usize,
    _l: PhantomData<(&'a K, &'a V, C)>,
}

impl<'a, K: 'a, V: 'a, C: IndexSTSeqDllIterOrdConfig<K, V>> IndexSTSeqDllIterOrdBase<'a, K, V, C> {
    #[inline(always)]
    fn new<Mc: Config<K, V>>(idx: &'a IndexSTSeqDll<K, V, Mc>) -> Self {
        Self {
            h: if idx.h.is_null() {
                ptr::null_mut()
            } else {
                unsafe {
                    // UNSAFE(@ohsayan): nullck
                    (*idx.h).p
                }
            },
            t: idx.h,
            r: idx.len(),
            _l: PhantomData,
        }
    }
    #[inline(always)]
    fn _next(&mut self) -> Option<C::Ret<'a>> {
        if self.h == self.t {
            None
        } else {
            self.r -= 1;
            unsafe {
                // UNSAFE(@ohsayan): Assuming we had a legal init, this should be fine
                let this = C::read_ret(self.h);
                self.h = (*self.h).p;
                this
            }
        }
    }
    #[inline(always)]
    fn _next_back(&mut self) -> Option<C::Ret<'a>> {
        if self.h == self.t {
            None
        } else {
            self.r -= 1;
            unsafe {
                // UNSAFE(@ohsayan): legal init, then ok
                self.t = (*self.t).n;
                // UNSAFE(@ohsayan): non-null (sentinel)
                C::read_ret(self.t)
            }
        }
    }
}

impl<'a, K: 'a, V: 'a, C: IndexSTSeqDllIterOrdConfig<K, V>> Debug
    for IndexSTSeqDllIterOrdBase<'a, K, V, C>
where
    C::Ret<'a>: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

impl<'a, K: 'a, V: 'a, C: IndexSTSeqDllIterOrdConfig<K, V>> Iterator
    for IndexSTSeqDllIterOrdBase<'a, K, V, C>
{
    type Item = C::Ret<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self._next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.r, Some(self.r))
    }
}

impl<'a, K: 'a, V: 'a, C: IndexSTSeqDllIterOrdConfig<K, V>> DoubleEndedIterator
    for IndexSTSeqDllIterOrdBase<'a, K, V, C>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self._next_back()
    }
}

impl<'a, K, V, C: IndexSTSeqDllIterOrdConfig<K, V>> ExactSizeIterator
    for IndexSTSeqDllIterOrdBase<'a, K, V, C>
{
    fn len(&self) -> usize {
        self.r
    }
}

impl<'a, K, V, C: IndexSTSeqDllIterOrdConfig<K, V>> Clone
    for IndexSTSeqDllIterOrdBase<'a, K, V, C>
{
    fn clone(&self) -> Self {
        Self { ..*self }
    }
}

#[derive(Debug)]
pub struct IndexSTSeqDllIterOrdKV<'a, K: 'a, V: 'a> {
    i: IndexSTSeqDllIterOrdBase<'a, K, V, IndexSTSeqDllIterOrdConfigFull>,
}
impl<'a, K: 'a, V: 'a> IndexSTSeqDllIterOrdKV<'a, K, V> {
    pub(super) fn new<C: Config<K, V>>(arg: &'a IndexSTSeqDll<K, V, C>) -> Self {
        Self {
            i: IndexSTSeqDllIterOrdBase::new(arg),
        }
    }
}

// UNSAFE(@ohsayan): aliasing guarantees correctness
unsafe_marker_impl!(unsafe impl for IndexSTSeqDllIterOrdKV<'a, K, V>);

impl<'a, K: 'a, V: 'a> Iterator for IndexSTSeqDllIterOrdKV<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        self.i.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.i.size_hint()
    }
}

impl<'a, K: 'a, V: 'a> DoubleEndedIterator for IndexSTSeqDllIterOrdKV<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.i.next_back()
    }
}

impl<'a, K, V> ExactSizeIterator for IndexSTSeqDllIterOrdKV<'a, K, V> {
    fn len(&self) -> usize {
        self.i.len()
    }
}

impl<'a, K, V> Clone for IndexSTSeqDllIterOrdKV<'a, K, V> {
    fn clone(&self) -> Self {
        Self { i: self.i.clone() }
    }
}

#[derive(Debug)]
pub struct IndexSTSeqDllIterOrdKey<'a, K: 'a, V: 'a> {
    i: IndexSTSeqDllIterOrdBase<'a, K, V, IndexSTSeqDllIterOrdConfigKey>,
}
impl<'a, K: 'a, V: 'a> IndexSTSeqDllIterOrdKey<'a, K, V> {
    pub(super) fn new<C: Config<K, V>>(arg: &'a IndexSTSeqDll<K, V, C>) -> Self {
        Self {
            i: IndexSTSeqDllIterOrdBase::new(arg),
        }
    }
}

// UNSAFE(@ohsayan): aliasing guarantees correctness
unsafe_marker_impl!(unsafe impl for IndexSTSeqDllIterOrdKey<'a, K, V>);

impl<'a, K: 'a, V: 'a> Iterator for IndexSTSeqDllIterOrdKey<'a, K, V> {
    type Item = &'a K;
    fn next(&mut self) -> Option<Self::Item> {
        self.i.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.i.size_hint()
    }
}

impl<'a, K: 'a, V: 'a> DoubleEndedIterator for IndexSTSeqDllIterOrdKey<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.i.next_back()
    }
}

impl<'a, K, V> ExactSizeIterator for IndexSTSeqDllIterOrdKey<'a, K, V> {
    fn len(&self) -> usize {
        self.i.len()
    }
}

impl<'a, K, V> Clone for IndexSTSeqDllIterOrdKey<'a, K, V> {
    fn clone(&self) -> Self {
        Self { i: self.i.clone() }
    }
}

#[derive(Debug)]
pub struct IndexSTSeqDllIterOrdValue<'a, K: 'a, V: 'a> {
    i: IndexSTSeqDllIterOrdBase<'a, K, V, IndexSTSeqDllIterOrdConfigValue>,
}
impl<'a, K: 'a, V: 'a> IndexSTSeqDllIterOrdValue<'a, K, V> {
    #[allow(dead_code)]
    pub(super) fn new<C: Config<K, V>>(arg: &'a IndexSTSeqDll<K, V, C>) -> Self {
        Self {
            i: IndexSTSeqDllIterOrdBase::new(arg),
        }
    }
}

// UNSAFE(@ohsayan): aliasing guarantees correctness
unsafe_marker_impl!(unsafe impl for IndexSTSeqDllIterOrdValue<'a, K, V>);

impl<'a, K: 'a, V: 'a> Iterator for IndexSTSeqDllIterOrdValue<'a, K, V> {
    type Item = &'a V;
    fn next(&mut self) -> Option<Self::Item> {
        self.i.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.i.size_hint()
    }
}

impl<'a, K: 'a, V: 'a> DoubleEndedIterator for IndexSTSeqDllIterOrdValue<'a, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.i.next_back()
    }
}

impl<'a, K, V> ExactSizeIterator for IndexSTSeqDllIterOrdValue<'a, K, V> {
    fn len(&self) -> usize {
        self.i.len()
    }
}

impl<'a, K, V> Clone for IndexSTSeqDllIterOrdValue<'a, K, V> {
    fn clone(&self) -> Self {
        Self { i: self.i.clone() }
    }
}
