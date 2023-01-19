/*
 * Created on Mon Jan 16 2023
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

use super::def::{AsKey, AsKeyRef, AsValue, IndexBaseSpec, STIndex, STIndexSeq};
use std::{
    alloc::{alloc as std_alloc, dealloc as std_dealloc, Layout},
    borrow::Borrow,
    collections::{
        hash_map::{Iter, Keys as StdMapIterKey, RandomState, Values as StdMapIterVal},
        HashMap as StdMap,
    },
    fmt::{self, Debug},
    hash::{BuildHasher, Hash, Hasher},
    iter::FusedIterator,
    marker::PhantomData,
    mem,
    ptr::{self, NonNull},
};

// re-exports for convenience
pub type IndexSTSeq<K, V, S> = IndexSTSeqDll<K, V, S>;
pub type IndexSTSeqDef<K, V> = IndexSTSeq<K, V, IndexSTSeqHasher>;
pub type IndexSTSeqHasher = RandomState;

/*
    For the ordered index impl, we resort to some crazy unsafe code, especially because there's no other way to
    deal with non-primitive Ks. That's why we'll ENTIRELY AVOID exporting any structures; if we end up using a node
    or a ptr struct anywhere inappropriate, it'll most likely SEGFAULT. So yeah, better be careful with this one.
    Second note, I'm not a big fan of the DLL and will most likely try a different approach in the future; this one
    is the most convenient option for now.

    -- Sayan (@ohsayan) // Jan. 16 '23
*/

#[repr(transparent)]
/// # WARNING: Segfault/UAF alert
///
/// Yeah, this type is going to segfault if you decide to use it in random places. Literally, don't use it if
/// you're unsure about it's validity. For example, if you simply `==` this or attempt to use it an a hashmap,
/// you can segfault. IFF, the ptr is valid will it not segfault
struct IndexSTSeqDllKeyptr<K> {
    p: *const K,
}

impl<K> IndexSTSeqDllKeyptr<K> {
    #[inline(always)]
    fn new(r: &K) -> Self {
        Self { p: r as *const _ }
    }
}

impl<K: Hash> Hash for IndexSTSeqDllKeyptr<K> {
    #[inline(always)]
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        unsafe {
            /*
                UNSAFE(@ohsayan): BAD. THIS IS NOT SAFE, but dang it, it's the only way we can do this without
                dynamic rule checking. I wish there was a `'self` lifetime
            */
            (*self.p).hash(state)
        }
    }
}

impl<K: PartialEq> PartialEq for IndexSTSeqDllKeyptr<K> {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            /*
                UNSAFE(@ohsayan): BAD. THIS IS NOT SAFE, but dang it, it's the only way we can do this without
                dynamic rule checking. I wish there was a `'self` lifetime
            */
            (*self.p).eq(&*other.p)
        }
    }
}

impl<K: Eq> Eq for IndexSTSeqDllKeyptr<K> {}

// stupid type for trait impl conflict riddance
#[derive(Debug, Hash, PartialEq, Eq)]
#[repr(transparent)]
struct IndexSTSeqDllQref<Q: ?Sized>(Q);

impl<Q: ?Sized> IndexSTSeqDllQref<Q> {
    #[inline(always)]
    unsafe fn from_ref(r: &Q) -> &Self {
        mem::transmute(r)
    }
}

impl<K, Q> Borrow<IndexSTSeqDllQref<Q>> for IndexSTSeqDllKeyptr<K>
where
    K: Borrow<Q>,
    Q: ?Sized,
{
    #[inline(always)]
    fn borrow(&self) -> &IndexSTSeqDllQref<Q> {
        unsafe {
            /*
                UNSAFE(@ohsayan): BAD. This deref ain't safe either. ref is good though
            */
            IndexSTSeqDllQref::from_ref((*self.p).borrow())
        }
    }
}

#[derive(Debug)]
struct IndexSTSeqDllNode<K, V> {
    k: K,
    v: V,
    n: *mut Self,
    p: *mut Self,
}

impl<K, V> IndexSTSeqDllNode<K, V> {
    const LAYOUT: Layout = Layout::new::<Self>();
    #[inline(always)]
    fn new(k: K, v: V, n: *mut Self, p: *mut Self) -> Self {
        Self { k, v, n, p }
    }
    #[inline(always)]
    fn new_null(k: K, v: V) -> Self {
        Self::new(k, v, ptr::null_mut(), ptr::null_mut())
    }
    #[inline(always)]
    fn _alloc_with_garbage() -> *mut Self {
        unsafe {
            // UNSAFE(@ohsayan): aight shut up, it's a malloc
            let ptr = std_alloc(Self::LAYOUT) as *mut Self;
            assert!(!ptr.is_null(), "damn the allocator failed");
            ptr
        }
    }
    #[inline(always)]
    fn _alloc<const WPTR_N: bool, const WPTR_P: bool>(Self { k, v, p, n }: Self) -> *mut Self {
        unsafe {
            // UNSAFE(@ohsayan): grow up, we're writing to a fresh block
            let ptr = Self::_alloc_with_garbage();
            (*ptr).k = k;
            (*ptr).v = v;
            if WPTR_N {
                (*ptr).n = n;
            }
            if WPTR_P {
                (*ptr).p = p;
            }
            ptr
        }
    }
    #[inline(always)]
    fn alloc_null(k: K, v: V) -> *mut Self {
        Self::_alloc::<false, false>(Self::new_null(k, v))
    }
    #[inline(always)]
    fn alloc(k: K, v: V, p: *mut Self, n: *mut Self) -> *mut Self {
        Self::_alloc::<true, true>(Self::new(k, v, p, n))
    }
    #[inline(always)]
    unsafe fn _drop(slf: *mut Self) {
        let _ = Box::from_raw(slf);
    }
    #[inline(always)]
    /// LEAK: K, V
    unsafe fn dealloc_headless(slf: *mut Self) {
        std_dealloc(slf as *mut u8, Self::LAYOUT)
    }
    #[inline(always)]
    unsafe fn unlink(node: *mut Self) {
        (*((*node).p)).n = (*node).n;
        (*((*node).n)).p = (*node).p;
    }
    #[inline(always)]
    unsafe fn link(from: *mut Self, to: *mut Self) {
        (*to).n = (*from).n;
        (*to).p = from;
        (*from).n = to;
        (*(*to).n).p = to;
    }
    #[inline(always)]
    fn alloc_box(node: IndexSTSeqDllNode<K, V>) -> NonNull<IndexSTSeqDllNode<K, V>> {
        unsafe {
            // UNSAFE(@ohsayan): Safe because of box alloc
            NonNull::new_unchecked(Box::into_raw(Box::new(node)))
        }
    }
}

type IndexSTSeqDllNodePtr<K, V> = NonNull<IndexSTSeqDllNode<K, V>>;

#[cfg(debug_assertions)]
pub struct IndexSTSeqDllMetrics {
    stat_f: usize,
}

impl IndexSTSeqDllMetrics {
    pub fn report_f(&self) -> usize {
        self.stat_f
    }
    fn new() -> IndexSTSeqDllMetrics {
        Self { stat_f: 0 }
    }
}

/// An ST-index with ordering. Inefficient ordered scanning since not in block
pub struct IndexSTSeqDll<K, V, S> {
    m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, S>,
    h: *mut IndexSTSeqDllNode<K, V>,
    f: *mut IndexSTSeqDllNode<K, V>,
    #[cfg(debug_assertions)]
    metrics: IndexSTSeqDllMetrics,
}

impl<K, V, S: BuildHasher> IndexSTSeqDll<K, V, S> {
    const DEF_CAP: usize = 0;
    #[inline(always)]
    fn _new(
        m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, S>,
        h: *mut IndexSTSeqDllNode<K, V>,
        f: *mut IndexSTSeqDllNode<K, V>,
    ) -> IndexSTSeqDll<K, V, S> {
        Self {
            m,
            h,
            f,
            metrics: IndexSTSeqDllMetrics::new(),
        }
    }
    #[inline(always)]
    fn _new_map(m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, S>) -> Self {
        Self::_new(m, ptr::null_mut(), ptr::null_mut())
    }
    #[inline(always)]
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(Self::DEF_CAP, hasher)
    }
    #[inline(always)]
    pub fn with_capacity_and_hasher(cap: usize, hasher: S) -> Self {
        Self::_new_map(StdMap::with_capacity_and_hasher(cap, hasher))
    }
}

impl<K, V> IndexSTSeqDll<K, V, RandomState> {
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_capacity(Self::DEF_CAP)
    }
    #[inline(always)]
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, RandomState::default())
    }
}

impl<K, V, S> IndexSTSeqDll<K, V, S> {
    #[inline(always)]
    fn metrics_update_f_decr(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.metrics.stat_f -= 1;
        }
    }
    #[inline(always)]
    fn metrics_update_f_incr(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.metrics.stat_f += 1;
        }
    }
    #[inline(always)]
    fn ensure_sentinel(&mut self) {
        if self.h.is_null() {
            let ptr = IndexSTSeqDllNode::_alloc_with_garbage();
            unsafe {
                //  UNSAFE(@ohsayan): Fresh alloc
                self.h = ptr;
                (*ptr).p = ptr;
                (*ptr).n = ptr;
            }
        }
    }
    #[inline(always)]
    /// ## Safety
    ///
    /// Head must not be null
    unsafe fn drop_nodes_full(&mut self) {
        // don't drop sentinenl
        let mut c = (*self.h).n;
        while c != self.h {
            let nx = (*c).n;
            IndexSTSeqDllNode::_drop(c);
            c = nx;
        }
    }
    #[inline(always)]
    fn vacuum_free(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): All nullck
            let mut c = self.f;
            while !c.is_null() {
                let nx = (*c).n;
                IndexSTSeqDllNode::dealloc_headless(c);
                c = nx;
                self.metrics_update_f_decr();
            }
        }
        self.f = ptr::null_mut();
    }
    #[inline(always)]
    fn recycle_or_alloc(&mut self, node: IndexSTSeqDllNode<K, V>) -> IndexSTSeqDllNodePtr<K, V> {
        if self.f.is_null() {
            IndexSTSeqDllNode::alloc_box(node)
        } else {
            self.metrics_update_f_decr();
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
    /// NOTE: `&mut Self` for aliasing
    /// ## Safety
    /// Ensure head is non null
    unsafe fn link(&mut self, node: IndexSTSeqDllNodePtr<K, V>) {
        IndexSTSeqDllNode::link(self.h, node.as_ptr())
    }
    pub fn len(&self) -> usize {
        self.m.len()
    }
}

impl<K: AsKey, V: AsValue, S: BuildHasher> IndexSTSeqDll<K, V, S> {
    #[inline(always)]
    /// Clean up unused and cached memory
    fn vacuum_full(&mut self) {
        self.m.shrink_to_fit();
        self.vacuum_free();
    }
}

impl<K: AsKey, V: AsValue, S: BuildHasher> IndexSTSeqDll<K, V, S> {
    const GET_REFRESH: bool = true;
    const GET_BYPASS: bool = false;
    #[inline(always)]
    fn _insert(&mut self, k: K, v: V) -> bool {
        if self.m.contains_key(&IndexSTSeqDllKeyptr::new(&k)) {
            return false;
        }
        self.__insert(k, v)
    }
    fn _get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: AsKeyRef,
    {
        self.m
            .get(unsafe {
                // UNSAFE(@ohsayan): Ref with correct bounds
                IndexSTSeqDllQref::from_ref(k)
            })
            .map(|e| unsafe {
                // UNSAFE(@ohsayan): ref is non-null and ensures aliasing reqs
                &(e.as_ref()).read_value().v
            })
    }
    #[inline(always)]
    fn _update<Q: ?Sized>(&mut self, k: &Q, v: V) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsKeyRef,
    {
        match self.m.get(unsafe {
            // UNSAFE(@ohsayan): Just got a ref with the right bounds
            IndexSTSeqDllQref::from_ref(k)
        }) {
            Some(e) => unsafe {
                // UNSAFE(@ohsayan): Impl guarantees that entry presence == nullck head
                self.__update(*e, v)
            },
            None => return None,
        }
    }
    #[inline(always)]
    fn _upsert(&mut self, k: K, v: V) -> Option<V> {
        match self.m.get(&IndexSTSeqDllKeyptr::new(&k)) {
            Some(e) => unsafe {
                // UNSAFE(@ohsayan): Impl guarantees that entry presence == nullck head
                self.__update(*e, v)
            },
            None => {
                let _ = self.__insert(k, v);
                None
            }
        }
    }
    #[inline(always)]
    fn _remove<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsKeyRef + ?Sized,
    {
        self.m
            .remove(unsafe {
                // UNSAFE(@ohsayan): good trait bounds and type
                IndexSTSeqDllQref::from_ref(k)
            })
            .map(|n| unsafe {
                let n = n.as_ptr();
                // UNSAFE(@ohsayan): Correct init and aligned to K
                drop(ptr::read(&(*n).k));
                // UNSAFE(@ohsayan): Correct init and aligned to V
                let v = ptr::read(&(*n).v);
                // UNSAFE(@ohsayan): non-null guaranteed by as_ptr
                IndexSTSeqDllNode::unlink(n);
                (*n).n = self.f;
                self.f = n;
                self.metrics_update_f_incr();
                v
            })
    }
    #[inline(always)]
    fn __insert(&mut self, k: K, v: V) -> bool {
        self.ensure_sentinel();
        let node = self.recycle_or_alloc(IndexSTSeqDllNode::new_null(k, v));
        let kptr = unsafe {
            // UNSAFE(@ohsayan): All g, we allocated it rn
            IndexSTSeqDllKeyptr::new(&node.as_ref().k)
        };
        let _ = self.m.insert(kptr, node);
        unsafe {
            // UNSAFE(@ohsayan): sentinel check done
            self.link(node);
        }
        true
    }
    #[inline(always)]
    /// ## Safety
    ///
    /// Has sentinel
    unsafe fn __update(&mut self, e: NonNull<IndexSTSeqDllNode<K, V>>, v: V) -> Option<V> {
        let old = unsafe {
            // UNSAFE(@ohsayan): Same type layout, alignments and non-null
            ptr::replace(&mut (*e.as_ptr()).v, v)
        };
        self._refresh(e);
        Some(old)
    }
    #[inline(always)]
    /// ## Safety
    ///
    /// Has sentinel
    unsafe fn _refresh(&mut self, e: NonNull<IndexSTSeqDllNode<K, V>>) {
        // UNSAFE(@ohsayan): Since it's in the collection, it is a valid ptr
        IndexSTSeqDllNode::unlink(e.as_ptr());
        // UNSAFE(@ohsayan): As we found a node, our impl guarantees that the head is not-null
        self.link(e);
    }
    #[inline(always)]
    fn _clear(&mut self) {
        self.m.clear();
        if !self.h.is_null() {
            unsafe {
                // UNSAFE(@ohsayan): nullck
                self.drop_nodes_full();
                // UNSAFE(@ohsayan): Drop won't kill sentinel; link back to self
                (*self.h).p = self.h;
                (*self.h).n = self.h;
            }
        }
    }
    #[inline(always)]
    fn _iter_unord_kv<'a>(&'a self) -> IndexSTSeqDllIterUnordKV<'a, K, V> {
        IndexSTSeqDllIterUnordKV::new(&self.m)
    }
    #[inline(always)]
    fn _iter_unord_k<'a>(&'a self) -> IndexSTSeqDllIterUnordKey<'a, K, V> {
        IndexSTSeqDllIterUnordKey::new(&self.m)
    }
    #[inline(always)]
    fn _iter_unord_v<'a>(&'a self) -> IndexSTSeqDllIterUnordValue<'a, K, V> {
        IndexSTSeqDllIterUnordValue::new(&self.m)
    }
    #[inline(always)]
    fn _iter_ord_kv<'a>(&'a self) -> IndexSTSeqDllIterOrdKV<'a, K, V> {
        IndexSTSeqDllIterOrdKV {
            i: IndexSTSeqDllIterOrdBase::new(self),
        }
    }
    #[inline(always)]
    fn _iter_ord_k<'a>(&'a self) -> IndexSTSeqDllIterOrdKey<'a, K, V> {
        IndexSTSeqDllIterOrdKey {
            i: IndexSTSeqDllIterOrdBase::new(self),
        }
    }
    #[inline(always)]
    fn _iter_ord_v<'a>(&'a self) -> IndexSTSeqDllIterOrdValue<'a, K, V> {
        IndexSTSeqDllIterOrdValue {
            i: IndexSTSeqDllIterOrdBase::new(self),
        }
    }
}

impl<K, V, S> Drop for IndexSTSeqDll<K, V, S> {
    fn drop(&mut self) {
        if !self.h.is_null() {
            unsafe {
                // UNSAFE(@ohsayan): nullck
                self.drop_nodes_full();
                // UNSAFE(@ohsayan): nullck: drop doesn't clear sentinel
                IndexSTSeqDllNode::dealloc_headless(self.h);
            }
        }
        self.vacuum_free();
    }
}

impl<K: AsKey, V: AsValue, S: BuildHasher + Default> FromIterator<(K, V)>
    for IndexSTSeqDll<K, V, S>
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut slf = Self::with_hasher(S::default());
        iter.into_iter()
            .for_each(|(k, v)| assert!(slf._insert(k, v)));
        slf
    }
}

impl<K, V, S: BuildHasher + Default> IndexBaseSpec<K, V> for IndexSTSeqDll<K, V, S>
where
    K: AsKey,
    V: AsValue,
{
    const PREALLOC: bool = true;

    type Metrics = IndexSTSeqDllMetrics;

    type IterKV<'a> = IndexSTSeqDllIterUnordKV<'a, K, V>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    type IterKey<'a> = IndexSTSeqDllIterUnordKey<'a, K, V>
    where
        Self: 'a,
        K: 'a;

    type IterValue<'a> = IndexSTSeqDllIterUnordValue<'a, K, V>
    where
        Self: 'a,
        V: 'a;

    fn idx_init() -> Self {
        Self::with_hasher(S::default())
    }

    fn idx_init_with(s: Self) -> Self {
        Self::from(s)
    }

    fn idx_iter_kv<'a>(&'a self) -> Self::IterKV<'a> {
        self._iter_unord_kv()
    }

    fn idx_iter_key<'a>(&'a self) -> Self::IterKey<'a> {
        self._iter_unord_k()
    }

    fn idx_iter_value<'a>(&'a self) -> Self::IterValue<'a> {
        self._iter_unord_v()
    }

    #[cfg(debug_assertions)]
    fn idx_metrics(&self) -> &Self::Metrics {
        &self.metrics
    }
}

impl<K, V, S: BuildHasher + Default> STIndex<K, V> for IndexSTSeqDll<K, V, S>
where
    K: AsKey,
    V: AsValue,
{
    fn st_clear(&mut self) {
        self._clear()
    }

    fn st_insert(&mut self, key: K, val: V) -> bool {
        self._insert(key, val)
    }

    fn st_upsert(&mut self, key: K, val: V) {
        let _ = self._upsert(key, val);
    }

    fn st_get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef,
    {
        self._get(key)
    }

    fn st_get_cloned<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef,
    {
        self._get(key).cloned()
    }

    fn st_update<Q>(&mut self, key: &Q, val: V) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef,
    {
        self._update(key, val).is_some()
    }

    fn st_update_return<Q>(&mut self, key: &Q, val: V) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef,
    {
        self._update(key, val)
    }

    fn st_delete<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef,
    {
        self._remove(key).is_none()
    }

    fn st_delete_return<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKeyRef,
    {
        self._remove(key)
    }
}

impl<K, V, S> STIndexSeq<K, V> for IndexSTSeqDll<K, V, S>
where
    K: AsKey,
    V: AsValue,
    S: BuildHasher + Default,
{
    type IterOrdKV<'a> = IndexSTSeqDllIterOrdKV<'a, K, V>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    type IterOrdKey<'a> = IndexSTSeqDllIterOrdKey<'a, K, V>
    where
        Self: 'a,
        K: 'a;

    type IterOrdValue<'a> = IndexSTSeqDllIterOrdValue<'a, K, V>
    where
        Self: 'a,
        V: 'a;

    fn stseq_ord_kv<'a>(&'a self) -> Self::IterOrdKV<'a> {
        self._iter_ord_kv()
    }

    fn stseq_ord_key<'a>(&'a self) -> Self::IterOrdKey<'a> {
        self._iter_ord_k()
    }

    fn stseq_ord_value<'a>(&'a self) -> Self::IterOrdValue<'a> {
        self._iter_ord_v()
    }
}

impl<K: AsKey, V: AsValue, S: BuildHasher + Default> Clone for IndexSTSeqDll<K, V, S> {
    fn clone(&self) -> Self {
        let mut slf = Self::with_capacity_and_hasher(self.len(), S::default());
        self._iter_ord_kv()
            .map(|(k, v)| (k.clone(), v.clone()))
            .for_each(|(k, v)| {
                slf._insert(k, v);
            });
        slf
    }
}

unsafe impl<K: Send, V: Send, S: Send> Send for IndexSTSeqDll<K, V, S> {}
unsafe impl<K: Sync, V: Sync, S: Sync> Sync for IndexSTSeqDll<K, V, S> {}

macro_rules! unsafe_marker_impl {
    (unsafe impl for $ty:ty) => {
        unsafe impl<'a, K: Send, V: Send> Send for $ty {}
        unsafe impl<'a, K: Sync, V: Sync> Sync for $ty {}
    };
}

pub struct IndexSTSeqDllIterUnordKV<'a, K: 'a, V: 'a> {
    i: Iter<'a, IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>>,
}

// UNSAFE(@ohsayan): aliasing guarantees correctness
unsafe_marker_impl!(unsafe impl for IndexSTSeqDllIterUnordKV<'a, K, V>);

impl<'a, K: 'a, V: 'a> IndexSTSeqDllIterUnordKV<'a, K, V> {
    #[inline(always)]
    fn new<S>(m: &'a StdMap<IndexSTSeqDllKeyptr<K>, NonNull<IndexSTSeqDllNode<K, V>>, S>) -> Self {
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
    fn new<S>(m: &'a StdMap<IndexSTSeqDllKeyptr<K>, NonNull<IndexSTSeqDllNode<K, V>>, S>) -> Self {
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
                &*(*k).p
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
    fn new<S>(m: &'a StdMap<IndexSTSeqDllKeyptr<K>, NonNull<IndexSTSeqDllNode<K, V>>, S>) -> Self {
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

struct IndexSTSeqDllIterOrdBase<'a, K: 'a, V: 'a, C: IndexSTSeqDllIterOrdConfig<K, V>> {
    h: *const IndexSTSeqDllNode<K, V>,
    t: *const IndexSTSeqDllNode<K, V>,
    r: usize,
    _l: PhantomData<(&'a K, &'a V, C)>,
}

impl<'a, K: 'a, V: 'a, C: IndexSTSeqDllIterOrdConfig<K, V>> IndexSTSeqDllIterOrdBase<'a, K, V, C> {
    #[inline(always)]
    fn new<S>(idx: &'a IndexSTSeqDll<K, V, S>) -> Self {
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
