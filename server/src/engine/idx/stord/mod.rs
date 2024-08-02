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

pub(super) mod config;
pub(super) mod iter;

use {
    self::{
        config::{AllocStrategy, Config},
        iter::{
            IndexSTSeqDllIterOrdKV, IndexSTSeqDllIterOrdKey, IndexSTSeqDllIterOrdValue,
            IndexSTSeqDllIterUnordKV, IndexSTSeqDllIterUnordKey, IndexSTSeqDllIterUnordValue,
        },
    },
    super::{
        AsKey, AsKeyClone, AsValue, AsValueClone, IndexBaseSpec, STIndex, STIndexExt, STIndexSeq,
    },
    crate::engine::mem::{unsafe_apis, StatelessLen},
    std::{
        alloc::Layout,
        borrow::Borrow,
        collections::HashMap as StdMap,
        fmt::{self, Debug},
        hash::{Hash, Hasher},
        mem,
        ptr::{self, NonNull},
    },
};

/*
    For the ordered index impl, we resort to some crazy unsafe code, especially because there's no other way to
    deal with non-primitive Ks. That's why we'll ENTIRELY AVOID exporting any structures; if we end up using a node
    or a ptr struct anywhere inappropriate, it'll most likely SEGFAULT. So yeah, better be careful with this one.
    Second note, I'm not a big fan of the DLL and will most likely try a different approach in the future; this one
    is the most convenient option for now.

    This work uses some ideas from the archived linked hash map crate which is now unmaintained[1]

    ---
    [1]: https://github.com/contain-rs/linked-hash-map (distributed under the MIT or Apache-2.0 License)

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
pub struct IndexSTSeqDllNode<K, V> {
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
            unsafe_apis::alloc_layout(Self::LAYOUT)
        }
    }
    #[inline(always)]
    unsafe fn _drop(slf: *mut Self) {
        let _ = Box::from_raw(slf);
    }
    #[inline(always)]
    /// LEAK: K, V
    unsafe fn dealloc_headless(slf: *mut Self) {
        unsafe_apis::dealloc_layout(slf as *mut u8, Self::LAYOUT)
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

pub type IndexSTSeqDllNodePtr<K, V> = NonNull<IndexSTSeqDllNode<K, V>>;

#[cfg(debug_assertions)]
pub struct IndexSTSeqDllMetrics {
    stat_f: usize,
}

#[cfg(debug_assertions)]
impl IndexSTSeqDllMetrics {
    #[cfg(test)]
    pub const fn raw_f(&self) -> usize {
        self.stat_f
    }
    const fn new() -> IndexSTSeqDllMetrics {
        Self { stat_f: 0 }
    }
}

/// An ST-index with ordering. Inefficient ordered scanning since not in block
pub struct IndexSTSeqDll<K, V, C: Config<K, V>> {
    m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, C::Hasher>,
    h: *mut IndexSTSeqDllNode<K, V>,
    a: C::AllocStrategy,
    #[cfg(debug_assertions)]
    metrics: IndexSTSeqDllMetrics,
}

impl<K, V, C: Config<K, V>> IndexSTSeqDll<K, V, C> {
    const DEF_CAP: usize = 0;
    #[inline(always)]
    const fn _new(
        m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, C::Hasher>,
        h: *mut IndexSTSeqDllNode<K, V>,
    ) -> IndexSTSeqDll<K, V, C> {
        Self {
            m,
            h,
            a: C::AllocStrategy::NEW,
            #[cfg(debug_assertions)]
            metrics: IndexSTSeqDllMetrics::new(),
        }
    }
    #[inline(always)]
    fn _new_map(m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, C::Hasher>) -> Self {
        Self::_new(m, ptr::null_mut())
    }
    #[inline(always)]
    pub fn with_hasher(hasher: C::Hasher) -> Self {
        Self::with_capacity_and_hasher(Self::DEF_CAP, hasher)
    }
    #[inline(always)]
    pub fn with_capacity_and_hasher(cap: usize, hasher: C::Hasher) -> Self {
        Self::_new_map(StdMap::with_capacity_and_hasher(cap, hasher))
    }
    fn metrics_update_f_empty(&mut self) {
        #[cfg(debug_assertions)]
        {
            self.metrics.stat_f = 0;
        }
    }
}

impl<K, V, C: Config<K, V>> IndexSTSeqDll<K, V, C> {
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, C::Hasher::default())
    }
}

impl<K, V, C: Config<K, V> + Default> Default for IndexSTSeqDll<K, V, C> {
    fn default() -> Self {
        Self::with_hasher(C::Hasher::default())
    }
}

impl<K, V, C: Config<K, V>> IndexSTSeqDll<K, V, C> {
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

impl<K, V, C: Config<K, V>> IndexSTSeqDll<K, V, C> {
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
        IndexSTSeqDllIterOrdKV::new(self)
    }
    #[inline(always)]
    fn _iter_ord_k<'a>(&'a self) -> IndexSTSeqDllIterOrdKey<'a, K, V> {
        IndexSTSeqDllIterOrdKey::new(self)
    }
    #[inline(always)]
    fn _iter_ord_v<'a>(&'a self) -> IndexSTSeqDllIterOrdValue<'a, K, V> {
        IndexSTSeqDllIterOrdValue::new(self)
    }
}

impl<K: AsKey, V: AsValue, C: Config<K, V>> IndexSTSeqDll<K, V, C> {
    #[inline(always)]
    /// Clean up unused and cached memory
    fn vacuum_full(&mut self) {
        self.m.shrink_to_fit();
        self.a.cleanup();
        if C::AllocStrategy::METRIC_REFRESH {
            self.metrics_update_f_empty();
        }
    }
}

impl<K: AsKey, V: AsValue, C: Config<K, V>> IndexSTSeqDll<K, V, C> {
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
        Q: AsKey,
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
    fn _get_entry<Q: ?Sized>(&self, k: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q>,
        Q: AsKey,
    {
        self.m
            .get(unsafe {
                // UNSAFE(@ohsayan): ref with correct bounds
                IndexSTSeqDllQref::from_ref(k)
            })
            .map(|e| unsafe {
                /*
                    UNSAFE(@ohsayan): immutable ref so neither key nor value are moving and
                    aliasing is satisifed
                */
                let e = e.as_ref();
                (&e.k, &e.v)
            })
    }
    fn _get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: AsKey,
    {
        self.m
            .get_mut(unsafe { IndexSTSeqDllQref::from_ref(k) })
            .map(|e| unsafe { &mut e.as_mut().v })
    }
    #[inline(always)]
    fn _update<Q: ?Sized>(&mut self, k: &Q, v: V) -> Option<V>
    where
        K: Borrow<Q>,
        Q: AsKey,
    {
        match self.m.get(unsafe {
            // UNSAFE(@ohsayan): Just got a ref with the right bounds
            IndexSTSeqDllQref::from_ref(k)
        }) {
            Some(e) => unsafe {
                // UNSAFE(@ohsayan): Impl guarantees that entry presence == nullck head
                self.__update(*e, v)
            },
            None => None,
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
        Q: AsKey + ?Sized,
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
                self.a.free(n);
                if C::AllocStrategy::METRIC_REFRESH {
                    self.metrics_update_f_incr();
                }
                v
            })
    }
    #[inline(always)]
    fn __insert(&mut self, k: K, v: V) -> bool {
        self.ensure_sentinel();
        let mut refresh = false;
        let node = self
            .a
            .alloc(IndexSTSeqDllNode::new_null(k, v), &mut refresh);
        if C::AllocStrategy::METRIC_REFRESH & cfg!(debug_assertions) & refresh {
            self.metrics_update_f_decr();
        }
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
}

impl<K, V, C: Config<K, V>> Drop for IndexSTSeqDll<K, V, C> {
    fn drop(&mut self) {
        if !self.h.is_null() {
            unsafe {
                // UNSAFE(@ohsayan): nullck
                self.drop_nodes_full();
                // UNSAFE(@ohsayan): nullck: drop doesn't clear sentinel
                IndexSTSeqDllNode::dealloc_headless(self.h);
            }
        }
        self.a.cleanup();
    }
}

impl<K: AsKey, V: AsValue, C: Config<K, V>> FromIterator<(K, V)> for IndexSTSeqDll<K, V, C> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut slf = Self::with_hasher(C::Hasher::default());
        iter.into_iter()
            .for_each(|(k, v)| assert!(slf._insert(k, v)));
        slf
    }
}

impl<K, V, C: Config<K, V>> IndexBaseSpec for IndexSTSeqDll<K, V, C> {
    const PREALLOC: bool = true;

    #[cfg(debug_assertions)]
    type Metrics = IndexSTSeqDllMetrics;

    fn idx_init() -> Self {
        Self::with_hasher(C::Hasher::default())
    }

    fn idx_init_with(s: Self) -> Self {
        s
    }

    fn idx_init_cap(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, C::Hasher::default())
    }

    #[cfg(debug_assertions)]
    fn idx_metrics(&self) -> &Self::Metrics {
        &self.metrics
    }
}

impl<K, V, C: Config<K, V>> STIndex<K, V> for IndexSTSeqDll<K, V, C>
where
    K: AsKey,
    V: AsValue,
{
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

    fn st_compact(&mut self) {
        self.vacuum_full();
    }

    fn st_len(&self) -> usize {
        self.len()
    }

    fn st_clear(&mut self) {
        self._clear()
    }

    fn st_insert(&mut self, key: K, val: V) -> bool {
        self._insert(key, val)
    }

    fn st_upsert(&mut self, key: K, val: V) {
        let _ = self._upsert(key, val);
    }

    fn st_contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q> + AsKey,
        Q: ?Sized + AsKey,
    {
        self.m.contains_key(unsafe {
            // UNSAFE(@ohsayan): Valid ref with correct bounds
            IndexSTSeqDllQref::from_ref(key)
        })
    }

    fn st_get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        self._get(key)
    }

    fn st_get_cloned<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKey,
        V: AsValueClone,
    {
        self._get(key).cloned()
    }

    fn st_get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        self._get_mut(k)
    }

    fn st_update<Q>(&mut self, key: &Q, val: V) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        self._update(key, val).is_some()
    }

    fn st_update_return<Q>(&mut self, key: &Q, val: V) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        self._update(key, val)
    }

    fn st_delete<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        self._remove(key).is_some()
    }

    fn st_delete_return<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        self._remove(key)
    }

    fn st_delete_if<Q>(&mut self, key: &Q, iff: impl Fn(&V) -> bool) -> Option<bool>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        match self._get(key) {
            Some(v) if iff(v) => {
                self._remove(key);
                Some(true)
            }
            Some(_) => Some(false),
            None => None,
        }
    }

    fn st_iter_kv<'a>(&'a self) -> Self::IterKV<'a> {
        self._iter_unord_kv()
    }

    fn st_iter_key<'a>(&'a self) -> Self::IterKey<'a> {
        self._iter_unord_k()
    }

    fn st_iter_value<'a>(&'a self) -> Self::IterValue<'a> {
        self._iter_unord_v()
    }
}

impl<K, V, C> STIndexExt<K, V> for IndexSTSeqDll<K, V, C>
where
    K: AsKey,
    V: AsValue,
    C: Config<K, V>,
{
    fn stext_get_key_value<Q>(&self, k: &Q) -> Option<(&K, &V)>
    where
        K: AsKey + Borrow<Q>,
        Q: ?Sized + AsKey,
    {
        self._get_entry(k)
    }
}

impl<K, V, C> STIndexSeq<K, V> for IndexSTSeqDll<K, V, C>
where
    K: AsKey,
    V: AsValue,
    C: Config<K, V>,
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
    type OwnedIterKV = iter::OrderedOwnedIteratorKV<K, V>;
    type OwnedIterKeys = iter::OrderedOwnedIteratorKey<K, V>;
    type OwnedIterValues = iter::OrderedOwnedIteratorValue<K, V>;
    fn stseq_ord_kv<'a>(&'a self) -> Self::IterOrdKV<'a> {
        self._iter_ord_kv()
    }
    fn stseq_ord_key<'a>(&'a self) -> Self::IterOrdKey<'a> {
        self._iter_ord_k()
    }
    fn stseq_ord_value<'a>(&'a self) -> Self::IterOrdValue<'a> {
        self._iter_ord_v()
    }
    fn stseq_owned_keys(self) -> Self::OwnedIterKeys {
        iter::OrderedOwnedIteratorKey(iter::OrderedOwnedIteratorRaw::new(self))
    }
    fn stseq_owned_values(self) -> Self::OwnedIterValues {
        iter::OrderedOwnedIteratorValue(iter::OrderedOwnedIteratorRaw::new(self))
    }
    fn stseq_owned_kv(self) -> Self::OwnedIterKV {
        iter::OrderedOwnedIteratorKV(iter::OrderedOwnedIteratorRaw::new(self))
    }
}

impl<K: AsKeyClone, V: AsValueClone, C: Config<K, V>> Clone for IndexSTSeqDll<K, V, C> {
    fn clone(&self) -> Self {
        let mut slf = Self::with_capacity_and_hasher(self.len(), C::Hasher::default());
        self._iter_ord_kv()
            .map(|(k, v)| (k.clone(), v.clone()))
            .for_each(|(k, v)| {
                slf._insert(k, v);
            });
        slf
    }
}

unsafe impl<K: Send, V: Send, C: Config<K, V> + Send> Send for IndexSTSeqDll<K, V, C> {}
unsafe impl<K: Sync, V: Sync, C: Config<K, V> + Sync> Sync for IndexSTSeqDll<K, V, C> {}

impl<K: fmt::Debug, V: fmt::Debug, C: Config<K, V> + fmt::Debug> fmt::Debug
    for IndexSTSeqDll<K, V, C>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self._iter_ord_kv()).finish()
    }
}

impl<K: AsKey, V: AsValue + PartialEq, C: Config<K, V>> PartialEq for IndexSTSeqDll<K, V, C> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                ._iter_ord_kv()
                .all(|(k, v)| other._get(k).unwrap().eq(v))
    }
}

impl<K, V, C: Config<K, V>> StatelessLen for IndexSTSeqDll<K, V, C> {
    fn stateless_len(&self) -> usize {
        self.len()
    }
}
