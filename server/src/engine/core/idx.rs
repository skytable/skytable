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

use super::def::{AsKey, AsValue};
use std::{
    alloc::{alloc as std_alloc, dealloc as std_dealloc, Layout},
    borrow::Borrow,
    collections::{hash_map::RandomState, HashMap as StdMap},
    hash::{BuildHasher, Hash, Hasher},
    mem,
    ptr::{self, NonNull},
};

// re-exports for convenience
pub type IndexSTSeq<K, V, S = RandomState> = IndexSTSeqDll<K, V, S>;

/*
    For the ordered index impl, we resort to some crazy unsafe code, especially because there's no other way to
    deal with non-primitive Ks. That's why we'll ENTIRELY AVOID exporting any structures; if we end up using a node
    or a ptr struct anywhere inappropriate, it'll most likely SEGFAULT. So yeah, better be careful with this one.
    Second note, I'm not a big fan of the DLL and will most likely try a different approach in the future; this one
    is the most convenient option for now.

    -- Sayan (@ohsayan) // Jan. 16 '23
*/

#[derive(Debug)]
#[repr(transparent)]
/// # WARNING: Segfault/UAF alert
///
/// Yeah, this type is going to segfault if you decide to use it in random places. Literally, don't use it if
/// you're unsure about it's validity. For example, if you simply `==` this or attempt to use it an a hashmap,
/// you can segfault. IFF, the ptr is valid will it not segfault
struct IndexSTSeqDllKeyptr<K> {
    p: *mut K,
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
#[derive(Debug, Hash, PartialEq)]
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
            assert!(ptr.is_null(), "damn the allocator failed");
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
    unsafe fn dealloc(slf: *mut Self) {
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
}

type IndexSTSeqDllNodePtr<K, V> = NonNull<IndexSTSeqDllNode<K, V>>;

/// An ST-index with ordering. Inefficient ordered scanning since not in block
pub struct IndexSTSeqDll<K, V, S> {
    m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, S>,
    h: *mut IndexSTSeqDllNode<K, V>,
    f: *mut IndexSTSeqDllNode<K, V>,
}

impl<K, V, S: BuildHasher> IndexSTSeqDll<K, V, S> {
    const DEF_CAP: usize = 0;
    #[inline(always)]
    fn _new(
        m: StdMap<IndexSTSeqDllKeyptr<K>, IndexSTSeqDllNodePtr<K, V>, S>,
        h: *mut IndexSTSeqDllNode<K, V>,
        f: *mut IndexSTSeqDllNode<K, V>,
    ) -> IndexSTSeqDll<K, V, S> {
        Self { m, h, f }
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
    fn ensure_sentinel(&mut self) {
        let ptr = IndexSTSeqDllNode::_alloc_with_garbage();
        unsafe {
            self.h = ptr;
            (*ptr).p = ptr;
            (*ptr).n = ptr;
        }
    }
    #[inline(always)]
    /// ## Safety
    ///
    /// Head must not be null
    unsafe fn drop_nodes_full(&mut self) {
        let mut c = self.h;
        while !c.is_null() {
            let nx = (*c).n;
            IndexSTSeqDllNode::dealloc(c);
            c = nx;
        }
    }
    #[inline(always)]
    fn vacuum_free(&mut self) {
        unsafe {
            let mut c = self.f;
            while !c.is_null() {
                let nx = (*c).n;
                IndexSTSeqDllNode::dealloc_headless(nx);
                c = nx;
            }
        }
        self.f = ptr::null_mut();
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
