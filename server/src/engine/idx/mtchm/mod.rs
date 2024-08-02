/*
 * Created on Thu Jan 26 2023
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

mod access;
pub mod imp;
pub(super) mod iter;
pub mod meta;
mod patch;
#[cfg(test)]
mod tests;

#[cfg(debug_assertions)]
use crate::engine::sync::atm::ORD_ACQ;
use {
    self::{
        iter::{IterKV, IterKey, IterVal},
        meta::{CompressState, Config, DefConfig, LNode, NodeFlag, TreeElement},
    },
    crate::engine::{
        idx::meta::Comparable,
        mem::UArray,
        sync::atm::{self, cpin, upin, Atomic, Guard, Owned, Shared, ORD_ACR, ORD_RLX},
    },
    crossbeam_epoch::CompareExchangeError,
    std::{
        fmt,
        hash::Hash,
        hash::{BuildHasher, Hasher},
        marker::PhantomData,
        mem,
        sync::atomic::AtomicUsize,
    },
};

#[cfg(debug_assertions)]
struct CHTMetricsData {
    split: AtomicUsize,
    hln: AtomicUsize,
}

pub struct CHTRuntimeLog {
    #[cfg(debug_assertions)]
    data: CHTMetricsData,
    #[cfg(not(debug_assertions))]
    data: (),
}

impl CHTRuntimeLog {
    #[cfg(debug_assertions)]
    const ZERO: AtomicUsize = AtomicUsize::new(0);
    #[cfg(not(debug_assertions))]
    const NEW: Self = Self { data: () };
    #[cfg(debug_assertions)]
    const NEW: Self = Self {
        data: CHTMetricsData {
            split: Self::ZERO,
            hln: Self::ZERO,
        },
    };
    const fn new() -> Self {
        Self::NEW
    }
    dbgfn! {
        fn hsplit(self: &Self) {
            self.data.split.fetch_add(1, ORD_ACQ);
        } else {
            ()
        }
        fn hlnode(self: &Self) {
            self.data.hln.fetch_add(1, ORD_ACQ);
        } else {
            ()
        }
        #[cfg(test)]
        fn replnode(self: &Self) -> usize {
            self.data.hln.load(ORD_RLX)
        } else {
            0
        }
    }
}

impl Drop for CHTRuntimeLog {
    fn drop(&mut self) {
        let _ = self.data;
    }
}

/*
    concurrent index impl
    ---
    This implementation borrows ideas from the research by Phil Bagwell on hash trees and concurrency[1][2]. This implementation
    simplifies some of the features as described in the original paper, with some implementation ideas from contrie[3] but makes
    several other custom changes for maintenance, performance and custom features tuned to the use-case for Skytable.

    ## Warning: High memory overhead (explanation)
    This implementation unfortunately is quite bad in terms of memory efficiency because it uses full-sized nodes
    instead of differentiating the nodes, for performance (reducing indirection). The original paper recommends using
    differently sized nodes.

    Noting this, expect signficant memory blowup. We'll also remove this implementation down the line.

    This is why, I do NOT recommend its use as a daily data structure.

    ---
    References:
    [1]: Aleksandar Prokopec, Nathan Grasso Bronson, Phil Bagwell, and Martin Odersky. 2012.
    Concurrent tries with efficient non-blocking snapshots. SIGPLAN Not. 47, 8 (August 2012),
    151–160. https://doi.org/10.1145/2370036.2145836
    [2]: https://lampwww.epfl.ch/papers/idealhashtrees.pdf
    [3]: https://github.com/vorner/contrie (distributed under the MIT or Apache-2.0 license)
    -- Sayan (@ohsayan)

    ---
    HACK(@ohsayan): Until https://github.com/rust-lang/rust/issues/76560 is stabilized which is likely to take a while,
    we need to settle for trait objects.
*/

pub struct Node<C: Config> {
    branch: [Atomic<Self>; <DefConfig as Config>::BRANCH_MX],
}

impl<C: Config> Node<C> {
    const NULL: Atomic<Self> = Atomic::null();
    const NULL_BRANCH: [Atomic<Self>; <DefConfig as Config>::BRANCH_MX] =
        [Self::NULL; <DefConfig as Config>::BRANCH_MX];
    const _SZ: usize = mem::size_of::<Self>() / mem::size_of::<Atomic<Self>>();
    const _ALIGN: usize = C::BRANCH_MX / Self::_SZ;
    const _EQ: () = assert!(Self::_ALIGN == 1);
    #[inline(always)]
    const fn null() -> Self {
        let _ = Self::_EQ;
        Self {
            branch: Self::NULL_BRANCH,
        }
    }
}

#[inline(always)]
fn gc(g: &Guard) {
    g.flush();
}

#[inline(always)]
fn ldfl<C: Config>(c: &Shared<Node<C>>) -> usize {
    c.tag()
}

#[inline(always)]
const fn hf(c: usize, f: NodeFlag) -> bool {
    (c & f.d()) == f.d()
}

#[inline(always)]
const fn cf(c: usize, r: NodeFlag) -> usize {
    c & !r.d()
}

trait CTFlagAlign {
    const FL_A: bool;
    const FL_B: bool;
    const FLCK_A: () = assert!(Self::FL_A & Self::FL_B);
    const FLCK: () = Self::FLCK_A;
}

impl<T, C: Config> CTFlagAlign for RawTree<T, C> {
    const FL_A: bool = atm::ensure_flag_align::<LNode<T>, { NodeFlag::bits() }>();
    const FL_B: bool = atm::ensure_flag_align::<Node<C>, { NodeFlag::bits() }>();
}

impl<T, C: Config> Default for RawTree<T, C> {
    fn default() -> Self {
        Self::_new(C::HState::default())
    }
}

pub struct RawTree<T, C: Config = DefConfig> {
    root: Atomic<Node<C>>,
    h: C::HState,
    l: AtomicUsize,
    m: CHTRuntimeLog,
    _m: PhantomData<T>,
}

impl<T, C: Config> RawTree<T, C> {
    #[inline(always)]
    const fn _new(h: C::HState) -> Self {
        let _ = Self::FLCK;
        Self {
            root: Atomic::null(),
            h,
            l: AtomicUsize::new(0),
            _m: PhantomData,
            m: CHTRuntimeLog::new(),
        }
    }
    #[inline(always)]
    fn len(&self) -> usize {
        self.l.load(ORD_RLX)
    }
    #[inline(always)]
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T, C: Config> RawTree<T, C> {
    #[inline(always)]
    fn new() -> Self {
        Self::_new(C::HState::default())
    }
}

impl<T, C: Config> RawTree<T, C> {
    fn hash<Q>(&self, k: &Q) -> u64
    where
        Q: ?Sized + Hash,
    {
        let mut state = self.h.build_hasher();
        k.hash(&mut state);
        state.finish()
    }
}

// iter
impl<T: TreeElement, C: Config> RawTree<T, C> {
    fn iter_kv<'t, 'g, 'v>(&'t self, g: &'g Guard) -> IterKV<'t, 'g, 'v, T, C> {
        IterKV::new(self, g)
    }
    fn iter_key<'t, 'g, 'v>(&'t self, g: &'g Guard) -> IterKey<'t, 'g, 'v, T, C> {
        IterKey::new(self, g)
    }
    #[allow(unused)]
    fn iter_val<'t, 'g, 'v>(&'t self, g: &'g Guard) -> IterVal<'t, 'g, 'v, T, C> {
        IterVal::new(self, g)
    }
}

impl<T: TreeElement, C: Config> RawTree<T, C> {
    fn transactional_clear(&self, g: &Guard) {
        self.iter_key(g).for_each(|k| {
            let _ = self.remove(k, g);
        });
    }
    fn patch<'g, P: patch::PatchWrite<T>>(&'g self, mut patch: P, g: &'g Guard) -> P::Ret<'g> {
        let hash = self.hash(patch.target());
        let mut level = C::LEVEL_ZERO;
        let mut current = &self.root;
        let mut parent = None;
        let mut child = None;
        loop {
            let node = current.ld_acq(g);
            match ldfl(&node) {
                flag if hf(flag, NodeFlag::PENDING_DELETE) => {
                    /*
                        FIXME(@ohsayan):
                        this node is about to be deleted (well, maybe) so we'll attempt a cleanup as well. we might not exactly
                        need to do this. also this is a potentially expensive thing since we're going all the way back to the root,
                        we might be able to optimize this with a fixed-size queue.
                    */
                    unsafe {
                        // UNSAFE(@ohsayan): we know that isn't the root and def doesn't have data (that's how the algorithm works)
                        Self::compress(parent.unwrap(), child.unwrap(), g);
                    }
                    level = C::LEVEL_ZERO;
                    current = &self.root;
                    parent = None;
                    child = None;
                }
                _ if node.is_null() => {
                    // this is an empty slot
                    if P::WMODE == patch::WRITEMODE_REFRESH {
                        // I call that a job well done
                        return P::nx_ret();
                    }
                    if (P::WMODE == patch::WRITEMODE_ANY) | (P::WMODE == patch::WRITEMODE_FRESH) {
                        let new = Self::new_data(patch.nx_new());
                        match current.cx_rel(node, new, g) {
                            Ok(_) => {
                                // we're done here
                                self.incr_len();
                                return P::nx_ret();
                            }
                            Err(CompareExchangeError { new, .. }) => unsafe {
                                /*
                                    UNSAFE(@ohsayan): so we attempted to CAS it but the CAS failed. in that case, destroy the
                                    lnode we created. We never published the value so no other thread has watched, making this
                                    safe
                                */
                                Self::ldrop(new.into_shared(g));
                            },
                        }
                    }
                }
                flag if hf(flag, NodeFlag::DATA) => {
                    // so we have an lnode. well maybe an snode
                    let data = unsafe {
                        // UNSAFE(@ohsayan): flagck
                        Self::read_data(node)
                    };
                    debug_assert!(!data.is_empty(), "logic,empty node not compressed");
                    if !patch.target().cmp_eq(data[0].key()) && level < C::MAX_TREE_HEIGHT_UB {
                        /*
                            so this is a collision and since we haven't reached the max height, we should always
                            create a new branch so let's do that
                        */
                        self.m.hsplit();
                        debug_assert_eq!(data.len(), 1, "logic,lnode before height ub");
                        if P::WMODE == patch::WRITEMODE_REFRESH {
                            // another job well done; an snode with the wrong key; so basically it's missing
                            return P::nx_ret();
                        }
                        let next_chunk = (self.hash(data[0].key()) >> level) & C::MASK;
                        let mut new_branch = Node::null();
                        // stick this one in
                        new_branch.branch[next_chunk as usize] = Atomic::from(node);
                        // we don't care about what happens
                        let _ = current.cx_rel(node, Owned::new(new_branch), g);
                    } else {
                        /*
                            in this case we either have the same key or we found an lnode. resolve any conflicts and attempt
                            to update
                        */
                        let p = data.iter().position(|e| patch.target().cmp_eq(e.key()));
                        match p {
                            Some(v) if P::WMODE == patch::WRITEMODE_FRESH => {
                                return P::ex_ret(&data[v])
                            }
                            Some(i)
                                if P::WMODE == patch::WRITEMODE_REFRESH
                                    || P::WMODE == patch::WRITEMODE_ANY =>
                            {
                                // update the entry and create a new node
                                let mut new_ln = LNode::new();
                                new_ln.extend(data[..i].iter().cloned());
                                new_ln.extend(data[i + 1..].iter().cloned());
                                new_ln.push(patch.ex_apply(&data[i]));
                                match current.cx_rel(node, Self::new_lnode(new_ln), g) {
                                    Ok(new) => {
                                        if cfg!(debug_assertions)
                                            && unsafe { Self::read_data(new) }.len() > 1
                                        {
                                            self.m.hlnode();
                                        }
                                        unsafe {
                                            /*
                                                UNSAFE(@ohsayan): swapped out, and we'll be the last thread to see this once the epoch proceeds
                                                sufficiently
                                            */
                                            g.defer_destroy(Shared::<LNode<T>>::from(
                                                node.as_raw() as *const LNode<_>
                                            ))
                                        }
                                        return P::ex_ret(&data[i]);
                                    }
                                    Err(CompareExchangeError { new, .. }) => {
                                        // failed to swap it in
                                        unsafe {
                                            Self::ldrop(new.into_shared(g));
                                        }
                                    }
                                }
                            }
                            None if P::WMODE == patch::WRITEMODE_ANY
                                || P::WMODE == patch::WRITEMODE_FRESH =>
                            {
                                // no funk here
                                let mut new_node = data.clone();
                                new_node.push(patch.nx_new());
                                match current.cx_rel(node, Self::new_lnode(new_node), g) {
                                    Ok(new) => {
                                        if cfg!(debug_assertions)
                                            && unsafe { Self::read_data(new) }.len() > 1
                                        {
                                            self.m.hlnode();
                                        }
                                        // swapped out
                                        unsafe {
                                            // UNSAFE(@ohsayan): last thread to see this (well, sorta)
                                            g.defer_destroy(Shared::<LNode<T>>::from(
                                                node.as_raw() as *const LNode<_>,
                                            ));
                                        }
                                        self.incr_len();
                                        return P::nx_ret();
                                    }
                                    Err(CompareExchangeError { new, .. }) => {
                                        // failed to swap it
                                        unsafe {
                                            // UNSAFE(@ohsayan): never published this, so we're the last one
                                            Self::ldrop(new.into_shared(g))
                                        }
                                    }
                                }
                            }
                            None if P::WMODE == patch::WRITEMODE_REFRESH => return P::nx_ret(),
                            _ => {
                                unreachable!("logic, WMODE mismatch: `{}`", P::WMODE);
                            }
                        }
                    }
                }
                _ => {
                    // branch
                    let nxidx = (hash >> level) & C::MASK;
                    level += C::BRANCH_LG;
                    parent = Some(current);
                    child = Some(node);
                    current = &unsafe { node.deref() }.branch[nxidx as usize];
                }
            }
        }
    }

    fn contains_key<'g, Q: ?Sized + Comparable<T::Key>>(&'g self, k: &Q, g: &'g Guard) -> bool {
        self._lookup(access::RModeExists::new(k), g)
    }
    fn get<'g, Q: ?Sized + Comparable<T::Key>>(
        &'g self,
        k: &Q,
        g: &'g Guard,
    ) -> Option<&'g T::Value> {
        self._lookup(access::RModeRef::new(k), g)
    }
    fn get_full<'g, Q: ?Sized + Comparable<T::Key>>(
        &'g self,
        k: &Q,
        g: &'g Guard,
    ) -> Option<&'g T> {
        self._lookup(access::RModeElementRef::new(k), g)
    }
    fn _lookup<'g, R: access::ReadMode<T>>(&'g self, read_spec: R, g: &'g Guard) -> R::Ret<'g> {
        let mut hash = self.hash(read_spec.target());
        let mut current = &self.root;
        loop {
            let node = current.ld_acq(g);
            match ldfl(&node) {
                _ if node.is_null() => {
                    // honestly, if this ran on the root I'm going to die laughing (@ohsayan)
                    return R::nx();
                }
                flag if hf(flag, NodeFlag::DATA) => {
                    let mut ret = R::nx();
                    return unsafe {
                        // UNSAFE(@ohsayan): checked flag + nullck
                        Self::read_data(node).iter().find_map(|e_current| {
                            read_spec.target().cmp_eq(e_current.key()).then(|| {
                                ret = R::ex(e_current);
                            })
                        });
                        ret
                    };
                }
                _ => {
                    // branch
                    current = &unsafe { node.deref() }.branch[(hash & C::MASK) as usize];
                    hash >>= C::BRANCH_LG;
                }
            }
        }
    }
    fn remove<'g, Q: Comparable<T::Key> + ?Sized>(&'g self, k: &Q, g: &'g Guard) -> bool {
        self._remove(patch::Delete::new(k), g)
    }
    fn remove_return<'g, Q: Comparable<T::Key> + ?Sized>(
        &'g self,
        k: &Q,
        g: &'g Guard,
    ) -> Option<&'g T::Value> {
        self._remove(patch::DeleteRet::new(k), g)
    }
    fn _remove<'g, P: patch::PatchDelete<T>>(&'g self, patch: P, g: &'g Guard) -> P::Ret<'g> {
        let hash = self.hash(patch.target());
        let mut current = &self.root;
        let mut level = C::LEVEL_ZERO;
        let mut levels = UArray::<{ <DefConfig as Config>::BRANCH_MX }, _>::new();
        'retry: loop {
            let node = current.ld_acq(g);
            match ldfl(&node) {
                _ if node.is_null() => {
                    // lol
                    return P::nx();
                }
                flag if hf(flag, NodeFlag::PENDING_DELETE) => {
                    let (p, c) = levels.pop().unwrap();
                    unsafe {
                        /*
                            we hit a node that might be deleted, we aren't allowed to change it, so we'll attempt a
                            compression as well. same thing here as the other routines....can we do anything to avoid
                            the expensive root traversal?
                        */
                        Self::compress(p, c, g);
                    }
                    levels.clear();
                    level = C::LEVEL_ZERO;
                    current = &self.root;
                }
                flag if hf(flag, NodeFlag::DATA) => {
                    let data = unsafe {
                        // UNSAFE(@ohsayan): flagck
                        Self::read_data(node)
                    };
                    let mut ret = P::nx();
                    let mut rem = false;
                    // this node shouldn't be empty
                    debug_assert!(!data.is_empty(), "logic,empty node not collected");
                    // build new lnode
                    let r: LNode<T> = data
                        .iter()
                        .filter_map(|this_elem| {
                            if patch.target().cmp_eq(this_elem.key()) {
                                ret = P::ex(this_elem);
                                rem = true;
                                None
                            } else {
                                Some(this_elem.clone())
                            }
                        })
                        .collect();
                    let replace = if r.is_empty() {
                        // don't create dead nodes
                        Shared::null()
                    } else {
                        Self::new_lnode(r).into_shared(g)
                    };
                    match current.cx_rel(node, replace, g) {
                        Ok(_) => {
                            // swapped it out
                            unsafe {
                                // UNSAFE(@ohsayan): flagck
                                g.defer_destroy(Shared::<LNode<T>>::from(
                                    node.as_raw() as *const LNode<_>
                                ));
                            }
                        }
                        Err(CompareExchangeError { new, .. }) if !new.is_null() => {
                            // failed to swap it in, and it had some data
                            unsafe {
                                // UNSAFE(@ohsayan): Never published it, all ours
                                g.defer_destroy(Shared::<LNode<T>>::from(
                                    new.as_raw() as *const LNode<_>
                                ));
                            }
                            continue 'retry;
                        }
                        Err(_) => continue 'retry,
                    }
                    // attempt compressions
                    for (p, c) in levels.into_iter().rev() {
                        let live_nodes = unsafe {
                            // UNSAFE(@ohsayan): guard
                            c.deref()
                        }
                        .branch
                        .iter()
                        .filter(|n| !n.ld_rlx(g).is_null())
                        .count();
                        if live_nodes > 1 {
                            break;
                        }
                        if unsafe {
                            // UNSAFE(@ohsayan): we know for a fact that we only have sensible levels
                            Self::compress(p, c, g)
                        } == CompressState::RESTORED
                        {
                            // simply restored the earlier state, so let's stop
                            break;
                        }
                    }
                    self.decr_len_by(rem as _);
                    gc(g);
                    return ret;
                }
                _ => {
                    // branch
                    levels.push((current, node));
                    let nxidx = (hash >> level) & C::MASK;
                    level += C::BRANCH_LG;
                    current = &unsafe { node.deref() }.branch[nxidx as usize];
                }
            }
        }
    }
}

// low-level methods
impl<T, C: Config> RawTree<T, C> {
    fn decr_len_by(&self, by: usize) {
        self.l.fetch_sub(by, ORD_RLX);
    }
    fn incr_len(&self) {
        self.l.fetch_add(1, ORD_RLX);
    }
    #[inline(always)]
    fn new_lnode(node: LNode<T>) -> Owned<Node<C>> {
        unsafe {
            Owned::<Node<_>>::from_raw(Box::into_raw(Box::new(node)) as *mut Node<_>)
                .with_tag(NodeFlag::DATA.d())
        }
    }
    /// Returns a new inner node, in the form of a data probe leaf
    /// ☢ WARNING ☢: Do not drop this naively for god's sake
    #[inline(always)]
    fn new_data(data: T) -> Owned<Node<C>> {
        let mut d = LNode::new();
        unsafe {
            // UNSAFE(@ohsayan): empty arr
            d.push_unchecked(data)
        };
        Self::new_lnode(d)
    }
    unsafe fn read_data<'g>(d: Shared<'g, Node<C>>) -> &'g LNode<T> {
        debug_assert!(hf(ldfl(&d), NodeFlag::DATA));
        (d.as_raw() as *const LNode<_>)
            .as_ref()
            .expect("logic,nullptr in lnode")
    }
    /// SAFETY: Ensure you have some actual data and not random garbage
    #[inline(always)]
    unsafe fn ldrop(leaf: Shared<Node<C>>) {
        debug_assert!(hf(ldfl(&leaf), NodeFlag::DATA));
        drop(Owned::<LNode<T>>::from_raw(leaf.as_raw() as *mut _))
    }
    unsafe fn _rdrop(node: Shared<Node<C>>) {
        match ldfl(&node) {
            _ if node.is_null() => {}
            flag if hf(flag, NodeFlag::DATA) => Self::ldrop(node),
            _ => {
                // a branch
                let this_branch = node.into_owned();
                for child in &this_branch.branch {
                    Self::rdrop(child)
                }
                drop(this_branch);
            }
        }
    }
    unsafe fn rdrop(n: &Atomic<Node<C>>) {
        let g = upin();
        let node = n.ld_acq(g);
        Self::_rdrop(node);
    }
    unsafe fn compress<'g>(
        parent: &Atomic<Node<C>>,
        child: Shared<'g, Node<C>>,
        g: &'g Guard,
    ) -> CompressState {
        /*
            We look at the child's children and determine whether we can clean the child up. Although the amount of
            memory we can save is not something very signficant but it becomes important with larger cardinalities
        */
        debug_assert!(!hf(ldfl(&child), NodeFlag::DATA), "logic,compress lnode");
        debug_assert_eq!(ldfl(&child), 0, "logic,compress pending delete node");
        let branch = child.deref();
        let mut continue_compress = true;
        let mut last_leaf = None;
        let mut new_child = Node::null();
        let mut cnt = 0_usize;

        let mut i = 0;
        while i < C::BRANCH_MX {
            let child_ref = &branch.branch[i];
            let this_child = child_ref.fetch_or(NodeFlag::PENDING_DELETE.d(), ORD_ACR, g);
            let this_child = this_child.with_tag(cf(ldfl(&this_child), NodeFlag::PENDING_DELETE));
            match ldfl(&this_child) {
                // lol, dangling child
                _ if this_child.is_null() => {}
                // some data in here
                flag if hf(flag, NodeFlag::DATA) => {
                    last_leaf = Some(this_child);
                    cnt += Self::read_data(this_child).len();
                }
                // branch
                _ => {
                    continue_compress = false;
                    cnt += 1;
                }
            }
            new_child.branch[i] = Atomic::from(this_child);
            i += 1;
        }

        let insert;
        let ret;
        let mut drop = None;

        match last_leaf {
            Some(node) if continue_compress && cnt == 1 => {
                // snode
                insert = node;
                ret = CompressState::SNODE;
            }
            None if cnt == 0 => {
                // a dangling branch
                insert = Shared::null();
                ret = CompressState::NULL;
            }
            _ => {
                // we can't compress this since we have a lot of children
                let new = Owned::new(new_child).into_shared(g);
                insert = new;
                drop = Some(new);
                ret = CompressState::RESTORED;
            }
        }

        // all logic done; let's see what fate the CAS brings us
        match parent.cx_rel(child, insert, g) {
            Ok(_) => {
                unsafe {
                    // UNSAFE(@ohsayan): We're the thread in the last epoch who's seeing this; so, we're good
                    g.defer_destroy(child);
                }
                ret
            }
            Err(_) => {
                mem::drop(drop.map(|n| Shared::into_owned(n)));
                CompressState::CASFAIL
            }
        }
    }
}

impl<T, C: Config> Drop for RawTree<T, C> {
    fn drop(&mut self) {
        unsafe {
            // UNSAFE(@ohsayan): sole live owner
            Self::rdrop(&self.root);
        }
        if !cfg!(miri) {
            gc(&cpin());
        }
    }
}

impl<T: TreeElement, C: Config> fmt::Debug for RawTree<T, C>
where
    T::Key: fmt::Debug,
    T::Value: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let g = cpin();
        f.debug_map().entries(self.iter_kv(&g)).finish()
    }
}
