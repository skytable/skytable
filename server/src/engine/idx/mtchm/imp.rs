/*
 * Created on Sat Jan 28 2023
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
use super::CHTRuntimeLog;
use {
    super::{
        iter::{IterKV, IterKey, IterVal},
        meta::{Config, Key, TreeElement, Value},
        patch::{PatchWrite, WriteFlag, WRITEMODE_ANY, WRITEMODE_FRESH, WRITEMODE_REFRESH},
        Tree,
    },
    crate::engine::{
        idx::{
            meta::{Comparable, ComparableUpgradeable},
            IndexBaseSpec, MTIndex,
        },
        sync::atm::{upin, Guard},
    },
    std::sync::Arc,
};

#[inline(always)]
fn arc<K, V>(k: K, v: V) -> Arc<(K, V)> {
    Arc::new((k, v))
}

pub type ChmArc<K, V, C> = Tree<Arc<(K, V)>, C>;

pub struct ArcInsert<K, V>(Arc<(K, V)>);

impl<K: Key, V: Value> PatchWrite<Arc<(K, V)>> for ArcInsert<K, V> {
    const WMODE: WriteFlag = WRITEMODE_FRESH;

    type Ret<'a> = bool;

    type Target = K;

    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }

    fn nx_new(&mut self) -> Arc<(K, V)> {
        self.0.clone()
    }

    fn nx_ret<'a>() -> Self::Ret<'a> {
        true
    }

    fn ex_apply(&mut self, _: &Arc<(K, V)>) -> Arc<(K, V)> {
        unreachable!()
    }

    fn ex_ret<'a>(_: &'a Arc<(K, V)>) -> Self::Ret<'a> {
        false
    }
}

pub struct ArcUpsert<K, V>(Arc<(K, V)>);

impl<K: Key, V: Value> PatchWrite<Arc<(K, V)>> for ArcUpsert<K, V> {
    const WMODE: WriteFlag = WRITEMODE_ANY;

    type Ret<'a> = ();

    type Target = K;

    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }

    fn nx_new(&mut self) -> Arc<(K, V)> {
        self.0.clone()
    }

    fn nx_ret<'a>() -> Self::Ret<'a> {
        ()
    }

    fn ex_apply(&mut self, _: &Arc<(K, V)>) -> Arc<(K, V)> {
        self.0.clone()
    }

    fn ex_ret<'a>(_: &'a Arc<(K, V)>) -> Self::Ret<'a> {
        ()
    }
}

pub struct ArcUpdate<K, V>(Arc<(K, V)>);

impl<K: Key, V: Value> PatchWrite<Arc<(K, V)>> for ArcUpdate<K, V> {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;

    type Ret<'a> = bool;

    type Target = K;

    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }

    fn nx_new(&mut self) -> Arc<(K, V)> {
        unreachable!()
    }

    fn nx_ret<'a>() -> Self::Ret<'a> {
        false
    }

    fn ex_apply(&mut self, _: &Arc<(K, V)>) -> Arc<(K, V)> {
        self.0.clone()
    }

    fn ex_ret<'a>(_: &'a Arc<(K, V)>) -> Self::Ret<'a> {
        true
    }
}
pub struct ArcUpdateRet<K, V>(Arc<(K, V)>);

impl<K: Key, V: Value> PatchWrite<Arc<(K, V)>> for ArcUpdateRet<K, V> {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;

    type Ret<'a> = Option<&'a V>;

    type Target = K;

    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }

    fn nx_new(&mut self) -> Arc<(K, V)> {
        unreachable!()
    }

    fn nx_ret<'a>() -> Self::Ret<'a> {
        None
    }

    fn ex_apply(&mut self, _: &Arc<(K, V)>) -> Arc<(K, V)> {
        self.0.clone()
    }

    fn ex_ret<'a>(c: &'a Arc<(K, V)>) -> Self::Ret<'a> {
        Some(c.val())
    }
}

impl<K, V, C> IndexBaseSpec<K, V> for ChmArc<K, V, C>
where
    C: Config,
{
    const PREALLOC: bool = false;

    #[cfg(debug_assertions)]
    type Metrics = CHTRuntimeLog;

    fn idx_init() -> Self {
        ChmArc::new()
    }

    fn idx_init_with(s: Self) -> Self {
        s
    }

    #[cfg(debug_assertions)]
    fn idx_metrics(&self) -> &Self::Metrics {
        &self.m
    }
}

impl<K, V, C> MTIndex<K, V> for ChmArc<K, V, C>
where
    C: Config,
    K: Key,
    V: Value,
{
    type IterKV<'t, 'g, 'v> = IterKV<'t, 'g, 'v, (K, V), C>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        V: 'v,
        Self: 't;

    type IterKey<'t, 'g, 'v> = IterKey<'t, 'g, 'v, (K, V), C>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        Self: 't;

    type IterVal<'t, 'g, 'v> = IterVal<'t, 'g, 'v, (K, V), C>
    where
        'g: 't + 'v,
        't: 'v,
        V: 'v,
        Self: 't;

    fn mt_clear(&self, g: &Guard) {
        self.nontransactional_clear(g)
    }

    fn mt_insert<U>(&self, key: U, val: V, g: &Guard) -> bool
    where
        U: ComparableUpgradeable<K>,
    {
        self.patch(ArcInsert(arc(key.upgrade(), val)), g)
    }

    fn mt_upsert(&self, key: K, val: V, g: &Guard) {
        self.patch(ArcUpsert(arc(key.upgrade(), val)), g)
    }

    fn mt_contains<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        Q: ?Sized + Comparable<K>,
    {
        self.contains_key(key, g)
    }

    fn mt_get<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v,
    {
        self.get(key, g)
    }

    fn mt_get_cloned<Q>(&self, key: &Q, g: &Guard) -> Option<V>
    where
        Q: ?Sized + Comparable<K>,
    {
        self.get(key, g).cloned()
    }

    fn mt_update(&self, key: K, val: V, g: &Guard) -> bool {
        self.patch(ArcUpdate(arc(key, val)), g)
    }

    fn mt_update_return<'t, 'g, 'v>(&'t self, key: K, val: V, g: &'g Guard) -> Option<&'v V>
    where
        't: 'v,
        'g: 't + 'v,
    {
        self.patch(ArcUpdateRet(arc(key, val)), g)
    }

    fn mt_delete<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        Q: ?Sized + Comparable<K>,
    {
        self.remove(key, g)
    }

    fn mt_delete_return<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v,
    {
        self.remove_return(key, g)
    }
}

pub type ChmCopy<K, V, C> = Tree<(K, V), C>;

impl<K, V, C> IndexBaseSpec<K, V> for ChmCopy<K, V, C>
where
    C: Config,
{
    const PREALLOC: bool = false;

    #[cfg(debug_assertions)]
    type Metrics = CHTRuntimeLog;

    fn idx_init() -> Self {
        ChmCopy::new()
    }

    fn idx_init_with(s: Self) -> Self {
        s
    }

    #[cfg(debug_assertions)]
    fn idx_metrics(&self) -> &Self::Metrics {
        &self.m
    }
}

impl<K, V, C> MTIndex<K, V> for ChmCopy<K, V, C>
where
    C: Config,
    K: Key,
    V: Value,
{
    type IterKV<'t, 'g, 'v> = IterKV<'t, 'g, 'v, (K, V), C>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        V: 'v,
        Self: 't;

    type IterKey<'t, 'g, 'v> = IterKey<'t, 'g, 'v, (K, V), C>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        Self: 't;

    type IterVal<'t, 'g, 'v> = IterVal<'t, 'g, 'v, (K, V), C>
    where
        'g: 't + 'v,
        't: 'v,
        V: 'v,
        Self: 't;

    fn mt_clear(&self, g: &Guard) {
        self.nontransactional_clear(g)
    }

    fn mt_insert<U>(&self, key: U, val: V, g: &Guard) -> bool
    where
        U: ComparableUpgradeable<K>,
    {
        self.patch_insert(key, val, g)
    }

    fn mt_upsert(&self, key: K, val: V, g: &Guard) {
        self.patch_upsert(key, val, g)
    }

    fn mt_contains<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        Q: ?Sized + Comparable<K>,
    {
        self.contains_key(key, g)
    }

    fn mt_get<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v,
    {
        self.get(key, g)
    }

    fn mt_get_cloned<Q>(&self, key: &Q, g: &Guard) -> Option<V>
    where
        Q: ?Sized + Comparable<K>,
    {
        self.get(key, g).cloned()
    }

    fn mt_update(&self, key: K, val: V, g: &Guard) -> bool {
        self.patch_update(key, val, g)
    }

    fn mt_update_return<'t, 'g, 'v>(&'t self, key: K, val: V, g: &'g Guard) -> Option<&'v V>
    where
        't: 'v,
        'g: 't + 'v,
    {
        self.patch_update_return(key, val, g)
    }

    fn mt_delete<Q>(&self, key: &Q, g: &Guard) -> bool
    where
        Q: ?Sized + Comparable<K>,
    {
        self.remove(key, g)
    }

    fn mt_delete_return<'t, 'g, 'v, Q>(&'t self, key: &Q, g: &'g Guard) -> Option<&'v V>
    where
        Q: ?Sized + Comparable<K>,
        't: 'v,
        'g: 't + 'v,
    {
        self.remove_return(key, g)
    }
}

impl<T: TreeElement, C: Config> FromIterator<T> for Tree<T, C> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let g = unsafe {
            // UNSAFE(@ohsayan): it's me, hi, I'm the problem, it's me. yeah, Taylor knows it too. it's just us
            upin()
        };
        let t = Tree::new();
        iter.into_iter()
            .for_each(|te| assert!(t.patch_insert(te.key().clone(), te.val().clone(), &g)));
        t
    }
}
