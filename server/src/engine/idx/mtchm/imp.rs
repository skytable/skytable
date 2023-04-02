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
        meta::{Config, TreeElement},
        patch::{VanillaInsert, VanillaUpdate, VanillaUpdateRet, VanillaUpsert},
        RawTree,
    },
    crate::engine::{
        idx::{meta::Comparable, AsKey, AsKeyClone, AsValue, AsValueClone, IndexBaseSpec, MTIndex},
        sync::atm::{upin, Guard},
    },
    std::sync::Arc,
};

#[inline(always)]
fn arc<K, V>(k: K, v: V) -> Arc<(K, V)> {
    Arc::new((k, v))
}

pub type ChmArc<K, V, C> = RawTree<Arc<(K, V)>, C>;

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
    K: AsKey,
    V: AsValue,
{
    type IterKV<'t, 'g, 'v> = IterKV<'t, 'g, 'v, Arc<(K, V)>, C>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        V: 'v,
        Self: 't;

    type IterKey<'t, 'g, 'v> = IterKey<'t, 'g, 'v, Arc<(K, V)>, C>
    where
        'g: 't + 'v,
        't: 'v,
        K: 'v,
        Self: 't;

    type IterVal<'t, 'g, 'v> = IterVal<'t, 'g, 'v, Arc<(K, V)>, C>
    where
        'g: 't + 'v,
        't: 'v,
        V: 'v,
        Self: 't;

    fn mt_clear(&self, g: &Guard) {
        self.nontransactional_clear(g)
    }

    fn mt_insert(&self, key: K, val: V, g: &Guard) -> bool {
        self.patch(VanillaInsert(arc(key, val)), g)
    }

    fn mt_upsert(&self, key: K, val: V, g: &Guard) {
        self.patch(VanillaUpsert(arc(key, val)), g)
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
        V: AsValueClone,
    {
        self.get(key, g).cloned()
    }

    fn mt_update(&self, key: K, val: V, g: &Guard) -> bool {
        self.patch(VanillaUpdate(arc(key, val)), g)
    }

    fn mt_update_return<'t, 'g, 'v>(&'t self, key: K, val: V, g: &'g Guard) -> Option<&'v V>
    where
        't: 'v,
        'g: 't + 'v,
    {
        self.patch(VanillaUpdateRet(arc(key, val)), g)
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

pub type ChmCopy<K, V, C> = RawTree<(K, V), C>;

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
    K: AsKeyClone,
    V: AsValueClone,
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

    fn mt_insert(&self, key: K, val: V, g: &Guard) -> bool {
        self.patch(VanillaInsert((key, val)), g)
    }

    fn mt_upsert(&self, key: K, val: V, g: &Guard) {
        self.patch(VanillaUpsert((key, val)), g)
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
        self.patch(VanillaUpdate((key, val)), g)
    }

    fn mt_update_return<'t, 'g, 'v>(&'t self, key: K, val: V, g: &'g Guard) -> Option<&'v V>
    where
        't: 'v,
        'g: 't + 'v,
    {
        self.patch(VanillaUpdateRet((key, val)), g)
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

impl<T: TreeElement, C: Config> FromIterator<T> for RawTree<T, C> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let g = unsafe {
            // UNSAFE(@ohsayan): it's me, hi, I'm the problem, it's me. yeah, Taylor knows it too. it's just us
            upin()
        };
        let t = RawTree::new();
        iter.into_iter()
            .for_each(|te| assert!(t.patch(VanillaInsert(te), g)));
        t
    }
}
