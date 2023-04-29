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

pub mod hash;

use core::{
    borrow::Borrow,
    hash::{BuildHasher, Hash},
};

pub trait AsHasher: BuildHasher + Default {}
impl<T> AsHasher for T where T: BuildHasher + Default {}

/// The [`Comparable`] trait is like [`PartialEq`], but is different due to its expectations, and escapes its scandalous relations with [`Eq`] and the consequential
/// implications across the [`std`].
///
/// ☢️ WARNING ☢️: In some cases implementations of the [`Comparable`] set of traits COMPLETELY VIOLATES [`Eq`]'s invariants. BE VERY CAREFUL WHEN USING IN EXPRESSIONS
/*
    FIXME(@ohsayan): The gradual idea is to completely move to Comparable, but that means we'll have to go ahead as much as replacing the impls for some items in the
    standard library. We don't have the time to do that right now, but I hope we can do it soon
*/
pub trait Comparable<K: ?Sized>: Hash {
    fn cmp_eq(&self, key: &K) -> bool;
}

pub trait ComparableUpgradeable<K>: Comparable<K> {
    fn upgrade(&self) -> K;
}

impl<K: Borrow<T>, T: Eq + Hash + ?Sized> Comparable<K> for T {
    fn cmp_eq(&self, key: &K) -> bool {
        self == key.borrow()
    }
}

impl<K: Hash, T: ToOwned<Owned = K> + Hash + Comparable<K> + ?Sized> ComparableUpgradeable<K>
    for T
{
    fn upgrade(&self) -> K {
        self.to_owned()
    }
}
