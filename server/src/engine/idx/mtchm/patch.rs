/*
 * Created on Sun Feb 19 2023
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
    super::meta::TreeElement,
    crate::engine::idx::meta::Comparable,
    std::{hash::Hash, marker::PhantomData},
};

/// write mode flag
pub type WriteFlag = u8;
/// fresh
pub const WRITEMODE_FRESH: WriteFlag = 0b01;
/// refresh
pub const WRITEMODE_REFRESH: WriteFlag = 0b10;
/// any
pub const WRITEMODE_ANY: WriteFlag = 0b11;

/// A [`Patch`] is intended to atomically update the state of the tree, which means that all your deltas should be atomic
///
/// Make sure you override the [`unreachable!`] behavior!
pub trait PatchWrite<E: TreeElement> {
    const WMODE: WriteFlag;
    type Ret<'a>;
    type Target: Hash + Comparable<E::Key>;
    fn target<'a>(&'a self) -> &Self::Target;
    fn nx_new(&mut self) -> E {
        unreachable!()
    }
    fn nx_ret<'a>() -> Self::Ret<'a>;
    fn ex_apply(&mut self, _: &E) -> E {
        unreachable!()
    }
    fn ex_ret<'a>(current: &'a E) -> Self::Ret<'a>;
}

/*
    vanilla
*/

pub struct VanillaInsert<T: TreeElement>(pub T);
impl<T: TreeElement> PatchWrite<T> for VanillaInsert<T> {
    const WMODE: WriteFlag = WRITEMODE_FRESH;
    type Ret<'a> = bool;
    type Target = T::Key;
    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }
    // nx
    fn nx_new(&mut self) -> T {
        self.0.clone()
    }
    fn nx_ret<'a>() -> Self::Ret<'a> {
        true
    }
    // ex
    fn ex_ret<'a>(_: &'a T) -> Self::Ret<'a> {
        false
    }
}

pub struct VanillaUpsert<T: TreeElement>(pub T);
impl<T: TreeElement> PatchWrite<T> for VanillaUpsert<T> {
    const WMODE: WriteFlag = WRITEMODE_ANY;
    type Ret<'a> = ();
    type Target = T::Key;
    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }
    // nx
    fn nx_new(&mut self) -> T {
        self.0.clone()
    }
    fn nx_ret<'a>() -> Self::Ret<'a> {}
    // ex
    fn ex_apply(&mut self, _: &T) -> T {
        self.0.clone()
    }
    fn ex_ret<'a>(_: &'a T) -> Self::Ret<'a> {}
}

pub struct VanillaUpsertRet<T: TreeElement>(pub T);
impl<T: TreeElement> PatchWrite<T> for VanillaUpsertRet<T> {
    const WMODE: WriteFlag = WRITEMODE_ANY;
    type Ret<'a> = Option<&'a T::Value>;
    type Target = T::Key;
    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }
    // nx
    fn nx_new(&mut self) -> T {
        self.0.clone()
    }
    fn nx_ret<'a>() -> Self::Ret<'a> {
        None
    }
    // ex
    fn ex_apply(&mut self, _: &T) -> T {
        self.0.clone()
    }
    fn ex_ret<'a>(c: &'a T) -> Self::Ret<'a> {
        Some(c.val())
    }
}

pub struct VanillaUpdate<T: TreeElement>(pub T);
impl<T: TreeElement> PatchWrite<T> for VanillaUpdate<T> {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;
    type Ret<'a> = bool;
    type Target = T::Key;
    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }
    // nx
    fn nx_ret<'a>() -> Self::Ret<'a> {
        false
    }
    // ex
    fn ex_apply(&mut self, _: &T) -> T {
        self.0.clone()
    }
    fn ex_ret<'a>(_: &'a T) -> Self::Ret<'a> {
        true
    }
}

pub struct VanillaUpdateRet<T: TreeElement>(pub T);
impl<T: TreeElement> PatchWrite<T> for VanillaUpdateRet<T> {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;
    type Ret<'a> = Option<&'a T::Value>;
    type Target = T::Key;
    fn target<'a>(&'a self) -> &Self::Target {
        self.0.key()
    }
    // nx
    fn nx_ret<'a>() -> Self::Ret<'a> {
        None
    }
    // ex
    fn ex_apply(&mut self, _: &T) -> T {
        self.0.clone()
    }
    fn ex_ret<'a>(c: &'a T) -> Self::Ret<'a> {
        Some(c.val())
    }
}

/*
    delete
*/

pub trait PatchDelete<T: TreeElement> {
    type Ret<'a>;
    type Target: Comparable<T::Key> + ?Sized + Hash;
    fn target(&self) -> &Self::Target;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a>;
    fn nx<'a>() -> Self::Ret<'a>;
}

pub struct Delete<'a, T: TreeElement, U: ?Sized> {
    target: &'a U,
    _m: PhantomData<T>,
}

impl<'a, T: TreeElement, U: ?Sized> Delete<'a, T, U> {
    pub fn new(target: &'a U) -> Self {
        Self {
            target,
            _m: PhantomData,
        }
    }
}

impl<'d, T: TreeElement, U: Comparable<T::Key> + ?Sized> PatchDelete<T> for Delete<'d, T, U> {
    type Ret<'a> = bool;
    type Target = U;
    fn target(&self) -> &Self::Target {
        &self.target
    }
    #[inline(always)]
    fn ex<'a>(_: &'a T) -> Self::Ret<'a> {
        true
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        false
    }
}

pub struct DeleteRetEntry<'a, T: TreeElement, U: ?Sized> {
    target: &'a U,
    _m: PhantomData<T>,
}

impl<'a, T: TreeElement, U: ?Sized> DeleteRetEntry<'a, T, U> {
    pub fn new(target: &'a U) -> Self {
        Self {
            target,
            _m: PhantomData,
        }
    }
}

impl<'dr, T: TreeElement, U: Comparable<T::Key> + ?Sized> PatchDelete<T>
    for DeleteRetEntry<'dr, T, U>
{
    type Ret<'a> = Option<&'a T>;

    type Target = U;

    fn target(&self) -> &Self::Target {
        self.target
    }

    fn ex<'a>(v: &'a T) -> Self::Ret<'a> {
        Some(v)
    }

    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}

pub struct DeleteRet<'a, T: TreeElement, U: ?Sized> {
    target: &'a U,
    _m: PhantomData<T>,
}

impl<'a, T: TreeElement, U: ?Sized> DeleteRet<'a, T, U> {
    pub fn new(target: &'a U) -> Self {
        Self {
            target,
            _m: PhantomData,
        }
    }
}

impl<'dr, T: TreeElement, U: Comparable<T::Key> + ?Sized> PatchDelete<T> for DeleteRet<'dr, T, U> {
    type Ret<'a> = Option<&'a T::Value>;
    type Target = U;
    fn target(&self) -> &Self::Target {
        &self.target
    }
    #[inline(always)]
    fn ex<'a>(c: &'a T) -> Self::Ret<'a> {
        Some(c.val())
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}
