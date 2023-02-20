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

use std::borrow::Borrow;

use {
    super::meta::TreeElement,
    core::{hash::Hash, marker::PhantomData},
};

/// write mode flag
pub type WriteFlag = u8;
pub const WRITEMODE_DELETE: WriteFlag = 0xFF;
/// fresh
pub const WRITEMODE_FRESH: WriteFlag = 0b01;
/// refresh
pub const WRITEMODE_REFRESH: WriteFlag = 0b10;
/// any
pub const WRITEMODE_ANY: WriteFlag = 0b11;

pub trait TreeKeyComparable<T: TreeElement>: Hash {
    fn cmp_eq(&self, them: &T) -> bool;
}

impl<E: TreeElement, T> TreeKeyComparable<E> for T
where
    E::Key: core::borrow::Borrow<T>,
    T: Hash + PartialEq,
{
    fn cmp_eq(&self, them: &E) -> bool {
        self == them.key().borrow()
    }
}

pub trait TreeKeyComparableUpgradeable<T: TreeElement>: TreeKeyComparable<T> {
    fn upgrade_key(&self) -> T::Key;
}

impl<E: TreeElement> TreeKeyComparableUpgradeable<E> for E::Key {
    fn upgrade_key(&self) -> E::Key {
        self.clone()
    }
}

/// A [`Patch`] is intended to atomically update the state of the tree, which means that all your deltas should be atomic
pub trait Patch<E: TreeElement> {
    const WMODE: WriteFlag;
    type Ret<'a>;
    type Target<'a>: TreeKeyComparable<E>
    where
        Self: 'a;
    fn target<'a>(&'a self) -> &Self::Target<'a>;
    fn nx_new(&mut self) -> E;
    fn nx_ret<'a>() -> Self::Ret<'a>;
    fn ex_apply(&mut self, current: &E) -> E;
    fn ex_ret<'a>(current: &'a E) -> Self::Ret<'a>;
}

/// insert
pub struct Insert<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> {
    target: U,
    new_data: E::Value,
    _m: PhantomData<E>,
}

impl<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> Insert<E, U> {
    pub fn new(target: U, new_data: E::Value) -> Self {
        Self {
            target,
            new_data,
            _m: PhantomData,
        }
    }
}

impl<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> Patch<E> for Insert<E, U> {
    const WMODE: WriteFlag = WRITEMODE_FRESH;
    type Ret<'a> = bool;
    type Target<'a> = U
    where
        Self: 'a;

    fn target<'a>(&'a self) -> &Self::Target<'a> {
        &self.target
    }
    fn nx_new(&mut self) -> E {
        E::new(self.target.upgrade_key(), self.new_data.clone())
    }
    fn nx_ret<'a>() -> Self::Ret<'a> {
        true
    }
    fn ex_apply(&mut self, _: &E) -> E {
        unreachable!()
    }
    fn ex_ret<'a>(_: &'a E) -> Self::Ret<'a> {
        false
    }
}

/// upsert
pub struct Upsert<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> {
    target: U,
    new_data: E::Value,
    _m: PhantomData<E>,
}

impl<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> Upsert<E, U> {
    pub fn new(target: U, new_data: E::Value) -> Self {
        Self {
            target,
            new_data,
            _m: PhantomData,
        }
    }
}

impl<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> Patch<E> for Upsert<E, U> {
    const WMODE: WriteFlag = WRITEMODE_ANY;
    type Ret<'a> = ();
    type Target<'a> = U
    where
        Self: 'a;

    fn target<'a>(&'a self) -> &Self::Target<'a> {
        &self.target
    }
    fn nx_new(&mut self) -> E {
        E::new(self.target.upgrade_key(), self.new_data.clone())
    }
    fn nx_ret<'a>() -> Self::Ret<'a> {
        ()
    }
    fn ex_apply(&mut self, _: &E) -> E {
        self.nx_new()
    }
    fn ex_ret<'a>(_: &'a E) -> Self::Ret<'a> {
        ()
    }
}

/// upsert return
pub struct UpsertReturn<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> {
    target: U,
    new_data: E::Value,
    _m: PhantomData<E>,
}

impl<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> UpsertReturn<E, U> {
    pub fn new(target: U, new_data: E::Value) -> Self {
        Self {
            target,
            new_data,
            _m: PhantomData,
        }
    }
}

impl<E: TreeElement, U: TreeKeyComparableUpgradeable<E>> Patch<E> for UpsertReturn<E, U> {
    const WMODE: WriteFlag = WRITEMODE_ANY;
    type Ret<'a> = Option<&'a E::Value>;
    type Target<'a> = U
    where
        Self: 'a;

    fn target<'a>(&'a self) -> &Self::Target<'a> {
        &self.target
    }
    fn nx_new(&mut self) -> E {
        E::new(self.target.upgrade_key(), self.new_data.clone())
    }
    fn nx_ret<'a>() -> Self::Ret<'a> {
        None
    }
    fn ex_apply(&mut self, _: &E) -> E {
        self.nx_new()
    }
    fn ex_ret<'a>(e: &'a E) -> Self::Ret<'a> {
        Some(e.val())
    }
}

/// update
pub struct UpdateReplace<E: TreeElement, U: TreeKeyComparable<E>> {
    target: U,
    new_data: E::Value,
    _m: PhantomData<E>,
}

impl<E: TreeElement, U: TreeKeyComparable<E>> UpdateReplace<E, U> {
    pub fn new(target: U, new_data: E::Value) -> Self {
        Self {
            target,
            new_data,
            _m: PhantomData,
        }
    }
}

impl<E: TreeElement, U: TreeKeyComparable<E>> Patch<E> for UpdateReplace<E, U> {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;

    type Ret<'a> = bool;

    type Target<'a> = U
    where
        Self: 'a;

    fn target<'a>(&'a self) -> &Self::Target<'a> {
        &self.target
    }

    fn nx_new(&mut self) -> E {
        unreachable!()
    }

    fn nx_ret<'a>() -> Self::Ret<'a> {
        false
    }

    fn ex_apply(&mut self, c: &E) -> E {
        E::new(c.key().clone(), self.new_data.clone())
    }

    fn ex_ret<'a>(_: &'a E) -> Self::Ret<'a> {
        true
    }
}

/// update_return
pub struct UpdateReplaceRet<E: TreeElement, U: TreeKeyComparable<E>> {
    target: U,
    new_data: E::Value,
    _m: PhantomData<E>,
}

impl<E: TreeElement, U: TreeKeyComparable<E>> UpdateReplaceRet<E, U> {
    pub fn new(target: U, new_data: E::Value) -> Self {
        Self {
            target,
            new_data,
            _m: PhantomData,
        }
    }
}

impl<E: TreeElement, U: TreeKeyComparable<E>> Patch<E> for UpdateReplaceRet<E, U> {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;

    type Ret<'a> = Option<&'a E::Value>;

    type Target<'a> = U
    where
        Self: 'a;

    fn target<'a>(&'a self) -> &Self::Target<'a> {
        &self.target
    }

    fn nx_new(&mut self) -> E {
        unreachable!()
    }

    fn nx_ret<'a>() -> Self::Ret<'a> {
        None
    }

    fn ex_apply(&mut self, c: &E) -> E {
        E::new(c.key().clone(), self.new_data.clone())
    }

    fn ex_ret<'a>(c: &'a E) -> Self::Ret<'a> {
        Some(c.val())
    }
}

pub struct InsertDirect<T: TreeElement> {
    data: T,
}

impl<T: TreeElement> InsertDirect<T> {
    pub fn new(key: T::Key, val: T::Value) -> Self {
        Self {
            data: T::new(key, val),
        }
    }
}

impl<T: TreeElement> Patch<T> for InsertDirect<T> {
    const WMODE: WriteFlag = WRITEMODE_FRESH;
    type Ret<'a> = bool;
    type Target<'a> = T::Key;
    fn target<'a>(&'a self) -> &Self::Target<'a> {
        self.data.key()
    }
    fn nx_ret<'a>() -> Self::Ret<'a> {
        true
    }
    fn nx_new(&mut self) -> T {
        self.data.clone()
    }
    fn ex_apply(&mut self, _: &T) -> T {
        unreachable!()
    }
    fn ex_ret<'a>(_: &'a T) -> Self::Ret<'a> {
        false
    }
}

pub trait PatchDelete<T>: 'static {
    type Ret<'a>;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a>;
    fn nx<'a>() -> Self::Ret<'a>;
}

pub struct Delete;
impl<T: TreeElement> PatchDelete<T> for Delete {
    type Ret<'a> = bool;
    #[inline(always)]
    fn ex<'a>(_: &'a T) -> Self::Ret<'a> {
        true
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        false
    }
}

pub struct DeleteRet;
impl<T: TreeElement> PatchDelete<T> for DeleteRet {
    type Ret<'a> = Option<&'a T::Value>;
    #[inline(always)]
    fn ex<'a>(v: &'a T) -> Self::Ret<'a> {
        Some(v.val())
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}
