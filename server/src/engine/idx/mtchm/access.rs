/*
 * Created on Fri Jan 27 2023
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
    core::{hash::Hash, marker::PhantomData},
};

pub trait ReadMode<T: TreeElement> {
    type Ret<'a>;
    type Target: Comparable<T::Key> + ?Sized + Hash;
    fn target(&self) -> &Self::Target;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a>;
    fn nx<'a>() -> Self::Ret<'a>;
}

pub struct RModeExists<'a, T, U: ?Sized> {
    target: &'a U,
    _d: PhantomData<T>,
}

impl<'a, T, U: ?Sized> RModeExists<'a, T, U> {
    pub fn new(target: &'a U) -> Self {
        Self {
            target,
            _d: PhantomData,
        }
    }
}

impl<'re, T: TreeElement, U: Comparable<T::Key> + ?Sized> ReadMode<T> for RModeExists<'re, T, U> {
    type Ret<'a> = bool;
    type Target = U;
    fn target(&self) -> &Self::Target {
        self.target
    }
    fn ex(_: &T) -> Self::Ret<'_> {
        true
    }
    fn nx<'a>() -> Self::Ret<'a> {
        false
    }
}

pub struct RModeRef<'a, T, U: ?Sized> {
    target: &'a U,
    _d: PhantomData<T>,
}

impl<'a, T, U: ?Sized> RModeRef<'a, T, U> {
    pub fn new(target: &'a U) -> Self {
        Self {
            target,
            _d: PhantomData,
        }
    }
}

impl<'re, T: TreeElement, U: Comparable<T::Key> + ?Sized> ReadMode<T> for RModeRef<'re, T, U> {
    type Ret<'a> = Option<&'a T::Value>;
    type Target = U;
    fn target(&self) -> &Self::Target {
        self.target
    }
    fn ex(c: &T) -> Self::Ret<'_> {
        Some(c.val())
    }
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}

pub struct RModeElementRef<'a, T, U: ?Sized> {
    target: &'a U,
    _d: PhantomData<T>,
}

impl<'a, T, U: ?Sized> RModeElementRef<'a, T, U> {
    pub fn new(target: &'a U) -> Self {
        Self {
            target,
            _d: PhantomData,
        }
    }
}

impl<'re, T: TreeElement, U: Comparable<T::Key> + ?Sized> ReadMode<T>
    for RModeElementRef<'re, T, U>
{
    type Ret<'a> = Option<&'a T>;
    type Target = U;
    fn target(&self) -> &Self::Target {
        self.target
    }
    fn ex(c: &T) -> Self::Ret<'_> {
        Some(c)
    }
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}
