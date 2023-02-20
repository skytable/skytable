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

use super::meta::TreeElement;

pub trait ReadMode<T>: 'static {
    type Ret<'a>;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a>;
    fn nx<'a>() -> Self::Ret<'a>;
}

pub struct RModeExists;
impl<T> ReadMode<T> for RModeExists {
    type Ret<'a> = bool;
    fn ex<'a>(_: &'a T) -> Self::Ret<'a> {
        true
    }
    fn nx<'a>() -> Self::Ret<'a> {
        false
    }
}

pub struct RModeRef;
impl<T: TreeElement> ReadMode<T> for RModeRef {
    type Ret<'a> = Option<&'a T::Value>;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a> {
        Some(v.val())
    }
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}

pub struct RModeClone;
impl<T: TreeElement> ReadMode<T> for RModeClone {
    type Ret<'a> = Option<T::Value>;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a> {
        Some(v.val().clone())
    }
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}
