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

/// write mode flag
type WriteFlag = u8;
pub const WRITEMODE_DELETE: WriteFlag = 0xFF;
/// fresh
pub const WRITEMODE_FRESH: WriteFlag = 0b01;
/// refresh
pub const WRITEMODE_REFRESH: WriteFlag = 0b10;
/// any
pub const WRITEMODE_ANY: WriteFlag = 0b11;

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

pub trait WriteMode<T>: 'static {
    const WMODE: WriteFlag;
    type Ret<'a>;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a>;
    fn nx<'a>() -> Self::Ret<'a>;
}

pub struct WModeFresh;
impl<T> WriteMode<T> for WModeFresh {
    const WMODE: WriteFlag = WRITEMODE_FRESH;
    type Ret<'a> = bool;
    #[inline(always)]
    fn ex(_: &T) -> Self::Ret<'static> {
        false
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        true
    }
}

pub struct WModeUpdate;
impl<T> WriteMode<T> for WModeUpdate {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;
    type Ret<'a> = bool;
    #[inline(always)]
    fn ex(_: &T) -> Self::Ret<'static> {
        true
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        false
    }
}

pub struct WModeUpdateRetClone;
impl<T: TreeElement> WriteMode<T> for WModeUpdateRetClone {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;
    type Ret<'a> = Option<T::Value>;
    #[inline(always)]
    fn ex(v: &T) -> Self::Ret<'static> {
        Some(v.val().clone())
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}

pub struct WModeUpdateRetRef;
impl<T: TreeElement> WriteMode<T> for WModeUpdateRetRef {
    const WMODE: WriteFlag = WRITEMODE_REFRESH;
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

pub struct WModeUpsert;
impl<T> WriteMode<T> for WModeUpsert {
    const WMODE: WriteFlag = WRITEMODE_ANY;
    type Ret<'a> = ();
    #[inline(always)]
    fn ex(_: &T) -> Self::Ret<'static> {
        ()
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        ()
    }
}

pub struct WModeUpsertRef;
impl<T: TreeElement> WriteMode<T> for WModeUpsertRef {
    const WMODE: WriteFlag = WRITEMODE_ANY;
    type Ret<'a> = Option<&'a T::Value>;
    fn ex<'a>(v: &'a T) -> Self::Ret<'a> {
        Some(v.val())
    }
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}

pub struct WModeUpsertClone;
impl<T: TreeElement> WriteMode<T> for WModeUpsertClone {
    const WMODE: WriteFlag = WRITEMODE_ANY;
    type Ret<'a> = Option<T::Value>;
    fn ex<'a>(v: &'a T) -> Self::Ret<'static> {
        Some(v.val().clone())
    }
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}
pub struct WModeDelete;
impl<T: TreeElement> WriteMode<T> for WModeDelete {
    const WMODE: WriteFlag = WRITEMODE_DELETE;
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

pub struct WModeDeleteRef;
impl<T: TreeElement> WriteMode<T> for WModeDeleteRef {
    const WMODE: WriteFlag = WRITEMODE_DELETE;
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

pub struct WModeDeleteClone;
impl<T: TreeElement> WriteMode<T> for WModeDeleteClone {
    const WMODE: WriteFlag = WRITEMODE_DELETE;
    type Ret<'a> = Option<T::Value>;
    #[inline(always)]
    fn ex<'a>(v: &'a T) -> Self::Ret<'a> {
        Some(v.val().clone())
    }
    #[inline(always)]
    fn nx<'a>() -> Self::Ret<'a> {
        None
    }
}
