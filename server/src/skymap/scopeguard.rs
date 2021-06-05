/*
 * Created on Sat Jun 05 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use core::ops::{Deref, DerefMut};

/// Runs a function after it has gone out of scope.
///
/// ## Considerations
/// - If you want the `dropfn` to run, just let this go out of scope
/// - If you **don't want** the `dropfn` to run, then deallocate this object's memory with [`mem::forget`]
/// (without calling the destructor)
pub struct ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    /// the function to run on drop
    dropfn: F,
    value: T,
}

impl<T, F> ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    pub fn new(value: T, dropfn: F) -> Self {
        ScopeGuard { dropfn, value }
    }
}

impl<T, F> Deref for ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T, F> DerefMut for ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

// this is the real magic: not really, just runs something on drop
impl<T, F> Drop for ScopeGuard<T, F>
where
    F: FnMut(&mut T),
{
    fn drop(&mut self) {
        (self.dropfn)(&mut self.value)
    }
}
