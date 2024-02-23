/*
 * Created on Thu Apr 06 2023
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
    crate::engine::mem::unsafe_apis,
    std::{borrow::Borrow, fmt, hash::Hash, marker::PhantomData, mem::ManuallyDrop, slice, str},
};

pub struct EntityID {
    sp: *mut u8,
    sl: usize,
    ep: *mut u8,
    el: usize,
}

impl EntityID {
    pub fn new(space: &str, entity: &str) -> Self {
        let mut space = ManuallyDrop::new(space.to_owned().into_boxed_str().into_boxed_bytes());
        let mut entity = ManuallyDrop::new(entity.to_owned().into_boxed_str().into_boxed_bytes());
        Self {
            sp: space.as_mut_ptr(),
            sl: space.len(),
            ep: entity.as_mut_ptr(),
            el: entity.len(),
        }
    }
    pub fn space(&self) -> &str {
        unsafe { str::from_utf8_unchecked(slice::from_raw_parts(self.sp, self.sl)) }
    }
    pub fn entity(&self) -> &str {
        unsafe { str::from_utf8_unchecked(slice::from_raw_parts(self.ep, self.el)) }
    }
}

impl Drop for EntityID {
    fn drop(&mut self) {
        unsafe {
            unsafe_apis::dealloc_array(self.sp, self.sl);
            unsafe_apis::dealloc_array(self.ep, self.el);
        }
    }
}

impl PartialEq for EntityID {
    fn eq(&self, other: &Self) -> bool {
        self.space() == other.space() && self.entity() == other.entity()
    }
}

impl Eq for EntityID {}

impl Hash for EntityID {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.space().hash(state);
        self.entity().hash(state);
    }
}

impl fmt::Debug for EntityID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityID")
            .field("space", &self.space())
            .field("entity", &self.entity())
            .finish()
    }
}

#[derive(Clone, Copy)]
pub struct EntityIDRef<'a> {
    sp: *const u8,
    sl: usize,
    ep: *const u8,
    el: usize,
    _lt: PhantomData<(&'a str, &'a str)>,
}

impl<'a> EntityIDRef<'a> {
    pub fn new(space: &'a str, entity: &'a str) -> Self {
        Self {
            sp: space.as_ptr(),
            sl: space.len(),
            ep: entity.as_ptr(),
            el: entity.len(),
            _lt: PhantomData,
        }
    }
    pub fn space(&self) -> &'a str {
        unsafe { str::from_utf8_unchecked(slice::from_raw_parts(self.sp, self.sl)) }
    }
    pub fn entity(&self) -> &'a str {
        unsafe { str::from_utf8_unchecked(slice::from_raw_parts(self.ep, self.el)) }
    }
}

impl<'a> PartialEq for EntityIDRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.space() == other.space() && self.entity() == other.entity()
    }
}

impl<'a> Eq for EntityIDRef<'a> {}

impl<'a> Hash for EntityIDRef<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.space().hash(state);
        self.entity().hash(state);
    }
}

impl<'a> fmt::Debug for EntityIDRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityIDRef")
            .field("space", &self.space())
            .field("entity", &self.entity())
            .finish()
    }
}

impl<'a> Borrow<EntityIDRef<'a>> for EntityID {
    fn borrow(&self) -> &EntityIDRef<'a> {
        unsafe { core::mem::transmute(self) }
    }
}

impl<'a> From<(&'a str, &'a str)> for EntityIDRef<'a> {
    fn from((s, e): (&'a str, &'a str)) -> Self {
        Self::new(s, e)
    }
}
