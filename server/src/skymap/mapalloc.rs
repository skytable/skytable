/*
 * Created on Fri Jun 04 2021
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

//! Primitive methods for allocation
pub use core::alloc::Layout;
use core::ptr::NonNull;

/// This trait defines an allocator. The reason we don't directly use the host allocator
/// and abstract it away with a trait is for future events when we may build our own
/// allocator (or maybe support embedded!? gosh, that'll be some task)
pub unsafe trait Allocator {
    /// A pointer to the new allocation is returned on success
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, ()>;
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout);
}

/// The global allocator
#[derive(Clone, Copy)]
pub struct Global;
impl Default for Global {
    fn default() -> Self {
        Global
    }
}

unsafe impl Allocator for Global {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, ()> {
        unsafe { NonNull::new(std::alloc::alloc(layout)).ok_or(()) }
    }
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        std::alloc::dealloc(ptr.as_ptr(), layout)
    }
}

/// Use a given allocator `A` to allocate for a given memory layout
pub fn self_allocate<A: Allocator>(allocator: &A, layout: Layout) -> Result<NonNull<u8>, ()> {
    allocator.allocate(layout)
}
