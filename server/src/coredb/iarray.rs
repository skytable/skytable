/*
 * Created on Sun Jul 04 2021
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

#![allow(dead_code)] // TODO(@ohsayan): Remove this once we're done

use std::mem;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::ptr;

pub trait MemoryBlock {
    type LayoutItem;
    fn size() -> usize;
}

impl<T, const N: usize> MemoryBlock for [T; N] {
    type LayoutItem = T;
    fn size() -> usize {
        N
    }
}

pub union InlineArray<A: MemoryBlock> {
    inline_store: ManuallyDrop<MaybeUninit<A>>,
    heap_ptr_len: (*mut A::LayoutItem, usize),
}

impl<A: MemoryBlock> InlineArray<A> {
    unsafe fn inline_ptr(&self) -> *const A::LayoutItem {
        self.inline_store.as_ptr() as *const _
    }
    unsafe fn inline_ptr_mut(&mut self) -> *mut A::LayoutItem {
        self.inline_store.as_mut_ptr() as *mut _
    }
    fn from_inline(inline_store: MaybeUninit<A>) -> Self {
        Self {
            inline_store: ManuallyDrop::new(inline_store),
        }
    }
    fn from_heap_ptr(start_ptr: *mut A::LayoutItem, len: usize) -> Self {
        Self {
            heap_ptr_len: (start_ptr, len),
        }
    }
}

pub struct IArray<A: MemoryBlock> {
    cap: usize,
    store: InlineArray<A>,
}

impl<A: MemoryBlock> IArray<A> {
    pub fn new() -> IArray<A> {
        Self {
            cap: 0,
            store: InlineArray::from_inline(MaybeUninit::uninit()),
        }
    }
    pub fn from_vec(mut vec: Vec<A::LayoutItem>) -> Self {
        if vec.capacity() <= Self::inline_capacity() {
            let mut store = InlineArray::<A>::from_inline(MaybeUninit::uninit());
            let len = vec.len();
            unsafe {
                ptr::copy_nonoverlapping(vec.as_ptr(), store.inline_ptr_mut(), len);
            }
            // done with the copy
            Self { cap: len, store }
        } else {
            let (start_ptr, cap, len) = (vec.as_mut_ptr(), vec.capacity(), vec.len());
            // leak the vec
            mem::forget(vec);
            IArray {
                cap,
                store: InlineArray::from_heap_ptr(start_ptr, len),
            }
        }
    }
    fn inline_capacity() -> usize {
        if mem::size_of::<A::LayoutItem>() > 0 {
            // not a ZST, so cap of array
            A::size()
        } else {
            usize::MAX
        }
    }
}
