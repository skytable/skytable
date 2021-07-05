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

use core::alloc::Layout;
use std::alloc as std_alloc;
use std::mem;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::ptr;
use std::ptr::NonNull;

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
    stack: ManuallyDrop<MaybeUninit<A>>,
    heap_ptr_len: (*mut A::LayoutItem, usize),
}

impl<A: MemoryBlock> InlineArray<A> {
    unsafe fn stack_ptr(&self) -> *const A::LayoutItem {
        self.stack.as_ptr() as *const _
    }
    unsafe fn stack_ptr_mut(&mut self) -> *mut A::LayoutItem {
        self.stack.as_mut_ptr() as *mut _
    }
    fn from_inline(stack: MaybeUninit<A>) -> Self {
        Self {
            stack: ManuallyDrop::new(stack),
        }
    }
    fn from_heap_ptr(start_ptr: *mut A::LayoutItem, len: usize) -> Self {
        Self {
            heap_ptr_len: (start_ptr, len),
        }
    }
    unsafe fn heap_size(&self) -> usize {
        self.heap_ptr_len.1
    }
    unsafe fn heap_ptr(&self) -> *const A::LayoutItem {
        self.heap_ptr_len.0
    }
    unsafe fn heap_ptr_mut(&mut self) -> *mut A::LayoutItem {
        self.heap_ptr_len.0 as *mut _
    }
    unsafe fn heap_size_mut(&mut self) -> &mut usize {
        &mut self.heap_ptr_len.1
    }
    unsafe fn heap(&self) -> (*mut A::LayoutItem, usize) {
        self.heap_ptr_len
    }
    unsafe fn heap_mut(&mut self) -> (*mut A::LayoutItem, &mut usize) {
        (self.heap_ptr_mut(), &mut self.heap_ptr_len.1)
    }
}

pub fn calculate_memory_layout<T>(count: usize) -> Result<Layout, ()> {
    let size = mem::size_of::<T>().checked_mul(count).ok_or(())?;
    // err is cap overflow
    let alignment = mem::align_of::<T>();
    Layout::from_size_align(size, alignment).map_err(|_| ())
}

/// Use the global allocator to deallocate the memory block for the given starting ptr
/// upto the given capacity
unsafe fn dealloc<T>(start_ptr: *mut T, capacity: usize) {
    std_alloc::dealloc(
        start_ptr as *mut u8,
        calculate_memory_layout::<T>(capacity).expect("Memory capacity overflow"),
    )
}

// Break free from Rust's aliasing rules with these typedefs
type DataptrLenptrCapacity<T> = (*const T, usize, usize);
type DataptrLenptrCapacityMut<'a, T> = (*mut T, &'a mut usize, usize);

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
        if vec.capacity() <= Self::stack_capacity() {
            let mut store = InlineArray::<A>::from_inline(MaybeUninit::uninit());
            let len = vec.len();
            unsafe {
                ptr::copy_nonoverlapping(vec.as_ptr(), store.stack_ptr_mut(), len);
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
    fn stack_capacity() -> usize {
        if mem::size_of::<A::LayoutItem>() > 0 {
            // not a ZST, so cap of array
            A::size()
        } else {
            // ZST. Just pile up some garbage and say that we have infinity
            usize::MAX
        }
    }
    fn meta_triple(&self) -> DataptrLenptrCapacity<A::LayoutItem> {
        unsafe {
            if self.went_off_stack() {
                let (data_ptr, len_ptr) = self.store.heap();
                (data_ptr, len_ptr, self.cap)
            } else {
                // still on stack
                (self.store.stack_ptr(), self.cap, Self::stack_capacity())
            }
        }
    }
    fn meta_triple_mut(&mut self) -> DataptrLenptrCapacityMut<A::LayoutItem> {
        unsafe {
            if self.went_off_stack() {
                // get heap
                let (data_ptr, len_ptr) = self.store.heap_mut();
                (data_ptr, len_ptr, self.cap)
            } else {
                // still on stack
                (
                    self.store.stack_ptr_mut(),
                    &mut self.cap,
                    Self::stack_capacity(),
                )
            }
        }
    }
    fn went_off_stack(&self) -> bool {
        self.cap > Self::stack_capacity()
    }
    pub fn len(&self) -> usize {
        if self.went_off_stack() {
            // so we're off the stack
            unsafe { self.store.heap_size() }
        } else {
            // still on the stack
            self.cap
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get_capacity(&self) -> usize {
        if self.went_off_stack() {
            self.cap
        } else {
            Self::stack_capacity()
        }
    }
    fn grow_block(&mut self, new_cap: usize) {
        // infallible
        unsafe {
            let (data_ptr, &mut len, cap) = self.meta_triple_mut();
            let still_on_stack = !self.went_off_stack();
            assert!(new_cap > len);
            if new_cap <= Self::stack_capacity() {
                if still_on_stack {
                    return;
                }
                // no branch
                self.store = InlineArray::from_inline(MaybeUninit::uninit());
                ptr::copy_nonoverlapping(data_ptr, self.store.stack_ptr_mut(), len);
                self.cap = len;
                dealloc(data_ptr, cap);
            } else if new_cap != cap {
                let layout =
                    calculate_memory_layout::<A::LayoutItem>(new_cap).expect("Capacity overflow");
                assert!(layout.size() > 0);
                let new_alloc;
                if still_on_stack {
                    new_alloc = NonNull::new(std_alloc::alloc(layout).cast())
                        .expect("Allocation error")
                        .as_ptr();
                    ptr::copy_nonoverlapping(data_ptr, new_alloc, len);
                } else {
                    // not on stack
                    let old_layout =
                        calculate_memory_layout::<A::LayoutItem>(cap).expect("Capacity overflow");
                    // realloc the earlier buffer
                    let new_memory_block_ptr =
                        std_alloc::realloc(data_ptr as *mut _, old_layout, layout.size());
                    new_alloc = NonNull::new(new_memory_block_ptr.cast())
                        .expect("Allocation error")
                        .as_ptr();
                }
                self.store = InlineArray::from_heap_ptr(new_alloc, len);
                self.cap = new_cap;
            }
        }
    }
    fn reserve(&mut self, additional: usize) {
        let (_, &mut len, cap) = self.meta_triple_mut();
        if cap - len >= additional {
            // already have enough space
            return;
        }
        let new_cap = len
            .checked_add(additional)
            .map(usize::next_power_of_two)
            .expect("Capacity overflow");
        self.grow_block(new_cap)
    }
    pub fn push(&mut self, val: A::LayoutItem) {
        unsafe {
            let (mut data_ptr, mut len, cap) = self.meta_triple_mut();
            if (*len).eq(&cap) {
                self.reserve(1);
                let (heap_ptr, heap_len) = self.store.heap_mut();
                data_ptr = heap_ptr;
                len = heap_len;
            }
            ptr::write(data_ptr.add(*len), val);
            *len += 1;
        }
    }
}
