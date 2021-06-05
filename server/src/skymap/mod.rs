/*
 * Created on Wed Jun 02 2021
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

//! A hashtable with SIMD lookup, quadratic probing and thread friendliness.
//! TODO(@ohsayan): Update this notice!
//!
//! ## Acknowledgements
//!
//! This implementation is inspired by:
//! - Google (the Abseil Developers and contributors) for the original implementation of the Swisstable, that can
//! [be found here](https://github.com/abseil/abseil-cpp/blob/master/absl/container/internal/raw_hash_set.h)
//! that is distributed under the [Apache-2.0 License](https://github.com/abseil/abseil-cpp/blob/master/LICENSE)
//! - The Rust Standard Library's hashtable implementation since 1.36, released under the
//! [Apache-2.0 License](https://github.com/rust-lang/hashbrown/blob/master/LICENSE-APACHE) OR
//! the [MIT License](https://github.com/rust-lang/hashbrown/blob/master/LICENSE-MIT) at your option

#![allow(dead_code)] // TODO(@ohsayan): Remove this lint once done

mod bitmask;
mod control_bytes;
mod mapalloc;

cfg_if::cfg_if! {
    if #[cfg(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64")
    ))] {
        mod sse2;
        use self::sse2 as imp;
    } else {
        mod generic;
        use self::generic as imp;
    }
}

use self::imp::Group;
use self::mapalloc::self_allocate;
use self::mapalloc::Allocator;
use self::mapalloc::Layout;
use core::alloc;
use core::mem;
use core::ptr::NonNull;
use std::alloc::handle_alloc_error;

#[cold]
/// Attribute for an LLVM optimization that indicates that this function won't be commonly
/// called. Look [here](https://llvm.org/docs/LangRef.html) for more information ("coldcc")
fn cold() {}

fn likely(b: bool) -> bool {
    if !b {
        cold()
    }
    b
}

fn unlikely(b: bool) -> bool {
    if b {
        cold()
    }
    b
}

/// Returns the offset of a pointer
///
/// ## Safety
/// This function is completely unsafe to be called on two pointers that aren't a part of
/// the same allocated object. It is only safe to call it if both the `to` and `from` ptrs
/// are a part of the same allocation (or atmost 1 byte past the end of the allocation)
unsafe fn offset_from<T>(to: *const T, from: *const T) -> usize {
    to.offset_from(from) as usize // cast to usize
}

/// The top bit is unset
const fn is_control_byte_full(ctrl: u8) -> bool {
    ctrl & 0x80 == 0
}

/// The top bit is set
/// Is control byte special? i.e, empty or deleted?
const fn is_control_byte_special(ctrl: u8) -> bool {
    ctrl & 0x80 != 0
}

/// Is the special control byte an empty control byte?
const fn is_special_empty(ctrl: u8) -> bool {
    // this will only check 1 bit
    ctrl & 0x01 != 0
}

/// Returns the H1 hash: this is the 57 bit hash value that will be used to identify the
/// index of the element itself (used for lookups)
const fn h1(hash: u64) -> usize {
    hash as usize
}

/// Returns the H2 hash: the remaining 7 bits of the hash value that we'll use for storing
/// metadata. This is stored separately (same allocation but different ptr offset) from the
/// H1 hash
fn h2(hash: u64) -> u8 {
    /*
     Grap the 7 high order bits of the hash. The below is done to get how many
     bits we should take as some hash functions may return an usize, so the higher
     order bits are just zeroed on 32-bit platforms. The shr is simple: say you're
     on an arch with 64-bit pointer width: you then have a 64 bit hash. The hash len
     will just be 8 because isize and u64 have the same sizes on 64-bit systems. Then
     we need to get the number of bits, easy enough, that's hash_len times 8. But we
     don't shr all the bits! We need to move away the difference of 7 and the total
     number of bytes: because we need the top 7. Soon enough, the higher order 57 bits
     are zeroed out
    */
    let hash_len = usize::min(mem::size_of::<isize>(), mem::size_of::<u64>());
    let top_seven_bits = hash >> (hash_len * 8 - 7);
    // truncate the higher order bits as we've already done our shrs
    (top_seven_bits & 0x7f) as u8
}

/// Probing sequence based on triangular numbers that ensures that we visit every group (whether 8 or 16
/// depending on the availability of SSE2 instructions) only once. The mathematical formula is:
/// T(n) = (n * n+1)/2
///
/// and the proof that each group is visited just once, can be found
/// [here](https://fgiesen.wordpress.com/2015/02/22/triangular-numbers-mod-2n)
struct ProbeSequence {
    pos: usize,
    stride: usize,
}

impl ProbeSequence {
    fn move_to_next(&mut self, bucket_mask: usize) {
        self.stride += Group::WIDTH;
        self.pos += self.stride;
        // this is the triangular magic
        self.pos &= bucket_mask;
    }
}

// Our goal is to keep the load factor at 87.5%. Now this will possibly never be the case
// due to the adjustment with the next power of two (2^p sized table, remember?)
const LOAD_FACTOR_NUMERATOR: usize = 7;
const LOAD_FACTOR_DENOMINATOR: usize = 8;

/// Returns the number of buckets needed to hold atleast the given
/// number of items. Hence, this will take the load factor into account
fn capacity_to_buckets(cap: usize) -> Option<usize> {
    if cap < 8 {
        // for small tables, we need atleast 1 empty bucket so that the lookup probe
        // can terminate
        // a table size of 2 buckets can only hold 1 element (because, metadata); instead look at 4 buckets
        // to store 3 elements
        return Some(if cap < 4 { 4 } else { 8 });
    }

    // large table, eh? let's look at the load factor; simple math here
    let lfactor_adjusted_target_capacity =
        cap.checked_mul(LOAD_FACTOR_DENOMINATOR)? / LOAD_FACTOR_NUMERATOR;

    // we always make the assumption that our table is always a size of 2^p
    Some(lfactor_adjusted_target_capacity.next_power_of_two())
}

/// Returns the maximum capacity for the given bucket mask
const fn bucket_mask_to_capacity(bucket_mask: usize) -> usize {
    if bucket_mask < 8 {
        // small table with {1, 2, 4, 8} buckets
        bucket_mask
    } else {
        ((bucket_mask + 1) / LOAD_FACTOR_DENOMINATOR) * LOAD_FACTOR_NUMERATOR
    }
}

struct TableLayout {
    size: usize,
    ctrl_byte_align: usize,
}

impl TableLayout {
    fn new<T>() -> Self {
        let layout = Layout::new::<T>();
        Self {
            size: layout.size(),
            ctrl_byte_align: usize::max(layout.align(), Group::WIDTH),
        }
    }

    fn calculate_layout_for(self, buckets: usize) -> Option<(Layout, usize)> {
        let TableLayout {
            size,
            ctrl_byte_align,
        } = self;

        /*
         Calculation of the control byte offset: we have as many control bytes
         as the number of buckets, so multiply it by the number of buckets. Add the
         bits needed for alignment.
        */

        let ctrl_byte_offset = size
            .checked_mul(buckets)?
            .checked_add(ctrl_byte_align - 1)?
            & !(ctrl_byte_align - 1);
        let len = ctrl_byte_offset.checked_add(buckets + Group::WIDTH)?;
        Some((
            unsafe {
                // UNSAFE(@ohsayan): We know that the alignment len is not 0, is a power of 2
                // and doesn't overflow
                Layout::from_size_align_unchecked(len, ctrl_byte_align)
            },
            ctrl_byte_offset,
        ))
    }
}

/// Returns the layout required for allocating the table. This will return none
/// if the number of buckets cause an overflow
fn calculate_layout<T>(buckets: usize) -> Option<(Layout, usize)> {
    TableLayout::new::<T>().calculate_layout_for(buckets)
}

/// A reference to a hash bucket containing some type `T`
pub struct Bucket<T> {
    // this will actually point to the next element
    ptr: NonNull<T>,
}

impl<T> Clone for Bucket<T> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr }
    }
}

impl<T> Bucket<T> {
    unsafe fn from_base_index(base: NonNull<T>, index: usize) -> Self {
        let ptr = if mem::size_of::<T>() == 0 {
            // uh oh, here comes a ZST
            (index + 1) as *mut T
        } else {
            base.as_ptr().sub(index)
        };
        Self {
            ptr: NonNull::new_unchecked(ptr),
        }
    }
    fn as_ptr(&self) -> *mut T {
        if mem::size_of::<T>() == 0 {
            // and here comes another ZST; just return some aligned garbage
            mem::align_of::<T>() as *mut T
        } else {
            unsafe { self.as_ptr().sub(1) }
        }
    }
    unsafe fn into_base_index(self, base: NonNull<T>) -> usize {
        if mem::size_of::<T>() == 0 {
            // ZST baby
            self.ptr.as_ptr() as usize - 1
        } else {
            offset_from(base.as_ptr(), self.ptr.as_ptr())
        }
    }
    unsafe fn drop(&self) {
        self.as_ptr().drop_in_place()
    }
    unsafe fn read(&self) -> T {
        self.as_ptr().read()
    }
    unsafe fn write(&self, val: T) {
        self.as_ptr().write(val)
    }
    unsafe fn as_ref<'b, 'a: 'b>(&'a self) -> &'b T {
        &*self.as_ptr()
    }
    unsafe fn as_mut<'b, 'a: 'b>(&'a self) -> &'b mut T {
        &mut *self.as_ptr()
    }
    unsafe fn copy_from_nonoverlapping(&self, other: &Self) {
        self.as_ptr().copy_from_nonoverlapping(other.as_ptr(), 1);
    }
}

struct LowTable<A> {
    /// the mask to get an index from a hash value
    bucket_mask: usize,
    /// points at the beginning of the control bytes in the allocation
    ///
    /// Our allocation looks like:
    /// ```text
    /// | padding |T1|T2|T3|..|T(n-1)|Tn|C_byte1|C_byte2|C_byte3|
    ///                                  ^
    ///                             ctrl points here
    /// ```
    ctrl: NonNull<u8>,
    /// Number of elements that can be inserted before rehashing the table
    growth_left: usize,
    /// number of elements in the table
    items: usize,
    /// and the allocator
    allocator: A,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TryReserveError {
    /// Error due to the computed capacity exceeding the maximum size
    CapacityOverflow,
    /// The memory allocator returned an error
    AllocatorError {
        /// The layout of the allocation that failed
        layout: alloc::Layout,
    },
}

#[derive(Clone, Copy)]
pub enum Fallibility {
    Fallible,
    Infallible,
}

impl Fallibility {
    fn capacity_overflow(self) -> TryReserveError {
        if let Self::Fallible = self {
            TryReserveError::CapacityOverflow
        } else {
            panic!("Hashtable capacity overflow")
        }
    }
    fn allocator_error(self, layout: Layout) -> TryReserveError {
        if let Self::Fallible = self {
            TryReserveError::AllocatorError { layout }
        } else {
            handle_alloc_error(layout)
        }
    }
}

impl<A> LowTable<A> {
    /// Get a raw pointer to the `index`th control byte
    unsafe fn ctrlof(&self, index: usize) -> *mut u8 {
        self.ctrl.as_ptr().add(index)
    }
    /// Gets the total number of control bytes
    const fn count_ctrl_bytes(&self) -> usize {
        self.bucket_mask + 1 + Group::WIDTH
    }
    const fn new_in(allocator: A) -> Self {
        Self {
            allocator,
            ctrl: unsafe { NonNull::new_unchecked(Group::empty_static() as *const _ as *mut u8) },
            bucket_mask: 0,
            items: 0,
            growth_left: 0,
        }
    }
    fn set_ctrl_h2(&self, index: usize, hash: u64) {
        unsafe { self.set_ctrlof(index, h2(hash)) }
    }
    unsafe fn set_ctrlof(&self, index: usize, ctrl: u8) {
        /*
         This idea is entirely taken from the hashbrown impl.
         The first Group::WIDTH control bytes are mirrored to the end of the array.
          - If the provided index >= Group::WIDTH, then index == index2
            So if we are on a 64-bit system, non-SSE (just for the sake of simplicity),
            then our Group WIDTH is 8. So if the index is at 8 or greater than 8, then we're at
            the end.
          - Else, idx2 = self.bucket_mask + 1 + index

          So, for a bucket size of 4, the _mirrored ctrl bytes_ look like this:
          | {A} | {B} | {EMPTY} | {EMPTY} | {A} | {B} |
          ^    real   ^                    ^ mirrored ^

          Why?
          We do this to avoid worrying about partially wrapping a SIMD access which would
          otherwise require us to wrap around the beginning of the ctrl_bytes
          ---
          Dark secret?
          bucket mask is just the number of buckets - 1
        */
        let idx2 = ((index.wrapping_sub(Group::WIDTH)) & self.bucket_mask) + Group::WIDTH;

        *self.ctrlof(index) = ctrl;
        *self.ctrlof(idx2) = ctrl;
    }

    fn probe_seq(&self, hash: u64) -> ProbeSequence {
        ProbeSequence {
            pos: h1(hash) & self.bucket_mask,
            stride: 0,
        }
    }
}

impl<A: Allocator + Clone> LowTable<A> {
    unsafe fn new_uinit(
        allocator: A,
        table_layout: TableLayout,
        buckets: usize,
        fallibility: Fallibility,
    ) -> Result<Self, TryReserveError> {
        let (layout, ctrl_offset) = match table_layout.calculate_layout_for(buckets) {
            Some(layout_n_ctrl) => layout_n_ctrl,
            None => return Err(fallibility.capacity_overflow()),
        };

        // To guard against allocating more than isize::MAX on 32-bit systems, we do
        // the same thing that alloc does for RawVec (the backing storage for Vec):
        // https://github.com/rust-lang/rust/blob/289ada5ed41fd1b9a3ffe2b694e6e73079528587/library/alloc/src/raw_vec.rs#L548
        if mem::size_of::<usize>() < 8 && layout.size() > isize::MAX as usize {
            return Err(fallibility.allocator_error(layout));
        }

        let ptr: NonNull<u8> = match self_allocate(&allocator, layout) {
            Ok(contiguous_block) => contiguous_block,
            Err(_) => return Err(fallibility.allocator_error(layout)),
        };

        let ctrl: NonNull<u8> = NonNull::new_unchecked(ptr.as_ptr().add(ctrl_offset));

        Ok(Self {
            allocator,
            bucket_mask: buckets - 1,
            items: 0,
            ctrl,
            growth_left: bucket_mask_to_capacity(buckets - 1),
        })
    }

    fn fallible_with_capacity(
        allocator: A,
        table_layout: TableLayout,
        capacity: usize,
        fallibility: Fallibility,
    ) -> Result<Self, TryReserveError> {
        if capacity == 0 {
            Ok(Self::new_in(allocator))
        } else {
            unsafe {
                let buckets =
                    capacity_to_buckets(capacity).ok_or_else(|| fallibility.capacity_overflow())?;
                let ret = Self::new_uinit(allocator, table_layout, buckets, fallibility)?;
                ret.ctrlof(0)
                    .write_bytes(control_bytes::EMPTY, ret.count_ctrl_bytes());
                Ok(ret)
            }
        }
    }

    fn find_insert_slot(&self, hash: u64) -> usize {
        let mut probe_seq = self.probe_seq(hash);
        loop {
            unsafe {
                let group = { Group::load_unaligned(self.ctrlof(probe_seq.pos)) };
                if let Some(bit) = group.match_empty_or_deleted().lowest_set_bit() {
                    let result = (probe_seq.pos + bit) & self.bucket_mask;
                    // TODO(@ohsayan): Explain this
                    if unlikely(is_control_byte_full(*self.ctrlof(result))) {
                        return Group::load_aligned(self.ctrlof(0))
                            .match_empty_or_deleted()
                            .lowest_set_bit_nonzero();
                    }
                    return result;
                }
            }
            probe_seq.move_to_next(self.bucket_mask);
        }
    }

    /// Look for an empty or deleted bucket that is suitable for inserting a new
    /// element. This function will set the hash 2 (h2) value for that slot
    ///
    /// There must be atleast 1 bucket in the table
    unsafe fn prepare_insert_slot(&self, hash: u64) -> (usize, u8) {
        let index = self.find_insert_slot(hash);
        let old_ctrl = *self.ctrlof(index);
        self.set_ctrl_h2(index, hash);
        (index, old_ctrl)
    }
}
