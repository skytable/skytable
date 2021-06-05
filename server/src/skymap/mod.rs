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

/*
 ANOTHER SEEMINGLY FRIENDLY WARNING
 ------------------------------------------------------------------------
 YOU ARE JUST ABOUT TO ENTER A PART OF THE UNIVERSE WHERE EVERYTHING IS WHAT
 YOU WOULD WANT IT TO BE. THIS IS UNLIKE ALICE IN WONDERLAND, BECAUSE COMPUTER
 MEMORY ISN'T UNFORTUNATELY THE WONDERLAND YOU'D DREAM OF.
 YOU CAN HAVE A MUTABLE REFERENCE IF YOU WANT IT. FROM AN IMMUTABLE REFERENCE.
 FROM A NON EXISTENT PIECE OF MEMORY. THAT'S THE WONDER WE'RE TALKING ABOUT.

 BE SURE TO KNOW WHAT YOU ARE DOING HERE, BEFORE DOING IT. CHANGING SOME
 LITTLE DEREFERENCE, ADDING 1 TO SOME VALUE OR EVEN REMOVING A SYMBOL CAN
 GIFT YOU INTANGIBLE HORRORS THAT MAY COST YOU YOUR SANITY, YOUR COMPUTER
 AND/OR YOUR DATA. KNOWING ALL THAT, SET YOUR MIND ADRIFT THE INFINITE COSMOS.

 -- Sayan N. <ohsayan@outlook.com>
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
mod scopeguard;
mod util;

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

use self::bitmask::Bitmask;
use self::bitmask::BitmaskIterator;
use self::imp::Group;
use self::mapalloc::self_allocate;
use self::mapalloc::Allocator;
use self::mapalloc::Global;
use self::mapalloc::Layout;
use self::scopeguard::ScopeGuard;
use self::util::likely;
use self::util::unlikely;
use core::alloc;
use core::hint::unreachable_unchecked;
use core::iter::FusedIterator;
use core::marker::PhantomData;
use core::mem;
use core::mem::ManuallyDrop;
use core::ptr::NonNull;
use std::alloc::handle_alloc_error;

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
     Grab the 7 high order bits of the hash. The below is done to get how many
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
    /// Move the current `pos` and `stride` to the next group that we'll be looking at
    fn move_to_next(&mut self, bucket_mask: usize) {
        self.stride += Group::WIDTH;
        self.pos += self.stride;
        // this is the triangular magic
        self.pos &= bucket_mask;
    }
}

// Our goal is to keep the load factor at 87.5%. Now this will possibly never be the case
// due to the adjustment with the next power of two (2^p sized table, remember?)
/// The load factor numerator
const LOAD_FACTOR_NUMERATOR: usize = 7;
/// The load factor denominator
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

#[derive(Clone, Copy)]
/// The layout of the table
///
/// This is done to primarily compute the position of the control bytes independently of
/// T
struct TableLayout {
    /// the size
    size: usize,
    /// the position of the ctrl bytes
    ctrl_byte_align: usize,
}

impl TableLayout {
    /// Create a new layout for type T
    fn new<T>() -> Self {
        let layout = Layout::new::<T>();
        Self {
            size: layout.size(),
            ctrl_byte_align: usize::max(layout.align(), Group::WIDTH),
        }
    }

    /// Calculate the table layout for a given number of buckets
    ///
    /// This returns the layout and the ctrl byte offset
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
    /// Creates a bucket at `index` from the given base ptr
    ///
    /// The base ptr points at the start of the ctrl bytes, so you'll have to do some
    /// basic ptr arithmetic and move back by the index to find the corresponding location
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
    /// Returns a raw pointer to the T stored in the bucket
    fn as_ptr(&self) -> *mut T {
        if mem::size_of::<T>() == 0 {
            // and here comes another ZST; just return some aligned garbage
            mem::align_of::<T>() as *mut T
        } else {
            unsafe { self.as_ptr().sub(1) }
        }
    }
    /// Gets the base index position from the given base ptr
    unsafe fn to_base_index(&self, base: NonNull<T>) -> usize {
        if mem::size_of::<T>() == 0 {
            // ZST baby
            self.ptr.as_ptr() as usize - 1
        } else {
            offset_from(base.as_ptr(), self.ptr.as_ptr())
        }
    }
    /// Drop the object that exists at the ptr
    unsafe fn drop(&self) {
        self.as_ptr().drop_in_place()
    }
    /// Read the bucket object at the current ptr (this does nothing to the allocation of the object)
    unsafe fn read(&self) -> T {
        self.as_ptr().read()
    }
    /// Write something to the current bucket ptr
    unsafe fn write(&self, val: T) {
        self.as_ptr().write(val)
    }
    /// Get a reference to the bucket
    pub unsafe fn as_ref<'a>(&self) -> &'a T {
        &*self.as_ptr()
    }
    /// Get a mutable reference to the bucket
    unsafe fn as_mut<'a>(&self) -> &'a mut T {
        &mut *self.as_ptr()
    }
    /// Copy bytes from another bucket to the current bucket
    unsafe fn copy_from_nonoverlapping(&self, other: &Self) {
        self.as_ptr().copy_from_nonoverlapping(other.as_ptr(), 1);
    }
    unsafe fn next_n(&self, offset: usize) -> Self {
        let ptr = if mem::size_of::<T>() == 0 {
            (self.ptr.as_ptr() as usize + offset) as *mut T
        } else {
            self.ptr.as_ptr().sub(offset)
        };
        Self {
            ptr: NonNull::new_unchecked(ptr),
        }
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
    /// Create a new empty table using the given allocator
    const fn new_in(allocator: A) -> Self {
        Self {
            allocator,
            ctrl: unsafe { NonNull::new_unchecked(Group::empty_static() as *const _ as *mut u8) },
            bucket_mask: 0,
            items: 0,
            growth_left: 0,
        }
    }
    /// Set the hash 2 (h2) of the provided index (and with the hash for truncation)
    fn set_ctrl_h2_of(&self, index: usize, hash: u64) {
        unsafe { self.set_ctrlof(index, h2(hash)) }
    }
    /// Set the `ctrl` byte for the provided `index`
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
    /// Returns the probe sequence for a given hash
    fn probe_seq(&self, hash: u64) -> ProbeSequence {
        ProbeSequence {
            pos: h1(hash) & self.bucket_mask,
            stride: 0,
        }
    }
    /// Returns the bucket count
    const fn buckets(&self) -> usize {
        self.bucket_mask + 1
    }
    /// Check if self.bucket_mask is 0 (i.e has one element)
    const fn is_empty_singleton(&self) -> bool {
        self.bucket_mask == 0
    }
}

impl<A: Allocator + Clone> LowTable<A> {
    /// Returns a completely uninitialized table with no ctrl bytes or data
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

    /// Returns a table with the provided capacity or returns an error if there is either an overflow
    /// or an error from the allocator (do note that passing infallible will trigger a runtime panic
    /// on erroring)
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

    /// Find the insert slot for a given hash
    ///
    /// This will return the index (wrt the data offset) for the insertion (empty/deleted). One more
    /// note: double lookups do happen, but they're insignificant (read the comments below)
    fn find_insert_slot(&self, hash: u64) -> usize {
        let mut probe_seq = self.probe_seq(hash);
        loop {
            unsafe {
                let group = { Group::load_unaligned(self.ctrlof(probe_seq.pos)) };
                if let Some(bit) = group.match_empty_or_deleted().lowest_set_bit() {
                    let result = (probe_seq.pos + bit) & self.bucket_mask;
                    /*
                     For tables smaller than the width (32/64 on non-SSE) or 128 on SSE,
                     the trailing control bytes outside the table are filled with empty
                     entries. This unfortunately triggers a match, and on applying the
                     mask may point at an already occupied bucket. To detect this, we
                     perform a second scan (from the beginning of the table
                     and this guarantees to find a second slot
                    */
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
        self.set_ctrl_h2_of(index, hash);
        (index, old_ctrl)
    }

    // TODO(@ohsayan): Explain the difference in reference mutability here! (IMPORTANT!)
    unsafe fn prepare_rehash_in_place(&self) {
        // this will bulk convert all full bytes to deleted and all deleted control bytes
        // to empty, effectively freeing the table of tombstones
        for i in (0..self.buckets()).step_by(Group::WIDTH) {
            let group = Group::load_aligned(self.ctrlof(i));
            let group = group.transform_full_to_deleted_and_special_to_empty();
            group.store_aligned(self.ctrlof(i));
        }
        // Now fix the mirrored ctrl bytes
        if self.buckets() < Group::WIDTH {
            self.ctrlof(0)
                .copy_to(self.ctrlof(Group::WIDTH), self.buckets())
        } else {
            self.ctrlof(0)
                .copy_to(self.ctrlof(self.buckets()), Group::WIDTH);
        }
    }

    /// Returns a non-null ptr to where the data ends (i.e where the ctrl bytes start in other words)
    unsafe fn data_terminal_ptr<T>(&self) -> NonNull<T> {
        NonNull::new_unchecked(self.ctrl.as_ptr().cast())
    }
    /// Returns a bucket for the given index
    unsafe fn get_bucket<T>(&self, index: usize) -> Bucket<T> {
        Bucket::from_base_index(self.data_terminal_ptr(), index)
    }

    // TODO(@ohsayan): Explain the difference in reference mutability here! (IMPORTANT!)
    unsafe fn record_item_insert_at(&mut self, index: usize, old_ctrl: u8, hash: u64) {
        self.growth_left -= is_special_empty(old_ctrl) as usize;
        self.set_ctrl_h2_of(index, hash);
        self.items += 1;
    }

    /// Check if an `idx` and `new_idx` belong to the same group for a given `hash`
    unsafe fn is_in_same_group(&self, idx: usize, new_idx: usize, hash: u64) -> bool {
        let probe_seq_pos = self.probe_seq(hash).pos;
        let probe_idx =
            |pos: usize| (pos.wrapping_sub(probe_seq_pos) & self.bucket_mask) / Group::WIDTH;
        probe_idx(idx) == probe_idx(new_idx)
    }

    /// Update the H2 value for a given `index` to the provided hash
    unsafe fn update_ctrl_h2_of(&self, index: usize, hash: u64) -> u8 {
        let last_ctrl = *self.ctrlof(index);
        self.set_ctrl_h2_of(index, hash);
        last_ctrl
    }

    // TODO(@ohsayan): Explain the difference in reference mutability here! (IMPORTANT!)
    /// Deallocate the buckets, **without** calling the destructors
    unsafe fn free_buckets(&self, table_layout: TableLayout) {
        let (layout, ctrl_offset) = match table_layout.calculate_layout_for(self.buckets()) {
            Some(contiguous_block) => contiguous_block,
            None => unreachable_unchecked(),
        };
        self.allocator.deallocate(
            NonNull::new_unchecked(self.ctrl.as_ptr().sub(ctrl_offset)),
            layout,
        );
    }

    /// Get ready for a resize event: this function returns a new table with the provided capacity. If
    /// the hash function panics, this will not leak memory -- use the scopeguard!
    unsafe fn prepare_resize(
        &self,
        table_layout: TableLayout,
        capacity: usize,
        fallibility: Fallibility,
    ) -> Result<ScopeGuard<Self, impl FnMut(&mut Self)>, TryReserveError> {
        let mut new_table = LowTable::fallible_with_capacity(
            self.allocator.clone(),
            table_layout,
            capacity,
            fallibility,
        )?;
        new_table.growth_left -= self.items;
        new_table.items = self.items;

        // the crazy part when the hash function panics; use our scopeguard to make sure
        // we free all the buckets, i.e not leak memory
        Ok(ScopeGuard::new(new_table, move |slf| {
            if !slf.is_empty_singleton() {
                slf.free_buckets(table_layout);
            }
        }))
    }

    // TODO(@ohsayan): Explain the difference in reference mutability here! (IMPORTANT!)
    /// Empty the table without calling the destructors on the individual `T`s. Ideally this will
    /// just set all metadata bits to EMPTY (`0b1111_1111`)
    fn clear_no_drop(&mut self) {
        if !self.is_empty_singleton() {
            unsafe {
                self.ctrlof(0)
                    .write_bytes(control_bytes::EMPTY, self.count_ctrl_bytes())
            }
        }
        self.items = 0;
        self.growth_left = bucket_mask_to_capacity(self.bucket_mask);
    }

    // TODO(@ohsayan): Explain the difference in reference mutability here! (IMPORTANT!)
    /// Remove an element from a table (again, **no drop** -- just update the metadata)
    unsafe fn erase(&mut self, index: usize) {
        let last_idx = index.wrapping_sub(Group::WIDTH) & self.bucket_mask;
        let empty_before = Group::load_unaligned(self.ctrlof(last_idx)).match_empty();
        let empty_after = Group::load_unaligned(self.ctrlof(index)).match_empty();

        let new_ctrl =
            if empty_before.leading_zeros() + empty_after.trailing_zeros() >= Group::WIDTH {
                control_bytes::DELETED
            } else {
                self.growth_left += 1;
                control_bytes::EMPTY
            };
        self.set_ctrlof(index, new_ctrl);
        self.items -= 1;
    }
}

pub struct RawTable<T, A: Allocator + Clone = Global> {
    table: LowTable<A>,
    _marker: PhantomData<T>,
}

impl<T> RawTable<T, Global> {
    pub const fn new() -> Self {
        Self {
            table: LowTable::new_in(Global),
            _marker: PhantomData,
        }
    }
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_in(cap, Global)
    }
}

impl<T, A: Allocator + Clone> RawTable<T, A> {
    pub fn new_in(allocator: A) -> Self {
        Self {
            table: LowTable::new_in(allocator),
            _marker: PhantomData,
        }
    }

    unsafe fn raw_new_uinit(
        allocator: A,
        buckets: usize,
        fallibility: Fallibility,
    ) -> Result<Self, TryReserveError> {
        Ok(Self {
            table: LowTable::new_uinit(allocator, TableLayout::new::<T>(), buckets, fallibility)?,
            _marker: PhantomData,
        })
    }

    fn fallible_with_capacity(
        allocator: A,
        capacity: usize,
        fallibility: Fallibility,
    ) -> Result<Self, TryReserveError> {
        Ok(Self {
            table: LowTable::fallible_with_capacity(
                allocator,
                TableLayout::new::<T>(),
                capacity,
                fallibility,
            )?,
            _marker: PhantomData,
        })
    }

    pub fn with_capacity_in(capacity: usize, allocator: A) -> Self {
        match Self::fallible_with_capacity(allocator, capacity, Fallibility::Infallible) {
            Ok(rtable) => rtable,
            Err(_) => unsafe { unreachable_unchecked() },
        }
    }

    pub fn allocator(&self) -> &A {
        &self.table.allocator
    }

    // TODO(@ohsayan): Change mut rules (IMPORTANT!)
    unsafe fn free_buckets(&self) {
        self.table.free_buckets(TableLayout::new::<T>())
    }

    pub unsafe fn data_terminal_ptr(&self) -> NonNull<T> {
        self.table.data_terminal_ptr()
    }

    pub unsafe fn data_start_ptr(&self) -> *mut T {
        self.data_terminal_ptr()
            .as_ptr()
            .wrapping_sub(self.buckets())
    }

    fn buckets(&self) -> usize {
        self.table.buckets()
    }

    pub unsafe fn index_of_bucket(&self, bucket: &Bucket<T>) -> usize {
        bucket.to_base_index(self.data_terminal_ptr())
    }

    pub unsafe fn get_bucket(&self, index: usize) -> Bucket<T> {
        Bucket::from_base_index(self.data_terminal_ptr(), index)
    }

    unsafe fn erase_without_drop(&mut self, item: &Bucket<T>) {
        let idx = self.index_of_bucket(&item);
        self.table.erase(idx);
    }

    pub unsafe fn erase(&mut self, item: Bucket<T>) {
        self.erase_without_drop(&item);
        item.drop();
    }

    pub fn clear_no_drop(&mut self) {
        self.table.clear_no_drop()
    }

    pub fn len(&self) -> usize {
        self.table.items
    }
}

// implment the iterators for lookups

pub struct RawIterRange<T> {
    /// The bitmask for all the buckets in the current group
    current_group: Bitmask,
    /// A pointer to buckets in the current group
    data: Bucket<T>,
    /// A pointer to the next _group_ of control bytes
    next_ctrl_ptr: *const u8,
    /// This is **one byte _past_** the last control byte
    end_of_alloc: *const u8,
}

impl<T> RawIterRange<T> {
    unsafe fn new(ctrl: *const u8, data: Bucket<T>, len: usize) -> Self {
        let end_of_alloc = ctrl.add(len);

        // the bliss! we don't need to look for all the empty ones!
        let current_group = Group::load_aligned(ctrl).match_full();
        // we've already probed this group, so point at the next ctrl
        let next_ctrl_ptr = ctrl.add(Group::WIDTH);

        Self {
            current_group,
            data,
            next_ctrl_ptr,
            end_of_alloc,
        }
    }
}

impl<T> Clone for RawIterRange<T> {
    fn clone(&self) -> Self {
        Self {
            current_group: self.current_group,
            // clones the ptr
            data: self.data.clone(),
            next_ctrl_ptr: self.next_ctrl_ptr,
            end_of_alloc: self.end_of_alloc,
        }
    }
}

impl<T> Iterator for RawIterRange<T> {
    type Item = Bucket<T>;

    fn next(&mut self) -> Option<Bucket<T>> {
        unsafe {
            loop {
                if let Some(index) = self.current_group.lowest_set_bit() {
                    // hey, we got the first item in the group that has something full
                    // pop that off so that we can look at the next items (it's the other way around
                    // remember), or the higher order bits
                    self.current_group = self.current_group.remove_lowest_bit();
                    return Some(self.data.next_n(index));
                }

                if self.next_ctrl_ptr >= self.end_of_alloc {
                    // ugh, we're done scanning
                    return None;
                }

                /*
                 Another situation may arise when we go _past_ the end ptr and look at the
                 empty ctrl bytes. That is fine because it happens on smaller tables, and
                 again, they are empty buckets.
                */
                self.current_group = Group::load_aligned(self.next_ctrl_ptr).match_full();
                // we probed all these, so move ahead (technically behind, but yeah)
                self.data = self.data.next_n(Group::WIDTH);
                // we probe all these (eqv to Group::WIDTH), so move the ctrl bytes ahead
                self.next_ctrl_ptr = self.next_ctrl_ptr.add(Group::WIDTH);
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            0,
            Some(unsafe {
                // We don't really know the size, so just use some ptr math and return the offset
                // + the next probe width; since size_hint is always an upper bound
                offset_from(self.end_of_alloc, self.next_ctrl_ptr) + Group::WIDTH
            }),
        )
    }
}

impl<T> FusedIterator for RawIterRange<T> {}

/// A raw iterator that returns raw pointers to every full bucket in the [`LowTable`]
///
/// ## Notes
/// - This iterator is not bound by a lifetime. This has a direct implication that it is unsafe
/// to create in the first place. So **you** are responsible for use-after-free violations
/// - A bucket can be erased while the iterator is iterating, but this will not be reflected by
/// the iterator unless the iterator is refreshed for deletes
/// - Because of the nature of hashes, it is _undefined_ whether an element inserted after the iterator
/// was created would appear or not, again, until the iterator is refreshed
/// - Remember the thing with lskeys? Correct, iterators have no meaningful order for hash tables (or
/// has based data structures in general, for that purpose)
pub struct RawIter<T> {
    /// The _actual_ iterator
    iter: RawIterRange<T>,
    /// The length of the table
    items: usize,
}

impl<T> RawIter<T> {
    /// This function will refresh the iterator so that it reflects the state of the given bucket
    /// It will do everything such as finding out if we've run through it, not even visited it or are
    /// currently at it.
    ///
    /// Now note: you should not attempt to call things as low-level as this, but instead use higher
    /// level abstractions
    fn refresh(&mut self, b: &Bucket<T>, is_insert: bool) {
        if b.as_ptr() > self.iter.data.as_ptr() {
            // we've moved past self's iterator data ptr, so we don't bother with anything at all
            return;
        }
        if unsafe {
            self.iter.next_ctrl_ptr < self.iter.end_of_alloc
                && b.as_ptr() <= self.iter.data.next_n(Group::WIDTH).as_ptr()
        } {
            /*
             In this case, the iterator hasn't crossed the bucket's group but instead we're
             behind it. But this has one implication: update self's item count because there is one item
             more or less now. (Just make sure the caller tells the truth, or assume they are).
            */
            // TODO(@ohsayan): Do some sort of validation
            if is_insert {
                // so an item was inserted ahead of us, and we need to iterate by one element more
                self.items += 1;
            } else {
                // an item was popped ahead of us, and we need to iterate by one element less
                self.items -= 1;
            }
            return;
        }

        /*
         Now the interesting case - we're at the bucket where the mishap (the change, come on) happened.
         These are the potential scenarios:

         - If there is a pending/complete insert at this bucket: we will update our iterator state
         to LOOK at it. At the same time, increment your own bucket count
         - If there is a pending/complete remove at this bucket: we will update our iterator state
         to NOT look at it. Similarly, reduce your bucket count
         - If we've already checked the bucket in question, let's just return
        */

        if let Some(index) = self.iter.current_group.lowest_set_bit() {
            // index is the ith bit in the group that has been set (from the lower order range)
            let ptr_to_next_bucket = unsafe { self.iter.data.next_n(index) };
            if b.as_ptr() > ptr_to_next_bucket.as_ptr() {
                /*
                 so the provided bucket is ahead of the ptr to the next bucket? now that's silly.
                 This means that we've already moved past the bucket. So changes in count? Nope,
                 because insertions/erases before the iterators current position wouldn't affect
                 the item count as we'll have to move across the same number of elements/groups
                */
            } else {
                /*
                 so the provided bucket is the bucket we were about to yield, and it has been
                 removed. let's update our state. But there's a problem here:
                 If we reload the group, we might look past inserts that we've already seen (basically
                 reading all those `GroupWord` count of entries again / 8) or we might unset the bits
                 for other removals. If we indeed do this, we'll have to decrement our count, but again
                 subsequent iterator refreshes can also decrement this count. Instead of getting ourselves
                 into that mess, we'll flip the bit for the bucket the caller gave us
                */
                unsafe {
                    let callers_ctrl_bit_index = offset_from(self.iter.data.as_ptr(), b.as_ptr());
                    let was_full = self.iter.current_group.flip(callers_ctrl_bit_index);
                    // TODO(@ohsayan): Explain why
                    debug_assert_ne!(was_full, is_insert);
                }

                if is_insert {
                    self.items += 1;
                } else {
                    self.items -= 1;
                }
            }
        } else {
            // since there was no lowest set bit, we've already removed _the removed_ buckets
            // just return
        }
    }

    /// Refresh the iterator so that it reflects the removal of the given bucket. Ensure that this is
    /// called _before the removal_ and _before `next()` is called_
    pub fn refresh_remove(&mut self, b: &Bucket<T>) {
        self.refresh(b, false)
    }

    /// Refresh the iterator so that it reflects the insertion of the given bucket. Ensure that this is
    /// called _before the insertion_ and _before `next()` is called_
    pub fn refresh_insert(&mut self, b: &Bucket<T>) {
        self.refresh(b, true)
    }

    unsafe fn drop_elements(&mut self) {
        if mem::needs_drop::<T>() && self.len() != 0 {
            for item in self {
                item.drop()
            }
        }
    }
}

impl<T> Clone for RawIter<T> {
    fn clone(&self) -> Self {
        Self {
            iter: self.iter.clone(),
            items: self.items,
        }
    }
}

impl<T> Iterator for RawIter<T> {
    type Item = Bucket<T>;

    fn next(&mut self) -> Option<Bucket<T>> {
        if let Some(bkt) = self.iter.next() {
            self.items -= 1;
            Some(bkt)
        } else {
            // so item length must be zero
            // let's just return none
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // we kinda know this already. but let's make the impl (and the caller) happy
        (self.items, Some(self.items))
    }
}

impl<T> ExactSizeIterator for RawIter<T> {}
impl<T> FusedIterator for RawIter<T> {}

// now add the iterator methods to the `RawTable`

impl<T, A: Allocator + Clone> RawTable<T, A> {
    /// Returns a RawIter over every element in the table. But calling this immediately
    /// gives you the responsibility of making sure the `RawTable` outlives the `RawIter`
    /// (because `RawIter` doesn't have any lifetime bound to it which would severly complicate
    /// the implementation)
    pub unsafe fn iter(&self) -> RawIter<T> {
        // start at the first data index
        let data = Bucket::from_base_index(self.data_terminal_ptr(), 0);
        RawIter {
            iter: RawIterRange::new(self.table.ctrl.as_ptr(), data, self.table.buckets()),
            items: self.table.items,
        }
    }

    /// Call a destructor on every element in the table
    unsafe fn drop_elements(&mut self) {
        if mem::needs_drop::<T>() && self.len() != 0 {
            for item in self.iter() {
                // i.e ptr::drop_in_place
                item.drop();
            }
        }
    }
}

impl<T, A: Allocator + Clone> Drop for RawTable<T, A> {
    fn drop(&mut self) {
        if !self.table.is_empty_singleton() {
            unsafe {
                // call the destructors
                self.drop_elements();
                // deallocate the memory
                self.free_buckets();
            }
        }
    }
}

pub struct RawConsumingIterator<T, A: Allocator + Clone = Global> {
    iter: RawIter<T>,
    allocation: Option<(NonNull<u8>, Layout)>,
    _marker: PhantomData<T>,
    allocator: A,
}

impl<T, A: Allocator + Clone> RawConsumingIterator<T, A> {
    // get a non-consuming iter from the consuming iter _for the time being_
    pub fn iter(&self) -> RawIter<T> {
        self.iter.clone()
    }
}

impl<T, A: Allocator + Clone> Iterator for RawConsumingIterator<T, A> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        unsafe {
            // read the raw ptr
            Some(self.iter.next()?.read())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T, A: Allocator + Clone> RawTable<T, A> {
    fn into_allocation(self) -> Option<(NonNull<u8>, Layout)> {
        let allocation = if self.table.is_empty_singleton() {
            // no allocated mem if table is empty
            None
        } else {
            let (layout, ctrl_offset) = match calculate_layout::<T>(self.table.buckets()) {
                Some(contiguous_block) => contiguous_block,
                None => unsafe {
                    // infallible
                    unreachable_unchecked()
                },
            };
            Some((
                unsafe { NonNull::new_unchecked(self.table.ctrl.as_ptr().sub(ctrl_offset)) },
                layout,
            ))
        };
        // leak the rawtable, don't drop!
        mem::forget(self);
        allocation
    }
    pub unsafe fn into_iter_from(self, iter: RawIter<T>) -> RawConsumingIterator<T, A> {
        let allocator = self.table.allocator.clone();
        let allocation = self.into_allocation();

        RawConsumingIterator {
            iter,
            allocation,
            _marker: PhantomData,
            allocator,
        }
    }
}

impl<T: Clone, A: Allocator + Clone> RawTable<T, A> {
    unsafe fn clone_from_impl(&mut self, source: &Self, mut on_panic: impl FnMut(&mut Self)) {
        source
            .table
            .ctrlof(0)
            .copy_to_nonoverlapping(self.table.ctrlof(0), self.table.count_ctrl_bytes());

        let mut guard = ScopeGuard::new((0, &mut *self), |(index, slf)| {
            if mem::needs_drop::<T>() && slf.len() != 0 {
                for i in 0..=*index {
                    if is_control_byte_full(*slf.table.ctrlof(i)) {
                        // call the destructor
                        slf.get_bucket(i).drop();
                    }
                }
            }
            // if we panic, do this (let our lovely scopeguard take care of that)
            on_panic(slf);
        });
        for from in source.iter() {
            // get the idx from the ptr (ptr math man)
            let idx = source.index_of_bucket(&from);
            let to = guard.1.get_bucket(idx);
            to.write(from.as_ref().clone());

            // update the index in case there is a panic somewhere
            guard.0 = idx;
        }

        // just forget the scopeguard as we've finished the clone successfully and DON'T CALL ITS DESTRUCTOR
        // because that will atempt to clear our new table! UB ALERT!
        mem::forget(guard);

        // update the lengths as usual
        self.table.items = source.table.items;
        self.table.growth_left = source.table.growth_left;
    }
}

impl<T: Clone, A: Allocator + Clone> Clone for RawTable<T, A> {
    fn clone(&self) -> Self {
        if self.table.is_empty_singleton() {
            Self::new_in(self.table.allocator.clone())
        } else {
            unsafe {
                // use manually drop to avoid calling the destructor in the event
                // of unforeseen panics because we have a scopeguard of doing the
                // clean up job. Calling the destructor would be disastrous because
                // it will try to call destructors on every element which can have UB
                // if the table wasn't cloned successfully
                let mut new_table = ManuallyDrop::new(
                    match Self::raw_new_uinit(
                        self.table.allocator.clone(),
                        self.buckets(),
                        Fallibility::Infallible,
                    ) {
                        Ok(table) => table,
                        Err(_) => {
                            // would've panicked already
                            unreachable_unchecked();
                        }
                    },
                );
                new_table.clone_from_impl(self, |new_table| {
                    // on panic, free buckets to avoid memory leaks
                    new_table.free_buckets();
                });
                // now take the value out of this magical container
                ManuallyDrop::into_inner(new_table)
            }
        }
    }
}

impl<T, A: Allocator + Clone> Drop for RawConsumingIterator<T, A> {
    fn drop(&mut self) {
        unsafe {
            // drop_in_place all the elements
            self.iter.drop_elements();

            // free the table (deallocate)
            if let Some((start_of_allocation, layout)) = self.allocation {
                self.allocator.deallocate(start_of_allocation, layout)
            }
        }
    }
}

impl<T, A: Allocator + Clone> IntoIterator for RawTable<T, A> {
    type Item = T;
    type IntoIter = RawConsumingIterator<T, A>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let iter = self.iter();
            self.into_iter_from(iter)
        }
    }
}

pub struct RawDrain<'a, T, A: Allocator + Clone = Global> {
    /// the iterator
    iter: RawIter<T>,
    /// keep the iterator here to avoid deallocating memory
    table: ManuallyDrop<RawTable<T, A>>,
    /// the actual table
    src_table: NonNull<RawTable<T, A>>,
    // make rawdrain covariant
    _marker: PhantomData<&'a RawTable<T, A>>,
}

impl<T, A: Allocator + Clone> RawDrain<'_, T, A> {
    pub fn iter(&self) -> RawIter<T> {
        self.iter.clone()
    }
}

impl<T, A: Allocator + Clone> Iterator for RawDrain<'_, T, A> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        unsafe {
            let item = self.iter.next()?;
            Some(item.read())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T, A: Allocator + Clone> ExactSizeIterator for RawDrain<'_, T, A> {}
impl<T, A: Allocator + Clone> FusedIterator for RawDrain<'_, T, A> {}

impl<T, A: Allocator + Clone> Drop for RawDrain<'_, T, A> {
    fn drop(&mut self) {
        unsafe {
            // Drop all remaining elements
            self.iter.drop_elements();

            // Reset the control bytes
            self.table.clear_no_drop();

            // Move the table back to where it was
            self.src_table
                .as_ptr()
                .copy_from_nonoverlapping(&*self.table, 1);
        }
    }
}

pub struct RawHashIter<'a, T, A: Allocator + Clone = Global> {
    inner: RawIterHashInner<'a, A>,
    _marker: PhantomData<T>,
}

struct RawIterHashInner<'a, A: Allocator + Clone> {
    /// The actual table
    table: &'a LowTable<A>,
    /// The top 7 bits of the hash
    h2_hash: u8,
    /// The triangular probe sequence
    probe_seq: ProbeSequence,
    /// The group
    group: Group,
    /// The elements within a group matching a given h2 hash
    bitmask: BitmaskIterator,
}

impl<'a, A: Allocator + Clone> RawIterHashInner<'a, A> {
    fn new(table: &'a LowTable<A>, hash: u64) -> Self {
        unsafe {
            let h2_hash = h2(hash);
            let probe_seq = table.probe_seq(hash);
            let group = Group::load_unaligned(table.ctrlof(probe_seq.pos));
            let bitmask = group.match_byte(h2_hash).into_iter();

            Self {
                table,
                h2_hash,
                probe_seq,
                group,
                bitmask,
            }
        }
    }
}

impl<'a, A: Allocator + Clone> Iterator for RawIterHashInner<'a, A> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            loop {
                if let Some(bit) = self.bitmask.next() {
                    let index = (self.probe_seq.pos + bit) & self.table.bucket_mask;
                    return Some(index);
                }
                if likely(self.group.match_empty().any_bit_set()) {
                    return None;
                }
                self.probe_seq.move_to_next(self.table.bucket_mask);
                self.group = Group::load_unaligned(self.table.ctrlof(self.probe_seq.pos));
                self.bitmask = self.group.match_byte(self.h2_hash).into_iter();
            }
        }
    }
}

impl<'a, T, A: Allocator + Clone> RawHashIter<'a, T, A> {
    fn new(table: &'a RawTable<T, A>, hash: u64) -> Self {
        RawHashIter {
            inner: RawIterHashInner::new(&table.table, hash),
            _marker: PhantomData,
        }
    }
}

impl<'a, T, A: Allocator + Clone> Iterator for RawHashIter<'a, T, A> {
    type Item = Bucket<T>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            match self.inner.next() {
                Some(idx) => Some(self.inner.table.get_bucket(idx)),
                None => None,
            }
        }
    }
}

// now that we've implemented the iterators, we can impl the lookup methods like get and get_mut

impl<T, A: Allocator + Clone> RawTable<T, A> {
    fn iter_hash(&self, hash: u64) -> RawHashIter<'_, T, A> {
        RawHashIter::new(&self, hash)
    }
    pub fn find(&self, hash: u64, mut predicate: impl FnMut(&T) -> bool) -> Option<Bucket<T>> {
        for bucket in self.iter_hash(hash) {
            unsafe {
                let candidate_bucket = bucket.as_ref();
                if likely(predicate(candidate_bucket)) {
                    return Some(bucket);
                }
            }
        }
        // probe over, nothing found
        None
    }
    /// Find and erase an element from the table
    pub fn erase_entry(&mut self, hash: u64, predicate: impl FnMut(&T) -> bool) -> bool {
        if let Some(bucket) = self.find(hash, predicate) {
            unsafe { self.erase(bucket) }
            true
        } else {
            false
        }
    }
    pub unsafe fn remove(&mut self, item: Bucket<T>) -> T {
        // We first remove the metadata
        self.erase_without_drop(&item);
        // and then read the data from the ptr
        item.read()
    }
    pub fn get(&self, hash: u64, predicate: impl FnMut(&T) -> bool) -> Option<&T> {
        // shut clippy up here because of this: https://github.com/rust-lang/rust/issues/68667
        #[allow(clippy::manual_map)]
        match self.find(hash, predicate) {
            Some(bucket) => Some(unsafe { bucket.as_ref() }),
            None => None,
        }
    }
    pub fn get_mut(&mut self, hash: u64, predicate: impl FnMut(&T) -> bool) -> Option<&mut T> {
        // shut clippy up here because of this: https://github.com/rust-lang/rust/issues/68667
        #[allow(clippy::manual_map)]
        match self.find(hash, predicate) {
            Some(bucket) => Some(unsafe { bucket.as_mut() }),
            None => None,
        }
    }
    pub fn clear(&mut self) {
        let mut slf = ScopeGuard::new(self, |slf| slf.clear_no_drop());
        unsafe {
            // deref coercion glue
            slf.drop_elements();
        }
    }
}
