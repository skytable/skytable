/*
 * Created on Wed Jul 07 2021
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

/*
 Encoding and decoding tested on 32-bit/64-bit Little Endian (Intel x86) and Big Endian
 (MIPS). Also tested UB with miri and memory leaks with valgrind
 -- Sayan on July 8, 2021
*/

//! # Storage engine
//!
//! This module contains code to rapidly encode/decode data. All sizes are encoded into unsigned
//! 64-bit integers for compatibility across 16/32/64 bit platforms. This means that a
//! data file generated on a 32-bit machine will work seamlessly on a 64-bit machine
//! and vice versa. Of course, provided that On a 32-bit system, 32 high bits are just zeroed.
//!
//! ## Endianness
//!
//! All sizes are stored in native endian. If a dataset is imported from a system from a different endian, it is
//! simply translated into the host's native endian. How everything else is stored is not worth
//! discussing here. Byte swaps just need one instruction on most architectures
//!
//! ## Safety
//!
//! > Trust me, all methods are bombingly unsafe. They do such crazy things that you might not
//! think of using them anywhere outside. This is a specialized parser built for the database.
//! -- Sayan (July 2021)

use crate::corestore::array::Array;
use crate::corestore::htable::Coremap;
use crate::corestore::Data;
use core::hash::Hash;
use core::mem;
use core::slice;
use std::collections::HashSet;
use std::io::Write;
// for some astronomical reasons do not mess with this
#[macro_use]
mod macros;
// endof do not mess
pub mod bytemarks;
pub mod flush;
pub mod interface;
pub mod iter;
pub mod preload;
pub mod sengine;
pub mod unflush;
// test
#[cfg(test)]
mod tests;

/*
    Endian and pointer "appendix":
    We assume a fixed size of 1 for all the cases. All sizes don't hit over isize::MAX as
    guaranteed by our allocation methods. Also, 32-bit to 64-bit and vice versa aren't
    worth discussing here (irrespective of endianness). That's because all sizes are stored
    as unsigned 64-bit integers. So, get the correct byte order, raw cast to u64, down cast
    or up cast as required by the target's pointer width.

    Current limitations:
    - 32-bit is compatible with 64-bit
    - 16-bit is compatible with 64-bit
    - But 64-bit may not be compatible with 32/16 bit due to the difference in sizes

    ------------------------------------------------------
    Appendix I: Same endian, different pointer width (R/W)
    ------------------------------------------------------
    (1) Little endian on little endian (64-bit)
    (A) Writing
    In-memory size: [0, 0, 0, 0, 0, 0, 0, 1] =(u64)> [0, 0, 0, 0, 0, 0, 0, 1] (no op)
    We write to file: [0, 0, 0, 0, 0, 0, 0, 1]
    (B) Reading
    This is read: [0, 0, 0, 0, 0, 0, 0, 1]
    Raw cast =(usize)> [0, 0, 0, 0, 0, 0, 0, 1] (one memcpy)

    (2) Little endian on little endian (32-bit)
    (A) Writing
    In-memory size: [0, 0, 0, 1] =(u64)> [0, 0, 0, 0, 0, 0, 0, 1] (up cast)
    We write to file: [0, 0, 0, 0, 0, 0, 0, 1]
    (B) Reading
    This is read: [0, 0, 0, 0, 0, 0, 0, 1]
    Raw cast =(u64)> [0, 0, 0, 0, 0, 0, 0, 1] (one memcpy)
    Lossy cast =(usize)> [0, 0, 0, 1]

    (3) Big endian on big endian (64-bit)
    (A) Writing
    In-memory size: [1, 0, 0, 0, 0, 0, 0, 0] =(u64)> [1, 0, 0, 0, 0, 0, 0, 0] (no op)
    We write to file: [1, 0, 0, 0, 0, 0, 0, 0]
    (B) Reading
    This is read: [1, 0, 0, 0, 0, 0, 0, 0]
    Raw cast =(usize)> [1, 0, 0, 0, 0, 0, 0, 0] (one memcpy)

    (4) Big endian (64-bit) on big endian (32-bit)
    (A) Writing
    In-memory size: [1, 0, 0, 0] =(u64)> [1, 0, 0, 0, 0, 0, 0, 0] (up cast)
    We write to file: [1, 0, 0, 0, 0, 0, 0, 0]
    (B) Reading
    This is read: [1, 0, 0, 0, 0, 0, 0, 0]
    Raw cast =(u64)> [1, 0, 0, 0, 0, 0, 0, 0] (one memcpy)
    Lossy cast =(usize)> [1, 0, 0, 0]

    ------------------------------------------------------
    Appendix II: Different endian, same pointer width (R/W)
    ------------------------------------------------------
    (1) Little endian on big endian (64-bit)
    (A) Writing
    ^^ See Appendix I/1/A
    (B) Reading
    This is read: [0, 0, 0, 0, 0, 0, 0, 1]
    Raw cast =(u64)> [0, 0, 0, 0, 0, 0, 0, 1] (one memcpy)
    Reverse the bits: [1, 0, 0, 0, 0, 0, 0, 0] (constant time ptr swap)
    Cast =(usize)> [1, 0, 0, 0, 0, 0, 0, 0] (no op)

    (2) Big endian on little endian (64-bit)
    (A) Writing
    ^^ See Appendix I/3/A
    (B) Reading
    This is read: [1, 0, 0, 0, 0, 0, 0, 0]
    Raw cast =(u64)> [1, 0, 0, 0, 0, 0, 0, 0] (one memcpy)
    Reverse the bits: [0, 0, 0, 0, 0, 0, 0, 1] (constant time ptr swap)
    Cast =(usize)> [0, 0, 0, 0, 0, 0, 0, 1] (no op)

    (3) Little endian on big endian (32-bit)
    (A) Writing
    ^^ See Appendix I/2/A
    (B) Reading
    This is read: [0, 0, 0, 0, 0, 0, 0, 1]
    Raw cast =(u64)> [0, 0, 0, 0, 0, 0, 0, 1] (one memcpy)
    Reverse the bits: [1, 0, 0, 0, 0, 0, 0, 0] (constant time ptr swap)
    Lossy cast =(usize)> [1, 0, 0, 0]

    (4) Big endian on little endian (32-bit)
    (A) Writing
    ^^ See Appendix I/4/A
    (B) Reading
    This is read: [1, 0, 0, 0, 0, 0, 0, 0]
    Raw cast =(u64)> [1, 0, 0, 0, 0, 0, 0, 0] (one memcpy)
    Reverse the bits: [0, 0, 0, 0, 0, 0, 0, 1] (constant time ptr swap)
    Lossy cast =(usize)> [0, 0, 0, 1]

    ------------------------------------------------------
    Appendix III: Warnings
    ------------------------------------------------------
    (1) Gotchas on 32-bit big endian
    (A) While writing
    Do not swap bytes before up cast
    (B) While reading
    Do not down cast before swapping bytes
*/

/// Get the raw bytes of anything.
///
/// DISCLAIMER: THIS FUNCTION CAN DO TERRIBLE THINGS (especially when you think about padding)
unsafe fn raw_byte_repr<'a, T: 'a>(len: &'a T) -> &'a [u8] {
    {
        let ptr: *const u8 = mem::transmute(len);
        slice::from_raw_parts::<'a>(ptr, mem::size_of::<T>())
    }
}

mod se {
    use super::*;
    use crate::kvengine::listmap::LockedVec;
    use crate::storage::flush::FlushableKeyspace;
    use crate::storage::flush::FlushableTable;
    use crate::IoResult;

    macro_rules! unsafe_sz_byte_repr {
        ($e:expr) => {
            raw_byte_repr(&to_64bit_native_endian!($e))
        };
    }

    #[cfg(test)]
    /// Serialize a map into a _writable_ thing
    pub fn serialize_map(map: &Coremap<Data, Data>) -> IoResult<Vec<u8>> {
        /*
        [LEN:8B][KLEN:8B|VLEN:8B][K][V][KLEN:8B][VLEN:8B]...
        */
        // write the len header first
        let mut w = Vec::with_capacity(128);
        self::raw_serialize_map(map, &mut w)?;
        Ok(w)
    }

    /// Serialize a map and write it to a provided buffer
    pub fn raw_serialize_map<W: Write>(map: &Coremap<Data, Data>, w: &mut W) -> IoResult<()> {
        unsafe {
            w.write_all(raw_byte_repr(&to_64bit_native_endian!(map.len())))?;
            // now the keys and values
            for kv in map.iter() {
                let (k, v) = (kv.key(), kv.value());
                w.write_all(raw_byte_repr(&to_64bit_native_endian!(k.len())))?;
                w.write_all(raw_byte_repr(&to_64bit_native_endian!(v.len())))?;
                w.write_all(k)?;
                w.write_all(v)?;
            }
        }
        Ok(())
    }

    /// Serialize a set and write it to a provided buffer
    pub fn raw_serialize_set<W, K, V>(map: &Coremap<K, V>, w: &mut W) -> IoResult<()>
    where
        W: Write,
        K: Eq + Hash + AsRef<[u8]>,
    {
        unsafe {
            w.write_all(raw_byte_repr(&to_64bit_native_endian!(map.len())))?;
            // now the keys and values
            for kv in map.iter() {
                let key = kv.key().as_ref();
                w.write_all(raw_byte_repr(&to_64bit_native_endian!(key.len())))?;
                w.write_all(key)?;
            }
        }
        Ok(())
    }

    /// Generate a partition map for the given keyspace
    /// ```text
    /// [8B: EXTENT]([8B: LEN][?B: PARTITION ID][1B: Storage type][1B: Model type])*
    /// ```
    pub fn raw_serialize_partmap<W: Write, Tbl: FlushableTable, K: FlushableKeyspace<Tbl>>(
        w: &mut W,
        keyspace: &K,
    ) -> IoResult<()> {
        unsafe {
            // extent
            w.write_all(raw_byte_repr(&to_64bit_native_endian!(
                keyspace.table_count()
            )))?;
            for table in keyspace.get_iter() {
                // partition ID len
                w.write_all(raw_byte_repr(&to_64bit_native_endian!(table.key().len())))?;
                // parition ID
                w.write_all(table.key())?;
                // now storage type
                w.write_all(raw_byte_repr(&table.storage_code()))?;
                // now model type
                w.write_all(raw_byte_repr(&table.model_code()))?;
            }
        }
        Ok(())
    }
    pub fn raw_serialize_list_map<W>(data: &Coremap<Data, LockedVec>, w: &mut W) -> IoResult<()>
    where
        W: Write,
    {
        /*
        [8B: Extent]([8B: Key extent][?B: Key][8B: Max index][?B: Payload])*
        */
        unsafe {
            // Extent
            w.write_all(unsafe_sz_byte_repr!(data.len()))?;
            // Enter iter
            '_1: for key in data.iter() {
                // key
                let k = key.key();
                // list payload
                let vread = key.value().read();
                let v: &Vec<Data> = &vread;
                // write the key extent
                w.write_all(unsafe_sz_byte_repr!(k.len()))?;
                // write the key
                w.write_all(k)?;
                // write the list payload
                self::raw_serialize_nested_list(w, &v)?;
            }
        }
        Ok(())
    }
    /// Serialize a `[[u8]]` (i.e a slice of slices)
    pub fn raw_serialize_nested_list<'a, W, T: 'a + ?Sized, U: 'a>(
        w: &mut W,
        inp: &'a T,
    ) -> IoResult<()>
    where
        T: AsRef<[U]>,
        U: AsRef<[u8]>,
        W: Write,
    {
        // ([8B:EL_EXTENT][?B: Payload])*
        let inp = inp.as_ref();
        unsafe {
            // write extent
            w.write_all(unsafe_sz_byte_repr!(inp.len()))?;
            // now enter loop and write elements
            for element in inp.iter() {
                let element = element.as_ref();
                // write element extent
                w.write_all(unsafe_sz_byte_repr!(element.len()))?;
                // write element
                w.write_all(element)?;
            }
        }
        Ok(())
    }
}

mod de {
    use super::iter::{RawSliceIter, RawSliceIterBorrowed};
    use super::{Array, Coremap, Data, Hash, HashSet};
    use crate::kvengine::listmap::LockedVec;
    use core::ptr;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    pub trait DeserializeFrom {
        fn is_expected_len(clen: usize) -> bool;
        fn from_slice(slice: &[u8]) -> Self;
    }

    pub trait DeserializeInto: Sized {
        fn new_empty() -> Self;
        fn from_slice(slice: &[u8]) -> Option<Self>;
    }

    impl DeserializeInto for Coremap<Data, Data> {
        fn new_empty() -> Self {
            Coremap::new()
        }
        fn from_slice(slice: &[u8]) -> Option<Self> {
            self::deserialize_map(slice)
        }
    }

    impl DeserializeInto for Coremap<Data, LockedVec> {
        fn new_empty() -> Self {
            Coremap::new()
        }
        fn from_slice(slice: &[u8]) -> Option<Self> {
            self::deserialize_list_map(slice)
        }
    }

    pub fn deserialize_into<T: DeserializeInto>(input: &[u8]) -> Option<T> {
        T::from_slice(input)
    }

    impl<const N: usize> DeserializeFrom for Array<u8, N> {
        fn is_expected_len(clen: usize) -> bool {
            clen <= N
        }
        fn from_slice(slice: &[u8]) -> Self {
            unsafe { Self::from_slice(slice) }
        }
    }

    /// Deserialize a set to a custom type
    pub fn deserialize_set_ctype<T>(data: &[u8]) -> Option<HashSet<T>>
    where
        T: DeserializeFrom + Eq + Hash,
    {
        let mut rawiter = RawSliceIter::new(data);
        let len = rawiter.next_64bit_integer_to_usize()?;
        let mut set = HashSet::with_capacity(len);
        for _ in 0..len {
            let lenkey = rawiter.next_64bit_integer_to_usize()?;
            if !T::is_expected_len(lenkey) {
                return None;
            }
            // get the key as a raw slice, we've already checked if end_ptr is less
            let key = T::from_slice(rawiter.next_borrowed_slice(lenkey)?);
            // push it in
            if !set.insert(key) {
                // repeat?; that's not what we wanted
                return None;
            }
        }
        if rawiter.end_of_allocation() {
            Some(set)
        } else {
            // nope, someone gave us more data
            None
        }
    }

    /// Deserializes a map-like set which has an 2x1B _bytemark_ for every entry
    pub fn deserialize_set_ctype_bytemark<T>(data: &[u8]) -> Option<HashMap<T, (u8, u8)>>
    where
        T: DeserializeFrom + Eq + Hash,
    {
        let mut rawiter = RawSliceIter::new(data);
        // so we have 8B. Just unsafe access and transmute it
        let len = rawiter.next_64bit_integer_to_usize()?;
        let mut set = HashMap::with_capacity(len);
        for _ in 0..len {
            let lenkey = rawiter.next_64bit_integer_to_usize()?;
            if !T::is_expected_len(lenkey) {
                return None;
            }
            // get the key as a raw slice, we've already checked if end_ptr is less
            let key = T::from_slice(rawiter.next_borrowed_slice(lenkey)?);
            let bytemark_a = rawiter.next_8bit_integer()?;
            let bytemark_b = rawiter.next_8bit_integer()?;
            // push it in
            if set.insert(key, (bytemark_a, bytemark_b)).is_some() {
                // repeat?; that's not what we wanted
                return None;
            }
        }
        if rawiter.end_of_allocation() {
            Some(set)
        } else {
            // nope, someone gave us more data
            None
        }
    }
    /// Deserialize a file that contains a serialized map. This also returns the model code
    pub fn deserialize_map(data: &[u8]) -> Option<Coremap<Data, Data>> {
        let mut rawiter = RawSliceIter::new(data);
        let len = rawiter.next_64bit_integer_to_usize()?;
        let hm = Coremap::with_capacity(len);
        for _ in 0..len {
            let (lenkey, lenval) = rawiter.next_64bit_integer_pair_to_usize()?;
            let key = rawiter.next_owned_data(lenkey)?;
            let val = rawiter.next_owned_data(lenval)?;
            // push it in
            hm.upsert(key, val);
        }
        if rawiter.end_of_allocation() {
            Some(hm)
        } else {
            // nope, someone gave us more data
            None
        }
    }

    pub fn deserialize_list_map(bytes: &[u8]) -> Option<Coremap<Data, LockedVec>> {
        let mut rawiter = RawSliceIter::new(bytes);
        // get the len
        let len = rawiter.next_64bit_integer_to_usize()?;
        // allocate a map
        let map = Coremap::with_capacity(len);
        // now enter a loop
        for _ in 0..len {
            let keylen = rawiter.next_64bit_integer_to_usize()?;
            // get key
            let key = rawiter.next_owned_data(keylen)?;
            let borrowed_iter = rawiter.get_borrowed_iter();
            let list = self::deserialize_nested_list(borrowed_iter)?;
            // push it in
            map.true_if_insert(key, RwLock::new(list));
        }
        if rawiter.end_of_allocation() {
            Some(map)
        } else {
            // someone returned more data
            None
        }
    }

    /// Deserialize a nested list: `[EXTENT]([EL_EXT][EL])*`
    ///
    pub fn deserialize_nested_list(mut iter: RawSliceIterBorrowed<'_>) -> Option<Vec<Data>> {
        // get list payload len
        let list_payload_extent = iter.next_64bit_integer_to_usize()?;
        let mut list = Vec::with_capacity(list_payload_extent);
        for _ in 0..list_payload_extent {
            // get element size
            let list_element_payload_size = iter.next_64bit_integer_to_usize()?;
            // now get element
            let element = iter.next_owned_data(list_element_payload_size)?;
            list.push(element);
        }
        Some(list)
    }

    pub(super) unsafe fn transmute_len(start_ptr: *const u8) -> usize {
        little_endian! {{
            return self::transmute_len_le(start_ptr);
        }};
        big_endian! {{
            return self::transmute_len_be(start_ptr);
        }}
    }

    #[allow(clippy::needless_return)] // Clippy really misunderstands this
    pub(super) unsafe fn transmute_len_le(start_ptr: *const u8) -> usize {
        little_endian!({
            // So we have an LE target
            is_64_bit!({
                // 64-bit LE
                return ptr::read_unaligned(start_ptr.cast());
            });
            not_64_bit!({
                // 32-bit LE
                let ret1: u64 = ptr::read_unaligned(start_ptr.cast());
                // lossy cast
                let ret = ret1 as usize;
                if ret > (isize::MAX as usize) {
                    // this is a backup method for us incase a giant 48-bit address is
                    // somehow forced to be read on this machine
                    panic!("RT panic: Very high size for current pointer width");
                }
                return ret;
            });
        });

        big_endian!({
            // so we have a BE target
            is_64_bit!({
                // 64-bit big endian
                let ret: usize = ptr::read_unaligned(start_ptr.cast());
                // swap byte order
                return ret.swap_bytes();
            });
            not_64_bit!({
                // 32-bit big endian
                let ret: u64 = ptr::read_unaligned(start_ptr.cast());
                // swap byte order and lossy cast
                let ret = (ret.swap_bytes()) as usize;
                // check if overflow
                if ret > (isize::MAX as usize) {
                    // this is a backup method for us incase a giant 48-bit address is
                    // somehow forced to be read on this machine
                    panic!("RT panic: Very high size for current pointer width");
                }
                return ret;
            });
        });
    }

    #[allow(clippy::needless_return)] // Clippy really misunderstands this
    pub(super) unsafe fn transmute_len_be(start_ptr: *const u8) -> usize {
        big_endian!({
            // So we have a BE target
            is_64_bit!({
                // 64-bit BE
                return ptr::read_unaligned(start_ptr.cast());
            });
            not_64_bit!({
                // 32-bit BE
                let ret1: u64 = ptr::read_unaligned(start_ptr.cast());
                // lossy cast
                let ret = ret1 as usize;
                if ret > (isize::MAX as usize) {
                    // this is a backup method for us incase a giant 48-bit address is
                    // somehow forced to be read on this machine
                    panic!("RT panic: Very high size for current pointer width");
                }
                return ret;
            });
        });

        little_endian!({
            // so we have an LE target
            is_64_bit!({
                // 64-bit little endian
                let ret: usize = ptr::read_unaligned(start_ptr.cast());
                // swap byte order
                return ret.swap_bytes();
            });
            not_64_bit!({
                // 32-bit little endian
                let ret: u64 = ptr::read_unaligned(start_ptr.cast());
                // swap byte order and lossy cast
                let ret = (ret.swap_bytes()) as usize;
                // check if overflow
                if ret > (isize::MAX as usize) {
                    // this is a backup method for us incase a giant 48-bit address is
                    // somehow forced to be read on this machine
                    panic!("RT panic: Very high size for current pointer width");
                }
                return ret;
            });
        });
    }
}
