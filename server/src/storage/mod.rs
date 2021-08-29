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
//! All sizes are stored in little endian. How everything else is stored is not worth
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
use core::ptr;
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
/// DISCLAIMER: THIS FUNCTION CAN DO TERRIBLE THINGS
unsafe fn raw_byte_repr<'a, T: 'a>(len: &'a T) -> &'a [u8] {
    {
        let ptr: *const u8 = mem::transmute(len);
        slice::from_raw_parts::<'a>(ptr, mem::size_of::<T>())
    }
}

mod se {
    use super::*;
    use crate::corestore::memstore::Keyspace;
    use crate::IoResult;

    macro_rules! unsafe_sz_byte_repr {
        ($e:expr) => {
            raw_byte_repr(&to_64bit_little_endian!($e))
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
            w.write_all(raw_byte_repr(&to_64bit_little_endian!(map.len())))?;
            // now the keys and values
            for kv in map.iter() {
                let (k, v) = (kv.key(), kv.value());
                w.write_all(raw_byte_repr(&to_64bit_little_endian!(k.len())))?;
                w.write_all(raw_byte_repr(&to_64bit_little_endian!(v.len())))?;
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
            w.write_all(raw_byte_repr(&to_64bit_little_endian!(map.len())))?;
            // now the keys and values
            for kv in map.iter() {
                let key = kv.key().as_ref();
                w.write_all(raw_byte_repr(&to_64bit_little_endian!(key.len())))?;
                w.write_all(key)?;
            }
        }
        Ok(())
    }

    /// Generate a partition map for the given keyspace
    /// ```text
    /// [8B: EXTENT]([8B: LEN][?B: PARTITION ID][1B: Storage type][1B: Model type])*
    /// ```
    pub fn raw_serialize_partmap<W: Write>(w: &mut W, keyspace: &Keyspace) -> IoResult<()> {
        unsafe {
            // extent
            w.write_all(raw_byte_repr(&to_64bit_little_endian!(keyspace
                .tables
                .len())))?;
            for table in keyspace.tables.iter() {
                // partition ID len
                w.write_all(raw_byte_repr(&to_64bit_little_endian!(table.key().len())))?;
                // parition ID
                w.write_all(table.key())?;
                // now storage type
                w.write_all(raw_byte_repr(&table.storage_type()))?;
                // now model type
                w.write_all(raw_byte_repr(&table.get_model_code()))?;
            }
        }
        Ok(())
    }
    pub fn raw_serialize_list_map<'a, W, T: 'a, U: 'a>(
        w: &mut W,
        data: Coremap<Data, T>,
    ) -> IoResult<()>
    where
        W: Write,
        T: AsRef<[&'a U]>,
        U: AsRef<[u8]>,
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
                let v = key.value().as_ref();
                // write the key extent
                w.write_all(unsafe_sz_byte_repr!(k.len()))?;
                // write the key
                w.write_all(k)?;
                // write the list payload
                self::raw_serialize_nested_list(w, v)?;
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
        T: AsRef<[&'a U]>,
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
    use super::*;
    use std::collections::HashMap;

    pub trait DeserializeFrom {
        fn is_expected_len(clen: usize) -> bool;
        fn from_slice(slice: &[u8]) -> Self;
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
        // First read the length header
        if data.len() < 8 {
            // so the file doesn't even have the length header? noice, just return
            None
        } else {
            unsafe {
                // so we have 8B. Just unsafe access and transmute it
                let len = transmute_len(data.as_ptr());
                let mut set = HashSet::with_capacity(len);
                // this is what we have left: [KLEN:8B]*
                // move 8 bytes ahead since we're done with len
                let mut ptr = data.as_ptr().add(8);
                let end_ptr = data.as_ptr().add(data.len());
                for _ in 0..len {
                    if (ptr.add(8)) >= end_ptr {
                        // not enough space and even if there is a len
                        // there is no value. This is even true for ZSTs
                        return None;
                    }
                    let lenkey = transmute_len(ptr);
                    ptr = ptr.add(8);
                    if (ptr.add(lenkey)) > end_ptr {
                        // not enough data left
                        return None;
                    }
                    if !T::is_expected_len(lenkey) {
                        return None;
                    }
                    // get the key as a raw slice, we've already checked if end_ptr is less
                    let key = T::from_slice(slice::from_raw_parts(ptr, lenkey));
                    // move the ptr ahead; done with the key
                    ptr = ptr.add(lenkey);
                    // push it in
                    if !set.insert(key) {
                        // repeat?; that's not what we wanted
                        return None;
                    }
                }
                if ptr == end_ptr {
                    Some(set)
                } else {
                    // nope, someone gave us more data
                    None
                }
            }
        }
    }

    /// Deserializes a map-like set which has an 2x1B _bytemark_ for every entry
    pub fn deserialize_set_ctype_bytemark<T>(data: &[u8]) -> Option<HashMap<T, (u8, u8)>>
    where
        T: DeserializeFrom + Eq + Hash,
    {
        // First read the length header
        if data.len() < 8 {
            // so the file doesn't even have the length header? noice, just return
            None
        } else {
            unsafe {
                // so we have 8B. Just unsafe access and transmute it
                let len = transmute_len(data.as_ptr());
                let mut set = HashMap::with_capacity(len);
                // this is what we have left: [KLEN:8B]*
                // move 8 bytes ahead since we're done with len
                let mut ptr = data.as_ptr().add(8);
                let end_ptr = data.as_ptr().add(data.len());
                for _ in 0..len {
                    if (ptr.add(8)) >= end_ptr {
                        // not enough space and even if there is a len
                        // there is no value. This is even true for ZSTs
                        return None;
                    }
                    let lenkey = transmute_len(ptr);
                    ptr = ptr.add(8);
                    if (ptr.add(lenkey + 2)) > end_ptr {
                        // not enough data left
                        return None;
                    }
                    if !T::is_expected_len(lenkey) {
                        return None;
                    }
                    // get the key as a raw slice, we've already checked if end_ptr is less
                    let key = T::from_slice(slice::from_raw_parts(ptr, lenkey));
                    // move the ptr ahead; done with the key
                    ptr = ptr.add(lenkey);
                    let bytemark_a = ptr::read(ptr);
                    ptr = ptr.add(1);
                    let bytemark_b = ptr::read(ptr);
                    ptr = ptr.add(1);
                    // push it in
                    if set.insert(key, (bytemark_a, bytemark_b)).is_some() {
                        // repeat?; that's not what we wanted
                        return None;
                    }
                }
                if ptr == end_ptr {
                    Some(set)
                } else {
                    // nope, someone gave us more data
                    None
                }
            }
        }
    }
    /// Deserialize a file that contains a serialized map. This also returns the model code
    pub fn deserialize_map(data: Vec<u8>) -> Option<Coremap<Data, Data>> {
        // First read the length header
        if data.len() < 8 {
            // so the file doesn't even have the length/model header? noice, just return
            None
        } else {
            unsafe {
                /*
                 UNSAFE(@ohsayan): Everything done here is unsafely safe. We
                 reinterpret bits of one type as another. What could be worse?
                 nah, it's not that bad. We know that the byte representations
                 would be in the way we expect. If the data is corrupted, we
                 can guarantee that we won't ever read incorrect lengths of data
                 and we won't read into others' memory (or corrupt our own)
                */
                let mut ptr = data.as_ptr();
                // so we have 8B. Just unsafe access and transmute it; nobody cares
                let len = transmute_len(ptr);
                // move 8 bytes ahead since we're done with len
                ptr = ptr.add(8);
                let hm = Coremap::with_capacity(len);
                // this is what we have left: [KLEN:8B][VLEN:8B]
                let end_ptr = data.as_ptr().add(data.len());
                for _ in 0..len {
                    if (ptr.add(16)) >= end_ptr {
                        // not enough space
                        return None;
                    }
                    let lenkey = transmute_len(ptr);
                    ptr = ptr.add(8);
                    let lenval = transmute_len(ptr);
                    ptr = ptr.add(8);
                    if (ptr.add(lenkey + lenval)) > end_ptr {
                        // not enough data left
                        return None;
                    }
                    // get the key as a raw slice, we've already checked if end_ptr is less
                    let key = Data::copy_from_slice(slice::from_raw_parts(ptr, lenkey));
                    // move the ptr ahead; done with the key
                    ptr = ptr.add(lenkey);
                    let val = Data::copy_from_slice(slice::from_raw_parts(ptr, lenval));
                    // move the ptr ahead; done with the value
                    ptr = ptr.add(lenval);
                    // push it in
                    hm.upsert(key, val);
                }
                if ptr == end_ptr {
                    Some(hm)
                } else {
                    // nope, someone gave us more data
                    None
                }
            }
        }
    }

    pub fn deserialize_list_map(bytes: &[u8]) -> Option<Coremap<Data, Vec<Data>>> {
        if bytes.len() < 8 {
            // 8B extent not here
            return None;
        }
        // now let's read in the extent
        unsafe {
            let mut ptr = bytes.as_ptr();
            let end_ptr = ptr.add(bytes.len());
            // get the len
            let len = transmute_len(ptr);
            // move ptr ahead by sizeof offset
            ptr = ptr.offset(8);
            // allocate a map
            let map = Coremap::with_capacity(len);
            // now enter a loop
            for _ in 0..len {
                if ptr.add(16) >= end_ptr {
                    return None;
                }
                let keylen = transmute_len(ptr);
                ptr = ptr.offset(8);
                if ptr.add(keylen) >= end_ptr {
                    return None;
                }
                // get key
                let key = Data::copy_from_slice(slice::from_raw_parts(ptr, keylen));
                // move ptr ahead
                ptr = ptr.add(keylen);
                let (nptr, list) = self::deserialize_nested_list(ptr, end_ptr)?;
                // update pointers
                ptr = nptr;
                // push it in
                map.true_if_insert(key, list);
            }
            if ptr == end_ptr {
                Some(map)
            } else {
                // someone returned more data
                None
            }
        }
    }

    /// Deserialize a nested list: `[EXTENT]([EL_EXT][EL])*`
    ///
    /// ## Safety
    ///
    /// This is unsafe because it doesn't verify the validity of the source ptr and end_ptr
    pub unsafe fn deserialize_nested_list(
        mut ptr: *const u8,
        end_ptr: *const u8,
    ) -> Option<(*const u8, Vec<Data>)> {
        if ptr.add(8) >= end_ptr {
            // size of list payload is missing
            return None;
        }
        // get list payload len
        let list_payload_extent = transmute_len(ptr);
        // move ptr ahead
        ptr = ptr.offset(8);
        let mut list = Vec::with_capacity(list_payload_extent);
        for _ in 0..list_payload_extent {
            // get element size
            if ptr.add(8) >= end_ptr {
                // size of list element is missing
                return None;
            }
            let list_element_payload_size = transmute_len(ptr);
            // move ptr ahead
            ptr = ptr.offset(8);

            // now get element
            if ptr.add(list_element_payload_size) >= end_ptr {
                // reached end of allocation without getting element
                return None;
            }
            let element =
                Data::copy_from_slice(slice::from_raw_parts(ptr, list_element_payload_size));
            list.push(element);
        }
        Some((ptr, list))
    }

    #[allow(clippy::needless_return)] // Clippy really misunderstands this
    pub(super) unsafe fn transmute_len(start_ptr: *const u8) -> usize {
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
}
