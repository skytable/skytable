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

#![allow(dead_code)] // TODO(@ohsayan): Remove this once we're done

//! # Storage engine
//!
//! This module contains code to rapidly encode/decode data. All sizes are encoded into unsigned
//! 64-bit integers for compatibility across 16/32/64 bit platforms. This means that a
//! data file generated on a 32-bit machine will work seamlessly on a 64-bit machine
//! and vice versa. Of course, provided that On a 32-bit system, 32 high bits are just zeroed.
//!
//! ## Safety
//!
//! Trust me, all methods are bombingly unsafe. They do such crazy things that you might not
//! think of using them anywhere outside. This is a specialized parser built for the database.
//!

/*
 TODO(@ohsayan): Currently the ser/de methods are only little endian compatible and will be
 modified to be endian independent.
*/

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

    Additional context:
    - RBIT doesn't exist on LE architectures like x86 (exclusive of fancy instructions)

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
    Reverse the bits: [1, 0, 0, 0, 0, 0, 0, 0] (constant time ptr swap except for hardware instructions like RBIT)
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
    Reverse the bits: [1, 0, 0, 0, 0, 0, 0, 0] (constant time ptr swap except for hardware instructions like RBIT)
    Lossy cast =(usize)> [1, 0, 0, 0]

    (4) Big endian on little endian (32-bit)
    (A) Writing
    ^^ See Appendix I/4/A
    (B) Reading
    This is read: [1, 0, 0, 0, 0, 0, 0, 0]
    Raw cast =(u64)> [1, 0, 0, 0, 0, 0, 0, 0] (one memcpy)
    Reverse the bits: [0, 0, 0, 0, 0, 0, 0, 1] (constant time ptr swap)
    Lossy cast =(usize)> [0, 0, 0, 1]
*/

use crate::coredb::htable::Coremap;
use crate::coredb::Data;
use core::mem;
use core::ptr;
use core::slice;
use std::io::Write;

/// Get the raw bytes of an unsigned 64-bit integer
unsafe fn raw_len<'a>(len: &'a u64) -> &'a [u8] {
    {
        let ptr: *const u8 = mem::transmute(len);
        slice::from_raw_parts::<'a>(ptr, mem::size_of::<u64>())
    }
}

/// Serialize a map into a _writable_ thing
pub fn serialize_map(map: &Coremap<Data, Data>) -> Result<Vec<u8>, std::io::Error> {
    /*
    [LEN:8B][KLEN:8B|VLEN:8B][K][V][KLEN:8B][VLEN:8B]...
    */
    // write the len header first
    let mut w = Vec::with_capacity(128);
    unsafe {
        w.write_all(raw_len(&(map.len() as u64)))?;
        // now the keys and values
        for kv in map.iter() {
            let (k, v) = (kv.key(), kv.value());
            let (klen, vlen) = (k.len(), v.len());
            w.write_all(raw_len(&(klen as u64)))?;
            w.write_all(raw_len(&(vlen as u64)))?;
            w.write_all(k)?;
            w.write_all(v)?;
        }
    }
    Ok(w)
}

/// Deserialize a file that contains a serialized map
pub fn deserialize(data: Vec<u8>) -> Option<Coremap<Data, Data>> {
    // First read the length header
    if data.len() < 8 {
        // so the file doesn't even have the length header? noice, just return
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

            // so we have 8B. Just unsafe access and transmute it; nobody cares
            let len = transmute_len(data.as_ptr());
            let hm = Coremap::with_capacity(len);
            // this is what we have left: [KLEN:8B][VLEN:8B]
            // move 8 bytes ahead since we're done with len
            let mut ptr = data.as_ptr().add(8);
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
                    // not enough space
                    return None;
                }
                // get the key as a raw slice, we've already checked if end_ptr is less
                let key = Data::from(slice::from_raw_parts(ptr, lenkey));
                // move the ptr ahead; done with the key
                ptr = ptr.add(lenkey);
                let val = Data::from(slice::from_raw_parts(ptr, lenval));
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

unsafe fn transmute_len(start_ptr: *const u8) -> usize {
    // guarantee that all addresses are aligned
    debug_assert!((start_ptr as usize % mem::align_of::<u8>() == 0));
    #[cfg(not(target_pointer_width = "64"))]
    return {
        // zero the higher bits on 32-bit
        let ret1: u64 = ptr::read(start_ptr.cast());
        if ret1 > isize::MAX {
            // this is a backup method for us incase a giant 48-bit address is
            // somehow forced to be read on this machine
            panic!("RT panic: Very high size for current pointer width");
        }
        ret1 as usize
    };
    #[cfg(target_pointer_width = "64")]
    return {
        {
            // no need for zeroing the bits
            ptr::read(start_ptr.cast())
        }
    };
}

#[test]
fn test_serialize_deserialize_empty() {
    let cmap = Coremap::new();
    let ser = serialize_map(&cmap).unwrap();
    let de = deserialize(ser).unwrap();
    assert!(de.len() == 0);
}

#[test]
fn test_ser_de_few_elements() {
    let cmap = Coremap::new();
    cmap.upsert("sayan".into(), "writes code".into());
    cmap.upsert("supersayan".into(), "writes super code".into());
    let ser = serialize_map(&cmap).unwrap();
    let de = deserialize(ser).unwrap();
    assert!(de.len() == cmap.len());
    assert!(de
        .iter()
        .all(|kv| cmap.get(kv.key()).unwrap().eq(&kv.value())));
}

cfg_test!(
    use libstress::utils::generate_random_string_vector;
    use rand::thread_rng;
    #[test]
    fn roast_the_serializer() {
        const COUNT: usize = 1000_usize;
        const LEN: usize = 8_usize;
        let mut rng = thread_rng();
        let (keys, values) = (
            generate_random_string_vector(COUNT, LEN, &mut rng, true),
            generate_random_string_vector(COUNT, LEN, &mut rng, false),
        );
        let cmap: Coremap<Data, Data> = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (Data::from(k.to_owned()), Data::from(v.to_owned())))
            .collect();
        let ser = serialize_map(&cmap).unwrap();
        let de = deserialize(ser).unwrap();
        assert!(de
            .iter()
            .all(|kv| cmap.get(kv.key()).unwrap().eq(&kv.value())));
        assert!(de.len() == cmap.len());
    }

    #[test]
    fn test_ser_de_safety() {
        const COUNT: usize = 1000_usize;
        const LEN: usize = 8_usize;
        let mut rng = thread_rng();
        let (keys, values) = (
            generate_random_string_vector(COUNT, LEN, &mut rng, true),
            generate_random_string_vector(COUNT, LEN, &mut rng, false),
        );
        let cmap: Coremap<Data, Data> = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (Data::from(k.to_owned()), Data::from(v.to_owned())))
            .collect();
        let mut se = serialize_map(&cmap).unwrap();
        // random chop
        se.truncate(124);
        // corrupted
        assert!(deserialize(se).is_none());
    }
    #[test]
    fn test_ser_de_excess_bytes() {
        // this test needs a lot of auxiliary space
        // we can approximate this to be: 100,000 x 30 bytes = 3,000,000 bytes
        // and then we may have a clone overhead + heap allocation by the map
        // so ~9,000,000 bytes or ~9MB
        const COUNT: usize = 1000_usize;
        const LEN: usize = 8_usize;
        let mut rng = thread_rng();
        let (keys, values) = (
            generate_random_string_vector(COUNT, LEN, &mut rng, true),
            generate_random_string_vector(COUNT, LEN, &mut rng, false),
        );
        let cmap: Coremap<Data, Data> = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (Data::from(k.to_owned()), Data::from(v.to_owned())))
            .collect();
        let mut se = serialize_map(&cmap).unwrap();
        // random patch
        let patch: Vec<u8> = (0u16..500u16).into_iter().map(|v| (v >> 7) as u8).collect();
        se.extend(patch);
        assert!(deserialize(se).is_none());
    }
);
