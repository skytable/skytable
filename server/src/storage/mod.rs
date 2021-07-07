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

use crate::coredb::htable::Coremap;
use crate::coredb::Data;
use core::mem;
use core::slice;
use std::io::Write;

/// Get the raw bytes of an unsigned 64-bit integer
fn write_raw_bytes<'a, T: 'a, W: Write>(len: &T, writer: &mut W) -> std::io::Result<()> {
    /*
     We get the raw byte representation which is quite fast and at the same time
     _defined_ because all sizes are casted to unsigned 64-bit integers.
    */
    unsafe {
        let ptr: *const u8 = mem::transmute(len);
        writer.write_all(slice::from_raw_parts::<'a>(ptr, mem::size_of::<u64>()))?;
    }
    Ok(())
}

/// Serialize a map into a _writable_ thing
pub fn serialize_map(map: &Coremap<Data, Data>) -> Result<Vec<u8>, std::io::Error> {
    /*
    [LEN:8B][KLEN:8B|VLEN:8B][K][V][KLEN:8B][VLEN:8B]...
    */
    // write the len header first
    let mut w = Vec::with_capacity(128);
    write_raw_bytes(&(map.len() as u64), &mut w)?;
    // now the keys and values
    for kv in map.iter() {
        let (k, v) = (kv.key(), kv.value());
        let (klen, vlen) = (k.len(), v.len());
        write_raw_bytes(&(klen as u64), &mut w)?;
        write_raw_bytes(&(vlen as u64), &mut w)?;
        w.write_all(k)?;
        w.write_all(v)?;
    }
    w.flush()?;
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
             can gurantee that we won't ever read incorrect lengths of data
             and we won't read into others' memory (or corrupt our own)
            */
            if data.len() - 8 == 0 {
                // empty
                return Some(Coremap::new());
            }

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
    /*
    The start_ptr can possibly never be aligned so it's better (instead of relying on the
    processor) to do unaligned reads from the start_ptr
    */
    let y: [u8; 8] = [
        start_ptr.read_unaligned(),
        start_ptr.add(1).read_unaligned(),
        start_ptr.add(2).read_unaligned(),
        start_ptr.add(3).read_unaligned(),
        start_ptr.add(4).read_unaligned(),
        start_ptr.add(5).read_unaligned(),
        start_ptr.add(6).read_unaligned(),
        start_ptr.add(7).read_unaligned(),
    ];
    /*
    Transmutation is safe here because we already know the exact sizes
    */
    #[cfg(target_pointer_width = "32")]
    return {
        // zero the higher bits on 32-bit
        let ret1: u64 = mem::transmute(y);
        ret1 as usize
    };
    #[cfg(target_pointer_width = "64")]
    return {
        {
            // no need for zeroing the bits
            mem::transmute(y)
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
        // this test needs a lot of auxiliary space
        // we can approximate this to be: 100,000 x 30 bytes = 3,000,000 bytes
        // and then we may have a clone overhead + heap allocation by the map
        // so ~9,000,000 bytes or ~9MB
        const COUNT: usize = 100_000_usize;
        const LEN: usize = 30_usize;
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
        // this test needs a lot of auxiliary space
        // we can approximate this to be: 100,000 x 30 bytes = 3,000,000 bytes
        // and then we may have a clone overhead + heap allocation by the map
        // so ~9,000,000 bytes or ~9MB
        const COUNT: usize = 100_000_usize;
        const LEN: usize = 30_usize;
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
        se.truncate(12409);
        // corrupted
        assert!(deserialize(se).is_none());
    }
    #[test]
    fn test_ser_de_excess_bytes() {
        // this test needs a lot of auxiliary space
        // we can approximate this to be: 100,000 x 30 bytes = 3,000,000 bytes
        // and then we may have a clone overhead + heap allocation by the map
        // so ~9,000,000 bytes or ~9MB
        const COUNT: usize = 100_000_usize;
        const LEN: usize = 30_usize;
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
