/*
 * Created on Sat Jul 17 2021
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

use super::*;

#[test]
fn test_serialize_deserialize_empty() {
    let cmap = Coremap::new();
    let ser = se::serialize_map(&cmap).unwrap();
    let de = de::deserialize_map(ser).unwrap();
    assert!(de.len() == 0);
}

#[test]
fn test_ser_de_few_elements() {
    let cmap = Coremap::new();
    cmap.upsert("sayan".into(), "writes code".into());
    cmap.upsert("supersayan".into(), "writes super code".into());
    let ser = se::serialize_map(&cmap).unwrap();
    let de = de::deserialize_map(ser).unwrap();
    assert!(de.len() == cmap.len());
    assert!(de
        .iter()
        .all(|kv| cmap.get(kv.key()).unwrap().eq(kv.value())));
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
        let ser = se::serialize_map(&cmap).unwrap();
        let de = de::deserialize_map(ser).unwrap();
        assert!(de
            .iter()
            .all(|kv| cmap.get(kv.key()).unwrap().eq(kv.value())));
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
        let mut se = se::serialize_map(&cmap).unwrap();
        // random chop
        se.truncate(124);
        // corrupted
        assert!(de::deserialize_map(se).is_none());
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
        let mut se = se::serialize_map(&cmap).unwrap();
        // random patch
        let patch: Vec<u8> = (0u16..500u16).into_iter().map(|v| (v >> 7) as u8).collect();
        se.extend(patch);
        assert!(de::deserialize_map(se).is_none());
    }
);

#[cfg(target_pointer_width = "32")]
#[test]
#[should_panic]
fn test_runtime_panic_32bit_or_lower() {
    let max = u64::MAX;
    let byte_stream = unsafe { raw_byte_repr(&max).to_owned() };
    let ptr = byte_stream.as_ptr();
    unsafe { de::transmute_len(ptr) };
}

mod interface_tests {
    use super::interface::{create_tree, DIR_KSROOT, DIR_SNAPROOT};
    use crate::corestore::memstore::Memstore;
    use std::fs;
    use std::path::PathBuf;
    #[test]
    fn test_tree() {
        create_tree(&Memstore::new_default()).unwrap();
        let read_ks: Vec<String> = fs::read_dir(DIR_KSROOT)
            .unwrap()
            .map(|dir| {
                let v = dir.unwrap().file_name();
                v.to_string_lossy().to_string()
            })
            .collect();
        assert!(read_ks.contains(&"system".to_owned()));
        assert!(read_ks.contains(&"default".to_owned()));
        // just read one level of the snaps dir
        let read_snaps: Vec<String> = fs::read_dir(DIR_SNAPROOT)
            .unwrap()
            .map(|dir| {
                let v = dir.unwrap().file_name();
                v.to_string_lossy().to_string()
            })
            .collect();
        assert_eq!(read_snaps, Vec::<String>::new());
        assert!(PathBuf::from("data/backups").is_dir());
    }
}

mod preload_tests {
    use super::*;
    use crate::corestore::memstore::Memstore;
    #[test]
    fn test_preload() {
        let memstore = Memstore::new_default();
        let mut v = Vec::new();
        preload::raw_generate_preload(&mut v, &memstore).unwrap();
        let de: Vec<String> = preload::read_preload_raw(v)
            .unwrap()
            .into_iter()
            .map(|each| unsafe { each.as_str().to_owned() })
            .collect();
        assert_veceq!(de, vec!["default".to_owned(), "system".to_owned()]);
    }
}

mod bytemark_set_tests {
    use super::*;
    use crate::corestore::memstore::{Keyspace, ObjectID};
    use crate::corestore::table::Table;
    use std::collections::HashMap;
    #[test]
    fn test_bytemark_for_nonvolatile() {
        let ks = Keyspace::empty_default();
        let mut v = Vec::new();
        se::raw_serialize_partmap(&mut v, &ks).unwrap();
        let ret: HashMap<ObjectID, (u8, u8)> = de::deserialize_set_ctype_bytemark(&v).unwrap();
        let mut expected = HashMap::new();
        unsafe {
            expected.insert(
                ObjectID::from_slice("default"),
                (
                    bytemarks::BYTEMARK_STORAGE_PERSISTENT,
                    bytemarks::BYTEMARK_MODEL_KV_BIN_BIN,
                ),
            );
        }
        assert_hmeq!(expected, ret);
    }
    #[test]
    fn test_bytemark_volatility_mixed() {
        let ks = Keyspace::empty();
        unsafe {
            ks.create_table(
                ObjectID::from_slice("cache"),
                Table::new_kve_with_volatile(true),
            );
            ks.create_table(
                ObjectID::from_slice("supersafe"),
                Table::new_kve_with_volatile(false),
            );
        }
        let mut v = Vec::new();
        se::raw_serialize_partmap(&mut v, &ks).unwrap();
        let ret: HashMap<ObjectID, (u8, u8)> = de::deserialize_set_ctype_bytemark(&v).unwrap();
        let mut expected = HashMap::new();
        unsafe {
            // our cache is volatile
            expected.insert(
                ObjectID::from_slice("cache"),
                (
                    bytemarks::BYTEMARK_STORAGE_VOLATILE,
                    bytemarks::BYTEMARK_MODEL_KV_BIN_BIN,
                ),
            );
            // our supersafe is non volatile
            expected.insert(
                ObjectID::from_slice("supersafe"),
                (
                    bytemarks::BYTEMARK_STORAGE_PERSISTENT,
                    bytemarks::BYTEMARK_MODEL_KV_BIN_BIN,
                ),
            );
        }
        assert_hmeq!(expected, ret);
    }
}

mod flush_routines {
    use crate::corestore::memstore::Keyspace;
    use crate::corestore::memstore::ObjectID;
    use crate::corestore::table::Table;
    use crate::corestore::Data;
    use std::fs;
    #[test]
    fn test_flush_unflush_table() {
        let tbl = Table::new_default_kve();
        tbl.get_kvstore()
            .unwrap()
            .set("hello".into(), "world".into())
            .unwrap();
        let tblid = unsafe { ObjectID::from_slice("mytbl1") };
        let ksid = unsafe { ObjectID::from_slice("myks1") };
        // create the temp dir for this test
        fs::create_dir_all("data/ks/myks1").unwrap();
        super::flush::oneshot::flush_table(&tblid, &ksid, &tbl).unwrap();
        // now that it's flushed, let's read the table using and unflush routine
        let ret = super::unflush::read_table(&ksid, &tblid, false, 0).unwrap();
        assert_eq!(
            ret.get_kvstore()
                .unwrap()
                .get(&Data::from("hello"))
                .unwrap()
                .unwrap()
                .clone(),
            Data::from("world")
        );
    }
    #[test]
    fn test_flush_unflush_keyspace() {
        // create the temp dir for this test
        fs::create_dir_all("data/ks/myks_1").unwrap();
        let ksid = unsafe { ObjectID::from_slice("myks_1") };
        let tbl1 = unsafe { ObjectID::from_slice("mytbl_1") };
        let tbl2 = unsafe { ObjectID::from_slice("mytbl_2") };
        let ks = Keyspace::empty();
        // a persistent table
        let mytbl = Table::new_default_kve();
        mytbl
            .get_kvstore()
            .unwrap()
            .set("hello".into(), "world".into())
            .unwrap();
        ks.create_table(tbl1.clone(), mytbl);
        // and a volatile table
        ks.create_table(tbl2.clone(), Table::new_kve_with_volatile(true));
        super::flush::flush_keyspace_full(&ksid, &ks).unwrap();
        let ret = super::unflush::read_keyspace(&ksid).unwrap();
        let tbl1_ret = ret.get(&tbl1).unwrap();
        let tbl2_ret = ret.get(&tbl2).unwrap();
        assert_eq!(
            tbl1_ret
                .get_kvstore()
                .unwrap()
                .get(&Data::from("hello"))
                .unwrap()
                .unwrap()
                .clone(),
            Data::from("world")
        );
        assert!(tbl2_ret.get_kvstore().unwrap().len() == 0);
    }
}

mod list_tests {
    use super::iter::RawSliceIter;
    use super::{de, se};
    use crate::corestore::{htable::Coremap, Data};
    #[test]
    fn test_list_se_de() {
        let mylist = vec![Data::from("a"), Data::from("b"), Data::from("c")];
        let mut v = Vec::new();
        se::raw_serialize_nested_list(&mut v, &mylist).unwrap();
        let mut rawiter = RawSliceIter::new(&v);
        let de = { de::deserialize_nested_list(rawiter.get_borrowed_iter()).unwrap() };
        assert_eq!(de, mylist);
    }
    #[test]
    fn test_list_se_de_with_empty_element() {
        let mylist = vec![
            Data::from("a"),
            Data::from("b"),
            Data::from("c"),
            Data::from(""),
        ];
        let mut v = Vec::new();
        se::raw_serialize_nested_list(&mut v, &mylist).unwrap();
        let mut rawiter = RawSliceIter::new(&v);
        let de = { de::deserialize_nested_list(rawiter.get_borrowed_iter()).unwrap() };
        assert_eq!(de, mylist);
    }
    #[test]
    fn test_empty_list_se_de() {
        let mylist: Vec<Data> = vec![];
        let mut v = Vec::new();
        se::raw_serialize_nested_list(&mut v, &mylist).unwrap();
        let mut rawiter = RawSliceIter::new(&v);
        let de = { de::deserialize_nested_list(rawiter.get_borrowed_iter()).unwrap() };
        assert_eq!(de, mylist);
    }
    #[test]
    fn test_list_map_monoelement_se_de() {
        let mymap = Coremap::new();
        let vals = vec!["apples", "bananas", "carrots"];
        mymap.true_if_insert(Data::from("mykey"), vals.clone());
        let mut v = Vec::new();
        se::raw_serialize_list_map(&mut v, &mymap).unwrap();
        let de = de::deserialize_list_map(&v).unwrap();
        assert_eq!(de.len(), 1);
        assert_eq!(
            de.get("mykey".as_bytes()).unwrap().value().clone(),
            vals.into_iter().map(Data::from).collect::<Vec<Data>>()
        );
    }
    #[test]
    fn test_list_map_se_de() {
        let mymap = Coremap::new();
        let key1: Data = "mykey1".into();
        let val1 = vec!["apples", "bananas", "carrots"];
        let key2: Data = "mykey2long".into();
        let val2 = vec!["code", "coffee", "cats"];
        mymap.true_if_insert(key1.clone(), val1.clone());
        mymap.true_if_insert(key2.clone(), val2.clone());
        let mut v = Vec::new();
        se::raw_serialize_list_map(&mut v, &mymap).unwrap();
        let de = de::deserialize_list_map(&v).unwrap();
        assert_eq!(de.len(), 2);
        assert_eq!(
            de.get(&key1).unwrap().value().clone(),
            val1.into_iter().map(Data::from).collect::<Vec<Data>>()
        );
        assert_eq!(
            de.get(&key2).unwrap().value().clone(),
            val2.into_iter().map(Data::from).collect::<Vec<Data>>()
        );
    }
    #[test]
    fn test_list_map_empty_se_de() {
        let mymap: Coremap<Data, Vec<Data>> = Coremap::new();
        let mut v = Vec::new();
        se::raw_serialize_list_map(&mut v, &mymap).unwrap();
        let de = de::deserialize_list_map(&v).unwrap();
        assert_eq!(de.len(), 0)
    }
}
