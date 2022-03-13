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
    let de = de::deserialize_map(&ser).unwrap();
    assert!(de.len() == 0);
}

#[test]
fn test_serialize_deserialize_map_with_empty_elements() {
    let cmap = Coremap::new();
    cmap.true_if_insert(Data::from("sayan"), Data::from(""));
    cmap.true_if_insert(Data::from("sayan's second key"), Data::from(""));
    cmap.true_if_insert(Data::from("sayan's third key"), Data::from(""));
    cmap.true_if_insert(Data::from(""), Data::from(""));
    let ser = se::serialize_map(&cmap).unwrap();
    let de = de::deserialize_map(&ser).unwrap();
    assert_eq!(de.len(), cmap.len());
    assert!(cmap.into_iter().all(|(k, v)| de.get(&k).unwrap().eq(&v)));
}

#[test]
fn test_ser_de_few_elements() {
    let cmap = Coremap::new();
    cmap.upsert("sayan".into(), "writes code".into());
    cmap.upsert("supersayan".into(), "writes super code".into());
    let ser = se::serialize_map(&cmap).unwrap();
    let de = de::deserialize_map(&ser).unwrap();
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
        let de = de::deserialize_map(&ser).unwrap();
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
        assert!(de::deserialize_map(&se).is_none());
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
        assert!(de::deserialize_map(&se).is_none());
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
    use super::interface::{create_tree_fresh, DIR_KSROOT, DIR_SNAPROOT};
    use crate::corestore::memstore::Memstore;
    use crate::storage::v1::flush::Autoflush;
    use std::fs;
    use std::path::PathBuf;
    #[test]
    fn test_tree() {
        create_tree_fresh(&Autoflush, &Memstore::new_default()).unwrap();
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
            ks.create_table(
                ObjectID::from_slice("safelist"),
                Table::new_kve_listmap_with_data(Coremap::new(), false, true, true),
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
            expected.insert(
                ObjectID::from_slice("safelist"),
                (
                    bytemarks::BYTEMARK_STORAGE_PERSISTENT,
                    bytemarks::BYTEMARK_MODEL_KV_STR_LIST_STR,
                ),
            );
        }
        assert_hmeq!(expected, ret);
    }
}

mod bytemark_actual_table_restore {
    use crate::corestore::{
        memstore::ObjectID,
        table::{DescribeTable, KVEList, Table, KVE},
        Data,
    };
    use crate::kvengine::LockedVec;
    use crate::storage::v1::{
        flush::{oneshot::flush_table, Autoflush},
        unflush::read_table,
    };

    macro_rules! insert {
        ($table:ident, $k:expr, $v:expr) => {
            assert!(gtable::<KVE>(&$table)
                .set(Data::from($k), Data::from($v))
                .unwrap())
        };
    }

    macro_rules! puthello {
        ($table:ident) => {
            insert!($table, "hello", "world")
        };
    }

    fn gtable<T: DescribeTable>(table: &Table) -> &T::Table {
        T::try_get(table).unwrap()
    }

    use std::fs;
    #[test]
    fn table_restore_bytemark_kve() {
        let default_keyspace = ObjectID::try_from_slice(b"actual_kve_restore").unwrap();
        fs::create_dir_all(format!("data/ks/{}", unsafe { default_keyspace.as_str() })).unwrap();
        let kve_bin_bin_name = ObjectID::try_from_slice(b"bin_bin").unwrap();
        let kve_bin_bin = Table::from_model_code(0, false).unwrap();
        puthello!(kve_bin_bin);
        let kve_bin_str_name = ObjectID::try_from_slice(b"bin_str").unwrap();
        let kve_bin_str = Table::from_model_code(1, false).unwrap();
        puthello!(kve_bin_str);
        let kve_str_str_name = ObjectID::try_from_slice(b"str_str").unwrap();
        let kve_str_str = Table::from_model_code(2, false).unwrap();
        puthello!(kve_str_str);
        let kve_str_bin_name = ObjectID::try_from_slice(b"str_bin").unwrap();
        let kve_str_bin = Table::from_model_code(3, false).unwrap();
        puthello!(kve_str_bin);
        let names: [(&ObjectID, &Table, u8); 4] = [
            (&kve_bin_bin_name, &kve_bin_bin, 0),
            (&kve_bin_str_name, &kve_bin_str, 1),
            (&kve_str_str_name, &kve_str_str, 2),
            (&kve_str_bin_name, &kve_str_bin, 3),
        ];
        // flush each of them
        for (tablename, table, _) in names {
            flush_table(&Autoflush, tablename, &default_keyspace, table).unwrap();
        }
        let mut read_tables: Vec<Table> = Vec::with_capacity(4);
        // read each of them
        for (tableid, _, modelcode) in names {
            read_tables.push(read_table(&default_keyspace, tableid, false, modelcode).unwrap());
        }
        for (index, (table, code)) in read_tables
            .iter()
            .map(|tbl| (gtable::<KVE>(tbl), tbl.get_model_code()))
            .enumerate()
        {
            assert_eq!(index, code as usize);
            assert!(table.get("hello".as_bytes()).unwrap().unwrap().eq(b"world"));
            assert_eq!(table.len(), 1);
        }
    }

    macro_rules! putlist {
        ($table:ident) => {
            gtable::<KVEList>(&$table)
                .get_inner_ref()
                .fresh_entry(Data::from("super"))
                .unwrap()
                .insert(LockedVec::new(vec![
                    Data::from("hello"),
                    Data::from("world"),
                ]))
        };
    }

    #[test]
    fn table_restore_bytemark_kvlist() {
        let default_keyspace = ObjectID::try_from_slice(b"actual_kvl_restore").unwrap();
        fs::create_dir_all(format!("data/ks/{}", unsafe { default_keyspace.as_str() })).unwrap();
        let kve_bin_listbin_name = ObjectID::try_from_slice(b"bin_listbin").unwrap();
        let kve_bin_listbin = Table::from_model_code(4, false).unwrap();
        putlist!(kve_bin_listbin);
        let kve_bin_liststr_name = ObjectID::try_from_slice(b"bin_liststr").unwrap();
        let kve_bin_liststr = Table::from_model_code(5, false).unwrap();
        putlist!(kve_bin_liststr);
        let kve_str_listbinstr_name = ObjectID::try_from_slice(b"str_listbinstr").unwrap();
        let kve_str_listbinstr = Table::from_model_code(6, false).unwrap();
        putlist!(kve_str_listbinstr);
        let kve_str_liststr_name = ObjectID::try_from_slice(b"str_liststr").unwrap();
        let kve_str_liststr = Table::from_model_code(7, false).unwrap();
        putlist!(kve_str_liststr);
        let names: [(&ObjectID, &Table, u8); 4] = [
            (&kve_bin_listbin_name, &kve_bin_listbin, 4),
            (&kve_bin_liststr_name, &kve_bin_liststr, 5),
            (&kve_str_listbinstr_name, &kve_str_listbinstr, 6),
            (&kve_str_liststr_name, &kve_str_liststr, 7),
        ];
        // flush each of them
        for (tablename, table, _) in names {
            flush_table(&Autoflush, tablename, &default_keyspace, table).unwrap();
        }
        let mut read_tables: Vec<Table> = Vec::with_capacity(4);
        // read each of them
        for (tableid, _, modelcode) in names {
            read_tables.push(read_table(&default_keyspace, tableid, false, modelcode).unwrap());
        }
        for (index, (table, code)) in read_tables
            .iter()
            .map(|tbl| (gtable::<KVEList>(tbl), tbl.get_model_code()))
            .enumerate()
        {
            // check code
            assert_eq!(index + 4, code as usize);
            // check payload
            let vec = table.get_inner_ref().get("super".as_bytes()).unwrap();
            assert_eq!(vec.read().len(), 2);
            assert_eq!(vec.read()[0], "hello");
            assert_eq!(vec.read()[1], "world");
            // check len
            assert_eq!(table.len(), 1);
        }
    }
}

mod flush_routines {
    use crate::corestore::memstore::Keyspace;
    use crate::corestore::memstore::ObjectID;
    use crate::corestore::table::DataModel;
    use crate::corestore::table::Table;
    use crate::corestore::Data;
    use crate::kvengine::LockedVec;
    use crate::storage::v1::bytemarks;
    use crate::storage::v1::flush::Autoflush;
    use crate::storage::v1::Coremap;
    use std::fs;
    #[test]
    fn test_flush_unflush_table_pure_kve() {
        let tbl = Table::new_default_kve();
        tbl.get_kvstore()
            .unwrap()
            .set("hello".into(), "world".into())
            .unwrap();
        let tblid = unsafe { ObjectID::from_slice("mytbl1") };
        let ksid = unsafe { ObjectID::from_slice("myks1") };
        // create the temp dir for this test
        fs::create_dir_all("data/ks/myks1").unwrap();
        super::flush::oneshot::flush_table(&Autoflush, &tblid, &ksid, &tbl).unwrap();
        // now that it's flushed, let's read the table using and unflush routine
        let ret = super::unflush::read_table::<Table>(
            &ksid,
            &tblid,
            false,
            bytemarks::BYTEMARK_MODEL_KV_BIN_BIN,
        )
        .unwrap();
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
    fn test_flush_unflush_table_kvext_listmap() {
        let tbl = Table::new_kve_listmap_with_data(Coremap::new(), false, true, true);
        if let DataModel::KVExtListmap(kvl) = tbl.get_model_ref() {
            kvl.add_list("mylist".into()).unwrap();
            let list = kvl.get("mylist".as_bytes()).unwrap().unwrap();
            list.write().push("mysupervalue".into());
        } else {
            panic!("Bad model!");
        }
        let tblid = unsafe { ObjectID::from_slice("mylists1") };
        let ksid = unsafe { ObjectID::from_slice("mylistyks") };
        // create the temp dir for this test
        fs::create_dir_all("data/ks/mylistyks").unwrap();
        super::flush::oneshot::flush_table(&Autoflush, &tblid, &ksid, &tbl).unwrap();
        // now that it's flushed, let's read the table using and unflush routine
        let ret = super::unflush::read_table::<Table>(
            &ksid,
            &tblid,
            false,
            bytemarks::BYTEMARK_MODEL_KV_STR_LIST_STR,
        )
        .unwrap();
        assert!(!ret.is_volatile());
        if let DataModel::KVExtListmap(kvl) = ret.get_model_ref() {
            let list = kvl.get("mylist".as_bytes()).unwrap().unwrap();
            let lread = list.read();
            assert_eq!(lread.len(), 1);
            assert_eq!(lread[0].as_ref(), "mysupervalue".as_bytes());
        } else {
            panic!("Bad model!");
        }
    }
    #[test]
    fn test_flush_unflush_keyspace() {
        // create the temp dir for this test
        fs::create_dir_all("data/ks/myks_1").unwrap();
        let ksid = unsafe { ObjectID::from_slice("myks_1") };
        let tbl1 = unsafe { ObjectID::from_slice("mytbl_1") };
        let tbl2 = unsafe { ObjectID::from_slice("mytbl_2") };
        let list_tbl = unsafe { ObjectID::from_slice("mylist_1") };
        let ks = Keyspace::empty();

        // a persistent table
        let mytbl = Table::new_default_kve();
        mytbl
            .get_kvstore()
            .unwrap()
            .set("hello".into(), "world".into())
            .unwrap();
        assert!(ks.create_table(tbl1.clone(), mytbl));

        // and a table with lists
        let cmap = Coremap::new();
        cmap.true_if_insert("mylist".into(), LockedVec::new(vec!["myvalue".into()]));
        let my_list_tbl = Table::new_kve_listmap_with_data(cmap, false, true, true);
        assert!(ks.create_table(list_tbl.clone(), my_list_tbl));

        // and a volatile table
        assert!(ks.create_table(tbl2.clone(), Table::new_kve_with_volatile(true)));

        // now flush it
        super::flush::flush_keyspace_full(&Autoflush, &ksid, &ks).unwrap();
        let ret = super::unflush::read_keyspace::<Keyspace>(&ksid).unwrap();
        let tbl1_ret = ret.tables.get(&tbl1).unwrap();
        let tbl2_ret = ret.tables.get(&tbl2).unwrap();
        let tbl3_ret_list = ret.tables.get(&list_tbl).unwrap();
        // should be a persistent table with the value we set
        assert_eq!(tbl1_ret.count(), 1);
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
        // should be a volatile table with no values
        assert_eq!(tbl2_ret.count(), 0);
        assert!(tbl2_ret.is_volatile());
        // should have a list with the `myvalue` element
        assert_eq!(tbl3_ret_list.count(), 1);
        if let DataModel::KVExtListmap(kvl) = tbl3_ret_list.get_model_ref() {
            assert_eq!(
                kvl.get("mylist".as_bytes()).unwrap().unwrap().read()[0].as_ref(),
                "myvalue".as_bytes()
            );
        } else {
            panic!(
                "Wrong model. Expected listmap, got: {}",
                tbl3_ret_list.get_model_code()
            );
        }
    }
}

mod list_tests {
    use super::iter::RawSliceIter;
    use super::{de, se};
    use crate::corestore::{htable::Coremap, Data};
    use crate::kvengine::LockedVec;
    use core::ops::Deref;
    use parking_lot::RwLock;
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
        let vals = lvec!["apples", "bananas", "carrots"];
        mymap.true_if_insert(Data::from("mykey"), RwLock::new(vals.read().clone()));
        let mut v = Vec::new();
        se::raw_serialize_list_map(&mymap, &mut v).unwrap();
        let de = de::deserialize_list_map(&v).unwrap();
        assert_eq!(de.len(), 1);
        let mykey_value = de
            .get("mykey".as_bytes())
            .unwrap()
            .value()
            .deref()
            .read()
            .clone();
        assert_eq!(
            mykey_value,
            vals.into_inner()
                .into_iter()
                .map(Data::from)
                .collect::<Vec<Data>>()
        );
    }
    #[test]
    fn test_list_map_se_de() {
        let mymap: Coremap<Data, LockedVec> = Coremap::new();
        let key1: Data = "mykey1".into();
        let val1 = lvec!["apples", "bananas", "carrots"];
        let key2: Data = "mykey2long".into();
        let val2 = lvec!["code", "coffee", "cats"];
        mymap.true_if_insert(key1.clone(), RwLock::new(val1.read().clone()));
        mymap.true_if_insert(key2.clone(), RwLock::new(val2.read().clone()));
        let mut v = Vec::new();
        se::raw_serialize_list_map(&mymap, &mut v).unwrap();
        let de = de::deserialize_list_map(&v).unwrap();
        assert_eq!(de.len(), 2);
        assert_eq!(
            de.get(&key1).unwrap().value().deref().read().clone(),
            val1.into_inner()
                .into_iter()
                .map(Data::from)
                .collect::<Vec<Data>>()
        );
        assert_eq!(
            de.get(&key2).unwrap().value().deref().read().clone(),
            val2.into_inner()
                .into_iter()
                .map(Data::from)
                .collect::<Vec<Data>>()
        );
    }
    #[test]
    fn test_list_map_empty_se_de() {
        let mymap: Coremap<Data, LockedVec> = Coremap::new();
        let mut v = Vec::new();
        se::raw_serialize_list_map(&mymap, &mut v).unwrap();
        let de = de::deserialize_list_map(&v).unwrap();
        assert_eq!(de.len(), 0)
    }
}

mod corruption_tests {
    use crate::corestore::htable::Coremap;
    use crate::corestore::Data;
    use crate::kvengine::LockedVec;
    #[test]
    fn test_corruption_map_basic() {
        let mymap = Coremap::new();
        let seresult = super::se::serialize_map(&mymap).unwrap();
        // now chop it; since this has 8B, let's drop some bytes
        assert!(super::de::deserialize_map(&seresult[..seresult.len() - 6]).is_none());
    }
    #[test]
    fn test_map_corruption_end_corruption() {
        let cmap = Coremap::new();
        cmap.upsert("sayan".into(), "writes code".into());
        cmap.upsert("supersayan".into(), "writes super code".into());
        let ser = super::se::serialize_map(&cmap).unwrap();
        // corrupt the last 16B
        assert!(super::de::deserialize_map(&ser[..ser.len() - 16]).is_none());
    }
    #[test]
    fn test_map_corruption_midway_corruption() {
        let cmap = Coremap::new();
        cmap.upsert("sayan".into(), "writes code".into());
        cmap.upsert("supersayan".into(), "writes super code".into());
        let mut ser = super::se::serialize_map(&cmap).unwrap();
        // middle chop
        ser.drain(16..ser.len() / 2);
        assert!(super::de::deserialize_map(&ser).is_none());
    }
    #[test]
    fn test_listmap_corruption_basic() {
        let mymap: Coremap<Data, LockedVec> = Coremap::new();
        mymap.upsert("hello".into(), lvec!("hello-1"));
        // current repr: [1u64][5u64]["hello"][1u64][7u64]["hello-1"]
        // sanity test
        let mut v = Vec::new();
        super::se::raw_serialize_list_map(&mymap, &mut v).unwrap();
        assert!(super::de::deserialize_list_map(&v).is_some());
        // now chop "hello-1"
        assert!(super::de::deserialize_list_map(&v[..v.len() - 7]).is_none());
    }
    #[test]
    fn test_listmap_corruption_midway() {
        let mymap: Coremap<Data, LockedVec> = Coremap::new();
        mymap.upsert("hello".into(), lvec!("hello-1"));
        // current repr: [1u64][5u64]["hello"][1u64][7u64]["hello-1"]
        // sanity test
        let mut v = Vec::new();
        super::se::raw_serialize_list_map(&mymap, &mut v).unwrap();
        assert!(super::de::deserialize_list_map(&v).is_some());
        assert_eq!(v.len(), 44);
        // now chop "7u64" (8+8+5+8+8+7)
        v.drain(29..37);
        assert!(super::de::deserialize_list_map(&v).is_none());
    }
}
