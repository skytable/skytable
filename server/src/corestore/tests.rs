/*
 * Created on Fri Jul 30 2021
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

mod memstore_keyspace_tests {
    use super::super::memstore::*;
    use super::super::table::Table;

    #[test]
    fn test_drop_keyspace_empty() {
        let ms = Memstore::new_empty();
        let obj = unsafe { ObjectID::from_slice("myks") };
        ms.create_keyspace(obj.clone());
        assert!(ms.drop_keyspace(obj).is_ok());
    }

    #[test]
    fn test_drop_keyspace_still_accessed() {
        let ms = Memstore::new_empty();
        let obj = unsafe { ObjectID::from_slice("myks") };
        ms.create_keyspace(obj.clone());
        let _ks_ref = ms.get_keyspace_atomic_ref(&obj);
        assert_eq!(ms.drop_keyspace(obj).unwrap_err(), DdlError::StillInUse);
    }

    #[test]
    fn test_drop_keyspace_not_empty() {
        let ms = Memstore::new_empty();
        let obj = unsafe { ObjectID::from_slice("myks") };
        ms.create_keyspace(obj.clone());
        let ks_ref = ms.get_keyspace_atomic_ref(&obj).unwrap();
        ks_ref.create_table(
            unsafe { ObjectID::from_slice("mytbl") },
            Table::new_default_kve(),
        );
        assert_eq!(ms.drop_keyspace(obj).unwrap_err(), DdlError::NotEmpty);
    }

    #[test]
    fn test_force_drop_keyspace_empty() {
        let ms = Memstore::new_empty();
        let obj = unsafe { ObjectID::from_slice("myks") };
        ms.create_keyspace(obj.clone());
        assert!(ms.force_drop_keyspace(obj).is_ok());
    }

    #[test]
    fn test_force_drop_keyspace_still_accessed() {
        let ms = Memstore::new_empty();
        let obj = unsafe { ObjectID::from_slice("myks") };
        ms.create_keyspace(obj.clone());
        let _ks_ref = ms.get_keyspace_atomic_ref(&obj);
        assert_eq!(
            ms.force_drop_keyspace(obj).unwrap_err(),
            DdlError::StillInUse
        );
    }

    #[test]
    fn test_force_drop_keyspace_table_referenced() {
        // the check here is to see if all the tables are not in active use
        let ms = Memstore::new_empty();
        let obj = unsafe { ObjectID::from_slice("myks") };
        let tblid = unsafe { ObjectID::from_slice("mytbl") };
        // create the ks
        ms.create_keyspace(obj.clone());
        // get an atomic ref to the keyspace
        let ks_ref = ms.get_keyspace_atomic_ref(&obj).unwrap();
        // create a table
        ks_ref.create_table(tblid.clone(), Table::new_default_kve());
        // ref to the table
        let _tbl_ref = ks_ref.get_table_atomic_ref(&tblid).unwrap();
        // drop ks ref
        drop(ks_ref);
        assert_eq!(
            ms.force_drop_keyspace(obj).unwrap_err(),
            DdlError::StillInUse
        );
    }

    #[test]
    fn test_force_drop_keyspace_nonempty_okay() {
        // the check here is to see if drop succeeds, provided that no
        // tables are in active use
        let ms = Memstore::new_empty();
        let obj = unsafe { ObjectID::from_slice("myks") };
        let tblid = unsafe { ObjectID::from_slice("mytbl") };
        // create the ks
        ms.create_keyspace(obj.clone());
        // get an atomic ref to the keyspace
        let ks_ref = ms.get_keyspace_atomic_ref(&obj).unwrap();
        // create a table
        ks_ref.create_table(tblid, Table::new_default_kve());
        // drop ks ref
        drop(ks_ref);
        // should succeed because the keyspace is non-empty, but no table is referenced to
        assert!(ms.force_drop_keyspace(obj).is_ok());
    }
}

mod modelcode_tests {
    use super::super::table::Table;
    use crate::kvengine::KVEListmap;
    use crate::kvengine::KVEngine;
    #[test]
    fn test_model_code_pure_kve() {
        // binstr, binstr
        let kv1 = KVEngine::init(false, false);
        // binstr, str
        let kv2 = KVEngine::init(false, true);
        // str, str
        let kv3 = KVEngine::init(true, true);
        // str, binstr
        let kv4 = KVEngine::init(true, false);

        // now check
        let tbl1 = Table::from_kve(kv1, false);
        assert_eq!(tbl1.get_model_code(), 0);
        let tbl2 = Table::from_kve(kv2, false);
        assert_eq!(tbl2.get_model_code(), 1);
        let tbl3 = Table::from_kve(kv3, false);
        assert_eq!(tbl3.get_model_code(), 2);
        let tbl4 = Table::from_kve(kv4, false);
        assert_eq!(tbl4.get_model_code(), 3);
    }
    #[test]
    fn test_model_code_kvext_listmap() {
        // binstr, list<binstr>
        let l1 = KVEListmap::init(false, false);
        // binstr, list<str>
        let l2 = KVEListmap::init(false, true);
        // str, list<binstr>
        let l3 = KVEListmap::init(true, false);
        // str, list<str>
        let l4 = KVEListmap::init(true, true);

        // now check
        let tbl1 = Table::from_kve_listmap(l1, false);
        assert_eq!(tbl1.get_model_code(), 4);
        let tbl2 = Table::from_kve_listmap(l2, false);
        assert_eq!(tbl2.get_model_code(), 5);
        let tbl3 = Table::from_kve_listmap(l3, false);
        assert_eq!(tbl3.get_model_code(), 6);
        let tbl4 = Table::from_kve_listmap(l4, false);
        assert_eq!(tbl4.get_model_code(), 7);
    }
}
