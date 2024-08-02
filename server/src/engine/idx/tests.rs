/*
 * Created on Wed Jan 18 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

mod idx_st_seq_dll {
    use super::{IndexBaseSpec, IndexSTSeqLib, STIndex, STIndexSeq};

    const SPAM_CNT: usize = if cfg!(miri) { 128 } else { 131_072 };
    type Index = IndexSTSeqLib<String, String>;

    /// Returns an index with: `i -> "{i+1}"` starting from 0 upto the value of [`SPAM_CNT`]
    fn mkidx() -> IndexSTSeqLib<usize, String> {
        let mut idx = IndexSTSeqLib::idx_init();
        for int in 0..SPAM_CNT {
            assert!(idx.st_insert(int, (int + 1).to_string()));
        }
        // verify data
        for int in 0..SPAM_CNT {
            assert_eq!(idx.st_get(&int).unwrap().as_str(), (int + 1).to_string());
        }
        assert_eq!(idx.idx_metrics().raw_f(), 0);
        idx
    }

    #[inline(always)]
    fn s(s: &str) -> String {
        s.to_owned()
    }
    #[sky_macros::test]
    fn empty_drop() {
        let idx = Index::idx_init();
        assert_eq!(idx.idx_metrics().raw_f(), 0);
        drop(idx);
    }
    #[sky_macros::test]
    fn spam_read_nx() {
        let idx = IndexSTSeqLib::<usize, String>::idx_init();
        for int in SPAM_CNT..SPAM_CNT * 2 {
            assert!(idx.st_get(&int).is_none());
        }
    }
    #[sky_macros::test]
    fn spam_insert_ex() {
        let mut idx = mkidx();
        for int in 0..SPAM_CNT {
            assert!(!idx.st_insert(int, (int + 2).to_string()));
        }
    }
    #[sky_macros::test]
    fn spam_update_nx() {
        let mut idx = IndexSTSeqLib::<usize, String>::idx_init();
        for int in 0..SPAM_CNT {
            assert!(!idx.st_update(&int, (int + 2).to_string()));
        }
    }
    #[sky_macros::test]
    fn spam_delete_nx() {
        let mut idx = IndexSTSeqLib::<usize, String>::idx_init();
        for int in 0..SPAM_CNT {
            assert!(!idx.st_delete(&int));
        }
    }
    #[sky_macros::test]
    fn simple_crud() {
        let mut idx = Index::idx_init();
        assert!(idx.st_insert(s("hello"), s("world")));
        assert_eq!(idx.st_get("hello").as_deref().unwrap(), "world");
        assert!(idx.st_update("hello", s("world2")));
        assert_eq!(idx.st_get("hello").as_deref().unwrap(), "world2");
        assert_eq!(idx.st_delete_return("hello").unwrap(), "world2");
        assert_eq!(idx.idx_metrics().raw_f(), 1);
    }
    #[sky_macros::test]
    fn spam_crud() {
        let mut idx = IndexSTSeqLib::idx_init();
        for int in 0..SPAM_CNT {
            assert!(idx.st_insert(int, int + 1));
            assert_eq!(*idx.st_get(&int).unwrap(), int + 1);
            assert!(idx.st_update(&int, int + 2));
            assert_eq!(*idx.st_get(&int).unwrap(), int + 2);
            assert_eq!(idx.st_delete_return(&int).unwrap(), int + 2);
        }
        assert_eq!(idx.idx_metrics().raw_f(), 1);
    }
    #[sky_macros::test]
    fn spam_read() {
        let mut idx = IndexSTSeqLib::idx_init();
        for int in 0..SPAM_CNT {
            let v = (int + 1).to_string();
            assert!(idx.st_insert(int, v.clone()));
            assert_eq!(idx.st_get(&int).as_deref().unwrap(), &v);
        }
        assert_eq!(idx.idx_metrics().raw_f(), 0);
    }
    #[sky_macros::test]
    fn spam_update() {
        let mut idx = mkidx();
        for int in 0..SPAM_CNT {
            assert_eq!(
                idx.st_update_return(&int, (int + 2).to_string()).unwrap(),
                (int + 1).to_string()
            );
        }
        assert_eq!(idx.idx_metrics().raw_f(), 0);
    }
    #[sky_macros::test]
    fn spam_delete() {
        let mut idx = mkidx();
        for int in 0..SPAM_CNT {
            assert_eq!(idx.st_delete_return(&int).unwrap(), (int + 1).to_string());
            assert_eq!(idx.idx_metrics().raw_f(), int + 1);
        }
        assert_eq!(idx.idx_metrics().raw_f(), SPAM_CNT);
    }
    #[sky_macros::test]
    fn compact() {
        let mut idx = mkidx();
        assert_eq!(idx.idx_metrics().raw_f(), 0);
        for int in 0..SPAM_CNT {
            let _ = idx.st_delete(&int);
        }
        assert_eq!(idx.idx_metrics().raw_f(), SPAM_CNT);
        idx.st_clear();
        assert_eq!(idx.idx_metrics().raw_f(), SPAM_CNT);
        idx.st_compact();
        assert_eq!(idx.idx_metrics().raw_f(), 0);
    }
    // pointless testing random iterators
    #[sky_macros::test]
    fn iter_ord() {
        let idx1 = mkidx();
        let idx2: Vec<(usize, String)> =
            idx1.stseq_ord_kv().map(|(k, v)| (*k, v.clone())).collect();
        (0..SPAM_CNT)
            .into_iter()
            .zip(idx2.into_iter())
            .for_each(|(i, (k, v))| {
                assert_eq!(i, k);
                assert_eq!((i + 1).to_string(), v);
            });
    }
    #[sky_macros::test]
    fn iter_ord_rev() {
        let idx1 = mkidx();
        let idx2: Vec<(usize, String)> = idx1
            .stseq_ord_kv()
            .rev()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        (0..SPAM_CNT)
            .rev()
            .into_iter()
            .zip(idx2.into_iter())
            .for_each(|(i, (k, v))| {
                assert_eq!(i, k);
                assert_eq!((i + 1).to_string(), v);
            });
    }
    #[sky_macros::test]
    fn empty_iter() {
        let idx1 = IndexSTSeqLib::<String, String>::idx_init();
        let v = idx1.stseq_ord_kv().next();
        assert!(v.is_none());
    }
}
