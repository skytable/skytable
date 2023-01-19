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
    use super::{
        def::{IndexBaseSpec, STIndex},
        idx::IndexSTSeqDef,
    };
    use rand::{distributions::Alphanumeric, Rng};

    const SPAM_CNT: usize = 100_000;
    const SPAM_SIZE: usize = 128;
    type Index = IndexSTSeqDef<String, String>;

    #[inline(always)]
    fn s<'a>(s: &'a str) -> String {
        s.to_owned()
    }
    fn ranstr(rand: &mut impl Rng) -> String {
        rand.sample_iter(Alphanumeric)
            .take(SPAM_SIZE)
            .map(char::from)
            .collect()
    }
    #[test]
    fn empty_drop() {
        let idx = Index::idx_init();
        drop(idx);
    }
    #[test]
    fn simple_crud() {
        let mut idx = Index::idx_init();
        assert!(idx.st_insert(s("hello"), s("world")));
        assert_eq!(idx.st_get("hello").as_deref().unwrap(), "world");
        assert!(idx.st_update("hello", s("world2")));
        assert_eq!(idx.st_get("hello").as_deref().unwrap(), "world2");
        assert_eq!(idx.st_delete_return("hello").unwrap(), "world2");
        assert_eq!(idx.idx_metrics().report_f(), 1);
    }
    #[test]
    fn spam_crud() {
        let mut idx = IndexSTSeqDef::idx_init();
        for int in 0..SPAM_CNT {
            assert!(idx.st_insert(int, int + 1));
            assert_eq!(*idx.st_get(&int).unwrap(), int + 1);
            assert!(idx.st_update(&int, int + 2));
            assert_eq!(*idx.st_get(&int).unwrap(), int + 2);
            assert_eq!(idx.st_delete_return(&int).unwrap(), int + 2);
        }
        assert_eq!(idx.idx_metrics().report_f(), 1);
    }
}
