/*
 * Created on Sun Jan 29 2023
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

use super::{
    super::{super::sync::atm::cpin, IndexBaseSpec, MTIndex},
    imp::ChmCopy as _ChmCopy,
    meta::DefConfig,
};
use std::{
    hash::{BuildHasher, Hasher},
    sync::{Arc, RwLock},
    thread::{self, JoinHandle},
};

type Chm<K, V> = ChmCopy<K, V>;
type ChmCopy<K, V> = _ChmCopy<K, V, DefConfig>;

struct LolHash {
    seed: usize,
}

impl LolHash {
    const fn with_seed(seed: usize) -> Self {
        Self { seed }
    }
    const fn init_default_seed() -> Self {
        Self::with_seed(0)
    }
}

impl Default for LolHash {
    fn default() -> Self {
        Self::init_default_seed()
    }
}

impl Hasher for LolHash {
    fn finish(&self) -> u64 {
        self.seed as _
    }
    fn write(&mut self, _: &[u8]) {}
}

struct LolState {
    seed: usize,
}

impl BuildHasher for LolState {
    type Hasher = LolHash;

    fn build_hasher(&self) -> Self::Hasher {
        LolHash::with_seed(self.seed)
    }
}

impl Default for LolState {
    fn default() -> Self {
        Self { seed: 0 }
    }
}

type ChmU8 = Chm<u8, u8>;

// empty
#[test]
fn drop_empty() {
    let idx = ChmU8::idx_init();
    drop(idx);
}

#[test]
fn get_empty() {
    let idx = ChmU8::idx_init();
    assert!(idx.mt_get(&10, &cpin()).is_none());
}

#[test]
fn update_empty() {
    let idx = ChmU8::idx_init();
    assert!(!idx.mt_update(10, 20, &cpin()));
}

const SPAM_INSERT: usize = 16_384;
const SPAM_TENANTS: usize = 32;

#[test]
fn multispam_insert() {
    let idx = Arc::new(ChmCopy::new());
    let token = Arc::new(RwLock::new(()));
    let hold = token.write();
    let data: Vec<(Arc<String>, Arc<String>)> = (0..SPAM_INSERT)
        .into_iter()
        .map(|int| (format!("{int}"), format!("x-{int}-{}", int + 1)))
        .map(|(k, v)| (Arc::new(k), Arc::new(v)))
        .collect();
    let distr_data: Vec<Vec<(Arc<String>, Arc<String>)>> = data
        .chunks(SPAM_INSERT / SPAM_TENANTS)
        .map(|chunk| {
            chunk
                .iter()
                .map(|(k, v)| (Arc::clone(k), Arc::clone(v)))
                .collect()
        })
        .collect();
    let threads: Vec<JoinHandle<_>> = distr_data
        .into_iter()
        .enumerate()
        .map(|(tid, this_data)| {
            let this_token = token.clone();
            let this_idx = idx.clone();
            thread::Builder::new()
                .name(tid.to_string())
                .spawn(move || {
                    let _token = this_token.read();
                    let g = cpin();
                    this_data.into_iter().for_each(|(k, v)| {
                        assert!(this_idx.insert((k, v), &g));
                    })
                })
                .unwrap()
        })
        .collect();
    // rush everyone to insert; superb intercore traffic
    drop(hold);
    let _x: Box<[()]> = threads
        .into_iter()
        .map(JoinHandle::join)
        .map(Result::unwrap)
        .collect();
    let pin = cpin();
    assert_eq!(idx.len(), SPAM_INSERT);
    data.into_iter().for_each(|(k, v)| {
        assert_eq!(idx.mt_get(&k, &pin).unwrap().as_str(), &*v);
    });
}
