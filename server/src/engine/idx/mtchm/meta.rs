/*
 * Created on Thu Jan 26 2023
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

use {
    crate::engine::{
        idx::{meta::AsHasher, AsKey, AsKeyClone, AsValue, AsValueClone},
        mem::VInline,
    },
    std::{collections::hash_map::RandomState, sync::Arc},
};

const LNODE_STACK: usize = 1;
pub type DefConfig = Config2B<RandomState>;
pub type LNode<T> = VInline<LNODE_STACK, T>;

pub trait PreConfig: Sized + 'static {
    type HState: AsHasher;
    const BITS: u32;
}

pub trait Config: PreConfig {
    const BRANCH_MX: usize = <Self as PreConfig>::BITS as _;
    const BRANCH_LG: usize = {
        let mut index = <Self as Config>::BRANCH_MX;
        let mut log = 0usize;
        while {
            index >>= 1;
            index != 0
        } {
            log += 1;
        }
        log
    };
    const MASK: u64 = (<Self as PreConfig>::BITS - 1) as _;
    const MAX_TREE_HEIGHT_UB: usize = 0x40;
    const MAX_TREE_HEIGHT: usize =
        <Self as Config>::MAX_TREE_HEIGHT_UB / <Self as Config>::BRANCH_LG;
    const LEVEL_ZERO: usize = 0;
}

impl<T: PreConfig> Config for T {}

pub struct Config2B<T: AsHasher + 'static>(T);
impl<T: AsHasher> PreConfig for Config2B<T> {
    const BITS: u32 = u16::BITS;
    type HState = T;
}

pub trait TreeElement: Clone + 'static {
    type Key: AsKey;
    type IKey;
    type Value: AsValue;
    type IValue;
    type VEx1;
    type VEx2;
    fn key(&self) -> &Self::Key;
    fn val(&self) -> &Self::Value;
    fn new(k: Self::IKey, v: Self::IValue, vex1: Self::VEx1, vex2: Self::VEx2) -> Self;
}

impl<K: AsKeyClone, V: AsValueClone> TreeElement for (K, V) {
    type IKey = K;
    type Key = K;
    type IValue = V;
    type Value = V;
    type VEx1 = ();
    type VEx2 = ();
    #[inline(always)]
    fn key(&self) -> &K {
        &self.0
    }
    #[inline(always)]
    fn val(&self) -> &V {
        &self.1
    }
    fn new(k: Self::Key, v: Self::Value, _: (), _: ()) -> Self {
        (k, v)
    }
}

impl<K: AsKey, V: AsValue> TreeElement for Arc<(K, V)> {
    type IKey = K;
    type Key = K;
    type IValue = V;
    type Value = V;
    type VEx1 = ();
    type VEx2 = ();
    #[inline(always)]
    fn key(&self) -> &K {
        &self.0
    }
    #[inline(always)]
    fn val(&self) -> &V {
        &self.1
    }
    fn new(k: Self::Key, v: Self::Value, _: (), _: ()) -> Self {
        Arc::new((k, v))
    }
}

flags! {
    pub struct NodeFlag: usize {
        PENDING_DELETE = 0b01,
        DATA = 0b10,
    }
}

flags! {
    #[derive(PartialEq, Eq)]
    pub struct CompressState: u8 {
        NULL = 0b00,
        SNODE = 0b01,
        CASFAIL = 0b10,
        RESTORED = 0b11,
    }
}
