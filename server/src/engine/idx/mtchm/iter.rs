/*
 * Created on Fri Jan 27 2023
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
    super::super::{
        mem::UArray,
        sync::atm::{Guard, Shared},
    },
    meta::{Config, DefConfig, NodeFlag, TreeElement},
    Node, Tree,
};
use std::marker::PhantomData;

pub struct IterKV<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
{
    i: RawIter<'t, 'g, 'v, T, S, C, CfgIterKV>,
}

impl<'t, 'g, 'v, T, S, C> IterKV<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
{
    pub fn new(t: &'t Tree<T, S, C>, g: &'g Guard) -> Self {
        Self {
            i: RawIter::new(t, g),
        }
    }
}

impl<'t, 'g, 'v, T, S, C> Iterator for IterKV<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
    T: TreeElement,
{
    type Item = &'v T;

    fn next(&mut self) -> Option<Self::Item> {
        self.i.next()
    }
}

pub struct IterKey<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
    T: TreeElement,
{
    i: RawIter<'t, 'g, 'v, T, S, C, CfgIterKey>,
}

impl<'t, 'g, 'v, T, S, C> IterKey<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
    T: TreeElement,
{
    pub fn new(t: &'t Tree<T, S, C>, g: &'g Guard) -> Self {
        Self {
            i: RawIter::new(t, g),
        }
    }
}

impl<'t, 'g, 'v, T, S, C> Iterator for IterKey<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
    T: TreeElement,
{
    type Item = &'v T::Key;

    fn next(&mut self) -> Option<Self::Item> {
        self.i.next()
    }
}

pub struct IterVal<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
    T: TreeElement,
{
    i: RawIter<'t, 'g, 'v, T, S, C, CfgIterVal>,
}

impl<'t, 'g, 'v, T, S, C> IterVal<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
    T: TreeElement,
{
    pub fn new(t: &'t Tree<T, S, C>, g: &'g Guard) -> Self {
        Self {
            i: RawIter::new(t, g),
        }
    }
}

impl<'t, 'g, 'v, T, S, C> Iterator for IterVal<'t, 'g, 'v, T, S, C>
where
    't: 'v,
    'g: 'v + 't,
    C: Config,
    T: TreeElement,
{
    type Item = &'v T::Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.i.next()
    }
}

trait IterConfig<T> {
    type Ret<'a>
    where
        T: 'a;
    fn some<'a>(v: &'a T) -> Option<Self::Ret<'a>>;
}

struct CfgIterKV;
impl<T> IterConfig<T> for CfgIterKV {
    type Ret<'a> = &'a T where T: 'a;
    fn some<'a>(v: &'a T) -> Option<Self::Ret<'a>> {
        Some(v)
    }
}

struct CfgIterKey;
impl<T: TreeElement> IterConfig<T> for CfgIterKey {
    type Ret<'a> = &'a T::Key where T::Key: 'a;
    fn some<'a>(v: &'a T) -> Option<Self::Ret<'a>> {
        Some(v.key())
    }
}

struct CfgIterVal;
impl<T: TreeElement> IterConfig<T> for CfgIterVal {
    type Ret<'a> = &'a T::Value where T::Value: 'a;
    fn some<'a>(v: &'a T) -> Option<Self::Ret<'a>> {
        Some(v.val())
    }
}

struct DFSCNodeCtx<'g, C: Config> {
    sptr: Shared<'g, Node<C>>,
    idx: usize,
}

struct RawIter<'t, 'g, 'v, T, S, C, I>
where
    't: 'v,
    'g: 'v + 't,
    I: IterConfig<T>,
    C: Config,
{
    g: &'g Guard,
    stack: UArray<{ <DefConfig as Config>::BRANCH_MX + 1 }, DFSCNodeCtx<'g, C>>,
    _m: PhantomData<(&'v T, C, &'t Tree<T, S, C>, I)>,
}

impl<'t, 'g, 'v, T, S, C, I> RawIter<'t, 'g, 'v, T, S, C, I>
where
    't: 'v,
    'g: 'v + 't,
    I: IterConfig<T>,
    C: Config,
{
    pub(super) fn new(tree: &'t Tree<T, S, C>, g: &'g Guard) -> Self {
        let mut stack = UArray::new();
        let sptr = tree.root.ld_acq(g);
        stack.push(DFSCNodeCtx { sptr, idx: 0 });
        Self {
            g,
            stack,
            _m: PhantomData,
        }
    }
    /// depth-first search the tree
    fn _next(&mut self) -> Option<I::Ret<'v>> {
        while !self.stack.is_empty() {
            let l = self.stack.len() - 1;
            let ref mut current = self.stack[l];
            let ref node = current.sptr;
            let flag = super::ldfl(&current.sptr);
            match flag {
                _ if node.is_null() => {
                    self.stack.pop();
                }
                flag if super::hf(flag, NodeFlag::DATA) => {
                    let data = unsafe {
                        // UNSAFE(@ohsayan): flagck
                        Tree::<T, S, C>::read_data(current.sptr)
                    };
                    if current.idx < data.len() {
                        let ref ret = data[current.idx];
                        current.idx += 1;
                        return I::some(ret);
                    } else {
                        self.stack.pop();
                    }
                }
                _ if current.idx < C::MAX_TREE_HEIGHT => {
                    let this_node = unsafe {
                        // UNSAFE(@ohsayan): guard
                        node.deref()
                    };
                    let sptr = this_node.branch[current.idx].ld_acq(&self.g);
                    current.idx += 1;
                    self.stack.push(DFSCNodeCtx { sptr, idx: 0 });
                }
                _ => {
                    self.stack.pop();
                }
            }
        }
        None
    }
}

impl<'t, 'g, 'v, T, S, C, I> Iterator for RawIter<'t, 'g, 'v, T, S, C, I>
where
    't: 'v,
    'g: 'v + 't,
    I: IterConfig<T>,
    C: Config,
{
    type Item = I::Ret<'v>;

    fn next(&mut self) -> Option<Self::Item> {
        self._next()
    }
}