/*
 * Created on Sun Jan 22 2023
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
mod scanner;
mod word;

mod vinline {
    use super::VInline;
    const CAP: usize = 8;
    #[sky_macros::test]
    fn drop_empty() {
        let vi = VInline::<CAP, String>::new();
        drop(vi);
    }
    /// This will:
    /// - returns an array [0..upto]
    /// - verify length
    /// - verify payload
    /// - verify capacity (if upto <= CAP)
    /// - verify stack/heap logic
    fn cmkvi<F, T: PartialEq>(upto: usize, map: F) -> VInline<CAP, T>
    where
        F: Clone + FnMut(usize) -> T,
    {
        let map2 = map.clone();
        let r: VInline<CAP, _> = (0..upto).map(map).collect();
        assert_eq!(r.len(), upto);
        if upto <= CAP {
            assert_eq!(r.capacity(), CAP);
            assert!(r.on_stack());
        } else {
            assert!(r.on_heap());
        }
        assert!((0..upto).map(map2).zip(r.iter()).all(|(x, y)| { x == *y }));
        r
    }
    fn mkvi(upto: usize) -> VInline<CAP, usize> {
        cmkvi(upto, |v| v)
    }
    fn mkvi_str(upto: usize) -> VInline<CAP, String> {
        cmkvi(upto, |v| v.to_string())
    }
    #[sky_macros::test]
    fn push_on_stack() {
        let vi = mkvi(CAP);
        assert!(vi.on_stack());
    }
    #[sky_macros::test]
    fn push_on_heap() {
        let vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
    }
    #[sky_macros::test]
    fn remove_on_stack() {
        let mut vi = mkvi(CAP);
        assert_eq!(vi.remove(6), 6);
        assert_eq!(vi.len(), CAP - 1);
        assert_eq!(vi.capacity(), CAP);
        assert_eq!(vi.as_ref(), [0, 1, 2, 3, 4, 5, 7]);
    }
    #[sky_macros::test]
    fn remove_on_heap() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.remove(6), 6);
        assert_eq!(vi.len(), CAP);
        assert_eq!(vi.capacity(), CAP * 2);
        assert_eq!(vi.as_ref(), [0, 1, 2, 3, 4, 5, 7, 8]);
    }
    #[sky_macros::test]
    fn optimize_capacity_none_on_stack() {
        let mut vi = mkvi(CAP);
        vi.optimize_capacity();
        assert_eq!(vi.capacity(), CAP);
        assert!(vi.on_stack());
    }
    #[sky_macros::test]
    fn optimize_capacity_none_on_heap() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
        vi.extend(CAP + 1..CAP * 2);
        assert_eq!(vi.capacity(), CAP * 2);
        vi.optimize_capacity();
        assert_eq!(vi.capacity(), CAP * 2);
    }
    #[sky_macros::test]
    fn optimize_capacity_on_heap() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
        vi.optimize_capacity();
        assert_eq!(vi.capacity(), CAP + 1);
    }
    #[sky_macros::test]
    fn optimize_capacity_mv_stack() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
        let _ = vi.remove_compact(0);
        assert_eq!(vi.len(), CAP);
        assert_eq!(vi.capacity(), CAP);
        assert!(vi.on_stack());
    }
    #[sky_macros::test]
    fn clear_stack() {
        let mut vi = mkvi(CAP);
        vi.clear();
        assert_eq!(vi.capacity(), CAP);
        assert_eq!(vi.len(), 0);
    }
    #[sky_macros::test]
    fn clear_heap() {
        let mut vi = mkvi(CAP + 1);
        vi.clear();
        assert_eq!(vi.capacity(), CAP * 2);
        assert_eq!(vi.len(), 0);
    }
    #[sky_macros::test]
    fn clone_stack() {
        let v1 = mkvi(CAP);
        let v2 = v1.clone();
        assert_eq!(v1, v2);
    }
    #[sky_macros::test]
    fn clone_heap() {
        let v1 = mkvi(CAP + 1);
        let v2 = v1.clone();
        assert_eq!(v1, v2);
    }
    #[sky_macros::test]
    fn into_iter_stack() {
        let v1 = mkvi_str(CAP);
        let v: Vec<String> = v1.into_iter().collect();
        (0..CAP)
            .zip(v)
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_stack_partial() {
        let v1 = mkvi_str(CAP);
        let v: Vec<String> = v1.into_iter().take(CAP / 2).collect();
        (0..CAP / 2)
            .zip(v)
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_heap() {
        let v1 = mkvi_str(CAP + 2);
        let v: Vec<String> = v1.into_iter().collect();
        (0..CAP)
            .zip(v)
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_heap_partial() {
        let v1 = mkvi_str(CAP + 2);
        let v: Vec<String> = v1.into_iter().take(CAP / 2).collect();
        (0..CAP / 2)
            .zip(v)
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_rev_stack() {
        let v1 = mkvi_str(CAP);
        let v: Vec<String> = v1.into_iter().rev().collect();
        (0..CAP)
            .rev()
            .zip(v)
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_rev_stack_partial() {
        let v1 = mkvi_str(CAP);
        let v: Vec<String> = v1.into_iter().rev().take(CAP / 2).collect();
        (CAP / 2..CAP)
            .rev()
            .zip(v.into_iter())
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_rev_heap() {
        let v1 = mkvi_str(CAP + 2);
        let v: Vec<String> = v1.into_iter().rev().collect();
        (0..CAP + 2)
            .rev()
            .zip(v)
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_rev_heap_partial() {
        let v1 = mkvi_str(CAP + 2);
        let v: Vec<String> = v1.into_iter().rev().take(CAP / 2).collect();
        (0..CAP + 2).rev().zip(v).for_each(|(x, y)| {
            assert_eq!(x.to_string(), y);
        })
    }
}

mod uarray {
    use super::UArray;
    const CAP: usize = 8;
    #[sky_macros::test]
    fn empty() {
        let a = UArray::<CAP, u8>::new();
        drop(a);
    }
    #[sky_macros::test]
    fn push_okay() {
        let mut a = UArray::<CAP, u8>::new();
        a.push(1);
        a.push(2);
        a.push(3);
        a.push(4);
    }
    #[sky_macros::test]
    #[should_panic(expected = "stack,capof")]
    fn push_panic() {
        let mut a = UArray::<CAP, u8>::new();
        a.push(1);
        a.push(2);
        a.push(3);
        a.push(4);
        a.push(5);
        a.push(6);
        a.push(7);
        a.push(8);
        a.push(9);
    }
    #[sky_macros::test]
    fn slice() {
        let a: UArray<CAP, _> = (1u8..=8).collect();
        assert_eq!(a.as_slice(), [1, 2, 3, 4, 5, 6, 7, 8]);
    }
    #[sky_macros::test]
    fn slice_mut() {
        let mut a: UArray<CAP, _> = (0u8..8).collect();
        a.iter_mut().for_each(|v| *v += 1);
        assert_eq!(a.as_slice(), [1, 2, 3, 4, 5, 6, 7, 8])
    }
    #[sky_macros::test]
    fn into_iter_empty() {
        let a: UArray<CAP, u8> = UArray::new();
        let r: Vec<u8> = a.into_iter().collect();
        assert!(r.is_empty());
    }
    #[sky_macros::test]
    fn into_iter() {
        let a: UArray<CAP, _> = (0u8..8).collect();
        let r: Vec<u8> = a.into_iter().collect();
        (0..8)
            .zip(r.into_iter())
            .for_each(|(x, y)| assert_eq!(x, y));
    }
    #[sky_macros::test]
    fn into_iter_partial() {
        let a: UArray<CAP, String> = (0u8..8).map(|v| ToString::to_string(&v)).collect();
        let r: Vec<String> = a.into_iter().take(4).collect();
        (0..4)
            .zip(r.into_iter())
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn clone() {
        let a: UArray<CAP, u8> = (0u8..CAP as _).collect();
        let b = a.clone();
        assert_eq!(a, b);
    }
    #[sky_macros::test]
    fn into_iter_rev() {
        let a: UArray<CAP, String> = (0u8..8).map(|v| v.to_string()).collect();
        let r: Vec<String> = a.into_iter().rev().collect();
        (0..8)
            .rev()
            .zip(r.into_iter())
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn into_iter_rev_partial() {
        let a: UArray<CAP, String> = (0u8..8).map(|v| v.to_string()).collect();
        let r: Vec<String> = a.into_iter().rev().take(4).collect();
        (4..8)
            .rev()
            .zip(r.into_iter())
            .for_each(|(x, y)| assert_eq!(x.to_string(), y));
    }
    #[sky_macros::test]
    fn pop_array() {
        let mut a: UArray<CAP, String> = (0u8..8).map(|v| v.to_string()).collect();
        assert_eq!(a.pop().unwrap(), "7");
        assert_eq!(a.len(), CAP - 1);
    }
    #[sky_macros::test]
    fn clear_array() {
        let mut a: UArray<CAP, String> = (0u8..8).map(|v| v.to_string()).collect();
        a.clear();
        assert!(a.is_empty());
    }
}
