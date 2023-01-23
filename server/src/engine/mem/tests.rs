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

mod vinline {
    use super::VInline;
    const CAP: usize = 8;
    #[test]
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
    fn mkvi(upto: usize) -> VInline<CAP, usize> {
        let r: VInline<CAP, _> = (0..upto).into_iter().collect();
        assert_eq!(r.len(), upto);
        if upto <= CAP {
            assert_eq!(r.capacity(), CAP);
            assert!(r.on_stack());
        } else {
            assert!(r.on_heap());
        }
        assert!((0..upto)
            .into_iter()
            .zip(r.iter())
            .all(|(x, y)| { x == *y }));
        r
    }
    #[test]
    fn push_on_stack() {
        let vi = mkvi(CAP);
        assert!(vi.on_stack());
    }
    #[test]
    fn push_on_heap() {
        let vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
    }
    #[test]
    fn remove_on_stack() {
        let mut vi = mkvi(CAP);
        assert_eq!(vi.remove(6), 6);
        assert_eq!(vi.len(), CAP - 1);
        assert_eq!(vi.capacity(), CAP);
        assert_eq!(vi.as_ref(), [0, 1, 2, 3, 4, 5, 7]);
    }
    #[test]
    fn remove_on_heap() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.remove(6), 6);
        assert_eq!(vi.len(), CAP);
        assert_eq!(vi.capacity(), CAP * 2);
        assert_eq!(vi.as_ref(), [0, 1, 2, 3, 4, 5, 7, 8]);
    }
    #[test]
    fn optimize_capacity_none_on_stack() {
        let mut vi = mkvi(CAP);
        vi.optimize_capacity();
        assert_eq!(vi.capacity(), CAP);
        assert!(vi.on_stack());
    }
    #[test]
    fn optimize_capacity_none_on_heap() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
        vi.extend((CAP + 1..CAP * 2).into_iter());
        assert_eq!(vi.capacity(), CAP * 2);
        vi.optimize_capacity();
        assert_eq!(vi.capacity(), CAP * 2);
    }
    #[test]
    fn optimize_capacity_on_heap() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
        vi.optimize_capacity();
        assert_eq!(vi.capacity(), CAP + 1);
    }
    #[test]
    fn optimize_capacity_mv_stack() {
        let mut vi = mkvi(CAP + 1);
        assert_eq!(vi.capacity(), CAP * 2);
        let _ = vi.remove_compact(0);
        assert_eq!(vi.len(), CAP);
        assert_eq!(vi.capacity(), CAP);
        assert!(vi.on_stack());
    }
    #[test]
    fn clear_stack() {
        let mut vi = mkvi(CAP);
        vi.clear();
        assert_eq!(vi.capacity(), CAP);
        assert_eq!(vi.len(), 0);
    }
    #[test]
    fn clear_heap() {
        let mut vi = mkvi(CAP + 1);
        vi.clear();
        assert_eq!(vi.capacity(), CAP * 2);
        assert_eq!(vi.len(), 0);
    }
}

mod uarray {
    use super::UArray;
    const CAP: usize = 8;
    #[test]
    fn empty() {
        let a = UArray::<CAP, u8>::new();
        drop(a);
    }
    #[test]
    fn push_okay() {
        let mut a = UArray::<CAP, u8>::new();
        a.push(1);
        a.push(2);
        a.push(3);
        a.push(4);
    }
    #[test]
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
    #[test]
    fn slice() {
        let a: UArray<CAP, _> = (1u8..=8).into_iter().collect();
        assert_eq!(a.as_slice(), [1, 2, 3, 4, 5, 6, 7, 8]);
    }
    #[test]
    fn slice_mut() {
        let mut a: UArray<CAP, _> = (0u8..8).into_iter().collect();
        a.iter_mut().for_each(|v| *v += 1);
        assert_eq!(a.as_slice(), [1, 2, 3, 4, 5, 6, 7, 8])
    }
}
