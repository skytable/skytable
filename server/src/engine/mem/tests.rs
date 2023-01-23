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
    use super::*;
    const CAP: usize = 512;
    #[test]
    fn vinline_drop_empty() {
        let array = VInline::<CAP, String>::new();
        drop(array);
    }

    #[test]
    fn vinline_test() {
        // first alloc on stack
        let mut array = VInline::<CAP, String>::new();
        (0..CAP).for_each(|i| array.push(format!("elem-{i}")));
        // check meta methods
        debug_assert!(array.on_stack());
        debug_assert!(!array.will_be_on_stack());
        // now iterate
        array
            .iter()
            .enumerate()
            .for_each(|(i, elem)| assert_eq!(elem, format!("elem-{i}").as_str()));
        // now iter_mut
        array
            .iter_mut()
            .enumerate()
            .for_each(|(i, st)| *st = format!("elem-{}", i + 1));
        // now let's get off the stack
        (0..10).for_each(|i| array.push(format!("elem-{}", CAP + i + 1)));
        // verify all elements
        array
            .iter()
            .enumerate()
            .for_each(|(i, st)| assert_eq!(st, format!("elem-{}", i + 1).as_str()));
        debug_assert!(!array.on_stack());
        debug_assert!(!array.will_be_on_stack());
    }
}

mod uarray {
    use super::*;
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
