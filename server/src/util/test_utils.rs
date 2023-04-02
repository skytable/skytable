/*
 * Created on Sat Sep 17 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

use rand::{
    distributions::{uniform::SampleUniform, Alphanumeric},
    Rng,
};

// TODO(@ohsayan): Use my own PRNG algo here. Maybe my quadratic one?

/// Generates a random boolean based on Bernoulli distributions
pub fn random_bool(rng: &mut impl Rng) -> bool {
    rng.gen_bool(0.5)
}
/// Generate a random number within the given range
pub fn random_number<T: SampleUniform + PartialOrd>(max: T, min: T, rng: &mut impl Rng) -> T {
    rng.gen_range(max..min)
}

pub fn random_string(rng: &mut impl Rng, l: usize) -> String {
    rng.sample_iter(Alphanumeric)
        .take(l)
        .map(char::from)
        .collect()
}

pub fn random_string_checked(rng: &mut impl Rng, l: usize, ck: impl Fn(&str) -> bool) -> String {
    loop {
        let r = random_string(rng, l);
        if ck(&r) {
            break r;
        }
    }
}

pub trait VecFuse<T> {
    fn fuse_append(self, arr: &mut Vec<T>);
}

impl<T> VecFuse<T> for T {
    fn fuse_append(self, arr: &mut Vec<T>) {
        arr.push(self);
    }
}

impl<T, const N: usize> VecFuse<T> for [T; N] {
    fn fuse_append(self, arr: &mut Vec<T>) {
        arr.extend(self)
    }
}

impl<T> VecFuse<T> for Vec<T> {
    fn fuse_append(self, arr: &mut Vec<T>) {
        arr.extend(self)
    }
}

#[macro_export]
macro_rules! vecfuse {
    ($($expr:expr),* $(,)?) => {{
        let mut v = Vec::new();
        $(<_ as $crate::util::test_utils::VecFuse<_>>::fuse_append($expr, &mut v);)*
        v
    }};
}
