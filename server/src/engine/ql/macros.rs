/*
 * Created on Fri Sep 16 2022
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

macro_rules! dict {
    () => {
        <::std::collections::HashMap<_, _> as ::core::default::Default>::default()
    };
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut hm: ::std::collections::HashMap<_, _> = ::core::default::Default::default();
        $(hm.insert($key.into(), $value.into());)*
        hm
    }};
}

macro_rules! set {
    () => {
        <::std::collections::HashSet<_> as ::core::default::Default>::default()
    };
    ($($key:expr),* $(,)?) => {{
        let mut hs: ::std::collections::HashSet<_> = ::core::default::Default::default();
        $(hs.insert($key.into());)*
        hs
    }};
}

macro_rules! multi_assert_eq {
    ($($lhs:expr),* => $rhs:expr) => {
        $(assert_eq!($lhs, $rhs);)*
    };
}

macro_rules! enum_impls {
    ($for:ty => {$($other:ty as $me:ident),*$(,)?}) => {
        $(impl ::core::convert::From<$other> for $for {fn from(v: $other) -> Self {Self::$me(v)}})*
    }
}

macro_rules! assertions {
    ($($assert:expr),*$(,)?) => {$(const _:()=::core::assert!($assert);)*}
}
