/*
 * Created on Tue Aug 31 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

macro_rules! d_encoder {
    ($fn:expr, $t:expr) => {
        DoubleEncoder {
            fn_ptr: $fn,
            v_t: $t,
        }
    };
}

macro_rules! borrow_hash_fn {
    (
        $(
            $(#[$attr:meta])*
            $vis:vis fn {borrow: $borrowas:ty} $fname:ident($($argname:ident: $argty:ty),*) -> $ret:ty
            $block:block
        )*
    ) => {
            $(
                $(#[$attr])*
                $vis fn $fname<Q: ?Sized>($($argname: $argty,)*) -> $ret
                where $borrowas: core::borrow::Borrow<Q>,
                Q: Eq + core::hash::Hash + AsRef<[u8]>,
                $block
            )*
    };
}

macro_rules! s_encoder {
    ($bool:expr) => {
        if $bool {
            $crate::kvengine::encoding::is_okay_encoded
        } else {
            $crate::kvengine::encoding::is_okay_no_encoding
        }
    };
    ($fn:expr, $t:expr) => {
        SingleEncoder {
            fn_ptr: $fn,
            v_t: $t,
        }
    };
}

macro_rules! s_encoder_booled {
    ($bool:expr) => {
        if $bool {
            s_encoder!($crate::kvengine::encoding::is_okay_encoded, TSYMBOL_UNICODE)
        } else {
            s_encoder!(
                $crate::kvengine::encoding::is_okay_no_encoding,
                TSYMBOL_BINARY
            )
        }
    };
}
