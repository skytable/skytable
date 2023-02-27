/*
 * Created on Mon Feb 27 2023
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

/// This is a pretty complex macro that emulates the behavior of an enumeration by making use of flags and macro hacks. You might literally feel it's like a lang match, but nope,
/// there's a lot of wizardry beneath. Well, it's important to know that it works and you shouldn't touch it UNLESS YOU ABSOLUTELY KNOW what you're doing
macro_rules! match_data {
    (match ref $dataitem:ident $tail:tt) => {match_data!(@branch [ #[deny(unreachable_patterns)] match crate::engine::data::tag::DataTag::tag_class(&crate::engine::data::spec::DataspecMeta1D::kind($dataitem))] $dataitem [] $tail)};
    (match $dataitem:ident $tail:tt) => {match_data!(@branch [ #[deny(unreachable_patterns)] match crate::engine::data::tag::DataTag::tag_class(&crate::engine::data::spec::DataspecMeta1D::kind(&$dataitem))] $dataitem [] $tail)};
    (@branch $decl:tt $dataitem:ident [$($branch:tt)*] {}) => {match_data!(@defeat0 $decl [$($branch)*])};
    (@branch $decl:tt $dataitem:ident [$($branch:tt)*] { $(#[$attr:meta])* $name:ident::$variant:ident($capture:ident) => $ret:expr, $($tail:tt)*}) => {
        match_data!(@branch $decl $dataitem [$($branch)* $(#[$attr])* crate::engine::data::tag::TagClass::$variant => {let $capture = unsafe { /* UNSAFE(@ohsayan): flagck */ match_data!(@extract $name $dataitem $variant) }; $ret},] {$($tail)*})
    };
    (@branch $decl:tt $dataitem:ident [$($branch:tt)*] { $(#[$attr:meta])* $name:ident::$variant:ident(_) => $ret:expr, $($tail:tt)*}) => {
        match_data!(@branch $decl $dataitem [$($branch)* $(#[$attr])* crate::engine::data::tag::TagClass::$variant => $ret,] {$($tail)*})
    };
    (@branch $decl:tt $dataitem:ident [$($branch:tt)*] { $(#[$attr:meta])* $name:ident::$variant:ident($capture:ident) if $guard:expr => $ret:expr, $($tail:tt)*}) => {
        match_data!(@branch $decl $dataitem [$($branch)* $(#[$attr])* crate::engine::data::tag::TagClass::$variant if { let $capture = unsafe { /* UNSAFE(@ohsayan): flagck */ match_data!(@extract $name $dataitem $variant) }; $guard } => {
            let $capture = unsafe { /* UNSAFE(@ohsayan): flagck */  match_data!(@extract $name $dataitem $variant) }; let _ = &$capture; $ret}, ] {$($tail)*}
        )
    };
    (@branch $decl:tt $dataitem:ident [$($branch:tt)*] { $(#[$attr:meta])* $name:ident::$variant:ident(_) if $guard:expr => $ret:expr, $($tail:tt)*}) => {
        match_data!(@branch $decl $dataitem [$($branch)* $(#[$attr])* crate::engine::data::tag::TagClass::$variant if $guard => $ret,] {$($tail)*})
    };
    (@branch $decl:tt $dataitem:ident [$($branch:tt)*] { $(#[$attr:meta])* _ => $ret:expr, $($tail:tt)*}) => {
        match_data!(@branch $decl $dataitem [$($branch)* $(#[$attr])* _ => $ret,] {$($tail)*})
    };
    (@branch $decl:tt $dataitem:ident [$($branch:tt)*] { $(#[$attr:meta])* $capture:ident => $ret:expr, $($tail:tt)* }) => {
        match_data!(@branch $decl $dataitem [ $($branch)* $(#[$attr])* $capture => { $ret},] {$($tail:tt)*})
    };
    (@defeat0 [$($decl:tt)*] [$($branch:tt)*]) => {$($decl)* { $($branch)* }};
    (@extract $name:ident $dataitem:ident Bool) => {<$name as crate::engine::data::spec::Dataspec1D>::read_bool_uck(&$dataitem)};
    (@extract $name:ident $dataitem:ident UnsignedInt) => {<$name as crate::engine::data::spec::Dataspec1D>::read_uint_uck(&$dataitem)};
    (@extract $name:ident $dataitem:ident SignedInt) => {<$name as crate::engine::data::spec::Dataspec1D>::read_sint_uck(&$dataitem)};
    (@extract $name:ident $dataitem:ident Float) => {<$name as crate::engine::data::spec::Dataspec1D>::read_float_uck(&$dataitem)};
    (@extract $name:ident $dataitem:ident Bin) => {<$name as crate::engine::data::spec::Dataspec1D>::read_bin_uck(&$dataitem)};
    (@extract $name:ident $dataitem:ident Str) => {<$name as crate::engine::data::spec::Dataspec1D>::read_str_uck(&$dataitem)};
}
