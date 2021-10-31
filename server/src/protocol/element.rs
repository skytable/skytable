/*
 * Created on Tue May 11 2021
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

use super::UnsafeSlice;
#[cfg(test)]
use bytes::Bytes;

#[non_exhaustive]
#[derive(Debug, PartialEq)]
/// # Unsafe elements
/// This enum represents the data types as **unsafe** elements, supported by the Skyhash Protocol
///
/// ## Safety
///
/// The instantiator must ensure that the [`UnsafeSlice`]s are valid. See its own safety contracts
/// for more information
pub enum UnsafeElement {
    /// Arrays can be nested! Their `<tsymbol>` is `&`
    Array(Box<[UnsafeElement]>),
    /// A String value; `<tsymbol>` is `+`
    String(UnsafeSlice),
    /// An unsigned integer value; `<tsymbol>` is `:`
    UnsignedInt(u64),
    /// A non-recursive String array; tsymbol: `_`
    FlatArray(Box<[UnsafeFlatElement]>),
    /// A type-less non-recursive array
    AnyArray(Box<[UnsafeSlice]>),
}

#[derive(Debug, PartialEq)]
/// An **unsafe** flat element, present in a flat array
pub enum UnsafeFlatElement {
    String(UnsafeSlice),
}

impl UnsafeElement {
    pub const fn is_any_array(&self) -> bool { 
        matches!(self, Self::AnyArray(_))
    }
}

// test impls are for our tests
#[cfg(test)]
impl UnsafeElement {
    pub unsafe fn to_owned_flat_array(inner: &[UnsafeFlatElement]) -> Vec<FlatElement> {
        inner
            .iter()
            .map(|v| match v {
                UnsafeFlatElement::String(st) => {
                    FlatElement::String(Bytes::copy_from_slice(st.as_slice()))
                }
            })
            .collect()
    }
    pub unsafe fn to_owned_any_array(inner: &[UnsafeSlice]) -> Vec<Bytes> {
        inner
            .iter()
            .map(|v| Bytes::copy_from_slice(v.as_slice()))
            .collect()
    }
    pub unsafe fn to_owned_array(inner: &[Self]) -> Vec<OwnedElement> {
        inner
            .iter()
            .map(|v| match &*v {
                UnsafeElement::String(st) => {
                    OwnedElement::String(Bytes::copy_from_slice(st.as_slice()))
                }
                UnsafeElement::UnsignedInt(int) => OwnedElement::UnsignedInt(*int),
                UnsafeElement::AnyArray(arr) => {
                    OwnedElement::AnyArray(Self::to_owned_any_array(arr))
                }
                UnsafeElement::Array(arr) => OwnedElement::Array(Self::to_owned_array(arr)),
                UnsafeElement::FlatArray(frr) => {
                    OwnedElement::FlatArray(Self::to_owned_flat_array(frr))
                }
            })
            .collect()
    }
    pub unsafe fn as_owned_element(&self) -> OwnedElement {
        match self {
            Self::AnyArray(arr) => OwnedElement::AnyArray(Self::to_owned_any_array(arr)),
            Self::FlatArray(frr) => OwnedElement::FlatArray(Self::to_owned_flat_array(frr)),
            Self::Array(arr) => OwnedElement::Array(Self::to_owned_array(arr)),
            Self::String(st) => OwnedElement::String(Bytes::copy_from_slice(st.as_slice())),
            Self::UnsignedInt(int) => OwnedElement::UnsignedInt(*int),
        }
    }
}

// owned variants to simplify equality in tests
#[derive(Debug, PartialEq)]
#[cfg(test)]
pub enum OwnedElement {
    Array(Vec<OwnedElement>),
    String(Bytes),
    UnsignedInt(u64),
    FlatArray(Vec<FlatElement>),
    AnyArray(Vec<Bytes>),
}

#[cfg(test)]
#[derive(Debug, PartialEq)]
pub enum FlatElement {
    String(Bytes),
}
