/*
 * Created on Mon May 15 2023
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

//! # Versioning
//!
//! Storage engine versioning utility
//!

pub mod server_version;

pub const HEADER_V1: HeaderVersion = HeaderVersion(0);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
/// The header version
///
/// The header version is part of the static record and *barely* changes (almost like once in a light year)
pub struct HeaderVersion(u64);

impl HeaderVersion {
    pub const fn __new(v: u64) -> Self {
        Self(v)
    }
    pub const fn little_endian_u64(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
/// The server version (based on tag index)
pub struct ServerVersion(u64);

impl ServerVersion {
    pub const fn __new(v: u64) -> Self {
        Self(v)
    }
    pub const fn little_endian(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
/// The driver version
pub struct DriverVersion(u64);

impl DriverVersion {
    pub const fn __new(v: u64) -> Self {
        Self(v)
    }
    pub const fn little_endian(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
/// The file specifier version
pub struct FileSpecifierVersion(u16);

impl FileSpecifierVersion {
    pub const fn __new(v: u16) -> Self {
        Self(v)
    }
    pub const fn little_endian(&self) -> [u8; 2] {
        self.0.to_le_bytes()
    }
}

pub mod v1 {
    //! The first SDSS based storage engine implementation.
    //! Target tag: 0.8.0 {beta.1, beta.2, beta.3}
    use super::{DriverVersion, HeaderVersion, ServerVersion};

    /// The SDSS header version UID
    pub const V1_HEADER_VERSION: HeaderVersion = HeaderVersion(0);
    /// The server version UID
    pub const V1_SERVER_VERSION: ServerVersion =
        ServerVersion(super::server_version::fetch_id("v0.8.0") as _);
    /// The driver version UID
    pub const V1_DRIVER_VERSION: DriverVersion = DriverVersion(0);
}

#[allow(unused)]
pub mod v2 {
    //! The second SDSS based storage implementation
    //!
    //! Target tag: 0.8.0 (GA)
    //!
    //! Same tags as [`super::v1`] but different [`DriverVersion`]
    use super::{DriverVersion, HeaderVersion, ServerVersion};
    pub const V2_HEADER_VERSION: HeaderVersion = super::v1::V1_HEADER_VERSION;
    pub const V2_SERVER_VERSION: ServerVersion = super::v1::V1_SERVER_VERSION;
    pub const V2_DRIVER_VERSION: DriverVersion = DriverVersion(1);
}
