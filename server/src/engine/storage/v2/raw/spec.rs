/*
 * Created on Thu Jan 11 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use crate::engine::storage::common::{
    sdss::{self, sdss_r1::HeaderV1},
    versions::{self, DriverVersion, FileSpecifierVersion, ServerVersion},
};

#[allow(unused)]
pub type Header = HeaderV1<HeaderImplV2>;

/// The file scope
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    sky_macros::EnumMethods,
    sky_macros::TaggedEnum,
)]
pub enum FileClass {
    EventLog = 0,
    Batch = 1,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    sky_macros::EnumMethods,
    sky_macros::TaggedEnum,
)]
#[repr(u8)]
pub enum FileSpecifier {
    GlobalNS = 0,
    ModelData = 1,
}

#[derive(Debug)]
pub struct HeaderImplV2;
impl sdss::sdss_r1::HeaderV1Spec for HeaderImplV2 {
    type FileClass = FileClass;
    type FileSpecifier = FileSpecifier;
    const CURRENT_SERVER_VERSION: ServerVersion = versions::v2::V2_SERVER_VERSION;
    const CURRENT_DRIVER_VERSION: DriverVersion = versions::v2::V2_DRIVER_VERSION;
}

pub struct SystemDatabaseV1;
impl sdss::sdss_r1::SimpleFileSpecV1 for SystemDatabaseV1 {
    type HeaderSpec = HeaderImplV2;
    const FILE_CLASS: FileClass = FileClass::EventLog;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::GlobalNS;
    const FILE_SPECFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}

pub struct ModelDataBatchAofV1;
impl sdss::sdss_r1::SimpleFileSpecV1 for ModelDataBatchAofV1 {
    type HeaderSpec = HeaderImplV2;
    const FILE_CLASS: FileClass = FileClass::Batch;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::ModelData;
    const FILE_SPECFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}
