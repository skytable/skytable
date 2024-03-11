/*
 * Created on Mon Sep 25 2023
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

use crate::engine::storage::common::{
    sdss,
    versions::{self, DriverVersion, FileSpecifierVersion, ServerVersion},
};

pub type Header = sdss::sdss_r1::HeaderV1<HeaderImplV1>;

#[derive(Debug)]
pub struct HeaderImplV1;
impl sdss::sdss_r1::HeaderV1Spec for HeaderImplV1 {
    type FileClass = FileScope;
    type FileSpecifier = FileSpecifier;
    const CURRENT_SERVER_VERSION: ServerVersion = versions::v1::V1_SERVER_VERSION;
    const CURRENT_DRIVER_VERSION: DriverVersion = versions::v1::V1_DRIVER_VERSION;
}

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
pub enum FileScope {
    Journal = 0,
    DataBatch = 1,
    FlatmapData = 2,
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
    GNSTxnLog = 0,
    TableDataBatch = 1,
    SysDB = 2,
    #[cfg(test)]
    TestTransactionLog = 0xFF,
}

/*
    file spec impls
*/

#[cfg(test)]
pub struct TestFile;
#[cfg(test)]
impl sdss::sdss_r1::SimpleFileSpecV1 for TestFile {
    type HeaderSpec = HeaderImplV1;
    const FILE_CLASS: FileScope = FileScope::FlatmapData;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::TestTransactionLog;
    const FILE_SPECFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}

/// The file specification for the GNS transaction log (impl v1)
pub struct GNSTransactionLogV1;
impl sdss::sdss_r1::SimpleFileSpecV1 for GNSTransactionLogV1 {
    type HeaderSpec = HeaderImplV1;
    const FILE_CLASS: FileScope = FileScope::Journal;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::GNSTxnLog;
    const FILE_SPECFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}

/// The file specification for a journal batch
pub struct DataBatchJournalV1;
impl sdss::sdss_r1::SimpleFileSpecV1 for DataBatchJournalV1 {
    type HeaderSpec = HeaderImplV1;
    const FILE_CLASS: FileScope = FileScope::DataBatch;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::TableDataBatch;
    const FILE_SPECFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}

/// The file specification for the system db
pub struct SysDBV1;
impl sdss::sdss_r1::SimpleFileSpecV1 for SysDBV1 {
    type HeaderSpec = HeaderImplV1;
    const FILE_CLASS: FileScope = FileScope::FlatmapData;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::SysDB;
    const FILE_SPECFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}
