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

/*
    Header specification
    ---
    We utilize two different kinds of headers:
    - Static header - Mostly to avoid data corruption
    - Variable header - For preserving dynamic information
*/

use {
    super::rw::{RawFSInterface, SDSSFileIO},
    crate::engine::{
        error::{RuntimeResult, StorageError},
        storage::common::versions::FileSpecifierVersion,
    },
};

/*
    meta
*/

/// The file scope
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
pub enum FileScope {
    Journal = 0,
    DataBatch = 1,
    FlatmapData = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum FileSpecifier {
    GNSTxnLog = 0,
    TableDataBatch = 1,
    SysDB = 2,
    #[cfg(test)]
    TestTransactionLog = 0xFF,
}

/// Specification for a SDSS file
pub trait FileSpec {
    /// The header spec for the file
    type Header: Header;
    /// Encode data
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs;
    /// Decode data
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs;
    /// Verify data
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs;
}

/*
    file spec impls
*/

#[cfg(test)]
pub(super) struct TestFile;
#[cfg(test)]
impl FileSpec for TestFile {
    type Header = super::Header;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::FlatmapData,
        FileSpecifier::TestTransactionLog,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/// The file specification for the GNS transaction log (impl v1)
pub(super) struct GNSTransactionLogV1;
impl FileSpec for GNSTransactionLogV1 {
    type Header = super::Header;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::Journal,
        FileSpecifier::GNSTxnLog,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/// The file specification for a journal batch
pub(super) struct DataBatchJournalV1;
impl FileSpec for DataBatchJournalV1 {
    type Header = super::Header;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::DataBatch,
        FileSpecifier::TableDataBatch,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/// The file specification for the system db
pub(super) struct SysDBV1;
impl FileSpec for SysDBV1 {
    type Header = super::Header;
    const ENCODE_DATA: <Self::Header as Header>::EncodeArgs = (
        FileScope::FlatmapData,
        FileSpecifier::SysDB,
        FileSpecifierVersion::__new(0),
    );
    const DECODE_DATA: <Self::Header as Header>::DecodeArgs = ();
    const VERIFY_DATA: <Self::Header as Header>::DecodeVerifyArgs = Self::ENCODE_DATA;
}

/*
    header spec
*/

/// SDSS Header specification
pub trait Header: Sized {
    /// Encode arguments
    type EncodeArgs;
    /// Decode arguments
    type DecodeArgs;
    /// Decode verify arguments
    type DecodeVerifyArgs;
    /// Encode the header
    fn encode<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        args: Self::EncodeArgs,
    ) -> RuntimeResult<()>;
    /// Decode the header
    fn decode<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        args: Self::DecodeArgs,
    ) -> RuntimeResult<Self>;
    /// Verify the header
    fn verify(&self, args: Self::DecodeVerifyArgs) -> RuntimeResult<()>;
    /// Decode and verify the header
    fn decode_verify<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        d_args: Self::DecodeArgs,
        v_args: Self::DecodeVerifyArgs,
    ) -> RuntimeResult<Self> {
        let h = Self::decode(f, d_args)?;
        h.verify(v_args)?;
        Ok(h)
    }
}

/*
    header impls
*/

impl Header for super::Header {
    type EncodeArgs = (FileScope, FileSpecifier, FileSpecifierVersion);
    type DecodeArgs = ();
    type DecodeVerifyArgs = Self::EncodeArgs;
    fn encode<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        (scope, spec, spec_v): Self::EncodeArgs,
    ) -> RuntimeResult<()> {
        let b = Self::_encode_auto(scope, spec, spec_v);
        f.fsynced_write(&b)
    }
    fn decode<Fs: RawFSInterface>(
        f: &mut SDSSFileIO<Fs>,
        _: Self::DecodeArgs,
    ) -> RuntimeResult<Self> {
        let mut buf = [0u8; 64];
        f.read_to_buffer(&mut buf)?;
        Self::decode(buf).map_err(Into::into)
    }
    fn verify(&self, (scope, spec, spec_v): Self::DecodeVerifyArgs) -> RuntimeResult<()> {
        if (self.file_class() == scope)
            & (self.file_specifier() == spec)
            & (self.file_specifier_version() == spec_v)
        {
            Ok(())
        } else {
            Err(StorageError::HeaderDecodeDataMismatch.into())
        }
    }
}
