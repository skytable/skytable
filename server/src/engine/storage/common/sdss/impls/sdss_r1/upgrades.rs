/*
 * This file is a part of Skytable
 *
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

use {
    super::{HeaderV1, SimpleFileSpecV1},
    crate::{
        engine::{
            fractal::context,
            storage::common::interface::fs::{File, FileExt, FileSystem, FileWrite},
            RuntimeResult,
        },
        util::os,
    },
    std::path::Path,
};

pub fn upgrade_file_header<S: SimpleFileSpecV1>(
    orig_path: impl AsRef<Path>,
    orig_md: HeaderV1<S::HeaderSpec>,
) -> RuntimeResult<(File, HeaderV1<S::HeaderSpec>)> {
    let orig_path = orig_path.as_ref();
    info!(
        "upgrading file {} from v{} to v{}",
        orig_path.to_str().unwrap(),
        orig_md.file_specifier_version().version(),
        S::FILE_SPECIFIER_VERSION.version(),
    );
    // prepare md
    let md = HeaderV1::_new_auto(
        S::FILE_CLASS,
        S::FILE_SPECIFIER,
        S::FILE_SPECIFIER_VERSION,
        os::get_epoch_time(),
        [0u8; 8],
    );
    // create a temporary copy
    let upgraded_file_path = format!("{}.tmp", orig_path.to_str().unwrap());
    context::set_dmsg("creating tmp file");
    FileSystem::copy(orig_path, &upgraded_file_path)?;
    // open tmp file, overwrite header and close file
    context::set_dmsg("upgrading metadata");
    let mut f = File::open_with_options(&upgraded_file_path, false, true)?;
    f.f_seek_start(0)?;
    f.fwrite_all(&md.encode_self())?;
    drop(f);
    // replace
    context::set_dmsg("pointing to new upgraded file");
    FileSystem::rename(upgraded_file_path, orig_path)?;
    // reopen file
    context::set_dmsg("reopening upgraded file");
    let mut f = File::open_rw(orig_path)?;
    f.f_seek_start(HeaderV1::<S::HeaderSpec>::SIZE as u64)?;
    Ok((f, md))
}

#[cfg(test)]
#[cfg(all(target_os = "macos", not(miri)))]
mod test_upgrade {
    use {
        crate::engine::{
            error::StorageError,
            storage::{
                common::{
                    interface::fs::{FSContext, File, FileSystem},
                    sdss::sdss_r1::{rw::SdssFile, HeaderV1, SimpleFileSpecV1},
                    versions::FileSpecifierVersion,
                },
                v2::raw::spec::{FileClass, FileSpecifier, HeaderImplV2},
            },
            RuntimeResult,
        },
        std::path::Path,
    };

    struct TestUpgradeModelDataFileV1;
    impl SimpleFileSpecV1 for TestUpgradeModelDataFileV1 {
        type HeaderSpec = HeaderImplV2;
        const FILE_CLASS: FileClass = FileClass::Batch;
        const FILE_SPECIFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
        const FILE_SPECIFIER: FileSpecifier = FileSpecifier::ModelData;
    }
    struct TestUpgradeModelDataFileV2;
    impl SimpleFileSpecV1 for TestUpgradeModelDataFileV2 {
        type HeaderSpec = HeaderImplV2;
        const FILE_CLASS: FileClass = FileClass::Batch;
        const FILE_SPECIFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(1);
        const FILE_SPECIFIER: FileSpecifier = FileSpecifier::ModelData;
        fn upgrade(
            orig_path: impl AsRef<Path>,
            f: File,
            orig_md: HeaderV1<Self::HeaderSpec>,
        ) -> RuntimeResult<(File, HeaderV1<Self::HeaderSpec>)> {
            drop(f);
            if orig_md.file_specifier_version() == FileSpecifierVersion::__new(0) {
                // this is rev.0, so we can upgrade it
                super::upgrade_file_header::<Self>(orig_path, orig_md)
            } else {
                if orig_md.file_specifier_version() > Self::FILE_SPECIFIER_VERSION {
                    Err(StorageError::RuntimeUpgradeFailureFileIsNewer.into())
                } else {
                    // can't be the same version version!
                    unreachable!()
                }
            }
        }
    }
    #[sky_macros::test]
    fn upgrade_test() {
        const FILE_PATH: &str = "upgrade_test_file.db";
        const FILE_DATA: &[u8] = b"hello freaking world";
        FileSystem::set_context(FSContext::Local);
        let mut fs = FileSystem::instance();
        fs.mark_file_for_removal(FILE_PATH);
        // create the old file
        {
            let mut f = SdssFile::<TestUpgradeModelDataFileV1>::create(FILE_PATH).unwrap();
            f.write_buffer(FILE_DATA).unwrap();
            assert_eq!(
                f.meta().file_specifier_version(),
                FileSpecifierVersion::__new(0)
            );
        }
        // reopen the file. it should get upgraded
        drop(SdssFile::<TestUpgradeModelDataFileV2>::open_rw(FILE_PATH).unwrap());
        // reopen the file aga. it should be upgraded
        {
            let mut f = SdssFile::<TestUpgradeModelDataFileV2>::open_rw(FILE_PATH).unwrap();
            assert_eq!(f.read_full().unwrap(), FILE_DATA);
            assert_eq!(
                f.meta().file_specifier_version(),
                FileSpecifierVersion::__new(1)
            );
            assert_eq!(
                f.file_length().unwrap(),
                (FILE_DATA.len() + HeaderV1::<HeaderImplV2>::SIZE) as u64
            );
        }
    }
}
