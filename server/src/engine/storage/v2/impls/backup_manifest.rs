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
    crate::{
        engine::{
            control_flow::{errors::StorageError, RuntimeResult},
            mem::BufferedScanner,
            storage::{
                common::{
                    sdss::{
                        self,
                        sdss_r1::{rw::SdssFile, FileSpecV1},
                    },
                    versions::FileSpecifierVersion,
                },
                common_encoding::r1::{dec, enc, PersistObject},
                v2::raw::spec::{FileClass, FileSpecifier, HeaderImplV2},
            },
        },
        util::{compiler::TaggedEnum, os},
    },
    chrono::{NaiveDateTime, Utc},
    std::{marker::PhantomData, path::Path, str},
};

pub struct BackupManifestV1;
impl sdss::sdss_r1::SimpleFileSpecV1 for BackupManifestV1 {
    type HeaderSpec = HeaderImplV2;
    const FILE_CLASS: FileClass = FileClass::BackupMetadata;
    const FILE_SPECIFIER: FileSpecifier = FileSpecifier::BackupManifest;
    const FILE_SPECIFIER_VERSION: FileSpecifierVersion = FileSpecifierVersion::__new(0);
}

pub struct BackupManifestMetadata {
    context_md: u64,
    hostname_l: u64,
    datefmt_l: u64,
    description_l: u64,
}

impl BackupManifestMetadata {
    fn new(context_md: u64, hostname_l: u64, datefmt_l: u64, description_l: u64) -> Self {
        Self {
            context_md,
            hostname_l,
            datefmt_l,
            description_l,
        }
    }
}

struct BackupManifestStorage<'a>(PhantomData<&'a (String, String, Option<String>)>);

impl<'a> PersistObject for BackupManifestStorage<'a> {
    const METADATA_SIZE: usize = sizeof!(u64, 4);
    type InputType = &'a (BackupContext, String, String, Option<String>);
    type OutputType = BackupManifest;
    type Metadata = BackupManifestMetadata;
    fn pretest_can_dec_object(scanner: &BufferedScanner, md: &Self::Metadata) -> bool {
        scanner.has_left((md.datefmt_l + md.description_l + md.hostname_l) as usize)
    }
    fn meta_enc(buf: &mut Vec<u8>, (ctx, hostname, datefmt, dscr): Self::InputType) {
        buf.extend(ctx.value_qword().to_le_bytes());
        buf.extend((hostname.len() as u64).to_le_bytes());
        buf.extend((datefmt.len() as u64).to_le_bytes());
        buf.extend(
            (if let Some(dscr) = dscr {
                dscr.len() as u64
            } else {
                0
            })
            .to_le_bytes(),
        );
    }
    unsafe fn meta_dec(scanner: &mut BufferedScanner) -> RuntimeResult<Self::Metadata> {
        Ok(BackupManifestMetadata::new(
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
            scanner.next_u64_le(),
        ))
    }
    fn obj_enc(buf: &mut Vec<u8>, (_, hostname, datefmt, dscr): Self::InputType) {
        buf.extend(hostname.as_bytes());
        buf.extend(datefmt.as_bytes());
        if let Some(dscr) = dscr {
            buf.extend(dscr.as_bytes())
        }
    }
    unsafe fn obj_dec(
        s: &mut BufferedScanner,
        md: Self::Metadata,
    ) -> RuntimeResult<Self::OutputType> {
        let context = BackupContext::try_from_raw(
            md.context_md
                .try_into()
                .map_err(|_| StorageError::InternalDecodeStructureIllegalData)?,
        )
        .ok_or(StorageError::InternalDecodeStructureCorrupted)?;
        let hostname = dec::utils::decode_string(s, md.hostname_l as usize)?;
        let date = NaiveDateTime::parse_from_str(
            str::from_utf8(s.next_chunk_variable(md.datefmt_l as _))
                .map_err(|_| StorageError::InternalDecodeStructureCorruptedPayload)?,
            "%Y%m%d%H%M%S",
        )
        .map_err(|_| StorageError::InternalDecodeStructureCorruptedPayload)?;
        let dscr = dec::utils::decode_string(s, md.description_l as usize)?;
        Ok(BackupManifest {
            context,
            hostname,
            date,
            description: if dscr.is_empty() { None } else { Some(dscr) },
        })
    }
}

#[derive(Debug, PartialEq, Clone, Copy, sky_macros::TaggedEnum, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum BackupContext {
    BeforeRepair = 0,
    BeforeCompaction = 1,
    Manual = 2,
}

impl BackupContext {
    pub const fn context_str(&self) -> &'static str {
        match self {
            Self::BeforeRepair => "before a repair operation",
            Self::BeforeCompaction => "before a compaction operation",
            Self::Manual => "manually",
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct BackupManifest {
    context: BackupContext,
    hostname: String,
    date: NaiveDateTime,
    description: Option<String>,
}

impl BackupManifest {
    pub fn create(
        path: impl AsRef<Path>,
        context: BackupContext,
        description: Option<String>,
    ) -> RuntimeResult<()> {
        let mut file = SdssFile::<BackupManifestV1>::create(path)?;
        let backup_manifest = Self::generate(context, description);
        file.write_buffer(&backup_manifest)?;
        file.fsync_all()?;
        Ok(())
    }
    pub fn open(
        path: impl AsRef<Path>,
    ) -> RuntimeResult<(Self, <BackupManifestV1 as FileSpecV1>::Metadata)> {
        let mut f = SdssFile::<BackupManifestV1>::open_rw(path)?;
        let manifest_payload = f.read_full()?;
        let meta = f.into_meta();
        dec::full::<BackupManifestStorage>(&manifest_payload).map(|bm| (bm, meta))
    }
    fn generate(context: BackupContext, description: Option<String>) -> Vec<u8> {
        let date = Utc::now().format("%Y%m%d%H%M%S").to_string();
        let hostname = os::get_hostname().as_str().to_string();
        enc::full::<BackupManifestStorage>(&(context, hostname, date, description))
    }
    pub fn context(&self) -> BackupContext {
        self.context
    }
    pub fn hostname(&self) -> &str {
        &self.hostname
    }
    pub fn date(&self) -> NaiveDateTime {
        self.date
    }
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}
