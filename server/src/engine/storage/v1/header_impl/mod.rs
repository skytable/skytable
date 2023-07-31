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

/*
 * SDSS Header layout:
 *
 * +--------------------------------------------------------------+
 * |                                                              |
 * |                        STATIC RECORD                         |
 * |                            128B                              |
 * +--------------------------------------------------------------+
 * +--------------------------------------------------------------+
 * |                                                              |
 * |                                                              |
 * |                       GENESIS RECORD                         |
 * |                         (256+56+?)B                          |
 * |        +--------------------------------------------+        |
 * |        |                                            |        |
 * |        |              METADATA RECORD               |        |
 * |        |                   256B                     |        |
 * |        +--------------------------------------------+        |
 * |        +--------------------------------------------+        |
 * |        |                                            |        |
 * |        |               HOST RECORD                  |        |
 * |        |                  >56B                      |        |
 * |        +--------------------------------------------+        |
 * |                                                              |
 * +--------------------------------------------------------------+
 * +--------------------------------------------------------------+
 * |                       DYNAMIC RECORD                         |
 * |                           >56B                               |
 * +--------------------------------------------------------------+
 * Note: The entire part of the header is little endian encoded
*/

use crate::util::copy_slice_to_array as cp;

use super::SDSSResult;

// (1) sr
mod sr;
// (2) gr
mod gr;
// (3) dr
mod dr;

/// The file scope
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
pub enum FileScope {
    TransactionLog = 0,
    TransactionLogCompacted = 1,
}

impl FileScope {
    pub const fn try_new(id: u64) -> Option<Self> {
        Some(match id {
            0 => Self::TransactionLog,
            1 => Self::TransactionLogCompacted,
            _ => return None,
        })
    }
    pub const fn new(id: u64) -> Self {
        match Self::try_new(id) {
            Some(v) => v,
            None => panic!("unknown filescope"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum FileSpecifier {
    GNSTxnLog = 0,
}

impl FileSpecifier {
    pub const fn try_new(v: u32) -> Option<Self> {
        Some(match v {
            0 => Self::GNSTxnLog,
            _ => return None,
        })
    }
    pub const fn new(v: u32) -> Self {
        match Self::try_new(v) {
            Some(v) => v,
            _ => panic!("unknown filespecifier"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileSpecifierVersion(u32);
impl FileSpecifierVersion {
    pub const fn __new(v: u32) -> Self {
        Self(v)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum HostRunMode {
    Dev = 0,
    Prod = 1,
}

impl HostRunMode {
    pub const fn try_new_with_val(v: u32) -> Option<Self> {
        Some(match v {
            0 => Self::Dev,
            1 => Self::Prod,
            _ => return None,
        })
    }
    pub const fn new_with_val(v: u32) -> Self {
        match Self::try_new_with_val(v) {
            Some(v) => v,
            None => panic!("unknown hostrunmode"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SDSSHeader {
    // static record
    sr: sr::StaticRecord,
    // genesis record
    gr_mdr: gr::GRMetadataRecord,
    gr_hr: gr::GRHostRecord,
    // dynamic record
    dr_hs: dr::DRHostSignature,
    dr_rs: dr::DRRuntimeSignature,
}

impl SDSSHeader {
    pub fn verify(
        &self,
        expected_file_scope: FileScope,
        expected_file_specifier: FileSpecifier,
        expected_file_specifier_version: FileSpecifierVersion,
    ) -> SDSSResult<()> {
        self.sr().verify()?;
        self.gr_mdr().verify(
            expected_file_scope,
            expected_file_specifier,
            expected_file_specifier_version,
        )?;
        self.gr_hr().verify()?;
        self.dr_hs().verify(expected_file_specifier_version)?;
        self.dr_rs().verify()?;
        Ok(())
    }
}

impl SDSSHeader {
    pub const fn new(
        sr: sr::StaticRecord,
        gr_mdr: gr::GRMetadataRecord,
        gr_hr: gr::GRHostRecord,
        dr_hs: dr::DRHostSignature,
        dr_rs: dr::DRRuntimeSignature,
    ) -> Self {
        Self {
            sr,
            gr_mdr,
            gr_hr,
            dr_hs,
            dr_rs,
        }
    }
    pub fn sr(&self) -> &sr::StaticRecord {
        &self.sr
    }
    pub fn gr_mdr(&self) -> &gr::GRMetadataRecord {
        &self.gr_mdr
    }
    pub fn gr_hr(&self) -> &gr::GRHostRecord {
        &self.gr_hr
    }
    pub fn dr_hs(&self) -> &dr::DRHostSignature {
        &self.dr_hs
    }
    pub fn dr_rs(&self) -> &dr::DRRuntimeSignature {
        &self.dr_rs
    }
    pub fn dr_rs_mut(&mut self) -> &mut dr::DRRuntimeSignature {
        &mut self.dr_rs
    }
    pub fn encoded(&self) -> SDSSHeaderRaw {
        SDSSHeaderRaw::new_full(
            self.sr.encoded(),
            self.gr_mdr().encoded(),
            self.gr_hr().encoded(),
            self.dr_hs().encoded(),
            self.dr_rs().encoded(),
        )
    }
}

#[derive(Clone)]
pub struct SDSSHeaderRaw {
    sr: sr::StaticRecordRaw,
    gr_0_mdr: gr::GRMetadataRecordRaw,
    gr_1_hr: gr::GRHostRecordRaw,
    dr_0_hs: dr::DRHostSignatureRaw,
    dr_1_rs: dr::DRRuntimeSignatureRaw,
}

impl SDSSHeaderRaw {
    const OFFSET_SR0: usize = 0;
    const OFFSET_SR1: usize = sizeof!(sr::StaticRecordRaw);
    const OFFSET_SR2: usize = Self::OFFSET_SR1 + sizeof!(gr::GRMetadataRecordRaw);
    const OFFSET_SR3: usize = Self::OFFSET_SR2 + sizeof!(gr::GRHostRecordRaw);
    const OFFSET_SR4: usize = Self::OFFSET_SR3 + sizeof!(dr::DRHostSignatureRaw);
    pub fn new_auto(
        gr_mdr_scope: FileScope,
        gr_mdr_specifier: FileSpecifier,
        gr_mdr_specifier_id: FileSpecifierVersion,
        gr_hr_setting_version: u32,
        gr_hr_run_mode: HostRunMode,
        gr_hr_startup_counter: u64,
        dr_rts_modify_count: u64,
    ) -> Self {
        Self::new_full(
            sr::StaticRecordRaw::new_auto(),
            gr::GRMetadataRecordRaw::new_auto(gr_mdr_scope, gr_mdr_specifier, gr_mdr_specifier_id),
            gr::GRHostRecordRaw::new_auto(
                gr_hr_setting_version,
                gr_hr_run_mode,
                gr_hr_startup_counter,
            ),
            dr::DRHostSignatureRaw::new_auto(gr_mdr_specifier_id),
            dr::DRRuntimeSignatureRaw::new_auto(dr_rts_modify_count),
        )
    }
    pub fn new_full(
        sr: sr::StaticRecordRaw,
        gr_mdr: gr::GRMetadataRecordRaw,
        gr_hr: gr::GRHostRecordRaw,
        dr_hs: dr::DRHostSignatureRaw,
        dr_rs: dr::DRRuntimeSignatureRaw,
    ) -> Self {
        Self {
            sr,
            gr_0_mdr: gr_mdr,
            gr_1_hr: gr_hr,
            dr_0_hs: dr_hs,
            dr_1_rs: dr_rs,
        }
    }
    pub fn new(
        sr: sr::StaticRecordRaw,
        gr_0_mdr: gr::GRMetadataRecordRaw,
        gr_1_hr: gr::GRHostRecordRaw,
        dr_hs: dr::DRHostSignatureRaw,
        dr_rs: dr::DRRuntimeSignatureRaw,
    ) -> Self {
        Self {
            sr,
            gr_0_mdr,
            gr_1_hr,
            dr_0_hs: dr_hs,
            dr_1_rs: dr_rs,
        }
    }
    pub fn get0_sr(&self) -> &[u8] {
        self.sr.base.get_ref()
    }
    pub fn get1_dr_0_mdr(&self) -> &[u8] {
        self.gr_0_mdr.data.slice()
    }
    pub fn get1_dr_1_hr_0(&self) -> &[u8] {
        self.gr_1_hr.data.slice()
    }
    pub const fn header_size() -> usize {
        sizeof!(sr::StaticRecordRaw)
            + sizeof!(gr::GRMetadataRecordRaw)
            + sizeof!(gr::GRHostRecordRaw)
            + sizeof!(dr::DRHostSignatureRaw)
            + sizeof!(dr::DRRuntimeSignatureRaw)
    }
    pub fn array(&self) -> [u8; Self::header_size()] {
        let mut data = [0u8; Self::header_size()];
        data[Self::OFFSET_SR0..Self::OFFSET_SR1].copy_from_slice(self.sr.base.get_ref());
        data[Self::OFFSET_SR1..Self::OFFSET_SR2].copy_from_slice(self.gr_0_mdr.data.slice());
        data[Self::OFFSET_SR2..Self::OFFSET_SR3].copy_from_slice(self.gr_1_hr.data.slice());
        data[Self::OFFSET_SR3..Self::OFFSET_SR4].copy_from_slice(self.dr_0_hs.data.slice());
        data[Self::OFFSET_SR4..].copy_from_slice(self.dr_1_rs.data.slice());
        data
    }
    /// **☢ WARNING ☢: This only decodes; it doesn't validate expected values!**
    pub fn decode_noverify(slice: [u8; Self::header_size()]) -> Option<SDSSHeader> {
        let sr =
            sr::StaticRecordRaw::decode_noverify(cp(&slice[Self::OFFSET_SR0..Self::OFFSET_SR1]))?;
        let gr_mdr = gr::GRMetadataRecordRaw::decode_noverify(cp(
            &slice[Self::OFFSET_SR1..Self::OFFSET_SR2]
        ))?;
        let gr_hr =
            gr::GRHostRecord::decode_noverify(cp(&slice[Self::OFFSET_SR2..Self::OFFSET_SR3]))?;
        let dr_sig =
            dr::DRHostSignature::decode_noverify(cp(&slice[Self::OFFSET_SR3..Self::OFFSET_SR4]))?;
        let dr_rt = dr::DRRuntimeSignature::decode_noverify(cp(&slice[Self::OFFSET_SR4..]))?;
        Some(SDSSHeader::new(sr, gr_mdr, gr_hr, dr_sig, dr_rt))
    }
}
