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

// (1) sr
mod sr;
// (2) gr
mod gr;
// (3) dr
mod dr;

use crate::engine::mem::ByteStack;

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

#[derive(Debug, PartialEq)]
pub struct SDSSHeader {
    // static record
    sr: sr::StaticRecord,
    // genesis record
    gr_mdr: gr::MetadataRecord,
    gr_hr: gr::HostRecord,
    // dynamic record
    dr_hs: dr::DRHostSignature,
    dr_rs: dr::DRRuntimeSignature,
}

impl SDSSHeader {
    pub const fn new(
        sr: sr::StaticRecord,
        gr_mdr: gr::MetadataRecord,
        gr_hr: gr::HostRecord,
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
    pub fn encode(&self) -> SDSSHeaderRaw {
        SDSSHeaderRaw::new_full(
            self.sr.encode(),
            self.gr_mdr.encode(),
            self.gr_hr.encode(),
            self.dr_hs().encoded(),
            self.dr_rs().encoded(),
        )
    }
    pub fn sr(&self) -> &sr::StaticRecord {
        &self.sr
    }
    pub fn gr_mdr(&self) -> &gr::MetadataRecord {
        &self.gr_mdr
    }
    pub fn gr_hr(&self) -> &gr::HostRecord {
        &self.gr_hr
    }
    pub fn dr_hs(&self) -> &dr::DRHostSignature {
        &self.dr_hs
    }
    pub fn dr_rs(&self) -> &dr::DRRuntimeSignature {
        &self.dr_rs
    }
}

pub struct SDSSHeaderRaw {
    sr: sr::StaticRecordRaw,
    gr_0_mdr: gr::MetadataRecordRaw,
    gr_1_hr: gr::HostRecordRaw,
    dr_0_hs: dr::DRHostSignatureRaw,
    dr_1_rs: dr::DRRuntimeSignatureRaw,
}

impl SDSSHeaderRaw {
    pub fn new_full(
        sr: sr::StaticRecordRaw,
        gr_mdr: gr::MetadataRecordRaw,
        gr_hr: gr::HostRecordRaw,
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
        gr_0_mdr: gr::MetadataRecordRaw,
        gr_1_hr_const_section: gr::HRConstSectionRaw,
        gr_1_hr_host_name: Box<[u8]>,
        dr_hs: dr::DRHostSignatureRaw,
        dr_rs_const: dr::DRRuntimeSignatureFixedRaw,
        dr_rs_host_name: Box<[u8]>,
    ) -> Self {
        Self {
            sr,
            gr_0_mdr,
            gr_1_hr: gr::HostRecordRaw {
                data: ByteStack::new(gr_1_hr_const_section),
                host_name: gr_1_hr_host_name,
            },
            dr_0_hs: dr_hs,
            dr_1_rs: dr::DRRuntimeSignatureRaw::new_with_sections(dr_rs_host_name, dr_rs_const),
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
    pub fn get1_dr_1_hr_1(&self) -> &[u8] {
        self.gr_1_hr.host_name.as_ref()
    }
    pub fn calculate_header_size(&self) -> usize {
        Self::calculate_fixed_header_size()
            + self.gr_1_hr.host_name.len()
            + self.dr_1_rs.host_name.len()
    }
    pub const fn calculate_fixed_header_size() -> usize {
        sizeof!(sr::StaticRecordRaw)
            + sizeof!(gr::MetadataRecordRaw)
            + sizeof!(gr::HRConstSectionRaw)
            + sizeof!(dr::DRHostSignatureRaw)
            + sizeof!(dr::DRRuntimeSignatureFixedRaw)
    }
}
