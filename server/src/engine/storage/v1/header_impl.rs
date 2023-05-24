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
 * |                       DYNAMIC RECORD                         |
 * |                         (256+56+?)B                          |
 * |        +--------------------------------------------+        |
 * |        |                                            |        |
 * |        |              METADATA RECORD               |        |
 * |        |                   256B                     |        |
 * |        +--------------------------------------------+        |
 * |        +--------------------------------------------+        |
 * |        |                                            |        |
 * |        |             GENESIS HOST RECORD            |        |
 * |        |                  >56B                      |        |
 * |        +--------------------------------------------+        |
 * |                                                              |
 * +--------------------------------------------------------------+
 * +--------------------------------------------------------------+
 * |                     RUNTIME HOST RECORD                      |
 * |                           >56B                               |
 * +--------------------------------------------------------------+
 * Note: The entire part of the header is little endian encoded
*/

use crate::engine::{
    mem::ByteStack,
    storage::{
        header::{StaticRecordUV, StaticRecordUVRaw},
        versions::{self, DriverVersion, ServerVersion},
    },
};

pub struct StaticRecord {
    sr: StaticRecordUV,
}

impl StaticRecord {
    pub const fn new(sr: StaticRecordUV) -> Self {
        Self { sr }
    }
    pub const fn encode(&self) -> StaticRecordRaw {
        StaticRecordRaw {
            base: self.sr.encode(),
        }
    }
    pub const fn sr(&self) -> &StaticRecordUV {
        &self.sr
    }
}

/// Static record
pub struct StaticRecordRaw {
    base: StaticRecordUVRaw,
}

impl StaticRecordRaw {
    pub const fn new() -> Self {
        Self {
            base: StaticRecordUVRaw::create(versions::v1::V1_HEADER_VERSION),
        }
    }
    pub const fn empty_buffer() -> [u8; sizeof!(Self)] {
        [0u8; sizeof!(Self)]
    }
    pub fn decode_from_bytes(buf: [u8; sizeof!(Self)]) -> Option<StaticRecord> {
        StaticRecordUVRaw::decode_from_bytes(buf).map(StaticRecord::new)
    }
}

/*
    Dynamic record (1/2)
    ---
    Metadata record (8B x 3 + (4B x 2)):
    +----------+----------+----------+---------+
    |  Server  |  Driver  |   File   |File|Spec|
    |  version |  Version |   Scope  |Spec|ID  |
    +----------+----------+----------+---------+
    0, 63
*/

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

pub struct MetadataRecord {
    server_version: ServerVersion,
    driver_version: DriverVersion,
    file_scope: FileScope,
    file_spec: FileSpecifier,
    file_spec_id: FileSpecifierVersion,
}

impl MetadataRecord {
    pub const fn new(
        server_version: ServerVersion,
        driver_version: DriverVersion,
        file_scope: FileScope,
        file_spec: FileSpecifier,
        file_spec_id: FileSpecifierVersion,
    ) -> Self {
        Self {
            server_version,
            driver_version,
            file_scope,
            file_spec,
            file_spec_id,
        }
    }
    pub const fn server_version(&self) -> ServerVersion {
        self.server_version
    }
    pub const fn driver_version(&self) -> DriverVersion {
        self.driver_version
    }
    pub const fn file_scope(&self) -> FileScope {
        self.file_scope
    }
    pub const fn file_spec(&self) -> FileSpecifier {
        self.file_spec
    }
    pub const fn file_spec_id(&self) -> FileSpecifierVersion {
        self.file_spec_id
    }
    pub const fn encode(&self) -> MetadataRecordRaw {
        MetadataRecordRaw::new_full(
            self.server_version(),
            self.driver_version(),
            self.file_scope(),
            self.file_spec(),
            self.file_spec_id(),
        )
    }
}

pub struct MetadataRecordRaw {
    data: ByteStack<32>,
}

impl MetadataRecordRaw {
    /// Decodes a given metadata record, validating all data for correctness.
    ///
    /// WARNING: That means you need to do contextual validation! This function is not aware of any context
    pub fn decode_from_bytes(data: [u8; 32]) -> Option<MetadataRecord> {
        let data = ByteStack::new(data);
        let server_version =
            ServerVersion::__new(u64::from_le(data.read_qword(Self::MDR_OFFSET_P0)));
        let driver_version =
            DriverVersion::__new(u64::from_le(data.read_qword(Self::MDR_OFFSET_P1)));
        let file_scope = FileScope::try_new(u64::from_le(data.read_qword(Self::MDR_OFFSET_P2)))?;
        let file_spec = FileSpecifier::try_new(u32::from_le(data.read_dword(Self::MDR_OFFSET_P3)))?;
        let file_spec_id =
            FileSpecifierVersion::__new(u32::from_le(data.read_dword(Self::MDR_OFFSET_P4)));
        Some(MetadataRecord::new(
            server_version,
            driver_version,
            file_scope,
            file_spec,
            file_spec_id,
        ))
    }
}

impl MetadataRecordRaw {
    const MDR_OFFSET_P0: usize = 0;
    const MDR_OFFSET_P1: usize = sizeof!(u64);
    const MDR_OFFSET_P2: usize = Self::MDR_OFFSET_P1 + sizeof!(u64);
    const MDR_OFFSET_P3: usize = Self::MDR_OFFSET_P2 + sizeof!(u64);
    const MDR_OFFSET_P4: usize = Self::MDR_OFFSET_P3 + sizeof!(u32);
    const _ENSURE: () = assert!(Self::MDR_OFFSET_P4 == (sizeof!(Self) - sizeof!(u32)));
    pub const fn empty_buffer() -> [u8; sizeof!(Self)] {
        [0u8; sizeof!(Self)]
    }
    pub const fn new_full(
        server_version: ServerVersion,
        driver_version: DriverVersion,
        scope: FileScope,
        specifier: FileSpecifier,
        specifier_id: FileSpecifierVersion,
    ) -> Self {
        let _ = Self::_ENSURE;
        let mut ret = [0u8; 32];
        let mut i = 0;
        // read buf
        let server_version = server_version.little_endian();
        let driver_version = driver_version.little_endian();
        let file_scope = scope.value_qword().to_le_bytes();
        // specifier + specifier ID
        let file_specifier_and_id: u64 = unsafe {
            core::mem::transmute([
                (specifier.value_u8() as u32).to_le(),
                specifier_id.0.to_le(),
            ])
        };
        let file_specifier_and_id = file_specifier_and_id.to_le_bytes();
        while i < sizeof!(u64) {
            ret[i] = server_version[i];
            ret[i + sizeof!(u64, 1)] = driver_version[i];
            ret[i + sizeof!(u64, 2)] = file_scope[i];
            ret[i + sizeof!(u64, 3)] = file_specifier_and_id[i];
            i += 1;
        }
        Self {
            data: ByteStack::new(ret),
        }
    }
    pub const fn new(
        scope: FileScope,
        specifier: FileSpecifier,
        specifier_id: FileSpecifierVersion,
    ) -> Self {
        Self::new_full(
            versions::v1::V1_SERVER_VERSION,
            versions::v1::V1_DRIVER_VERSION,
            scope,
            specifier,
            specifier_id,
        )
    }
}

impl MetadataRecordRaw {
    pub const fn read_p0_server_version(&self) -> ServerVersion {
        ServerVersion::__new(self.data.read_qword(Self::MDR_OFFSET_P0))
    }
    pub const fn read_p1_driver_version(&self) -> DriverVersion {
        DriverVersion::__new(self.data.read_qword(Self::MDR_OFFSET_P1))
    }
    pub const fn read_p2_file_scope(&self) -> FileScope {
        FileScope::new(self.data.read_qword(Self::MDR_OFFSET_P2))
    }
    pub const fn read_p3_file_spec(&self) -> FileSpecifier {
        FileSpecifier::new(self.data.read_dword(Self::MDR_OFFSET_P3))
    }
    pub const fn read_p4_file_spec_version(&self) -> FileSpecifierVersion {
        FileSpecifierVersion(self.data.read_dword(Self::MDR_OFFSET_P4))
    }
}

/*
    Dynamic Record (2/2)
    ---
    Host record (?B; > 56B):
    - 16B: Host epoch time in nanoseconds
    - 16B: Host uptime in nanoseconds
    - 08B:
     - 04B: Host setting version ID
     - 04B: Host run mode
    - 08B: Host startup counter
    - 08B: Host name length
    - ??B: Host name
*/

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

type HRConstSectionRaw = [u8; 56];

#[derive(Debug, PartialEq, Clone)]
pub struct HostRecord {
    hr_cr: HRConstSection,
    host_name: Box<[u8]>,
}

impl HostRecord {
    pub fn new(hr_cr: HRConstSection, host_name: Box<[u8]>) -> Self {
        Self { hr_cr, host_name }
    }
    pub fn hr_cr(&self) -> &HRConstSection {
        &self.hr_cr
    }
    pub fn host_name(&self) -> &[u8] {
        self.host_name.as_ref()
    }
    pub fn encode(&self) -> HostRecordRaw {
        HostRecordRaw::new(
            self.hr_cr().host_epoch_time(),
            self.hr_cr().host_uptime(),
            self.hr_cr().host_setting_version_id(),
            self.hr_cr().host_run_mode(),
            self.hr_cr().host_startup_counter(),
            self.host_name().into(),
        )
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HRConstSection {
    host_epoch_time: u128,
    host_uptime: u128,
    host_setting_version_id: u32,
    host_run_mode: HostRunMode,
    host_startup_counter: u64,
}

impl HRConstSection {
    pub const fn new(
        host_epoch_time: u128,
        host_uptime: u128,
        host_setting_version_id: u32,
        host_run_mode: HostRunMode,
        host_startup_counter: u64,
    ) -> Self {
        Self {
            host_epoch_time,
            host_uptime,
            host_setting_version_id,
            host_run_mode,
            host_startup_counter,
        }
    }
    pub const fn host_epoch_time(&self) -> u128 {
        self.host_epoch_time
    }
    pub const fn host_uptime(&self) -> u128 {
        self.host_uptime
    }
    pub const fn host_setting_version_id(&self) -> u32 {
        self.host_setting_version_id
    }
    pub const fn host_run_mode(&self) -> HostRunMode {
        self.host_run_mode
    }
    pub const fn host_startup_counter(&self) -> u64 {
        self.host_startup_counter
    }
}

pub struct HostRecordRaw {
    data: ByteStack<{ sizeof!(HRConstSectionRaw) }>,
    host_name: Box<[u8]>,
}

impl HostRecordRaw {
    pub const fn empty_buffer_const_section() -> [u8; sizeof!(HRConstSectionRaw)] {
        [0u8; sizeof!(HRConstSectionRaw)]
    }
    /// Decodes and validates the [`HRConstSection`] of a [`HostRecord`]. Use the returned result to construct this
    pub fn decode_from_bytes_const_sec(data: HRConstSectionRaw) -> Option<(HRConstSection, usize)> {
        let s = ByteStack::new(data);
        let host_epoch_time = s.read_xmmword(Self::HR_OFFSET_P0);
        if host_epoch_time > crate::util::os::get_epoch_time() {
            // and what? we have a file from the future. Einstein says hi. (ok, maybe the host time is incorrect)
            return None;
        }
        let host_uptime = s.read_xmmword(Self::HR_OFFSET_P1);
        let host_setting_version_id = s.read_dword(Self::HR_OFFSET_P2A);
        let host_setting_run_mode =
            HostRunMode::try_new_with_val(s.read_dword(Self::HR_OFFSET_P2B))?;
        let host_startup_counter = s.read_qword(Self::HR_OFFSET_P3);
        let host_name_length = s.read_qword(Self::HR_OFFSET_P4);
        if host_name_length as usize > usize::MAX {
            // too large for us to load. per DNS standards this shouldn't be more than 255 but who knows, some people like it wild
            return None;
        }
        Some((
            HRConstSection::new(
                host_epoch_time,
                host_uptime,
                host_setting_version_id,
                host_setting_run_mode,
                host_startup_counter,
            ),
            host_name_length as usize,
        ))
    }
    pub fn decoded(&self) -> HostRecord {
        HostRecord::new(
            HRConstSection::new(
                self.read_p0_epoch_time(),
                self.read_p1_uptime(),
                self.read_p2a_setting_version_id(),
                self.read_p2b_run_mode(),
                self.read_p3_startup_counter(),
            ),
            self.host_name.clone(),
        )
    }
}

impl HostRecordRaw {
    const HR_OFFSET_P0: usize = 0;
    const HR_OFFSET_P1: usize = sizeof!(u128);
    const HR_OFFSET_P2A: usize = Self::HR_OFFSET_P1 + sizeof!(u128);
    const HR_OFFSET_P2B: usize = Self::HR_OFFSET_P2A + sizeof!(u32);
    const HR_OFFSET_P3: usize = Self::HR_OFFSET_P2B + sizeof!(u32);
    const HR_OFFSET_P4: usize = Self::HR_OFFSET_P3 + sizeof!(u64);
    const _ENSURE: () = assert!(Self::HR_OFFSET_P4 == sizeof!(HRConstSectionRaw) - sizeof!(u64));
    pub fn new(
        p0_host_epoch_time: u128,
        p1_host_uptime: u128,
        p2a_host_setting_version_id: u32,
        p2b_host_run_mode: HostRunMode,
        p3_host_startup_counter: u64,
        p5_host_name: Box<[u8]>,
    ) -> Self {
        let _ = Self::_ENSURE;
        let p4_host_name_length = p5_host_name.len();
        let mut host_record_fl = [0u8; 56];
        host_record_fl[0..16].copy_from_slice(&p0_host_epoch_time.to_le_bytes());
        host_record_fl[16..32].copy_from_slice(&p1_host_uptime.to_le_bytes());
        host_record_fl[32..36].copy_from_slice(&p2a_host_setting_version_id.to_le_bytes());
        host_record_fl[36..40]
            .copy_from_slice(&(p2b_host_run_mode.value_u8() as u32).to_le_bytes());
        host_record_fl[40..48].copy_from_slice(&p3_host_startup_counter.to_le_bytes());
        host_record_fl[48..56].copy_from_slice(&(p4_host_name_length as u64).to_le_bytes());
        Self {
            data: ByteStack::new(host_record_fl),
            host_name: p5_host_name,
        }
    }
    pub fn new_auto(
        p2a_host_setting_version_id: u32,
        p2b_host_run_mode: HostRunMode,
        p3_host_startup_counter: u64,
        p5_host_name: Box<[u8]>,
    ) -> Self {
        let p0_host_epoch_time = crate::util::os::get_epoch_time();
        let p1_host_uptime = crate::util::os::get_uptime();
        Self::new(
            p0_host_epoch_time,
            p1_host_uptime,
            p2a_host_setting_version_id,
            p2b_host_run_mode,
            p3_host_startup_counter,
            p5_host_name,
        )
    }
}

impl HostRecordRaw {
    pub const fn read_p0_epoch_time(&self) -> u128 {
        self.data.read_xmmword(Self::HR_OFFSET_P0)
    }
    pub const fn read_p1_uptime(&self) -> u128 {
        self.data.read_xmmword(Self::HR_OFFSET_P1)
    }
    pub const fn read_p2a_setting_version_id(&self) -> u32 {
        self.data.read_dword(Self::HR_OFFSET_P2A)
    }
    pub const fn read_p2b_run_mode(&self) -> HostRunMode {
        HostRunMode::new_with_val(self.data.read_dword(Self::HR_OFFSET_P2B))
    }
    pub const fn read_p3_startup_counter(&self) -> u64 {
        self.data.read_qword(Self::HR_OFFSET_P3)
    }
    pub const fn read_p4_host_name_length(&self) -> u64 {
        self.data.read_qword(Self::HR_OFFSET_P4)
    }
    pub fn read_p5_host_name(&self) -> &[u8] {
        &self.host_name
    }
}

pub struct SDSSHeader {
    sr: StaticRecord,
    mdr: MetadataRecord,
    hr: HostRecord,
}

impl SDSSHeader {
    pub const fn new(sr: StaticRecord, mdr: MetadataRecord, hr: HostRecord) -> Self {
        Self { sr, mdr, hr }
    }
    pub const fn sr(&self) -> &StaticRecord {
        &self.sr
    }
    pub const fn mdr(&self) -> &MetadataRecord {
        &self.mdr
    }
    pub const fn hr(&self) -> &HostRecord {
        &self.hr
    }
    pub fn encode(&self) -> SDSSHeaderRaw {
        SDSSHeaderRaw::new_full(self.sr.encode(), self.mdr.encode(), self.hr.encode())
    }
}

pub struct SDSSHeaderRaw {
    sr: StaticRecordRaw,
    dr_0_mdr: MetadataRecordRaw,
    dr_1_hr: HostRecordRaw,
}

impl SDSSHeaderRaw {
    pub fn new_full(sr: StaticRecordRaw, mdr: MetadataRecordRaw, hr: HostRecordRaw) -> Self {
        Self {
            sr,
            dr_0_mdr: mdr,
            dr_1_hr: hr,
        }
    }
    pub fn new(
        sr: StaticRecordRaw,
        dr_0_mdr: MetadataRecordRaw,
        dr_1_hr_const_section: HRConstSectionRaw,
        dr_1_hr_host_name: Box<[u8]>,
    ) -> Self {
        Self {
            sr,
            dr_0_mdr,
            dr_1_hr: HostRecordRaw {
                data: ByteStack::new(dr_1_hr_const_section),
                host_name: dr_1_hr_host_name,
            },
        }
    }
    pub fn init(
        mdr_file_scope: FileScope,
        mdr_file_specifier: FileSpecifier,
        mdr_file_specifier_id: FileSpecifierVersion,
        hr_host_setting_id: u32,
        hr_host_run_mode: HostRunMode,
        hr_host_startup_counter: u64,
        hr_host_name: Box<[u8]>,
    ) -> Self {
        Self {
            sr: StaticRecordRaw::new(),
            dr_0_mdr: MetadataRecordRaw::new(
                mdr_file_scope,
                mdr_file_specifier,
                mdr_file_specifier_id,
            ),
            dr_1_hr: HostRecordRaw::new_auto(
                hr_host_setting_id,
                hr_host_run_mode,
                hr_host_startup_counter,
                hr_host_name,
            ),
        }
    }
    pub fn get0_sr(&self) -> &[u8] {
        self.sr.base.get_ref()
    }
    pub fn get1_dr_0_mdr(&self) -> &[u8] {
        self.dr_0_mdr.data.slice()
    }
    pub fn get1_dr_1_hr_0(&self) -> &[u8] {
        self.dr_1_hr.data.slice()
    }
    pub fn get1_dr_1_hr_1(&self) -> &[u8] {
        self.dr_1_hr.host_name.as_ref()
    }
    pub fn calculate_header_size(&self) -> usize {
        Self::calculate_fixed_header_size() + self.dr_1_hr.host_name.len()
    }
    pub const fn calculate_fixed_header_size() -> usize {
        sizeof!(StaticRecordRaw) + sizeof!(MetadataRecordRaw) + sizeof!(HRConstSectionRaw)
    }
}

#[test]
fn test_metadata_record_encode_decode() {
    let md = MetadataRecordRaw::new(
        FileScope::TransactionLog,
        FileSpecifier::GNSTxnLog,
        FileSpecifierVersion(1),
    );
    assert_eq!(md.read_p0_server_version(), versions::v1::V1_SERVER_VERSION);
    assert_eq!(md.read_p1_driver_version(), versions::v1::V1_DRIVER_VERSION);
    assert_eq!(md.read_p2_file_scope(), FileScope::TransactionLog);
    assert_eq!(md.read_p3_file_spec(), FileSpecifier::GNSTxnLog);
    assert_eq!(md.read_p4_file_spec_version(), FileSpecifierVersion(1));
}

#[test]
fn test_host_record_encode_decode() {
    const HOST_UPTIME: u128 = u128::MAX - 434324903;
    const HOST_SETTING_VERSION_ID: u32 = 245;
    const HOST_RUN_MODE: HostRunMode = HostRunMode::Prod;
    const HOST_STARTUP_COUNTER: u64 = u32::MAX as _;
    const HOST_NAME: &str = "skycloud";
    use std::time::*;
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let hr = HostRecordRaw::new(
        time,
        HOST_UPTIME,
        HOST_SETTING_VERSION_ID,
        HOST_RUN_MODE,
        HOST_STARTUP_COUNTER,
        HOST_NAME.as_bytes().to_owned().into_boxed_slice(),
    );
    assert_eq!(hr.read_p0_epoch_time(), time);
    assert_eq!(hr.read_p1_uptime(), HOST_UPTIME);
    assert_eq!(hr.read_p2a_setting_version_id(), HOST_SETTING_VERSION_ID);
    assert_eq!(hr.read_p2b_run_mode(), HOST_RUN_MODE);
    assert_eq!(hr.read_p3_startup_counter(), HOST_STARTUP_COUNTER);
    assert_eq!(hr.read_p4_host_name_length(), HOST_NAME.len() as u64);
    assert_eq!(hr.read_p5_host_name(), HOST_NAME.as_bytes());
}
