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
 * |        |            VARIABLE HOST RECORD            |        |
 * |        |                  >56B                      |        |
 * |        +--------------------------------------------+        |
 * +--------------------------------------------------------------+
 *
 * Note: The entire part of the header is little endian encoded
*/

use crate::engine::storage::{
    header::{FileScope, StaticRecordUV},
    versions::{self, DriverVersion, ServerVersion},
};

/// Static record
pub struct StaticRecord {
    base: StaticRecordUV,
}

impl StaticRecord {
    pub const fn new() -> Self {
        Self {
            base: StaticRecordUV::create(versions::v1::V1_HEADER_VERSION),
        }
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
*/

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum FileSpecifier {
    GNSTxnLog = 0,
}

impl FileSpecifier {
    pub const fn new(v: u32) -> Self {
        match v {
            0 => Self::GNSTxnLog,
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
    data: [u8; 32],
}

impl_stack_read_primitives!(unsafe impl for MetadataRecord {});

impl MetadataRecord {
    pub const fn new(
        scope: FileScope,
        specifier: FileSpecifier,
        specifier_id: FileSpecifierVersion,
    ) -> Self {
        let mut ret = [0u8; 32];
        let mut i = 0;
        // read buf
        let server_version = versions::v1::V1_SERVER_VERSION.little_endian();
        let driver_version = versions::v1::V1_DRIVER_VERSION.little_endian();
        let file_scope = scope.value_qword().to_le_bytes();
        // specifier + specifier ID
        let file_specifier_and_id: u64 =
            unsafe { core::mem::transmute([specifier.value_u8() as u32, specifier_id.0]) };
        let file_specifier_and_id = file_specifier_and_id.to_le_bytes();
        while i < sizeof!(u64) {
            ret[i] = server_version[i];
            ret[i + sizeof!(u64, 1)] = driver_version[i];
            ret[i + sizeof!(u64, 2)] = file_scope[i];
            ret[i + sizeof!(u64, 3)] = file_specifier_and_id[i];
            i += 1;
        }
        Self { data: ret }
    }
    pub const fn read_p0_server_version(&self) -> ServerVersion {
        ServerVersion::__new(self.read_qword(0))
    }
    pub const fn read_p1_driver_version(&self) -> DriverVersion {
        DriverVersion::__new(self.read_qword(sizeof!(u64)))
    }
    pub const fn read_p2_file_scope(&self) -> FileScope {
        FileScope::new(self.read_qword(sizeof!(u128)) as u32)
    }
    pub const fn read_p3_file_spec(&self) -> FileSpecifier {
        FileSpecifier::new(self.read_dword(sizeof!(u64, 3)))
    }
    pub const fn read_p4_file_spec_version(&self) -> FileSpecifierVersion {
        FileSpecifierVersion(self.read_dword(sizeof!(u64, 3) + sizeof!(u32)))
    }
}

/*
    Dynamic Record (2/2)
    ---
    Variable record (?B; > 56B):
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
    pub const fn new_with_val(v: u8) -> Self {
        match v {
            0 => Self::Dev,
            1 => Self::Prod,
            _ => panic!("unknown hostrunmode"),
        }
    }
}

type VHRConstSection = [u8; 56];

impl_stack_read_primitives!(unsafe impl for VariableHostRecord {});

pub struct VariableHostRecord {
    data: VHRConstSection,
    host_name: Box<[u8]>,
}

impl VariableHostRecord {
    pub fn new(
        p0_host_epoch_time: u128,
        p1_host_uptime: u128,
        p2a_host_setting_version_id: u32,
        p2b_host_run_mode: HostRunMode,
        p3_host_startup_counter: u64,
        p5_host_name: Box<[u8]>,
    ) -> Self {
        let p4_host_name_length = p5_host_name.len();
        let mut variable_record_fl = [0u8; 56];
        variable_record_fl[0..16].copy_from_slice(&p0_host_epoch_time.to_le_bytes());
        variable_record_fl[16..32].copy_from_slice(&p1_host_uptime.to_le_bytes());
        variable_record_fl[32..36].copy_from_slice(&p2a_host_setting_version_id.to_le_bytes());
        variable_record_fl[36..40]
            .copy_from_slice(&(p2b_host_run_mode.value_u8() as u32).to_le_bytes());
        variable_record_fl[40..48].copy_from_slice(&p3_host_startup_counter.to_le_bytes());
        variable_record_fl[48..56].copy_from_slice(&(p4_host_name_length as u64).to_le_bytes());
        Self {
            data: variable_record_fl,
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
    pub const fn read_p0_epoch_time(&self) -> u128 {
        self.read_xmmword(0)
    }
    pub const fn read_p1_uptime(&self) -> u128 {
        self.read_xmmword(sizeof!(u128))
    }
    pub const fn read_p2a_setting_version_id(&self) -> u32 {
        self.read_dword(sizeof!(u128, 2))
    }
    pub const fn read_p2b_run_mode(&self) -> HostRunMode {
        HostRunMode::new_with_val(self.read_dword(sizeof!(u128, 2) + sizeof!(u32)) as u8)
    }
    pub const fn read_p3_startup_counter(&self) -> u64 {
        self.read_qword(sizeof!(u128, 2) + sizeof!(u32, 2))
    }
    pub const fn read_p4_host_name_length(&self) -> u64 {
        self.read_qword(sizeof!(u128, 2) + sizeof!(u32, 2) + sizeof!(u64))
    }
    pub fn read_p5_host_name(&self) -> &[u8] {
        &self.host_name
    }
}

pub struct SDSSHeader {
    sr: StaticRecord,
    dr_0_mdr: MetadataRecord,
    dr_1_vhr: VariableHostRecord,
}

impl SDSSHeader {
    pub fn new(
        sr: StaticRecord,
        dr_0_mdr: MetadataRecord,
        dr_1_vhr_const_section: VHRConstSection,
        dr_1_vhr_host_name: Box<[u8]>,
    ) -> Self {
        Self {
            sr,
            dr_0_mdr,
            dr_1_vhr: VariableHostRecord {
                data: dr_1_vhr_const_section,
                host_name: dr_1_vhr_host_name,
            },
        }
    }
    pub fn init(
        mdr_file_scope: FileScope,
        mdr_file_specifier: FileSpecifier,
        mdr_file_specifier_id: FileSpecifierVersion,
        vhr_host_setting_id: u32,
        vhr_host_run_mode: HostRunMode,
        vhr_host_startup_counter: u64,
        vhr_host_name: Box<[u8]>,
    ) -> Self {
        Self {
            sr: StaticRecord::new(),
            dr_0_mdr: MetadataRecord::new(
                mdr_file_scope,
                mdr_file_specifier,
                mdr_file_specifier_id,
            ),
            dr_1_vhr: VariableHostRecord::new_auto(
                vhr_host_setting_id,
                vhr_host_run_mode,
                vhr_host_startup_counter,
                vhr_host_name,
            ),
        }
    }
    pub fn get0_sr(&self) -> &[u8] {
        self.sr.base.get_ref()
    }
    pub fn get1_dr_0_mdr(&self) -> &[u8] {
        &self.dr_0_mdr.data
    }
    pub fn get1_dr_1_vhr_0(&self) -> &[u8] {
        &self.dr_1_vhr.data
    }
    pub fn get1_dr_1_vhr_1(&self) -> &[u8] {
        self.dr_1_vhr.host_name.as_ref()
    }
    pub fn calculate_header_size(&self) -> usize {
        sizeof!(StaticRecord)
            + sizeof!(MetadataRecord)
            + sizeof!(VHRConstSection)
            + self.dr_1_vhr.host_name.len()
    }
}

#[test]
fn test_metadata_record_encode_decode() {
    let md = MetadataRecord::new(
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
fn test_variable_host_record_encode_decode() {
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
    let vhr = VariableHostRecord::new(
        time,
        HOST_UPTIME,
        HOST_SETTING_VERSION_ID,
        HOST_RUN_MODE,
        HOST_STARTUP_COUNTER,
        HOST_NAME.as_bytes().to_owned().into_boxed_slice(),
    );
    assert_eq!(vhr.read_p0_epoch_time(), time);
    assert_eq!(vhr.read_p1_uptime(), HOST_UPTIME);
    assert_eq!(vhr.read_p2a_setting_version_id(), HOST_SETTING_VERSION_ID);
    assert_eq!(vhr.read_p2b_run_mode(), HOST_RUN_MODE);
    assert_eq!(vhr.read_p3_startup_counter(), HOST_STARTUP_COUNTER);
    assert_eq!(vhr.read_p4_host_name_length(), HOST_NAME.len() as u64);
    assert_eq!(vhr.read_p5_host_name(), HOST_NAME.as_bytes());
}
