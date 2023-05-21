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
    SDSS Header
    ---
    SDSS headers have two sections:
        - Static record: fixed-size record with fixed-layout
        - Dynamic record: variable-size record with version-dependent layout (> 256B)
    +--------------------------------------------------------------+
    |                                                              |
    |                        STATIC RECORD                         |
    |                            128B                              |
    +--------------------------------------------------------------+
    +--------------------------------------------------------------+
    |                                                              |
    |                                                              |
    |                       DYNAMIC RECORD                         |
    |                          (256+?)B                            |
    |                                                              |
    +--------------------------------------------------------------+

    We collectively define this as the SDSS Header. We'll attempt to statically compute
    most of the sections, but for variable records we can't do the same. Also, our target
    is to keep the SDSS Header at around 4K with page-padding.
*/

/*
    Static record
    ---
    [MAGIC (8B), [HEADER_VERSION(4B), PTR_WIDTH(1B), ENDIAN(1B), ARCH(1B), OPERATING SYSTEM(1B)]]

    ☢ HEADS UP: Static record is always little endian ☢
*/

use super::versions::HeaderVersion;

const SR0_MAGIC: u64 = 0x4F48534159414E21;
const SR2_PTR_WIDTH: u8 = HostPointerWidth::new().value_u8();
const SR3_ENDIAN: u8 = HostEndian::new().value_u8();
const SR4_ARCH: u8 = HostArch::new().value_u8();
const SR5_OS: u8 = HostOS::new().value_u8();

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
pub enum HostArch {
    X86 = 0,
    X86_64 = 1,
    ARM = 2,
    ARM64 = 3,
    MIPS = 4,
    PowerPC = 5,
}

impl HostArch {
    pub const fn new() -> Self {
        if cfg!(target_arch = "x86") {
            HostArch::X86
        } else if cfg!(target_arch = "x86_64") {
            HostArch::X86_64
        } else if cfg!(target_arch = "arm") {
            HostArch::ARM
        } else if cfg!(target_arch = "aarch64") {
            HostArch::ARM64
        } else if cfg!(target_arch = "mips") {
            HostArch::MIPS
        } else if cfg!(target_arch = "powerpc") {
            HostArch::PowerPC
        } else {
            panic!("Unsupported target architecture")
        }
    }
    pub const fn new_with_val(v: u8) -> Self {
        match v {
            0 => HostArch::X86,
            1 => HostArch::X86_64,
            2 => HostArch::ARM,
            3 => HostArch::ARM64,
            4 => HostArch::MIPS,
            5 => HostArch::PowerPC,
            _ => panic!("unknown arch"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
pub enum HostOS {
    // T1
    Linux = 0,
    Windows = 1,
    MacOS = 2,
    // T2
    Android = 3,
    AppleiOS = 4,
    FreeBSD = 5,
    OpenBSD = 6,
    NetBSD = 7,
    WASI = 8,
    Emscripten = 9,
    // T3
    Solaris = 10,
    Fuchsia = 11,
    Redox = 12,
    DragonFly = 13,
}

impl HostOS {
    pub const fn new() -> Self {
        if cfg!(target_os = "linux") {
            HostOS::Linux
        } else if cfg!(target_os = "windows") {
            HostOS::Windows
        } else if cfg!(target_os = "macos") {
            HostOS::MacOS
        } else if cfg!(target_os = "android") {
            HostOS::Android
        } else if cfg!(target_os = "ios") {
            HostOS::AppleiOS
        } else if cfg!(target_os = "freebsd") {
            HostOS::FreeBSD
        } else if cfg!(target_os = "openbsd") {
            HostOS::OpenBSD
        } else if cfg!(target_os = "netbsd") {
            HostOS::NetBSD
        } else if cfg!(target_os = "dragonfly") {
            HostOS::DragonFly
        } else if cfg!(target_os = "redox") {
            HostOS::Redox
        } else if cfg!(target_os = "fuchsia") {
            HostOS::Fuchsia
        } else if cfg!(target_os = "solaris") {
            HostOS::Solaris
        } else if cfg!(target_os = "emscripten") {
            HostOS::Emscripten
        } else if cfg!(target_os = "wasi") {
            HostOS::WASI
        } else {
            panic!("unknown os")
        }
    }
    pub const fn new_with_val(v: u8) -> Self {
        match v {
            0 => HostOS::Linux,
            1 => HostOS::Windows,
            2 => HostOS::MacOS,
            3 => HostOS::Android,
            4 => HostOS::AppleiOS,
            5 => HostOS::FreeBSD,
            6 => HostOS::OpenBSD,
            7 => HostOS::NetBSD,
            8 => HostOS::WASI,
            9 => HostOS::Emscripten,
            10 => HostOS::Solaris,
            11 => HostOS::Fuchsia,
            12 => HostOS::Redox,
            13 => HostOS::DragonFly,
            _ => panic!("unknown OS"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
pub enum HostEndian {
    Big = 0,
    Little = 1,
}

impl HostEndian {
    pub const fn new() -> Self {
        if cfg!(target_endian = "little") {
            Self::Little
        } else {
            Self::Big
        }
    }
    pub const fn new_with_val(v: u8) -> Self {
        match v {
            0 => HostEndian::Big,
            1 => HostEndian::Little,
            _ => panic!("Unknown endian"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
#[repr(u8)]
pub enum HostPointerWidth {
    P32 = 0,
    P64 = 1,
}

impl HostPointerWidth {
    pub const fn new() -> Self {
        match sizeof!(usize) {
            4 => Self::P32,
            8 => Self::P64,
            _ => panic!("unknown pointer width"),
        }
    }
    pub const fn new_with_val(v: u8) -> Self {
        match v {
            0 => HostPointerWidth::P32,
            1 => HostPointerWidth::P64,
            _ => panic!("Unknown pointer width"),
        }
    }
}

#[derive(Debug)]
pub struct StaticRecordUV {
    data: [u8; 16],
}

impl StaticRecordUV {
    pub const fn create(sr1_version: HeaderVersion) -> Self {
        let mut data = [0u8; 16];
        let magic_buf = SR0_MAGIC.to_le_bytes();
        let version_buf = sr1_version.little_endian_u64();
        let mut i = 0usize;
        while i < sizeof!(u64) {
            data[i] = magic_buf[i];
            data[i + sizeof!(u64)] = version_buf[i];
            i += 1;
        }
        data[sizeof!(u64, 2) - 4] = SR2_PTR_WIDTH;
        data[sizeof!(u64, 2) - 3] = SR3_ENDIAN;
        data[sizeof!(u64, 2) - 2] = SR4_ARCH;
        data[sizeof!(u64, 2) - 1] = SR5_OS;
        Self { data }
    }
    pub const fn get_ref(&self) -> &[u8] {
        &self.data
    }
    pub const fn read_p0_magic(&self) -> u64 {
        self.read_qword(0)
    }
    pub const fn read_p1_header_version(&self) -> HeaderVersion {
        HeaderVersion::__new(self.read_dword(sizeof!(u64)))
    }
    pub const fn read_p2_ptr_width(&self) -> HostPointerWidth {
        HostPointerWidth::new_with_val(self.read_byte(12))
    }
    pub const fn read_p3_endian(&self) -> HostEndian {
        HostEndian::new_with_val(self.read_byte(13))
    }
    pub const fn read_p4_arch(&self) -> HostArch {
        HostArch::new_with_val(self.read_byte(14))
    }
    pub const fn read_p5_os(&self) -> HostOS {
        HostOS::new_with_val(self.read_byte(15))
    }
}

impl_stack_read_primitives!(unsafe impl for StaticRecordUV {});

/*
    File identity
*/

/// The file scope
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
pub enum FileScope {
    TransactionLog = 0,
    TransactionLogCompacted = 1,
}

impl FileScope {
    pub const fn new(id: u32) -> Self {
        match id {
            0 => Self::TransactionLog,
            1 => Self::TransactionLogCompacted,
            _ => panic!("unknown filescope"),
        }
    }
}

#[test]
fn test_static_record_encode_decode() {
    let static_record = StaticRecordUV::create(super::versions::v1::V1_HEADER_VERSION);
    assert_eq!(static_record.read_p0_magic(), SR0_MAGIC);
    assert_eq!(
        static_record.read_p1_header_version(),
        super::versions::v1::V1_HEADER_VERSION
    );
    assert_eq!(static_record.read_p2_ptr_width(), HostPointerWidth::new());
    assert_eq!(static_record.read_p3_endian(), HostEndian::new());
    assert_eq!(static_record.read_p4_arch(), HostArch::new());
    assert_eq!(static_record.read_p5_os(), HostOS::new());
}
