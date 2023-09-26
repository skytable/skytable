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

use {super::versions::HeaderVersion, crate::engine::mem::ByteStack};

/// magic
const SR0_MAGIC: u64 = 0x4F48534159414E21;
/// host ptr width
const SR2_PTR_WIDTH: HostPointerWidth = HostPointerWidth::new();
/// host endian
const SR3_ENDIAN: HostEndian = HostEndian::new();
/// host arch
const SR4_ARCH: HostArch = HostArch::new();
/// host os
const SR5_OS: HostOS = HostOS::new();

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
/// Host architecture enumeration for common platforms
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
    pub const fn try_new_with_val(v: u8) -> Option<Self> {
        Some(match v {
            0 => HostArch::X86,
            1 => HostArch::X86_64,
            2 => HostArch::ARM,
            3 => HostArch::ARM64,
            4 => HostArch::MIPS,
            5 => HostArch::PowerPC,
            _ => return None,
        })
    }
    pub const fn new_with_val(v: u8) -> Self {
        match Self::try_new_with_val(v) {
            Some(v) => v,
            None => panic!("unknown arch"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
/// Host OS enumeration for common operating systems
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
    pub const fn try_new_with_val(v: u8) -> Option<Self> {
        Some(match v {
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
            _ => return None,
        })
    }
    pub const fn new_with_val(v: u8) -> Self {
        match Self::try_new_with_val(v) {
            Some(v) => v,
            None => panic!("unknown OS"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
/// Host endian enumeration
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
    pub const fn try_new_with_val(v: u8) -> Option<Self> {
        Some(match v {
            0 => HostEndian::Big,
            1 => HostEndian::Little,
            _ => return None,
        })
    }
    pub const fn new_with_val(v: u8) -> Self {
        match Self::try_new_with_val(v) {
            Some(v) => v,
            None => panic!("Unknown endian"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, sky_macros::EnumMethods)]
#[repr(u8)]
/// Host pointer width enumeration
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
    pub const fn try_new_with_val(v: u8) -> Option<Self> {
        Some(match v {
            0 => HostPointerWidth::P32,
            1 => HostPointerWidth::P64,
            _ => return None,
        })
    }
    pub const fn new_with_val(v: u8) -> Self {
        match Self::try_new_with_val(v) {
            Some(v) => v,
            None => panic!("Unknown pointer width"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct StaticRecordUV {
    header_version: HeaderVersion,
    ptr_width: HostPointerWidth,
    endian: HostEndian,
    arch: HostArch,
    os: HostOS,
}

impl StaticRecordUV {
    pub const fn new(
        header_version: HeaderVersion,
        ptr_width: HostPointerWidth,
        endian: HostEndian,
        arch: HostArch,
        os: HostOS,
    ) -> Self {
        Self {
            header_version,
            ptr_width,
            endian,
            arch,
            os,
        }
    }
    pub const fn header_version(&self) -> HeaderVersion {
        self.header_version
    }
    pub const fn ptr_width(&self) -> HostPointerWidth {
        self.ptr_width
    }
    pub const fn endian(&self) -> HostEndian {
        self.endian
    }
    pub const fn arch(&self) -> HostArch {
        self.arch
    }
    pub const fn os(&self) -> HostOS {
        self.os
    }
    pub const fn encoded(&self) -> StaticRecordUVRaw {
        StaticRecordUVRaw::new(
            self.header_version(),
            self.ptr_width(),
            self.endian(),
            self.arch(),
            self.os(),
        )
    }
}

#[derive(Debug, PartialEq, Clone)]
/// The static record
pub struct StaticRecordUVRaw {
    data: ByteStack<16>,
}

impl StaticRecordUVRaw {
    const OFFSET_P0: usize = 0;
    const OFFSET_P1: usize = sizeof!(u64);
    const OFFSET_P2: usize = Self::OFFSET_P1 + sizeof!(u32);
    const OFFSET_P3: usize = Self::OFFSET_P2 + 1;
    const OFFSET_P4: usize = Self::OFFSET_P3 + 1;
    const OFFSET_P5: usize = Self::OFFSET_P4 + 1;
    const _ENSURE: () = assert!(Self::OFFSET_P5 == (sizeof!(Self) - 1));
    #[inline(always)]
    pub const fn new(
        sr1_version: HeaderVersion,
        sr2_ptr_width: HostPointerWidth,
        sr3_endian: HostEndian,
        sr4_arch: HostArch,
        sr5_os: HostOS,
    ) -> Self {
        let mut data = [0u8; 16];
        let magic_buf = SR0_MAGIC.to_le_bytes();
        let version_buf = sr1_version.little_endian_u64();
        let mut i = 0usize;
        while i < sizeof!(u64) {
            data[i] = magic_buf[i];
            data[i + sizeof!(u64)] = version_buf[i];
            i += 1;
        }
        data[sizeof!(u64, 2) - 4] = sr2_ptr_width.value_u8();
        data[sizeof!(u64, 2) - 3] = sr3_endian.value_u8();
        data[sizeof!(u64, 2) - 2] = sr4_arch.value_u8();
        data[sizeof!(u64, 2) - 1] = sr5_os.value_u8();
        Self {
            data: ByteStack::new(data),
        }
    }
    #[inline(always)]
    pub const fn create(sr1_version: HeaderVersion) -> Self {
        Self::new(sr1_version, SR2_PTR_WIDTH, SR3_ENDIAN, SR4_ARCH, SR5_OS)
    }
    /// Decode and validate a SR
    ///
    /// WARNING: NOT CONTEXTUAL! VALIDATE YOUR OWN STUFF!
    pub fn decode_from_bytes(data: [u8; 16]) -> Option<StaticRecordUV> {
        let _ = Self::_ENSURE;
        let slf = Self {
            data: ByteStack::new(data),
        };
        // p0: magic; the magic HAS to be the same
        if u64::from_le(slf.data.read_qword(Self::OFFSET_P0)) != SR0_MAGIC {
            return None;
        }
        let sr1_header_version = HeaderVersion::__new(slf.data.read_dword(Self::OFFSET_P1) as _);
        let sr2_ptr = HostPointerWidth::try_new_with_val(slf.data.read_byte(Self::OFFSET_P2))?; // p2: ptr width
        let sr3_endian = HostEndian::try_new_with_val(slf.data.read_byte(Self::OFFSET_P3))?; // p3: endian
        let sr4_arch = HostArch::try_new_with_val(slf.data.read_byte(Self::OFFSET_P4))?; // p4: arch
        let sr5_os = HostOS::try_new_with_val(slf.data.read_byte(Self::OFFSET_P5))?; // p5: os
        Some(StaticRecordUV::new(
            sr1_header_version,
            sr2_ptr,
            sr3_endian,
            sr4_arch,
            sr5_os,
        ))
    }
}

impl StaticRecordUVRaw {
    pub const fn get_ref(&self) -> &[u8] {
        self.data.slice()
    }
    pub const fn read_p0_magic(&self) -> u64 {
        self.data.read_qword(Self::OFFSET_P0)
    }
    pub const fn read_p1_header_version(&self) -> HeaderVersion {
        HeaderVersion::__new(self.data.read_dword(Self::OFFSET_P1) as _)
    }
    pub const fn read_p2_ptr_width(&self) -> HostPointerWidth {
        HostPointerWidth::new_with_val(self.data.read_byte(Self::OFFSET_P2))
    }
    pub const fn read_p3_endian(&self) -> HostEndian {
        HostEndian::new_with_val(self.data.read_byte(Self::OFFSET_P3))
    }
    pub const fn read_p4_arch(&self) -> HostArch {
        HostArch::new_with_val(self.data.read_byte(Self::OFFSET_P4))
    }
    pub const fn read_p5_os(&self) -> HostOS {
        HostOS::new_with_val(self.data.read_byte(Self::OFFSET_P5))
    }
    pub const fn decoded(&self) -> StaticRecordUV {
        StaticRecordUV::new(
            self.read_p1_header_version(),
            self.read_p2_ptr_width(),
            self.read_p3_endian(),
            self.read_p4_arch(),
            self.read_p5_os(),
        )
    }
}

#[test]
fn test_static_record() {
    let static_record = StaticRecordUVRaw::create(super::versions::v1::V1_HEADER_VERSION);
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

#[test]
fn test_static_record_encode_decode() {
    let static_record = StaticRecordUVRaw::create(super::versions::v1::V1_HEADER_VERSION);
    let static_record_decoded =
        StaticRecordUVRaw::decode_from_bytes(static_record.data.data_copy()).unwrap();
    assert_eq!(static_record.decoded(), static_record_decoded);
}
