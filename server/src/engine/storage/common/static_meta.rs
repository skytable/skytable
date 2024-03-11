/*
 * Created on Tue Jan 09 2024
 *
 * This file is a part of Skytable
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

//! # Static metadata
//!
//! Compile-time metadata used by storage engine implementations
//!

/// The 8B SDSS magic block
pub const SDSS_MAGIC_8B: u64 = 0x4F48534159414E21;

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
}

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
}

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
}
