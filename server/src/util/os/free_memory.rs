/*
 * Created on Sat Sep 02 2023
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

#[cfg(any(target_os = "linux", target_os = "macos"))]
extern crate libc;

pub fn free_memory_in_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        use windows::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
        let mut statex = MEMORYSTATUSEX::default();
        statex.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
        unsafe {
            // UNSAFE(@ohsayan): correct call to windows API
            GlobalMemoryStatusEx(&mut statex).unwrap();
        }
        // Return free physical memory
        return statex.ullAvailPhys;
    }

    #[cfg(target_os = "linux")]
    {
        use libc::sysinfo;
        let mut info: libc::sysinfo = unsafe { core::mem::zeroed() };

        unsafe {
            if sysinfo(&mut info) == 0 {
                // Return free memory
                return (info.freeram as u64) * (info.mem_unit as u64);
            }
        }

        return 0;
    }

    #[cfg(target_os = "macos")]
    {
        use std::mem;
        unsafe {
            let page_size = libc::sysconf(libc::_SC_PAGESIZE);
            let mut count: u32 = libc::HOST_VM_INFO64_COUNT as _;
            let mut stat: libc::vm_statistics64 = mem::zeroed();
            libc::host_statistics64(
                libc::mach_host_self(),
                libc::HOST_VM_INFO64,
                &mut stat as *mut libc::vm_statistics64 as *mut _,
                &mut count,
            );

            // see this: https://opensource.apple.com/source/xnu/xnu-4570.31.3/osfmk/mach/vm_statistics.h.auto.html
            return (stat.free_count as u64)
                .saturating_add(stat.inactive_count as _)
                .saturating_add(stat.compressor_page_count as u64)
                .saturating_mul(page_size as _);
        }
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        return 0;
    }
}
