/*
 * Created on Wed Oct 04 2023
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
// Add this to Cargo.toml:
// windows = "0.33.0" or the latest version

#[cfg(unix)]
extern crate libc;
#[cfg(windows)]
use {
    std::os::windows::io::AsRawHandle,
    windows::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{
            LockFileEx, UnlockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
        },
        System::IO::OVERLAPPED,
    },
};

use std::{fs::File, io, path::Path};

pub struct FileLock {
    _file: File,
    #[cfg(windows)]
    handle: HANDLE,
}

impl FileLock {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        #[cfg(windows)]
        {
            let handle = file.as_raw_handle();
            let mut overlapped = OVERLAPPED::default();
            unsafe {
                LockFileEx(
                    HANDLE(handle as isize),
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    u32::MAX as u32,
                    u32::MAX as u32,
                    &mut overlapped,
                )
            }?;
            return Ok(Self {
                _file: file,
                handle: HANDLE(handle as isize),
            });
        }
        #[cfg(unix)]
        {
            use {
                libc::{flock, LOCK_EX, LOCK_NB},
                std::os::unix::io::AsRawFd,
            };
            let result = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
            if result != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "file is already locked",
                ));
            }
            return Ok(Self { _file: file });
        }
    }
    pub fn release(self) -> io::Result<()> {
        #[cfg(windows)]
        {
            let mut overlapped = OVERLAPPED::default();
            unsafe {
                UnlockFileEx(
                    self.handle,
                    0,
                    u32::MAX as u32,
                    u32::MAX as u32,
                    &mut overlapped,
                )
            }?;
        }
        #[cfg(unix)]
        {
            use {
                libc::{flock, LOCK_UN},
                std::os::unix::io::AsRawFd,
            };
            let result = unsafe { flock(self._file.as_raw_fd(), LOCK_UN) };
            if result != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}
