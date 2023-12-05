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

// unix imports
#[cfg(unix)]
extern crate libc;
// windows imports
#[cfg(windows)]
extern crate winapi;
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

use std::{fs::File, io, path::Path};

pub struct FileLock {
    _file: File,
    #[cfg(windows)]
    handle: winapi::um::winnt::HANDLE,
}

impl FileLock {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(path)?;
        #[cfg(windows)]
        {
            use {
                std::mem,
                winapi::um::{
                    fileapi::LockFileEx,
                    minwinbase::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY},
                    winnt::HANDLE,
                },
            };
            let handle = file.as_raw_handle();
            let mut overlapped = unsafe { mem::zeroed() };
            let result = unsafe {
                LockFileEx(
                    handle as HANDLE,
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    u32::MAX,
                    u32::MAX,
                    &mut overlapped,
                )
            };
            if result == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "file is already locked",
                ));
            }
            return Ok(Self {
                _file: file,
                handle,
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
            use {
                std::mem,
                winapi::um::{fileapi::UnlockFileEx, winnt::HANDLE},
            };

            let mut overlapped = unsafe { mem::zeroed() };
            let result = unsafe {
                UnlockFileEx(
                    self.handle as HANDLE,
                    0,
                    u32::MAX,
                    u32::MAX,
                    &mut overlapped,
                )
            };

            if result == 0 {
                return Err(io::Error::last_os_error());
            }
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
