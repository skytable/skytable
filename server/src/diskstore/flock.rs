/*
 * Created on Fri Apr 16 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! # File Locking
//!
//! This module provides the `FileLock` struct that can be used for locking and/or unlocking files on
//! unix-based systems and Windows systems

// TODO(@ohsayan): Add support for solaris

use std::fs::File;
use std::fs::OpenOptions;
use std::io::Result;
use std::io::Write;

#[derive(Debug)]
/// # File Lock
/// A file lock object holds a `std::fs::File` that is used to `lock()` and `unlock()` a file with a given
/// `filename` passed into the `lock()` method. The file lock is configured to drop the file lock when the
/// object is dropped. The `file` field is essentially used to get the raw file descriptor for passing to
/// the C function `lock_file` or `unlock_file` provided by the `native/fscposix.c` file (or `libflock-posix.a`)
///
/// **Note:** You need to lock a file first using this object before unlocking it!
///
/// ## Suggestions
///
/// It is always a good idea to attempt a lock release (unlock) explicitly than letting the `Drop` implementation
/// run it for you as that may cause some Wild West panic if the lock release fails (haha!)
///
pub struct FileLock {
    file: File,
}

impl FileLock {
    pub fn lock(filename: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(false)
            .write(true)
            .open(filename)?;
        Self::_lock(&file)?;
        Ok(Self { file })
    }
    fn _lock(file: &File) -> Result<()> {
        __sys::try_lock_ex(file)
    }
    pub fn unlock(&self) -> Result<()> {
        __sys::unlock_file(&self.file)
    }
    pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if self.unlock().is_err() {
            // This is wild; uh, oh
            panic!("Failed to unlock file when dropping value");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_basic_file_lock() {
        let mut file = FileLock::lock("data.bin").unwrap();
        file.write(&[1, 2, 3]).unwrap();
        file.unlock().unwrap();
    }
}

#[cfg(windows)]
mod __sys {
    use std::io::{Error, Result};
    use std::mem;
    use std::fs::File;
    use std::os::windows::io::AsRawHandle;
    use winapi::shared::minwindef::{BOOL, DWORD};
    use winapi::um::fileapi::{LockFileEx, UnlockFile};
    use winapi::um::minwinbase::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};
    pub fn lock_ex(file: &File) -> Result<()> {
        lock_file(file, LOCKFILE_EXCLUSIVE_LOCK)
    }
    pub fn try_lock_ex(file: &File) -> Result<()> {
        lock_file(file, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY)
    }
    fn lock_file(file: &File, flags: DWORD) -> Result<()> {
        unsafe {
            let mut overlapped = mem::zeroed();
            let ret = LockFileEx(file.as_raw_handle(), flags, 0, !0, !0, &mut overlapped);
            if ret == 0 {
                Err(Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }
    pub fn unlock_file(file: &File) -> Result<()> {
        let ret = UnlockFile(file.as_raw_handle(), 0, 0, !0, !0);
        if ret == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(unix)]
mod __sys {
    use libc::c_int;
    use std::fs::File;
    use std::io::Error;
    use std::io::Result;
    use std::os::unix::io::AsRawFd;

    extern "C" {
        fn lock_exclusive(fd: i32) -> c_int;
        fn try_lock_exclusive(fd: i32) -> c_int;
        fn unlock(fd: i32) -> c_int;
    }
    pub fn lock_ex(file: &File) -> Result<()> {
        let errno = unsafe {
            // UNSAFE(@ohsayan): This is completely fine to do as we've already written the function
            // ourselves and are very much aware that it is safe
            lock_exclusive(file.as_raw_fd())
        };
        match errno {
            0 => Ok(()),
            x @ _ => Err(Error::from_raw_os_error(x)),
        }
    }
    pub fn try_lock_ex(file: &File) -> Result<()> {
        let errno = unsafe {
            // UNSAFE(@ohsayan): Again, we've written the function ourselves and know what is going on!
            try_lock_exclusive(file.as_raw_fd())
        };
        match errno {
            0 => Ok(()),
            x @ _ => Err(Error::from_raw_os_error(x)),
        }
    }
    pub fn unlock_file(file: &File) -> Result<()> {
        let errno = unsafe { unlock(file.as_raw_fd()) };
        match errno {
            0 => Ok(()),
            x @ _ => Err(Error::from_raw_os_error(x)),
        }
    }
}
