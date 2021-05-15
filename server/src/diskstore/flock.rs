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
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Debug)]
/// # File Lock
/// A file lock object holds a `std::fs::File` that is used to `lock()` and `unlock()` a file with a given
/// `filename` passed into the `lock()` method. The file lock is configured to drop the file lock when the
/// object is dropped. The `file` field is essentially used to get the raw file descriptor for passing to
/// the platform-specific lock/unlock methods.
///
/// **Note:** You need to lock a file first using this object before unlocking it!
///
/// ## Suggestions
///
/// It is always a good idea to attempt a lock release (unlock) explicitly than letting the `Drop` implementation
/// run it for you as that may cause some Wild West panic if the lock release fails (haha!).
/// If you manually run unlock, then `Drop`'s implementation won't call another unlock to avoid an extra
/// syscall; this is achieved with the `unlocked` flag (field) which is set to true when the `unlock()` function
/// is called.
///
pub struct FileLock {
    file: File,
    unlocked: bool,
}

impl FileLock {
    /// Initialize a new `FileLock` by locking a file
    ///
    /// This function will create and lock a file if it doesn't exist or it
    /// will create and lock a new file
    pub fn lock(filename: impl Into<PathBuf>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(filename.into())?;
        Self::_lock(&file)?;
        Ok(Self {
            file,
            unlocked: false,
        })
    }
    /// The internal lock function
    ///
    /// This is the function that actually locks the file and is kept separate only for purposes
    /// of maintainability
    fn _lock(file: &File) -> Result<()> {
        __sys::try_lock_ex(file)
    }
    /// Unlock the file
    ///
    /// This sets the `unlocked` flag to true
    pub fn unlock(&mut self) -> Result<()> {
        __sys::unlock_file(&self.file)?;
        self.unlocked = true;
        Ok(())
    }
    /// Write something to this file
    pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
        // empty the file
        self.file.set_len(0)?;
        // set the cursor to start
        self.file.seek(SeekFrom::Start(0))?;
        // Now write to the file
        self.file.write_all(bytes)
    }
    pub fn try_clone(&self) -> Result<Self> {
        Ok(FileLock {
            file: self.file.try_clone()?,
            unlocked: self.unlocked,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_basic_file_lock() {
        let mut file = FileLock::lock("datalock.bin").unwrap();
        file.write(&[1, 2, 3]).unwrap();
        file.unlock().unwrap();
    }
    #[test]
    #[should_panic]
    fn test_fail_with_two_flocks() {
        let _file = FileLock::lock("data2.bin").unwrap();
        let _file2 = FileLock::lock("data2.bin").unwrap();
        std::fs::remove_file("data2.bin").unwrap();
    }
    #[cfg(windows)]
    #[test]
    #[should_panic]
    fn test_windows_with_two_unlock_attempts() {
        // This is a windows specific test to ensure that our logic with the `unlocked` field is correct
        let mut file = FileLock::lock("data3.bin").unwrap();
        file.unlock().unwrap();
        file.unlock().unwrap();
    }
    #[cfg(windows)]
    #[test]
    fn test_windows_lock_and_then_unlock() {
        let mut file = FileLock::lock("data4.bin").unwrap();
        file.unlock().unwrap();
        drop(file);
        let mut file2 = FileLock::lock("data4.bin").unwrap();
        file2.unlock().unwrap();
        drop(file2);
    }
    #[test]
    fn test_cloned_lock_writes() {
        let mut file = FileLock::lock("data5.bin").unwrap();
        let mut cloned = file.try_clone().unwrap();
        // this writes 1, 2, 3
        file.write(&[1, 2, 3]).unwrap();
        // this will truncate the entire previous file and write 4, 5, 6
        cloned.write(&[4, 5, 6]).unwrap();
        drop(cloned);
        // this will again truncate the entire previous file and write 7, 8
        file.write(&[7, 8]).unwrap();
        drop(file);
        let res = std::fs::read("data5.bin").unwrap();
        // hence ultimately we'll have 7, 8
        assert_eq!(res, vec![7, 8]);
    }
}

#[cfg(windows)]
mod __sys {
    //! # Windows platform-specific file locking
    //! This module contains methods used by the `FileLock` object in this module to lock and/or
    //! unlock files.
    use std::fs::File;
    use std::io::{Error, Result};
    use std::mem;
    use std::os::windows::io::AsRawHandle;
    use winapi::shared::minwindef::DWORD;
    use winapi::um::fileapi::{LockFileEx, UnlockFile};
    use winapi::um::minwinbase::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};
    /// Obtain an exclusive lock and **block** until we acquire it
    pub fn lock_ex(file: &File) -> Result<()> {
        lock_file(file, LOCKFILE_EXCLUSIVE_LOCK)
    }
    /// Try to obtain an exclusive lock and **immediately return an error if this is blocking**
    pub fn try_lock_ex(file: &File) -> Result<()> {
        lock_file(file, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY)
    }
    /// Use the LockFileEx method from Windows fileapi.h to set flags on a file
    ///
    /// This is the internal function that is used by `lock_ex` and `try_lock_ex` to lock and/or
    /// unlock files on Windows platforms.
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
    /// Attempt to unlock a file
    pub fn unlock_file(file: &File) -> Result<()> {
        let ret = unsafe { UnlockFile(file.as_raw_handle(), 0, 0, !0, !0) };
        if ret == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[cfg(unix)]
mod __sys {
    #![allow(dead_code)] // TODO: Enable this lint or remove the offending methods
    //! # Unix platform-specific file locking
    //! This module contains methods used by the `FileLock` object in this module to lock and/or
    //! unlock files.
    use libc::c_int;
    use std::fs::File;
    use std::io::Error;
    use std::io::Result;
    use std::os::unix::io::AsRawFd;

    extern "C" {
        /// Block and acquire an exclusive lock with `libc`'s `flock`
        fn lock_exclusive(fd: i32) -> c_int;
        /// Attempt to acquire an exclusive lock in a non-blocking manner with `libc`'s `flock`
        fn try_lock_exclusive(fd: i32) -> c_int;
        /// Attempt to unlock a file with `libc`'s flock
        fn unlock(fd: i32) -> c_int;
    }
    /// Obtain an exclusive lock and **block** until we acquire it
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
    /// Try to obtain an exclusive lock and **immediately return an error if this is blocking**
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
    /// Attempt to unlock a file
    pub fn unlock_file(file: &File) -> Result<()> {
        let errno = unsafe { unlock(file.as_raw_fd()) };
        match errno {
            0 => Ok(()),
            x @ _ => Err(Error::from_raw_os_error(x)),
        }
    }
}
