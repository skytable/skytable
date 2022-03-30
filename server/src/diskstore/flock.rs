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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
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

#![allow(dead_code)] // TODO(@ohsayan): Remove lint or remove offending methods

use std::{
    fs::{File, OpenOptions},
    io::{Result, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Debug)]
/// # File Lock
/// A file lock object holds a `std::fs::File` that is used to `lock()` and `unlock()` a file with a given
/// `filename` passed into the `lock()` method. The file lock is **not configured** to drop the file lock when the
/// object is dropped. The `file` field is essentially used to get the raw file descriptor for passing to
/// the platform-specific lock/unlock methods.
///
/// **Note:** You need to lock a file first using this object before unlocking it!
///
/// ## Suggestions
///
/// It is always a good idea to attempt a lock release (unlock) explicitly than leaving it to the operating
/// system. If you manually run unlock, another unlock won't be called to avoid an extra costly (is it?)
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
    /// will lock the existing file
    /// **This will immediately fail if locking fails, i.e it is non-blocking**
    pub fn lock(filename: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(filename.as_ref())?;
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
        if !self.unlocked {
            __sys::unlock_file(&self.file)?;
            self.unlocked = true;
            Ok(())
        } else {
            Ok(())
        }
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
    /// Sync all metadata and flush buffers before returning
    pub fn fsync(&self) -> Result<()> {
        self.file.sync_all()
    }
    #[cfg(test)]
    pub fn try_clone(&self) -> Result<Self> {
        Ok(FileLock {
            file: __sys::duplicate(&self.file)?,
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
    use std::os::windows::io::FromRawHandle;
    use std::ptr;
    use winapi::shared::minwindef::{BOOL, DWORD};
    use winapi::um::fileapi::{LockFileEx, UnlockFile};
    use winapi::um::handleapi::DuplicateHandle;
    use winapi::um::minwinbase::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};
    use winapi::um::processthreadsapi::GetCurrentProcess;
    use winapi::um::winnt::{DUPLICATE_SAME_ACCESS, MAXDWORD};
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
            // UNSAFE(@ohsayan): Interfacing with low-level winapi stuff, and we know what's happening here :D
            let mut overlapped = mem::zeroed();
            let ret = LockFileEx(
                file.as_raw_handle(), // handle
                flags,                // flags
                0,                    // reserved DWORD, has to be 0
                MAXDWORD, // nNumberOfBytesToLockLow; low-order (LOWORD) 32-bits of file range to lock
                MAXDWORD, // nNumberOfBytesToLockHigh; high-order (HIWORD) 32-bits of file range to lock
                &mut overlapped,
            );
            if ret == 0 {
                Err(Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }
    /// Attempt to unlock a file
    pub fn unlock_file(file: &File) -> Result<()> {
        let ret = unsafe {
            // UNSAFE(@ohsayan): Interfacing with low-level winapi stuff, and we know what's happening here :D
            UnlockFile(
                file.as_raw_handle(), // handle
                0,                    // LOWORD of starting byte offset
                0,                    // HIWORD of starting byte offset
                MAXDWORD,             // LOWORD of file range to unlock
                MAXDWORD,             // HIWORD of file range to unlock
            )
        };
        if ret == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }
    /// Duplicate a file
    ///
    /// The most important part is the `DUPLICATE_SAME_ACCESS` DWORD. It ensures that the cloned file
    /// has the same permissions as the original file
    pub fn duplicate(file: &File) -> Result<File> {
        unsafe {
            // UNSAFE(@ohsayan): Interfacing with low-level winapi stuff, and we know what's happening here :D
            let mut handle = ptr::null_mut();
            let current_process = GetCurrentProcess();
            let ret = DuplicateHandle(
                current_process,
                file.as_raw_handle(),
                current_process,
                &mut handle,
                0,
                true as BOOL,
                DUPLICATE_SAME_ACCESS,
            );
            if ret == 0 {
                Err(Error::last_os_error())
            } else {
                Ok(File::from_raw_handle(handle))
            }
        }
    }
}

#[cfg(all(not(target_os = "solaris"), unix))]
mod __sys {
    //! # Unix platform-specific file locking
    //! This module contains methods used by the `FileLock` object in this module to lock and/or
    //! unlock files.
    use libc::c_int;
    use std::fs::File;
    use std::io::Error;
    use std::io::Result;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::io::FromRawFd;

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
            x => Err(Error::from_raw_os_error(x)),
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
            x => Err(Error::from_raw_os_error(x)),
        }
    }
    /// Attempt to unlock a file
    pub fn unlock_file(file: &File) -> Result<()> {
        let errno = unsafe {
            // UNSAFE(@ohsayan): Again, we know what's going on here. Good ol' C stuff
            unlock(file.as_raw_fd())
        };
        match errno {
            0 => Ok(()),
            x => Err(Error::from_raw_os_error(x)),
        }
    }
    /// Duplicate a file
    ///
    /// Good ol' libc dup() calls
    pub fn duplicate(file: &File) -> Result<File> {
        unsafe {
            // UNSAFE(@ohsayan): Completely safe, just that this is FFI
            let fd = libc::dup(file.as_raw_fd());
            if fd < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(File::from_raw_fd(fd))
            }
        }
    }
}

#[cfg(all(target_os = "solaris", unix))]
mod __sys {
    //! Solaris doesn't have flock so we'll have to simulate that using fcntl
    use std::fs::File;
    use std::io::Error;
    use std::io::Result;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::io::FromRawFd;

    fn simulate_flock(file: &File, flag: libc::c_int) -> Result<()> {
        let mut fle = libc::flock {
            l_whence: 0,
            l_start: 0,
            l_len: 0,
            l_type: 0,
            l_pad: [0; 4],
            l_pid: 0,
            l_sysid: 0,
        };
        let (cmd, op) = match flag & libc::LOCK_NB {
            0 => (libc::F_SETLKW, flag),
            _ => (libc::F_SETLK, flag & !libc::LOCK_NB),
        };
        match op {
            libc::LOCK_SH => fle.l_type |= libc::F_RDLCK,
            libc::LOCK_EX => fle.l_type |= libc::F_WRLCK,
            libc::LOCK_UN => fle.l_type |= libc::F_UNLCK,
            _ => return Err(Error::from_raw_os_error(libc::EINVAL)),
        }
        let ret = unsafe { libc::fcntl(file.as_raw_fd(), cmd, &fle) };
        match ret {
            -1 => match Error::last_os_error().raw_os_error() {
                Some(libc::EACCES) => {
                    // this is the 'sort of' solaris equivalent to EWOULDBLOCK
                    Err(Error::from_raw_os_error(libc::EWOULDBLOCK))
                }
                _ => return Err(Error::last_os_error()),
            },
            _ => Ok(()),
        }
    }
    pub fn lock_ex(file: &File) -> Result<()> {
        simulate_flock(file, libc::LOCK_EX)
    }
    pub fn try_lock_ex(file: &File) -> Result<()> {
        simulate_flock(file, libc::LOCK_EX | libc::LOCK_NB)
    }
    pub fn unlock_file(file: &File) -> Result<()> {
        simulate_flock(file, libc::LOCK_UN)
    }
    pub fn duplicate(file: &File) -> Result<File> {
        unsafe {
            let fd = libc::dup(file.as_raw_fd());
            if fd < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(File::from_raw_fd(fd))
            }
        }
    }
}
