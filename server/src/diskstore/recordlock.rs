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

//! # POSIX Advisory locking
//!
//! This module provides the `FileLock` struct that can be used for locking and/or unlocking files on
//! POSIX-compliant systems

use libc::c_int;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Error;
use std::os::unix::io::AsRawFd;

extern "C" {
    fn lock_file(fd: i32) -> c_int;
    fn unlock_file(fd: i32) -> c_int;
}

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
    /// Lock a file with `filename`
    /// 
    /// If C's `fcntl` returns any error, then it is converted into the _Rust equivalent_ and returned
    /// by this function
    pub fn lock(filename: &str) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(&filename)?;
        let raw_err = unsafe { lock_file(file.as_raw_fd()) };
        match raw_err {
            0 => Ok(FileLock { file }),
            x @ _ => Err(Error::from_raw_os_error(x)),
        }
    }
    /// Unlock a file with `filename`
    /// 
    /// If C's `fctnl` returns any error, then it is converted into the _Rust equivalent_ and returned
    /// by this function
    pub fn unlock(&self) -> Result<(), Error> {
        let raw_err = unsafe { unlock_file(self.file.as_raw_fd()) };
        match raw_err {
            0 => Ok(()),
            x @ _ => Err(Error::from_raw_os_error(x)),
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if self.unlock().is_err() {
            // This is wild; uh oh
            panic!("Failed to release file lock!");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[test]
    fn test_basic_file_locking() {
        let _ = fs::File::create("blahblah.bin").unwrap();
        let lock = FileLock::lock("blahblah.bin").unwrap();
        lock.unlock().unwrap();
        // delete the file
        fs::remove_file("blahblah.bin").unwrap();
    }
}