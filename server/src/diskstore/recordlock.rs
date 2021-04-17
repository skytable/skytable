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

use std::fs::File;

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

#[cfg(unix)]
mod __sys {
    use libc;
    use std::fs::File;
    use std::io::Error;
    use std::io::Result;
    use std::os::unix::io::AsRawFd;
    // TODO(@ohsayan): Support SOLARIS
    #[cfg(not(target_os = "solaris"))]
    fn flock(file: &File, flag: libc::c_int) -> Result<()> {
        let ret = unsafe { libc::flock(file.as_raw_fd(), flag) };
        if ret < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(())
        }
    }

    fn lock_exclusive(file: &File) -> Result<()> {
        flock(file, libc::LOCK_EX)
    }

    fn try_lock_exclusive(file: &File) -> Result<()> {
        flock(file, libc::LOCK_EX | libc::LOCK_NB)
    }
}
