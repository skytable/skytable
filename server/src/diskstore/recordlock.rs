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
pub struct FileLock {
    file: File,
}

impl FileLock {
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
        assert!(self.unlock().is_ok());
    }
}
