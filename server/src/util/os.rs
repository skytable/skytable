/*
 * Created on Sat Jan 29 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

#[cfg(unix)]
pub use unix::*;

#[cfg(unix)]
mod unix {
    use libc::{rlimit, RLIMIT_NOFILE};
    use std::io::Error as IoError;

    #[derive(Debug)]
    pub struct ResourceLimit {
        cur: u64,
        max: u64,
    }

    impl ResourceLimit {
        const fn new(cur: u64, max: u64) -> Self {
            Self { cur, max }
        }
        pub const fn is_over_limit(&self, expected: usize) -> bool {
            expected as u64 > self.cur
        }
        /// Returns the maximum number of open files
        pub fn get() -> Result<Self, IoError> {
            unsafe {
                let rlim = rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                let ret = libc::getrlimit(RLIMIT_NOFILE, &rlim as *const _ as *mut _);
                if ret != 0 {
                    Err(IoError::last_os_error())
                } else {
                    Ok(ResourceLimit::new(rlim.rlim_cur, rlim.rlim_max))
                }
            }
        }
    }

    #[test]
    fn test_ulimit() {
        let _ = ResourceLimit::get().unwrap();
    }
}
