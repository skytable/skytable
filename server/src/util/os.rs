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
#[cfg(windows)]
pub use windows::*;

use {
    crate::IoResult,
    std::{ffi::OsStr, fs, path::Path},
};

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
                    // clippy doesn't realize that rlimit has a different size on 32-bit
                    #[allow(clippy::useless_conversion)]
                    Ok(ResourceLimit::new(
                        rlim.rlim_cur.into(),
                        rlim.rlim_max.into(),
                    ))
                }
            }
        }
        /// Returns the current limit
        pub const fn current(&self) -> u64 {
            self.cur
        }
        /// Returns the max limit
        pub const fn max(&self) -> u64 {
            self.max
        }
    }

    #[test]
    fn test_ulimit() {
        let _ = ResourceLimit::get().unwrap();
    }

    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::signal::unix::{signal, Signal, SignalKind};

    pub struct TerminationSignal {
        sigint: Signal,
        sigterm: Signal,
    }

    impl TerminationSignal {
        pub fn init() -> crate::IoResult<Self> {
            let sigint = signal(SignalKind::interrupt())?;
            let sigterm = signal(SignalKind::terminate())?;
            Ok(Self { sigint, sigterm })
        }
    }

    impl Future for TerminationSignal {
        type Output = Option<()>;
        fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
            let int = self.sigint.poll_recv(ctx);
            let term = self.sigterm.poll_recv(ctx);
            match (int, term) {
                // when either of them have closed or received a signal, return
                (Poll::Ready(p), _) | (_, Poll::Ready(p)) => Poll::Ready(p),
                _ => Poll::Pending,
            }
        }
    }
}

#[cfg(windows)]
mod windows {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::signal::windows::{ctrl_break, ctrl_c, CtrlBreak, CtrlC};

    pub struct TerminationSignal {
        ctrl_c: CtrlC,
        ctrl_break: CtrlBreak,
    }
    impl TerminationSignal {
        pub fn init() -> crate::IoResult<Self> {
            let ctrl_c = ctrl_c()?;
            let ctrl_break = ctrl_break()?;
            Ok(Self { ctrl_c, ctrl_break })
        }
    }
    impl Future for TerminationSignal {
        type Output = Option<()>;
        fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
            let ctrl_c = self.ctrl_c.poll_recv(ctx);
            let ctrl_break = self.ctrl_break.poll_recv(ctx);
            match (ctrl_c, ctrl_break) {
                // if any of them are ready or closed, simply return
                (Poll::Ready(p), _) | (_, Poll::Ready(p)) => Poll::Ready(p),
                _ => Poll::Pending,
            }
        }
    }
}

/// Recursively copy files from the given `src` to the provided `dest`
pub fn recursive_copy(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> IoResult<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        match entry.file_type()? {
            ft if ft.is_dir() => {
                // this is a directory, so we'll recursively create it and its contents
                recursive_copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
            }
            _ => {
                // this directory has files (or symlinks?)
                fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
            }
        }
    }
    Ok(())
}

#[test]
fn rcopy_okay() {
    let dir_paths = [
        "testdata/backups",
        "testdata/ks/default",
        "testdata/ks/system",
        "testdata/rsnaps",
        "testdata/snaps",
    ];
    let file_paths = [
        "testdata/ks/default/default",
        "testdata/ks/default/PARTMAP",
        "testdata/ks/PRELOAD",
        "testdata/ks/system/PARTMAP",
    ];
    let new_file_paths = [
        "my-backups/ks/default/default",
        "my-backups/ks/default/PARTMAP",
        "my-backups/ks/PRELOAD",
        "my-backups/ks/system/PARTMAP",
    ];
    let x = move || -> IoResult<()> {
        for dir in dir_paths {
            fs::create_dir_all(dir)?;
        }
        for file in file_paths {
            fs::File::create(file)?;
        }
        Ok(())
    };
    x().unwrap();
    // now copy all files inside testdata/* to my-backups/*
    recursive_copy("testdata", "my-backups").unwrap();
    new_file_paths
        .iter()
        .for_each(|path| assert!(Path::new(path).exists()));
    // now remove the directories
    fs::remove_dir_all("testdata").unwrap();
    fs::remove_dir_all("my-backups").unwrap();
}

#[derive(Debug, PartialEq)]
pub enum EntryKind {
    Directory(String),
    File(String),
}

impl EntryKind {
    pub fn into_inner(self) -> String {
        match self {
            Self::Directory(path) | Self::File(path) => path,
        }
    }
    pub fn get_inner(&self) -> &str {
        match self {
            Self::Directory(rf) | Self::File(rf) => rf,
        }
    }
}

impl ToString for EntryKind {
    fn to_string(&self) -> String {
        self.get_inner().to_owned()
    }
}

impl AsRef<str> for EntryKind {
    fn as_ref(&self) -> &str {
        self.get_inner()
    }
}

impl AsRef<OsStr> for EntryKind {
    fn as_ref(&self) -> &OsStr {
        OsStr::new(self.get_inner())
    }
}

/// Returns a vector with a complete list of entries (both directories and files)
/// in the given path (recursive extraction)
pub fn rlistdir(path: impl AsRef<Path>) -> crate::IoResult<Vec<EntryKind>> {
    let mut ret = Vec::new();
    rlistdir_inner(path.as_ref(), &mut ret)?;
    Ok(ret)
}

fn rlistdir_inner(path: &Path, paths: &mut Vec<EntryKind>) -> crate::IoResult<()> {
    let dir = fs::read_dir(path)?;
    for entry in dir {
        let entry = entry?;
        let path = entry.path();
        let path_str = path.to_string_lossy().to_string();
        // we want both directory names and file names
        if path.is_dir() {
            paths.push(EntryKind::Directory(path_str));
            rlistdir_inner(&path, paths)?;
        } else {
            paths.push(EntryKind::File(path_str));
        }
    }
    Ok(())
}

fn dir_size_inner(dir: fs::ReadDir) -> IoResult<u64> {
    let mut ret = 0;
    for entry in dir {
        let entry = entry?;
        let size = match entry.metadata()? {
            meta if meta.is_dir() => dir_size_inner(fs::read_dir(entry.path())?)?,
            meta => meta.len(),
        };
        ret += size;
    }
    Ok(ret)
}

/// Returns the size of a directory by recursively scanning it
pub fn dirsize(path: impl AsRef<Path>) -> IoResult<u64> {
    dir_size_inner(fs::read_dir(path.as_ref())?)
}
