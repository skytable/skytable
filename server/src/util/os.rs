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
mod flock;
mod free_memory;

use {
    crate::IoResult,
    std::{
        ffi::OsStr,
        fmt, fs,
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    },
};
pub use {
    flock::{FileLock, FileLocks},
    free_memory::free_memory_in_bytes,
};

#[derive(Debug)]
#[repr(transparent)]
/// A wrapper around [`std`]'s I/O [Error](std::io::Error) type for simplicity with equality
pub struct SysIOError(std::io::Error);

impl SysIOError {
    pub fn into_inner(self) -> std::io::Error {
        self.0
    }
    pub fn kind(&self) -> std::io::ErrorKind {
        self.0.kind()
    }
    pub fn inner(&self) -> &std::io::Error {
        &self.0
    }
}

impl From<std::io::Error> for SysIOError {
    fn from(e: std::io::Error) -> Self {
        Self(e)
    }
}

impl From<std::io::ErrorKind> for SysIOError {
    fn from(e: std::io::ErrorKind) -> Self {
        Self(e.into())
    }
}

impl fmt::Display for SysIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
impl PartialEq for SysIOError {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_string() == other.0.to_string()
    }
}

#[cfg(unix)]
mod unix {
    use {
        libc::{rlimit, RLIMIT_NOFILE},
        std::{
            future::Future,
            io::Error as IoError,
            pin::Pin,
            task::{Context, Poll},
        },
        tokio::signal::unix::{signal, Signal, SignalKind},
    };

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

    #[sky_macros::non_miri_test]
    fn test_ulimit() {
        let _ = ResourceLimit::get().unwrap();
    }

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
    use {
        std::{
            future::Future,
            pin::Pin,
            task::{Context, Poll},
        },
        tokio::signal::windows::{ctrl_break, ctrl_c, CtrlBreak, CtrlC},
    };

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

/// Returns the current system uptime in milliseconds
pub fn get_uptime() -> u128 {
    uptime_impl::uptime().unwrap()
}

/// Returns the current epoch time in nanoseconds
pub fn get_epoch_time() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// Returns the hostname
pub fn get_hostname() -> hostname_impl::Hostname {
    hostname_impl::Hostname::get()
}

mod uptime_impl {
    #[cfg(target_os = "linux")]
    pub(super) fn uptime() -> std::io::Result<u128> {
        let mut sysinfo: libc::sysinfo = unsafe { std::mem::zeroed() };
        let res = unsafe { libc::sysinfo(&mut sysinfo) };
        if res == 0 {
            Ok(sysinfo.uptime as u128 * 1_000)
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd"
    ))]
    pub(super) fn uptime() -> std::io::Result<u128> {
        use libc::{c_void, size_t, sysctl, timeval};
        use std::ptr;

        let mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];
        let mut boottime = timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        let mut size = std::mem::size_of::<libc::timeval>() as size_t;

        let result = unsafe {
            sysctl(
                // this cast is fine. sysctl only needs to access the ptr to array base (read)
                &mib as *const _ as *mut _,
                2,
                &mut boottime as *mut timeval as *mut c_void,
                &mut size,
                ptr::null_mut(),
                0,
            )
        };

        if result == 0 {
            let current_time = unsafe { libc::time(ptr::null_mut()) };
            let uptime_secs = current_time - boottime.tv_sec;
            Ok((uptime_secs as u128) * 1_000)
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(target_os = "windows")]
    pub(super) fn uptime() -> std::io::Result<u128> {
        Ok(unsafe { windows::Win32::System::SystemInformation::GetTickCount64() as u128 })
    }
}

mod hostname_impl {
    use std::ffi::CStr;

    pub struct Hostname {
        len: u8,
        raw: [u8; 255],
    }

    impl Hostname {
        pub fn get() -> Self {
            get_hostname()
        }
        unsafe fn new_from_raw_buf(buf: &[u8; 256]) -> Self {
            let mut raw = [0u8; 255];
            raw.copy_from_slice(&buf[..255]);
            Self {
                len: CStr::from_ptr(buf.as_ptr().cast()).to_bytes().len() as _,
                raw,
            }
        }
        pub fn as_str(&self) -> &str {
            unsafe {
                core::str::from_utf8_unchecked(core::slice::from_raw_parts(
                    self.raw.as_ptr(),
                    self.len as _,
                ))
            }
        }
        pub fn raw(&self) -> [u8; 255] {
            self.raw
        }
        pub fn len(&self) -> u8 {
            self.len
        }
    }

    #[cfg(target_family = "unix")]
    fn get_hostname() -> Hostname {
        use libc::gethostname;

        let mut buf: [u8; 256] = [0; 256];
        unsafe {
            gethostname(buf.as_mut_ptr().cast(), buf.len());
            Hostname::new_from_raw_buf(&buf)
        }
    }

    #[cfg(target_family = "windows")]
    fn get_hostname() -> Hostname {
        use windows::{
            core::PSTR,
            Win32::System::SystemInformation::{
                ComputerNamePhysicalDnsHostname, GetComputerNameExA,
            },
        };
        let mut buf: [u8; 256] = [0; 256];
        let mut size: u32 = buf.len() as u32;
        unsafe {
            // UNSAFE(@ohsayan): correct call to the windows API
            GetComputerNameExA(
                ComputerNamePhysicalDnsHostname,
                PSTR(buf.as_mut_ptr()),
                &mut size as *mut u32,
            )
            .unwrap();
            Hostname::new_from_raw_buf(&buf)
        }
    }

    #[cfg(test)]
    mod test {
        use std::process::Command;

        fn test_get_hostname() -> String {
            let x = if cfg!(target_os = "windows") {
                // Windows command to get hostname
                Command::new("cmd")
                    .args(&["/C", "hostname"])
                    .output()
                    .expect("Failed to execute command")
                    .stdout
            } else {
                // Unix command to get hostname
                Command::new("uname")
                    .args(&["-n"])
                    .output()
                    .expect("Failed to execute command")
                    .stdout
            };
            String::from_utf8_lossy(&x).trim().to_string()
        }

        #[sky_macros::non_miri_test]
        fn t_get_hostname() {
            assert_eq!(
                test_get_hostname().as_str(),
                super::Hostname::get().as_str()
            );
        }
    }
}

pub fn move_files_recursively(src: &str, dst: &str) -> std::io::Result<()> {
    let src = Path::new(src);
    let dst = Path::new(dst);
    rmove(src, dst)
}

fn rmove(src: &Path, dst: &Path) -> std::io::Result<()> {
    if !dst.exists() {
        fs::create_dir_all(dst)?;
    }
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            // Compute the new destination path for this directory
            let new_dst = dst.join(entry.file_name());
            rmove(&path, &new_dst)?;
        } else if path.is_file() {
            // Compute the destination path for this file
            let dest_file = dst.join(entry.file_name());
            fs::rename(&path, &dest_file)?;
        }
    }

    Ok(())
}

#[sky_macros::non_miri_test]
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

#[sky_macros::non_miri_test]
fn t_uptime() {
    use std::{thread, time::Duration};
    let uptime_1 = get_uptime();
    thread::sleep(Duration::from_secs(1));
    let uptime_2 = get_uptime();
    // we're putting a 10s tolerance
    assert!(
        Duration::from_millis(uptime_2.try_into().unwrap())
            <= (Duration::from_millis(uptime_1.try_into().unwrap()) + Duration::from_secs(10))
    )
}
