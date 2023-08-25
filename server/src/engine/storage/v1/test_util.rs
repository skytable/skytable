/*
 * Created on Thu Aug 24 2023
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

#[cfg(test)]
use super::{
    header_impl::{FileScope, FileSpecifier, FileSpecifierVersion, HostRunMode},
    rw::{FileOpen, SDSSFileIO},
};
use {
    super::{
        rw::{RawFileIOInterface, RawFileOpen},
        SDSSResult,
    },
    crate::engine::sync::cell::Lazy,
    parking_lot::RwLock,
    std::{
        collections::hash_map::{Entry, HashMap},
        io::{Error, ErrorKind},
    },
};

static VFS: Lazy<RwLock<HashMap<Box<str>, VFile>>, fn() -> RwLock<HashMap<Box<str>, VFile>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug)]
struct VFile {
    read: bool,
    write: bool,
    data: Vec<u8>,
    pos: usize,
}

impl VFile {
    fn new(read: bool, write: bool, data: Vec<u8>, pos: usize) -> Self {
        Self {
            read,
            write,
            data,
            pos,
        }
    }
    fn current(&self) -> &[u8] {
        &self.data[self.pos..]
    }
}

#[derive(Debug)]
pub struct VirtualFS(Box<str>);

impl RawFileIOInterface for VirtualFS {
    fn fopen_or_create_rw(file_path: &str) -> super::SDSSResult<RawFileOpen<Self>> {
        match VFS.write().entry(file_path.into()) {
            Entry::Occupied(mut oe) => {
                oe.get_mut().read = true;
                oe.get_mut().write = true;
                oe.get_mut().pos = 0;
                Ok(RawFileOpen::Existing(Self(file_path.into())))
            }
            Entry::Vacant(v) => {
                v.insert(VFile::new(true, true, vec![], 0));
                Ok(RawFileOpen::Created(Self(file_path.into())))
            }
        }
    }
    fn fread_exact(&mut self, buf: &mut [u8]) -> SDSSResult<()> {
        let mut vfs = VFS.write();
        let file = vfs
            .get_mut(&self.0)
            .ok_or(Error::new(ErrorKind::NotFound, "File not found"))?;

        if !file.read {
            return Err(Error::new(ErrorKind::PermissionDenied, "Read permission denied").into());
        }
        let available_bytes = file.current().len();
        if available_bytes < buf.len() {
            return Err(Error::from(ErrorKind::UnexpectedEof).into());
        }
        buf.copy_from_slice(&file.data[file.pos..file.pos + buf.len()]);
        file.pos += buf.len();
        Ok(())
    }
    fn fwrite_all(&mut self, bytes: &[u8]) -> SDSSResult<()> {
        let mut vfs = VFS.write();
        let file = vfs
            .get_mut(&self.0)
            .ok_or(Error::new(ErrorKind::NotFound, "File not found"))?;

        if !file.write {
            return Err(Error::new(ErrorKind::PermissionDenied, "Write permission denied").into());
        }

        if file.pos + bytes.len() > file.data.len() {
            file.data.resize(file.pos + bytes.len(), 0);
        }
        file.data[file.pos..file.pos + bytes.len()].copy_from_slice(bytes);
        file.pos += bytes.len();

        Ok(())
    }
    fn fsync_all(&mut self) -> super::SDSSResult<()> {
        // pretty redundant for us
        Ok(())
    }
    fn fseek_ahead(&mut self, by: u64) -> SDSSResult<()> {
        let mut vfs = VFS.write();
        let file = vfs
            .get_mut(&self.0)
            .ok_or(Error::new(ErrorKind::NotFound, "File not found"))?;

        if by > file.data.len() as u64 {
            return Err(Error::new(ErrorKind::InvalidInput, "Can't seek beyond file's end").into());
        }

        file.pos = by as usize;
        Ok(())
    }

    fn flen(&self) -> SDSSResult<u64> {
        let vfs = VFS.read();
        let file = vfs
            .get(&self.0)
            .ok_or(Error::new(ErrorKind::NotFound, "File not found"))?;

        Ok(file.data.len() as u64)
    }

    fn flen_set(&mut self, to: u64) -> SDSSResult<()> {
        let mut vfs = VFS.write();
        let file = vfs
            .get_mut(&self.0)
            .ok_or(Error::new(ErrorKind::NotFound, "File not found"))?;

        if !file.write {
            return Err(Error::new(ErrorKind::PermissionDenied, "Write permission denied").into());
        }

        if to as usize > file.data.len() {
            file.data.resize(to as usize, 0);
        } else {
            file.data.truncate(to as usize);
        }

        if file.pos > file.data.len() {
            file.pos = file.data.len();
        }

        Ok(())
    }
    fn fcursor(&mut self) -> SDSSResult<u64> {
        let vfs = VFS.read();
        let file = vfs
            .get(&self.0)
            .ok_or(Error::new(ErrorKind::NotFound, "File not found"))?;

        Ok(file.pos as u64)
    }
}

#[test]
fn sdss_file() {
    let f = SDSSFileIO::<VirtualFS>::open_or_create_perm_rw(
        "this_is_a_test_file.db",
        FileScope::Journal,
        FileSpecifier::TestTransactionLog,
        FileSpecifierVersion::__new(0),
        0,
        HostRunMode::Prod,
        128,
    )
    .unwrap();

    let FileOpen::Created(mut f) = f else {
        panic!()
    };

    f.fsynced_write(b"hello, world\n").unwrap();
    f.fsynced_write(b"hello, again\n").unwrap();

    let f = SDSSFileIO::<VirtualFS>::open_or_create_perm_rw(
        "this_is_a_test_file.db",
        FileScope::Journal,
        FileSpecifier::TestTransactionLog,
        FileSpecifierVersion::__new(0),
        0,
        HostRunMode::Prod,
        128,
    )
    .unwrap();

    let FileOpen::Existing(mut f, _) = f else {
        panic!()
    };

    let mut buf1 = [0u8; 13];
    f.read_to_buffer(&mut buf1).unwrap();
    let mut buf2 = [0u8; 13];
    f.read_to_buffer(&mut buf2).unwrap();

    assert_eq!(&buf1, b"hello, world\n");
    assert_eq!(&buf2, b"hello, again\n");
}
