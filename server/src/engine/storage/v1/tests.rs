/*
 * Created on Thu Jul 23 2023
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

use {
    super::{
        rw::{RawFileIOInterface, RawFileOpen},
        SDSSError, SDSSResult,
    },
    crate::engine::sync::cell::Lazy,
    parking_lot::RwLock,
    std::{
        collections::{hash_map::Entry, HashMap},
        io::{ErrorKind, Read, Write},
    },
};

static VFS: Lazy<
    RwLock<HashMap<String, VirtualFile>>,
    fn() -> RwLock<HashMap<String, VirtualFile>>,
> = Lazy::new(|| RwLock::new(HashMap::new()));

fn vfs<T>(fname: &str, mut func: impl FnMut(&mut VirtualFile) -> SDSSResult<T>) -> SDSSResult<T> {
    let mut vfs = VFS.write();
    let f = vfs
        .get_mut(fname)
        .ok_or(SDSSError::from(std::io::Error::from(ErrorKind::NotFound)))?;
    func(f)
}

struct VirtualFile {
    pos: u64,
    read: bool,
    write: bool,
    data: Vec<u8>,
}

impl VirtualFile {
    fn new(read: bool, write: bool, data: Vec<u8>) -> Self {
        Self {
            read,
            write,
            data,
            pos: 0,
        }
    }
    fn rw(data: Vec<u8>) -> Self {
        Self::new(true, true, data)
    }
    fn w(data: Vec<u8>) -> Self {
        Self::new(false, true, data)
    }
    fn r(data: Vec<u8>) -> Self {
        Self::new(true, false, data)
    }
    fn seek_forward(&mut self, by: u64) {
        self.pos += by;
        assert!(self.pos <= self.data.len() as u64);
    }
    fn data(&self) -> &[u8] {
        &self.data[self.pos as usize..]
    }
    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.pos as usize..]
    }
}

struct VirtualFileInterface(Box<str>);

impl RawFileIOInterface for VirtualFileInterface {
    fn fopen_or_create_rw(file_path: &str) -> SDSSResult<RawFileOpen<Self>> {
        match VFS.write().entry(file_path.to_owned()) {
            Entry::Occupied(_) => Ok(RawFileOpen::Existing(Self(file_path.into()))),
            Entry::Vacant(ve) => {
                ve.insert(VirtualFile::rw(vec![]));
                Ok(RawFileOpen::Created(Self(file_path.into())))
            }
        }
    }
    fn fread_exact(&mut self, buf: &mut [u8]) -> super::SDSSResult<()> {
        vfs(&self.0, |f| {
            assert!(f.read);
            f.data().read_exact(buf)?;
            Ok(())
        })
    }
    fn fwrite_all(&mut self, bytes: &[u8]) -> super::SDSSResult<()> {
        vfs(&self.0, |f| {
            assert!(f.write);
            if f.data.len() < bytes.len() {
                f.data.extend(bytes);
            } else {
                f.data_mut().write_all(bytes)?;
            }
            Ok(())
        })
    }
    fn fsync_all(&mut self) -> super::SDSSResult<()> {
        Ok(())
    }
    fn flen(&self) -> SDSSResult<u64> {
        vfs(&self.0, |f| Ok(f.data.len() as _))
    }
    fn fseek_ahead(&mut self, by: u64) -> SDSSResult<()> {
        vfs(&self.0, |f| {
            f.seek_forward(by);
            Ok(())
        })
    }
}

mod rw {
    use {
        super::VirtualFileInterface,
        crate::engine::storage::v1::{
            header_impl::{FileScope, FileSpecifier, FileSpecifierVersion, HostRunMode},
            rw::{FileOpen, SDSSFileIO},
        },
    };

    #[test]
    fn create_delete() {
        let f = SDSSFileIO::<VirtualFileInterface>::open_or_create_perm_rw(
            "hello_world.db-tlog",
            FileScope::TransactionLogCompacted,
            FileSpecifier::GNSTxnLog,
            FileSpecifierVersion::__new(0),
            0,
            HostRunMode::Prod,
            0,
        )
        .unwrap();
        match f {
            FileOpen::Existing(_, _) => panic!(),
            FileOpen::Created(_) => {}
        };
        let open = SDSSFileIO::<VirtualFileInterface>::open_or_create_perm_rw(
            "hello_world.db-tlog",
            FileScope::TransactionLogCompacted,
            FileSpecifier::GNSTxnLog,
            FileSpecifierVersion::__new(0),
            0,
            HostRunMode::Prod,
            0,
        )
        .unwrap();
        let h = match open {
            FileOpen::Existing(_, header) => header,
            _ => panic!(),
        };
        assert_eq!(h.gr_mdr().file_scope(), FileScope::TransactionLogCompacted);
        assert_eq!(h.gr_mdr().file_spec(), FileSpecifier::GNSTxnLog);
        assert_eq!(h.gr_mdr().file_spec_id(), FileSpecifierVersion::__new(0));
        assert_eq!(h.gr_hr().run_mode(), HostRunMode::Prod);
        assert_eq!(h.gr_hr().setting_version(), 0);
        assert_eq!(h.gr_hr().startup_counter(), 0);
    }
}
