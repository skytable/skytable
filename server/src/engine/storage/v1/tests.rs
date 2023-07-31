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
    fn close(&mut self) {
        self.pos = 0;
        self.read = false;
        self.write = false;
    }
}

struct VirtualFileInterface(Box<str>);

impl Drop for VirtualFileInterface {
    fn drop(&mut self) {
        vfs(&self.0, |f| {
            f.close();
            Ok(())
        })
        .unwrap();
    }
}

impl RawFileIOInterface for VirtualFileInterface {
    fn fopen_or_create_rw(file_path: &str) -> SDSSResult<RawFileOpen<Self>> {
        match VFS.write().entry(file_path.to_owned()) {
            Entry::Occupied(mut oe) => {
                let file_md = oe.get_mut();
                file_md.read = true;
                file_md.write = true;
                Ok(RawFileOpen::Existing(Self(file_path.into())))
            }
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

type VirtualFS = VirtualFileInterface;
type RealFS = std::fs::File;

mod rw {
    use crate::engine::storage::v1::{
        header_impl::{FileScope, FileSpecifier, FileSpecifierVersion, HostRunMode},
        rw::{FileOpen, SDSSFileIO},
    };

    #[test]
    fn create_delete() {
        let f = SDSSFileIO::<super::VirtualFS>::open_or_create_perm_rw(
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
        let open = SDSSFileIO::<super::VirtualFS>::open_or_create_perm_rw(
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

mod tx {
    use crate::engine::storage::v1::header_impl::{
        FileSpecifier, FileSpecifierVersion, HostRunMode,
    };

    type FileInterface = super::RealFS;

    use {
        crate::{
            engine::storage::v1::{
                txn::{self, TransactionLogAdapter, TransactionLogWriter},
                SDSSError, SDSSResult,
            },
            util,
        },
        std::cell::RefCell,
    };
    pub struct Database {
        data: RefCell<[u8; 10]>,
    }
    impl Database {
        fn copy_data(&self) -> [u8; 10] {
            *self.data.borrow()
        }
        fn new() -> Self {
            Self {
                data: RefCell::new([0; 10]),
            }
        }
        fn reset(&self) {
            *self.data.borrow_mut() = [0; 10];
        }
        fn txn_reset(
            &self,
            txn_writer: &mut TransactionLogWriter<FileInterface, DatabaseTxnAdapter>,
        ) -> SDSSResult<()> {
            self.reset();
            txn_writer.append_event(TxEvent::Reset)
        }
        fn set(&self, pos: usize, val: u8) {
            self.data.borrow_mut()[pos] = val;
        }
        fn txn_set(
            &self,
            pos: usize,
            val: u8,
            txn_writer: &mut TransactionLogWriter<FileInterface, DatabaseTxnAdapter>,
        ) -> SDSSResult<()> {
            self.set(pos, val);
            txn_writer.append_event(TxEvent::Set(pos, val))
        }
    }
    pub enum TxEvent {
        Reset,
        Set(usize, u8),
    }
    #[derive(Debug)]
    pub struct DatabaseTxnAdapter;
    impl TransactionLogAdapter for DatabaseTxnAdapter {
        type TransactionEvent = TxEvent;

        type GlobalState = Database;

        fn encode(event: Self::TransactionEvent) -> Box<[u8]> {
            /*
                [1B: opcode][8B:Index][1B: New value]
            */
            let opcode = match event {
                TxEvent::Reset => 0u8,
                TxEvent::Set(_, _) => 1u8,
            };
            let index = match event {
                TxEvent::Reset => 0u64,
                TxEvent::Set(index, _) => index as u64,
            };
            let new_value = match event {
                TxEvent::Reset => 0,
                TxEvent::Set(_, val) => val,
            };
            let mut ret = Vec::with_capacity(10);
            ret.push(opcode);
            ret.extend(index.to_le_bytes());
            ret.push(new_value);
            ret.into_boxed_slice()
        }

        fn decode_and_update_state(payload: &[u8], gs: &Self::GlobalState) -> SDSSResult<()> {
            if payload.len() != 10 {
                return Err(SDSSError::CorruptedFile("testtxn.log"));
            }
            let opcode = payload[0];
            let index = u64::from_le_bytes(util::copy_slice_to_array(&payload[1..9]));
            let new_value = payload[9];
            match opcode {
                0 if index == 0 && new_value == 0 => gs.reset(),
                1 if index < 10 && index < isize::MAX as u64 => gs.set(index as usize, new_value),
                _ => return Err(SDSSError::TransactionLogEntryCorrupted),
            }
            Ok(())
        }
    }

    #[test]
    fn two_set() {
        // create log
        let db1 = Database::new();
        let x = || -> SDSSResult<()> {
            let mut log = txn::open_log(
                "testtxn.log",
                FileSpecifier::TestTransactionLog,
                FileSpecifierVersion::__new(0),
                0,
                HostRunMode::Prod,
                1,
                &db1,
            )?;
            db1.txn_set(0, 20, &mut log)?;
            db1.txn_set(9, 21, &mut log)?;
            log.close_log()
        };
        x().unwrap();
        // backup original data
        let original_data = db1.copy_data();
        // restore log
        let empty_db2 = Database::new();
        {
            let log = txn::open_log::<DatabaseTxnAdapter, FileInterface>(
                "testtxn.log",
                FileSpecifier::TestTransactionLog,
                FileSpecifierVersion::__new(0),
                0,
                HostRunMode::Prod,
                1,
                &empty_db2,
            )
            .unwrap();
            log.close_log().unwrap();
        }
        assert_eq!(original_data, empty_db2.copy_data());
        std::fs::remove_file("testtxn.log").unwrap();
    }
}
