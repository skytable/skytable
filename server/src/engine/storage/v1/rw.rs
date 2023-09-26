/*
 * Created on Tue Jul 23 2023
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
        spec::{FileSpec, Header},
        SDSSResult,
    },
    crate::{
        engine::storage::{v1::SDSSError, SCrc},
        util::os::SysIOError,
    },
    std::{
        fs::{self, File},
        io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
        marker::PhantomData,
    },
};

#[derive(Debug)]
/// Log whether
pub enum FileOpen<CF, EF = CF> {
    Created(CF),
    Existing(EF),
}

impl<CF, EF> FileOpen<CF, EF> {
    pub const fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }
    pub const fn is_existing(&self) -> bool {
        !self.is_created()
    }
    pub fn into_existing(self) -> Option<EF> {
        match self {
            Self::Existing(e) => Some(e),
            Self::Created(_) => None,
        }
    }
    pub fn into_created(self) -> Option<CF> {
        match self {
            Self::Created(f) => Some(f),
            Self::Existing(_) => None,
        }
    }
}

impl<F> FileOpen<F> {
    pub fn into_inner(self) -> F {
        match self {
            Self::Created(f) => f,
            Self::Existing(f) => f,
        }
    }
}

/// The specification for a file system interface (our own abstraction over the fs)
pub trait RawFSInterface {
    /// asserts that the file system is not a null filesystem (like `/dev/null` for example)
    const NOT_NULL: bool = true;
    /// the file descriptor that is returned by the file system when a file is opened
    type File: RawFileInterface;
    /// Remove a file
    fn fs_remove_file(fpath: &str) -> SDSSResult<()>;
    /// Rename a file
    fn fs_rename_file(from: &str, to: &str) -> SDSSResult<()>;
    /// Create a directory
    fn fs_create_dir(fpath: &str) -> SDSSResult<()>;
    /// Create a directory and all corresponding path components
    fn fs_create_dir_all(fpath: &str) -> SDSSResult<()>;
    /// Delete a directory
    fn fs_delete_dir(fpath: &str) -> SDSSResult<()>;
    /// Delete a directory and recursively remove all (if any) children
    fn fs_delete_dir_all(fpath: &str) -> SDSSResult<()>;
    /// Open or create a file in R/W mode
    ///
    /// This will:
    /// - Create a file if it doesn't exist
    /// - Open a file it it does exist
    fn fs_fopen_or_create_rw(fpath: &str) -> SDSSResult<FileOpen<Self::File>>;
    /// Open an existing file
    fn fs_fopen_rw(fpath: &str) -> SDSSResult<Self::File>;
    /// Create a new file
    fn fs_fcreate_rw(fpath: &str) -> SDSSResult<Self::File>;
}

/// A file (well, probably) that can be used for RW operations along with advanced write and extended operations (such as seeking)
pub trait RawFileInterface
where
    Self: RawFileInterfaceRead
        + RawFileInterfaceWrite
        + RawFileInterfaceWriteExt
        + RawFileInterfaceExt,
{
    type Reader: RawFileInterfaceRead + RawFileInterfaceExt;
    type Writer: RawFileInterfaceWrite + RawFileInterfaceExt;
    fn into_buffered_reader(self) -> SDSSResult<Self::Reader>;
    fn into_buffered_writer(self) -> SDSSResult<Self::Writer>;
}

/// A file interface that supports read operations
pub trait RawFileInterfaceRead {
    fn fr_read_exact(&mut self, buf: &mut [u8]) -> SDSSResult<()>;
}

impl<R: Read> RawFileInterfaceRead for R {
    fn fr_read_exact(&mut self, buf: &mut [u8]) -> SDSSResult<()> {
        self.read_exact(buf).map_err(From::from)
    }
}

/// A file interface that supports write operations
pub trait RawFileInterfaceWrite {
    fn fw_write_all(&mut self, buf: &[u8]) -> SDSSResult<()>;
}

impl<W: Write> RawFileInterfaceWrite for W {
    fn fw_write_all(&mut self, buf: &[u8]) -> SDSSResult<()> {
        self.write_all(buf).map_err(From::from)
    }
}

/// A file interface that supports advanced write operations
pub trait RawFileInterfaceWriteExt {
    fn fw_fsync_all(&mut self) -> SDSSResult<()>;
    fn fw_truncate_to(&mut self, to: u64) -> SDSSResult<()>;
}

/// A file interface that supports advanced file operations
pub trait RawFileInterfaceExt {
    fn fext_file_length(&self) -> SDSSResult<u64>;
    fn fext_cursor(&mut self) -> SDSSResult<u64>;
    fn fext_seek_ahead_from_start_by(&mut self, ahead_by: u64) -> SDSSResult<()>;
}

fn cvt<T>(v: std::io::Result<T>) -> SDSSResult<T> {
    let r = v?;
    Ok(r)
}

/// The actual local host file system (as an abstraction [`fs`])
#[derive(Debug)]
pub struct LocalFS;

impl RawFSInterface for LocalFS {
    type File = File;
    fn fs_remove_file(fpath: &str) -> SDSSResult<()> {
        cvt(fs::remove_file(fpath))
    }
    fn fs_rename_file(from: &str, to: &str) -> SDSSResult<()> {
        cvt(fs::rename(from, to))
    }
    fn fs_create_dir(fpath: &str) -> SDSSResult<()> {
        cvt(fs::create_dir(fpath))
    }
    fn fs_create_dir_all(fpath: &str) -> SDSSResult<()> {
        cvt(fs::create_dir_all(fpath))
    }
    fn fs_delete_dir(fpath: &str) -> SDSSResult<()> {
        cvt(fs::remove_dir(fpath))
    }
    fn fs_delete_dir_all(fpath: &str) -> SDSSResult<()> {
        cvt(fs::remove_dir_all(fpath))
    }
    fn fs_fopen_or_create_rw(fpath: &str) -> SDSSResult<FileOpen<Self::File>> {
        let f = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(fpath)?;
        let md = f.metadata()?;
        if md.len() == 0 {
            Ok(FileOpen::Created(f))
        } else {
            Ok(FileOpen::Existing(f))
        }
    }
    fn fs_fcreate_rw(fpath: &str) -> SDSSResult<Self::File> {
        let f = File::options()
            .create_new(true)
            .read(true)
            .write(true)
            .open(fpath)?;
        Ok(f)
    }
    fn fs_fopen_rw(fpath: &str) -> SDSSResult<Self::File> {
        let f = File::options().read(true).write(true).open(fpath)?;
        Ok(f)
    }
}

impl RawFileInterface for File {
    type Reader = BufReader<Self>;
    type Writer = BufWriter<Self>;
    fn into_buffered_reader(self) -> SDSSResult<Self::Reader> {
        Ok(BufReader::new(self))
    }
    fn into_buffered_writer(self) -> SDSSResult<Self::Writer> {
        Ok(BufWriter::new(self))
    }
}

impl RawFileInterfaceWriteExt for File {
    fn fw_fsync_all(&mut self) -> SDSSResult<()> {
        cvt(self.sync_all())
    }
    fn fw_truncate_to(&mut self, to: u64) -> SDSSResult<()> {
        cvt(self.set_len(to))
    }
}

trait LocalFSFile {
    fn file_mut(&mut self) -> &mut File;
    fn file(&self) -> &File;
}

impl LocalFSFile for File {
    fn file_mut(&mut self) -> &mut File {
        self
    }
    fn file(&self) -> &File {
        self
    }
}

impl LocalFSFile for BufReader<File> {
    fn file_mut(&mut self) -> &mut File {
        self.get_mut()
    }
    fn file(&self) -> &File {
        self.get_ref()
    }
}

impl LocalFSFile for BufWriter<File> {
    fn file_mut(&mut self) -> &mut File {
        self.get_mut()
    }
    fn file(&self) -> &File {
        self.get_ref()
    }
}

impl<F: LocalFSFile> RawFileInterfaceExt for F {
    fn fext_file_length(&self) -> SDSSResult<u64> {
        Ok(self.file().metadata()?.len())
    }
    fn fext_cursor(&mut self) -> SDSSResult<u64> {
        cvt(self.file_mut().stream_position())
    }
    fn fext_seek_ahead_from_start_by(&mut self, by: u64) -> SDSSResult<()> {
        cvt(self.file_mut().seek(SeekFrom::Start(by)).map(|_| ()))
    }
}

pub struct SDSSFileTrackedWriter<Fs: RawFSInterface> {
    f: SDSSFileIO<Fs>,
    cs: SCrc,
}

impl<Fs: RawFSInterface> SDSSFileTrackedWriter<Fs> {
    pub fn new(f: SDSSFileIO<Fs>) -> Self {
        Self { f, cs: SCrc::new() }
    }
    pub fn unfsynced_write(&mut self, block: &[u8]) -> SDSSResult<()> {
        match self.f.unfsynced_write(block) {
            Ok(()) => {
                self.cs.recompute_with_new_var_block(block);
                Ok(())
            }
            e => e,
        }
    }
    pub fn fsync_all(&mut self) -> SDSSResult<()> {
        self.f.fsync_all()
    }
    pub fn reset_and_finish_checksum(&mut self) -> u64 {
        let mut scrc = SCrc::new();
        core::mem::swap(&mut self.cs, &mut scrc);
        scrc.finish()
    }
    pub fn inner_file(&mut self) -> &mut SDSSFileIO<Fs> {
        &mut self.f
    }
}

/// [`SDSSFileLenTracked`] simply maintains application level length and checksum tracking to avoid frequent syscalls because we
/// do not expect (even though it's very possible) users to randomly modify file lengths while we're reading them
pub struct SDSSFileTrackedReader<Fs: RawFSInterface> {
    f: SDSSFileIO<Fs>,
    len: u64,
    pos: u64,
    cs: SCrc,
}

impl<Fs: RawFSInterface> SDSSFileTrackedReader<Fs> {
    /// Important: this will only look at the data post the current cursor!
    pub fn new(mut f: SDSSFileIO<Fs>) -> SDSSResult<Self> {
        let len = f.file_length()?;
        let pos = f.retrieve_cursor()?;
        Ok(Self {
            f,
            len,
            pos,
            cs: SCrc::new(),
        })
    }
    pub fn remaining(&self) -> u64 {
        self.len - self.pos
    }
    pub fn is_eof(&self) -> bool {
        self.len == self.pos
    }
    pub fn has_left(&self, v: u64) -> bool {
        self.remaining() >= v
    }
    pub fn read_into_buffer(&mut self, buf: &mut [u8]) -> SDSSResult<()> {
        if self.remaining() >= buf.len() as u64 {
            match self.f.read_to_buffer(buf) {
                Ok(()) => {
                    self.pos += buf.len() as u64;
                    self.cs.recompute_with_new_var_block(buf);
                    Ok(())
                }
                Err(e) => return Err(e),
            }
        } else {
            Err(SDSSError::IoError(SysIOError::from(
                std::io::ErrorKind::InvalidInput,
            )))
        }
    }
    pub fn read_byte(&mut self) -> SDSSResult<u8> {
        let mut buf = [0u8; 1];
        self.read_into_buffer(&mut buf).map(|_| buf[0])
    }
    pub fn __reset_checksum(&mut self) -> u64 {
        let mut crc = SCrc::new();
        core::mem::swap(&mut crc, &mut self.cs);
        crc.finish()
    }
    pub fn inner_file(&mut self) -> &mut SDSSFileIO<Fs> {
        &mut self.f
    }
    pub fn into_inner_file(self) -> SDSSFileIO<Fs> {
        self.f
    }
    pub fn __cursor_ahead_by(&mut self, sizeof: usize) {
        self.pos += sizeof as u64;
    }
    pub fn read_block<const N: usize>(&mut self) -> SDSSResult<[u8; N]> {
        if !self.has_left(N as _) {
            return Err(SDSSError::IoError(SysIOError::from(
                std::io::ErrorKind::InvalidInput,
            )));
        }
        let mut buf = [0; N];
        self.read_into_buffer(&mut buf)?;
        Ok(buf)
    }
    pub fn read_u64_le(&mut self) -> SDSSResult<u64> {
        Ok(u64::from_le_bytes(self.read_block()?))
    }
}

#[derive(Debug)]
pub struct SDSSFileIO<Fs: RawFSInterface> {
    f: Fs::File,
    _fs: PhantomData<Fs>,
}

impl<Fs: RawFSInterface> SDSSFileIO<Fs> {
    pub fn open<F: FileSpec>(fpath: &str) -> SDSSResult<(Self, F::Header)> {
        let mut f = Self::_new(Fs::fs_fopen_rw(fpath)?);
        let header = F::Header::decode_verify(&mut f, F::DECODE_DATA, F::VERIFY_DATA)?;
        Ok((f, header))
    }
    pub fn create<F: FileSpec>(fpath: &str) -> SDSSResult<Self> {
        let mut f = Self::_new(Fs::fs_fcreate_rw(fpath)?);
        F::Header::encode(&mut f, F::ENCODE_DATA)?;
        Ok(f)
    }
    pub fn open_or_create_perm_rw<F: FileSpec>(
        fpath: &str,
    ) -> SDSSResult<FileOpen<Self, (Self, F::Header)>> {
        match Fs::fs_fopen_or_create_rw(fpath)? {
            FileOpen::Created(c) => {
                let mut f = Self::_new(c);
                F::Header::encode(&mut f, F::ENCODE_DATA)?;
                Ok(FileOpen::Created(f))
            }
            FileOpen::Existing(e) => {
                let mut f = Self::_new(e);
                let header = F::Header::decode_verify(&mut f, F::DECODE_DATA, F::VERIFY_DATA)?;
                Ok(FileOpen::Existing((f, header)))
            }
        }
    }
}

impl<Fs: RawFSInterface> SDSSFileIO<Fs> {
    pub fn _new(f: Fs::File) -> Self {
        Self {
            f,
            _fs: PhantomData,
        }
    }
    pub fn unfsynced_write(&mut self, data: &[u8]) -> SDSSResult<()> {
        self.f.fw_write_all(data)
    }
    pub fn fsync_all(&mut self) -> SDSSResult<()> {
        self.f.fw_fsync_all()?;
        Ok(())
    }
    pub fn fsynced_write(&mut self, data: &[u8]) -> SDSSResult<()> {
        self.f.fw_write_all(data)?;
        self.f.fw_fsync_all()
    }
    pub fn read_to_buffer(&mut self, buffer: &mut [u8]) -> SDSSResult<()> {
        self.f.fr_read_exact(buffer)
    }
    pub fn file_length(&self) -> SDSSResult<u64> {
        self.f.fext_file_length()
    }
    pub fn seek_from_start(&mut self, by: u64) -> SDSSResult<()> {
        self.f.fext_seek_ahead_from_start_by(by)
    }
    pub fn trim_file_to(&mut self, to: u64) -> SDSSResult<()> {
        self.f.fw_truncate_to(to)
    }
    pub fn retrieve_cursor(&mut self) -> SDSSResult<u64> {
        self.f.fext_cursor()
    }
    pub fn read_byte(&mut self) -> SDSSResult<u8> {
        let mut r = [0; 1];
        self.read_to_buffer(&mut r).map(|_| r[0])
    }
    pub fn load_remaining_into_buffer(&mut self) -> SDSSResult<Vec<u8>> {
        let len = self.file_length()? - self.retrieve_cursor()?;
        let mut buf = vec![0; len as usize];
        self.read_to_buffer(&mut buf)?;
        Ok(buf)
    }
}
