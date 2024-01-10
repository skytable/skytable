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
    super::spec::{FileSpec, Header},
    crate::{
        engine::{error::RuntimeResult, storage::common::checksum::SCrc},
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

#[cfg(test)]
impl<CF, EF> FileOpen<CF, EF> {
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

#[cfg(test)]
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
    fn fs_remove_file(fpath: &str) -> RuntimeResult<()>;
    /// Rename a file
    fn fs_rename_file(from: &str, to: &str) -> RuntimeResult<()>;
    /// Create a directory
    fn fs_create_dir(fpath: &str) -> RuntimeResult<()>;
    /// Create a directory and all corresponding path components
    fn fs_create_dir_all(fpath: &str) -> RuntimeResult<()>;
    /// Delete a directory
    fn fs_delete_dir(fpath: &str) -> RuntimeResult<()>;
    /// Delete a directory and recursively remove all (if any) children
    fn fs_delete_dir_all(fpath: &str) -> RuntimeResult<()>;
    /// Open or create a file in R/W mode
    ///
    /// This will:
    /// - Create a file if it doesn't exist
    /// - Open a file it it does exist
    fn fs_fopen_or_create_rw(fpath: &str) -> RuntimeResult<FileOpen<Self::File>>;
    /// Open an existing file
    fn fs_fopen_rw(fpath: &str) -> RuntimeResult<Self::File>;
    /// Create a new file
    fn fs_fcreate_rw(fpath: &str) -> RuntimeResult<Self::File>;
}

/// A file (well, probably) that can be used for RW operations along with advanced write and extended operations (such as seeking)
pub trait RawFileInterface: Sized
where
    Self: RawFileInterfaceRead
        + RawFileInterfaceWrite
        + RawFileInterfaceWriteExt
        + RawFileInterfaceExt,
{
    type BufReader: RawFileInterfaceBufferedReader;
    type BufWriter: RawFileInterfaceBufferedWriter;
    fn into_buffered_reader(self) -> RuntimeResult<Self::BufReader>;
    fn downgrade_reader(r: Self::BufReader) -> RuntimeResult<Self>;
    fn into_buffered_writer(self) -> RuntimeResult<Self::BufWriter>;
    fn downgrade_writer(w: Self::BufWriter) -> RuntimeResult<Self>;
}

pub trait RawFileInterfaceBufferedReader: RawFileInterfaceRead + RawFileInterfaceExt {}
impl<R: RawFileInterfaceRead + RawFileInterfaceExt> RawFileInterfaceBufferedReader for R {}

pub trait RawFileInterfaceBufferedWriter: RawFileInterfaceWrite + RawFileInterfaceExt {
    fn sync_write_cache(&mut self) -> RuntimeResult<()> {
        Ok(())
    }
}

/// A file interface that supports read operations
pub trait RawFileInterfaceRead {
    fn fr_read_exact(&mut self, buf: &mut [u8]) -> RuntimeResult<()>;
}

impl<R: Read> RawFileInterfaceRead for R {
    fn fr_read_exact(&mut self, buf: &mut [u8]) -> RuntimeResult<()> {
        self.read_exact(buf).map_err(From::from)
    }
}

/// A file interface that supports write operations
pub trait RawFileInterfaceWrite {
    fn fw_write_all(&mut self, buf: &[u8]) -> RuntimeResult<()>;
}

impl<W: Write> RawFileInterfaceWrite for W {
    fn fw_write_all(&mut self, buf: &[u8]) -> RuntimeResult<()> {
        self.write_all(buf).map_err(From::from)
    }
}

/// A file interface that supports advanced write operations
pub trait RawFileInterfaceWriteExt {
    fn fwext_fsync_all(&mut self) -> RuntimeResult<()>;
    fn fwext_truncate_to(&mut self, to: u64) -> RuntimeResult<()>;
}

/// A file interface that supports advanced file operations
pub trait RawFileInterfaceExt {
    fn fext_file_length(&self) -> RuntimeResult<u64>;
    fn fext_cursor(&mut self) -> RuntimeResult<u64>;
    fn fext_seek_ahead_from_start_by(&mut self, ahead_by: u64) -> RuntimeResult<()>;
}

fn cvt<T>(v: std::io::Result<T>) -> RuntimeResult<T> {
    let r = v?;
    Ok(r)
}

/// The actual local host file system (as an abstraction [`fs`])
#[derive(Debug)]
pub struct LocalFS;

impl RawFSInterface for LocalFS {
    type File = File;
    fn fs_remove_file(fpath: &str) -> RuntimeResult<()> {
        cvt(fs::remove_file(fpath))
    }
    fn fs_rename_file(from: &str, to: &str) -> RuntimeResult<()> {
        cvt(fs::rename(from, to))
    }
    fn fs_create_dir(fpath: &str) -> RuntimeResult<()> {
        cvt(fs::create_dir(fpath))
    }
    fn fs_create_dir_all(fpath: &str) -> RuntimeResult<()> {
        cvt(fs::create_dir_all(fpath))
    }
    fn fs_delete_dir(fpath: &str) -> RuntimeResult<()> {
        cvt(fs::remove_dir(fpath))
    }
    fn fs_delete_dir_all(fpath: &str) -> RuntimeResult<()> {
        cvt(fs::remove_dir_all(fpath))
    }
    fn fs_fopen_or_create_rw(fpath: &str) -> RuntimeResult<FileOpen<Self::File>> {
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
    fn fs_fcreate_rw(fpath: &str) -> RuntimeResult<Self::File> {
        let f = File::options()
            .create_new(true)
            .read(true)
            .write(true)
            .open(fpath)?;
        Ok(f)
    }
    fn fs_fopen_rw(fpath: &str) -> RuntimeResult<Self::File> {
        let f = File::options().read(true).write(true).open(fpath)?;
        Ok(f)
    }
}

impl RawFileInterface for File {
    type BufReader = BufReader<Self>;
    type BufWriter = BufWriter<Self>;
    fn into_buffered_reader(self) -> RuntimeResult<Self::BufReader> {
        Ok(BufReader::new(self))
    }
    fn downgrade_reader(r: Self::BufReader) -> RuntimeResult<Self> {
        Ok(r.into_inner())
    }
    fn into_buffered_writer(self) -> RuntimeResult<Self::BufWriter> {
        Ok(BufWriter::new(self))
    }
    fn downgrade_writer(mut w: Self::BufWriter) -> RuntimeResult<Self> {
        w.flush()?; // TODO(@ohsayan): handle rare case where writer does panic
        let (w, _) = w.into_parts();
        Ok(w)
    }
}

impl RawFileInterfaceBufferedWriter for BufWriter<File> {
    fn sync_write_cache(&mut self) -> RuntimeResult<()> {
        self.flush()?;
        self.get_mut().sync_all()?;
        Ok(())
    }
}

impl RawFileInterfaceWriteExt for File {
    fn fwext_fsync_all(&mut self) -> RuntimeResult<()> {
        cvt(self.sync_all())
    }
    fn fwext_truncate_to(&mut self, to: u64) -> RuntimeResult<()> {
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
    fn fext_file_length(&self) -> RuntimeResult<u64> {
        Ok(self.file().metadata()?.len())
    }
    fn fext_cursor(&mut self) -> RuntimeResult<u64> {
        cvt(self.file_mut().stream_position())
    }
    fn fext_seek_ahead_from_start_by(&mut self, by: u64) -> RuntimeResult<()> {
        cvt(self.file_mut().seek(SeekFrom::Start(by)).map(|_| ()))
    }
}

pub struct SDSSFileTrackedWriter<Fs: RawFSInterface> {
    f: SDSSFileIO<Fs, <Fs::File as RawFileInterface>::BufWriter>,
    cs: SCrc,
}

impl<Fs: RawFSInterface> SDSSFileTrackedWriter<Fs> {
    pub fn new(f: SDSSFileIO<Fs>) -> RuntimeResult<Self> {
        Ok(Self {
            f: f.into_buffered_sdss_writer()?,
            cs: SCrc::new(),
        })
    }
    pub fn tracked_write_unfsynced(&mut self, block: &[u8]) -> RuntimeResult<()> {
        self.untracked_write(block)
            .map(|_| self.cs.recompute_with_new_var_block(block))
    }
    pub fn untracked_write(&mut self, block: &[u8]) -> RuntimeResult<()> {
        match self.f.unfsynced_write(block) {
            Ok(()) => Ok(()),
            e => e,
        }
    }
    pub fn sync_writes(&mut self) -> RuntimeResult<()> {
        self.f.f.sync_write_cache()
    }
    pub fn reset_and_finish_checksum(&mut self) -> u64 {
        let scrc = core::mem::replace(&mut self.cs, SCrc::new());
        scrc.finish()
    }
    pub fn into_inner_file(self) -> RuntimeResult<SDSSFileIO<Fs>> {
        self.f.downgrade_writer()
    }
}

/// [`SDSSFileLenTracked`] simply maintains application level length and checksum tracking to avoid frequent syscalls because we
/// do not expect (even though it's very possible) users to randomly modify file lengths while we're reading them
pub struct SDSSFileTrackedReader<Fs: RawFSInterface> {
    f: SDSSFileIO<Fs, <Fs::File as RawFileInterface>::BufReader>,
    len: u64,
    pos: u64,
    cs: SCrc,
}

impl<Fs: RawFSInterface> SDSSFileTrackedReader<Fs> {
    /// Important: this will only look at the data post the current cursor!
    pub fn new(mut f: SDSSFileIO<Fs>) -> RuntimeResult<Self> {
        let len = f.file_length()?;
        let pos = f.retrieve_cursor()?;
        let f = f.into_buffered_sdss_reader()?;
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
    pub fn read_into_buffer(&mut self, buf: &mut [u8]) -> RuntimeResult<()> {
        self.untracked_read(buf)
            .map(|_| self.cs.recompute_with_new_var_block(buf))
    }
    pub fn read_byte(&mut self) -> RuntimeResult<u8> {
        let mut buf = [0u8; 1];
        self.read_into_buffer(&mut buf).map(|_| buf[0])
    }
    pub fn __reset_checksum(&mut self) -> u64 {
        let mut crc = SCrc::new();
        core::mem::swap(&mut crc, &mut self.cs);
        crc.finish()
    }
    pub fn untracked_read(&mut self, buf: &mut [u8]) -> RuntimeResult<()> {
        if self.remaining() >= buf.len() as u64 {
            match self.f.read_to_buffer(buf) {
                Ok(()) => {
                    self.pos += buf.len() as u64;
                    Ok(())
                }
                Err(e) => return Err(e),
            }
        } else {
            Err(SysIOError::from(std::io::ErrorKind::InvalidInput).into())
        }
    }
    pub fn into_inner_file(self) -> RuntimeResult<SDSSFileIO<Fs>> {
        self.f.downgrade_reader()
    }
    pub fn read_block<const N: usize>(&mut self) -> RuntimeResult<[u8; N]> {
        if !self.has_left(N as _) {
            return Err(SysIOError::from(std::io::ErrorKind::InvalidInput).into());
        }
        let mut buf = [0; N];
        self.read_into_buffer(&mut buf)?;
        Ok(buf)
    }
    pub fn read_u64_le(&mut self) -> RuntimeResult<u64> {
        Ok(u64::from_le_bytes(self.read_block()?))
    }
}

#[derive(Debug)]
pub struct SDSSFileIO<Fs: RawFSInterface, F = <Fs as RawFSInterface>::File> {
    f: F,
    _fs: PhantomData<Fs>,
}

impl<Fs: RawFSInterface> SDSSFileIO<Fs> {
    pub fn open<F: FileSpec>(fpath: &str) -> RuntimeResult<(Self, F::Header)> {
        let mut f = Self::_new(Fs::fs_fopen_rw(fpath)?);
        let header = F::Header::decode_verify(&mut f, F::DECODE_DATA, F::VERIFY_DATA)?;
        Ok((f, header))
    }
    pub fn create<F: FileSpec>(fpath: &str) -> RuntimeResult<Self> {
        let mut f = Self::_new(Fs::fs_fcreate_rw(fpath)?);
        F::Header::encode(&mut f, F::ENCODE_DATA)?;
        Ok(f)
    }
    pub fn open_or_create_perm_rw<F: FileSpec>(
        fpath: &str,
    ) -> RuntimeResult<FileOpen<Self, (Self, F::Header)>> {
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
    pub fn into_buffered_sdss_reader(
        self,
    ) -> RuntimeResult<SDSSFileIO<Fs, <Fs::File as RawFileInterface>::BufReader>> {
        self.f.into_buffered_reader().map(SDSSFileIO::_new)
    }
    pub fn into_buffered_sdss_writer(
        self,
    ) -> RuntimeResult<SDSSFileIO<Fs, <Fs::File as RawFileInterface>::BufWriter>> {
        self.f.into_buffered_writer().map(SDSSFileIO::_new)
    }
}

impl<Fs: RawFSInterface> SDSSFileIO<Fs, <Fs::File as RawFileInterface>::BufReader> {
    pub fn downgrade_reader(self) -> RuntimeResult<SDSSFileIO<Fs, Fs::File>> {
        let me = <Fs::File as RawFileInterface>::downgrade_reader(self.f)?;
        Ok(SDSSFileIO::_new(me))
    }
}

impl<Fs: RawFSInterface> SDSSFileIO<Fs, <Fs::File as RawFileInterface>::BufWriter> {
    pub fn downgrade_writer(self) -> RuntimeResult<SDSSFileIO<Fs>> {
        let me = <Fs::File as RawFileInterface>::downgrade_writer(self.f)?;
        Ok(SDSSFileIO::_new(me))
    }
}

impl<Fs: RawFSInterface, F> SDSSFileIO<Fs, F> {
    pub fn _new(f: F) -> Self {
        Self {
            f,
            _fs: PhantomData,
        }
    }
}

impl<Fs: RawFSInterface, F: RawFileInterfaceRead> SDSSFileIO<Fs, F> {
    pub fn read_to_buffer(&mut self, buffer: &mut [u8]) -> RuntimeResult<()> {
        self.f.fr_read_exact(buffer)
    }
}

impl<Fs: RawFSInterface, F: RawFileInterfaceExt> SDSSFileIO<Fs, F> {
    pub fn retrieve_cursor(&mut self) -> RuntimeResult<u64> {
        self.f.fext_cursor()
    }
    pub fn file_length(&self) -> RuntimeResult<u64> {
        self.f.fext_file_length()
    }
    pub fn seek_from_start(&mut self, by: u64) -> RuntimeResult<()> {
        self.f.fext_seek_ahead_from_start_by(by)
    }
}

impl<Fs: RawFSInterface, F: RawFileInterfaceRead + RawFileInterfaceExt> SDSSFileIO<Fs, F> {
    pub fn load_remaining_into_buffer(&mut self) -> RuntimeResult<Vec<u8>> {
        let len = self.file_length()? - self.retrieve_cursor()?;
        let mut buf = vec![0; len as usize];
        self.read_to_buffer(&mut buf)?;
        Ok(buf)
    }
}

impl<Fs: RawFSInterface, F: RawFileInterfaceWrite> SDSSFileIO<Fs, F> {
    pub fn unfsynced_write(&mut self, data: &[u8]) -> RuntimeResult<()> {
        self.f.fw_write_all(data)
    }
}

impl<Fs: RawFSInterface, F: RawFileInterfaceWrite + RawFileInterfaceWriteExt> SDSSFileIO<Fs, F> {
    pub fn fsync_all(&mut self) -> RuntimeResult<()> {
        self.f.fwext_fsync_all()?;
        Ok(())
    }
    pub fn fsynced_write(&mut self, data: &[u8]) -> RuntimeResult<()> {
        self.f.fw_write_all(data)?;
        self.f.fwext_fsync_all()
    }
}
