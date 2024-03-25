/*
 * Created on Thu Feb 29 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

#![allow(dead_code)]

/*
    file system
*/

use crate::util;

#[cfg(test)]
use super::vfs::{VFileDescriptor, VirtualFS};
use {
    crate::IoResult,
    std::{
        fs as std_fs,
        io::{BufReader, BufWriter, Error, ErrorKind, Read, Seek, SeekFrom, Write},
    },
};

pub struct FileSystem {}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum FSContext {
    Local,
    Virtual,
}

impl FileSystem {
    fn context() -> FSContext {
        local! { static CTX: FSContext = FSContext::Virtual; }
        local_ref!(CTX, |ctx| *ctx)
    }
}

impl FileSystem {
    #[inline(always)]
    pub fn copy_directory(from: &str, to: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_copy(from, to),
            }
        }
        util::os::recursive_copy(from, to)
    }
    #[inline(always)]
    pub fn copy(from: &str, to: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_copy(from, to),
            }
        }
        std_fs::copy(from, to).map(|_| ())
    }
    #[inline(always)]
    pub fn read(path: &str) -> IoResult<Vec<u8>> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().read().get_data(path),
            }
        }
        std_fs::read(path)
    }
    #[inline(always)]
    pub fn create_dir(path: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_create_dir(path),
            }
        }
        std_fs::create_dir(path)
    }
    #[inline(always)]
    pub fn create_dir_all(path: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_create_dir_all(path),
            }
        }
        std_fs::create_dir_all(path)
    }
    #[inline(always)]
    pub fn remove_dir(path: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_delete_dir(path),
            }
        }
        std_fs::remove_dir(path)
    }
    #[inline(always)]
    pub fn remove_dir_all(path: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_delete_dir_all(path),
            }
        }
        std_fs::remove_dir_all(path)
    }
    #[inline(always)]
    pub fn remove_file(path: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_remove_file(path),
            }
        }
        std_fs::remove_file(path)
    }
    #[inline(always)]
    pub fn rename(from: &str, to: &str) -> IoResult<()> {
        #[cfg(test)]
        {
            match Self::context() {
                FSContext::Local => {}
                FSContext::Virtual => return VirtualFS::instance().write().fs_rename(from, to),
            }
        }
        std_fs::rename(from, to)
    }
}

/*
    file traits
*/

pub trait FileRead {
    fn fread_exact(&mut self, buf: &mut [u8]) -> IoResult<()>;
    fn fread_exact_block<const N: usize>(&mut self) -> IoResult<[u8; N]> {
        let mut blk = [0; N];
        self.fread_exact(&mut blk).map(|_| blk)
    }
}

pub trait FileWrite {
    fn fwrite(&mut self, buf: &[u8]) -> IoResult<u64>;
    fn fwrite_all(&mut self, buf: &[u8]) -> IoResult<()> {
        self.fwrite_all_count(buf).1
    }
    fn fwrite_all_count(&mut self, buf: &[u8]) -> (u64, IoResult<()>) {
        let len = buf.len() as u64;
        let mut written = 0;
        while written != len {
            match self.fwrite(buf) {
                Ok(0) => {
                    return (
                        written,
                        Err(Error::new(
                            ErrorKind::WriteZero,
                            format!("could only write {} of {} bytes", written, buf.len()),
                        )
                        .into()),
                    )
                }
                Ok(n) => written += n,
                Err(e) => return (written, Err(e)),
            }
        }
        (written, Ok(()))
    }
}

pub trait FileWriteExt {
    fn fsync_all(&mut self) -> IoResult<()>;
    fn fsync_data(&mut self) -> IoResult<()>;
    fn f_truncate(&mut self, new_size: u64) -> IoResult<()>;
}

pub trait FileExt {
    fn f_len(&self) -> IoResult<u64>;
    fn f_cursor(&mut self) -> IoResult<u64>;
    fn f_seek_start(&mut self, offset: u64) -> IoResult<()>;
}

/*
    file impls
*/

impl FileWrite for File {
    fn fwrite(&mut self, buf: &[u8]) -> IoResult<u64> {
        self.f.fwrite(buf)
    }
}

impl FileRead for File {
    fn fread_exact(&mut self, buf: &mut [u8]) -> IoResult<()> {
        self.f.fread_exact(buf)
    }
}

impl FileWriteExt for File {
    fn fsync_all(&mut self) -> IoResult<()> {
        self.f.fsync_all()
    }
    fn fsync_data(&mut self) -> IoResult<()> {
        self.f.fsync_data()
    }
    fn f_truncate(&mut self, new_size: u64) -> IoResult<()> {
        self.f.f_truncate(new_size)
    }
}

impl FileExt for File {
    fn f_len(&self) -> IoResult<u64> {
        self.f.f_len()
    }
    fn f_cursor(&mut self) -> IoResult<u64> {
        self.f.f_cursor()
    }
    fn f_seek_start(&mut self, offset: u64) -> IoResult<()> {
        self.f.f_seek_start(offset)
    }
}

/*
    impls for local file
*/

trait LocalFile {
    fn _mut(&mut self) -> &mut std_fs::File;
    fn _ref(&self) -> &std_fs::File;
}

impl LocalFile for BufReader<std_fs::File> {
    fn _mut(&mut self) -> &mut std_fs::File {
        self.get_mut()
    }
    fn _ref(&self) -> &std_fs::File {
        self.get_ref()
    }
}

impl LocalFile for std_fs::File {
    fn _mut(&mut self) -> &mut std_fs::File {
        self
    }
    fn _ref(&self) -> &std_fs::File {
        self
    }
}

impl<W: Write> FileWrite for W {
    fn fwrite(&mut self, buf: &[u8]) -> IoResult<u64> {
        self.write(buf).map(|x| x as u64)
    }
}

impl<R: Read> FileRead for R {
    fn fread_exact(&mut self, buf: &mut [u8]) -> IoResult<()> {
        self.read_exact(buf)
    }
}

impl<Lf: LocalFile> FileWriteExt for Lf {
    fn fsync_all(&mut self) -> IoResult<()> {
        self._mut().sync_all()
    }
    fn fsync_data(&mut self) -> IoResult<()> {
        self._mut().sync_data()
    }
    fn f_truncate(&mut self, new_size: u64) -> IoResult<()> {
        self._mut().set_len(new_size)
    }
}

impl<Lf: LocalFile> FileExt for Lf {
    fn f_len(&self) -> IoResult<u64> {
        self._ref().metadata().map(|md| md.len())
    }
    fn f_cursor(&mut self) -> IoResult<u64> {
        self._mut().stream_position()
    }
    fn f_seek_start(&mut self, offset: u64) -> IoResult<()> {
        self._mut().seek(SeekFrom::Start(offset)).map(|_| ())
    }
}

/*
    impls for vfile
*/

#[cfg(test)]
impl<Lf: FileWrite> FileWrite for AnyFile<Lf> {
    fn fwrite(&mut self, buf: &[u8]) -> IoResult<u64> {
        match self {
            Self::Local(lf) => lf.fwrite(buf),
            Self::Virtual(vf) => VirtualFS::instance()
                .read()
                .with_file_mut(&vf.0, |f| f.fwrite(buf)),
        }
    }
}

#[cfg(test)]
impl<Lf: FileRead> FileRead for AnyFile<Lf> {
    fn fread_exact(&mut self, buf: &mut [u8]) -> IoResult<()> {
        match self {
            Self::Local(lf) => lf.fread_exact(buf),
            Self::Virtual(vf) => VirtualFS::instance()
                .read()
                .with_file_mut(&vf.0, |f| f.fread_exact(buf)),
        }
    }
}

#[cfg(test)]
impl<Lf: FileWriteExt> FileWriteExt for AnyFile<Lf> {
    fn fsync_all(&mut self) -> IoResult<()> {
        match self {
            Self::Local(lf) => lf.fsync_all(),
            Self::Virtual(_) => Ok(()),
        }
    }
    fn fsync_data(&mut self) -> IoResult<()> {
        match self {
            Self::Local(lf) => lf.fsync_data(),
            Self::Virtual(_) => Ok(()),
        }
    }
    fn f_truncate(&mut self, new_size: u64) -> IoResult<()> {
        match self {
            Self::Local(lf) => lf.f_truncate(new_size),
            Self::Virtual(vf) => VirtualFS::instance()
                .read()
                .with_file_mut(&vf.0, |f| f.truncate(new_size)),
        }
    }
}

#[cfg(test)]
impl<Lf: FileExt> FileExt for AnyFile<Lf> {
    fn f_len(&self) -> IoResult<u64> {
        match self {
            Self::Local(lf) => lf.f_len(),
            Self::Virtual(vf) => VirtualFS::instance()
                .read()
                .with_file(&vf.0, |f| f.length()),
        }
    }
    fn f_cursor(&mut self) -> IoResult<u64> {
        match self {
            Self::Local(lf) => lf.f_cursor(),
            Self::Virtual(vf) => VirtualFS::instance()
                .read()
                .with_file(&vf.0, |f| f.cursor()),
        }
    }
    fn f_seek_start(&mut self, offset: u64) -> IoResult<()> {
        match self {
            Self::Local(lf) => lf.f_seek_start(offset),
            Self::Virtual(vf) => VirtualFS::instance()
                .read()
                .with_file_mut(&vf.0, |f| f.seek_from_start(offset)),
        }
    }
}

/*
    file abstraction
*/

#[cfg(test)]
#[derive(Debug)]
enum AnyFile<Lf = std_fs::File> {
    Local(Lf),
    Virtual(VFileDescriptor),
}

#[derive(Debug)]
pub struct File {
    #[cfg(test)]
    f: AnyFile,
    #[cfg(not(test))]
    f: std_fs::File,
}

impl File {
    pub fn open(path: &str) -> IoResult<Self> {
        #[cfg(test)]
        {
            match FileSystem::context() {
                FSContext::Local => {}
                FSContext::Virtual => {
                    return VirtualFS::instance()
                        .write()
                        .fs_fopen_rw(path)
                        .map(|f| Self {
                            f: AnyFile::Virtual(f),
                        })
                }
            }
        }
        let file = std_fs::File::options().read(true).write(true).open(path)?;
        Ok(Self {
            #[cfg(test)]
            f: AnyFile::Local(file),
            #[cfg(not(test))]
            f: file,
        })
    }
    pub fn create(path: &str) -> IoResult<Self> {
        #[cfg(test)]
        {
            match FileSystem::context() {
                FSContext::Local => {}
                FSContext::Virtual => {
                    return VirtualFS::instance()
                        .write()
                        .fs_fcreate_rw(path)
                        .map(|f| Self {
                            f: AnyFile::Virtual(f),
                        })
                }
            }
        }
        let file = std_fs::File::options()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Self {
            #[cfg(test)]
            f: AnyFile::Local(file),
            #[cfg(not(test))]
            f: file,
        })
    }
    pub fn into_buffered_reader(self) -> BufferedReader {
        BufferedReader::new(self.f)
    }
    pub fn into_buffered_writer(self) -> BufferedWriter {
        BufferedWriter::new(self.f)
    }
}

/*
    buffered readers and writers
*/

pub struct BufferedReader {
    #[cfg(test)]
    f: AnyFile<BufReader<std_fs::File>>,
    #[cfg(not(test))]
    f: BufReader<std_fs::File>,
}

impl BufferedReader {
    fn new(#[cfg(test)] f: AnyFile<std_fs::File>, #[cfg(not(test))] f: std_fs::File) -> Self {
        Self {
            #[cfg(test)]
            f: match f {
                AnyFile::Local(lf) => AnyFile::Local(BufReader::new(lf)),
                AnyFile::Virtual(vf) => AnyFile::Virtual(vf),
            },
            #[cfg(not(test))]
            f: BufReader::new(f),
        }
    }
    pub fn into_inner(self) -> File {
        File {
            #[cfg(test)]
            f: match self.f {
                AnyFile::Local(lf) => AnyFile::Local(lf.into_inner()),
                AnyFile::Virtual(vf) => AnyFile::Virtual(vf),
            },
            #[cfg(not(test))]
            f: self.f.into_inner(),
        }
    }
}

impl FileRead for BufferedReader {
    fn fread_exact(&mut self, buf: &mut [u8]) -> IoResult<()> {
        self.f.fread_exact(buf)
    }
}

impl FileExt for BufferedReader {
    fn f_len(&self) -> IoResult<u64> {
        self.f.f_len()
    }
    fn f_cursor(&mut self) -> IoResult<u64> {
        self.f.f_cursor()
    }
    fn f_seek_start(&mut self, offset: u64) -> IoResult<()> {
        self.f.f_seek_start(offset)
    }
}

pub struct BufferedWriter {
    #[cfg(test)]
    f: AnyFile<BufWriter<std_fs::File>>,
    #[cfg(not(test))]
    f: BufWriter<std_fs::File>,
}

impl BufferedWriter {
    pub fn into_inner(self) -> IoResult<File> {
        let mut local;
        #[cfg(test)]
        {
            match self.f {
                AnyFile::Local(lf) => local = lf,
                AnyFile::Virtual(vf) => {
                    return Ok(File {
                        f: AnyFile::Virtual(vf),
                    })
                }
            }
        }
        #[cfg(not(test))]
        {
            local = self.f;
        }
        local.flush()?;
        let local = local.into_inner().unwrap();
        Ok(File {
            #[cfg(test)]
            f: AnyFile::Local(local),
            #[cfg(not(test))]
            f: local,
        })
    }
    pub fn flush(&mut self) -> IoResult<()> {
        let local;
        #[cfg(test)]
        {
            match self.f {
                AnyFile::Local(ref mut l) => local = l,
                AnyFile::Virtual(_) => return Ok(()),
            }
        }
        #[cfg(not(test))]
        {
            local = &mut self.f;
        }
        local.flush()
    }
    fn new(#[cfg(test)] f: AnyFile<std_fs::File>, #[cfg(not(test))] f: std_fs::File) -> Self {
        Self {
            #[cfg(test)]
            f: match f {
                AnyFile::Local(lf) => AnyFile::Local(BufWriter::new(lf)),
                AnyFile::Virtual(vf) => AnyFile::Virtual(vf),
            },
            #[cfg(not(test))]
            f: BufWriter::new(f),
        }
    }
}
