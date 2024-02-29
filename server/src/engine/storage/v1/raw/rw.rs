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

use crate::{
    engine::{
        storage::common::{
            checksum::SCrc64,
            interface::fs::{
                BufferedReader, BufferedWriter, File, FileExt, FileRead, FileWrite, FileWriteExt,
            },
            sdss,
        },
        RuntimeResult,
    },
    util::os::SysIOError,
    IoResult,
};

pub struct TrackedWriter {
    file: SDSSFileIO<BufferedWriter>,
    _cs: SCrc64,
}

impl TrackedWriter {
    pub fn new(f: SDSSFileIO<File>) -> IoResult<Self> {
        Ok(Self {
            file: f.into_buffered_writer(),
            _cs: SCrc64::new(),
        })
    }
    pub fn sync_into_inner(self) -> IoResult<SDSSFileIO<File>> {
        self.file.downgrade_writer()
    }
}

/// [`SDSSFileLenTracked`] simply maintains application level length and checksum tracking to avoid frequent syscalls because we
/// do not expect (even though it's very possible) users to randomly modify file lengths while we're reading them
pub struct TrackedReader {
    f: SDSSFileIO<BufferedReader>,
    len: u64,
    cursor: u64,
    cs: SCrc64,
}

impl TrackedReader {
    /// Important: this will only look at the data post the current cursor!
    pub fn new(mut f: SDSSFileIO<BufferedReader>) -> IoResult<Self> {
        let len = f.file_length()?;
        let pos = f.file_cursor()?;
        Ok(Self {
            f,
            len,
            cursor: pos,
            cs: SCrc64::new(),
        })
    }
    pub fn remaining(&self) -> u64 {
        self.len - self.cursor
    }
    pub fn is_eof(&self) -> bool {
        self.len == self.cursor
    }
    pub fn has_left(&self, v: u64) -> bool {
        self.remaining() >= v
    }
    pub fn tracked_read(&mut self, buf: &mut [u8]) -> IoResult<()> {
        self.untracked_read(buf).map(|_| self.cs.update(buf))
    }
    pub fn read_byte(&mut self) -> IoResult<u8> {
        let mut buf = [0u8; 1];
        self.tracked_read(&mut buf).map(|_| buf[0])
    }
    pub fn __reset_checksum(&mut self) -> u64 {
        let mut crc = SCrc64::new();
        core::mem::swap(&mut crc, &mut self.cs);
        crc.finish()
    }
    pub fn untracked_read(&mut self, buf: &mut [u8]) -> IoResult<()> {
        if self.remaining() >= buf.len() as u64 {
            match self.f.read_buffer(buf) {
                Ok(()) => {
                    self.cursor += buf.len() as u64;
                    Ok(())
                }
                Err(e) => return Err(e),
            }
        } else {
            Err(SysIOError::from(std::io::ErrorKind::InvalidInput).into_inner())
        }
    }
    pub fn into_inner_file(self) -> SDSSFileIO<File> {
        self.f.downgrade_reader()
    }
    pub fn read_block<const N: usize>(&mut self) -> IoResult<[u8; N]> {
        if !self.has_left(N as _) {
            return Err(SysIOError::from(std::io::ErrorKind::InvalidInput).into_inner());
        }
        let mut buf = [0; N];
        self.tracked_read(&mut buf)?;
        Ok(buf)
    }
    pub fn read_u64_le(&mut self) -> IoResult<u64> {
        Ok(u64::from_le_bytes(self.read_block()?))
    }
}

#[derive(Debug)]
pub struct SDSSFileIO<F> {
    f: F,
}
impl<F> SDSSFileIO<F> {
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl SDSSFileIO<File> {
    pub fn open<S: sdss::sdss_r1::FileSpecV1<DecodeArgs = ()>>(
        fpath: &str,
    ) -> RuntimeResult<(Self, S::Metadata)> {
        let mut f = Self::_new(File::open(fpath)?);
        let v = S::read_metadata(&mut f.f, ())?;
        Ok((f, v))
    }
    pub fn into_buffered_reader(self) -> SDSSFileIO<BufferedReader> {
        SDSSFileIO::new(self.f.into_buffered_reader())
    }
    pub fn into_buffered_writer(self) -> SDSSFileIO<BufferedWriter> {
        SDSSFileIO::new(self.f.into_buffered_writer())
    }
}

impl SDSSFileIO<BufferedReader> {
    pub fn downgrade_reader(self) -> SDSSFileIO<File> {
        SDSSFileIO::_new(self.f.into_inner())
    }
}

impl SDSSFileIO<BufferedWriter> {
    pub fn downgrade_writer(self) -> IoResult<SDSSFileIO<File>> {
        self.f.into_inner().map(SDSSFileIO::_new)
    }
}

impl<F> SDSSFileIO<F> {
    fn _new(f: F) -> Self {
        Self { f }
    }
}

impl<F: FileRead> SDSSFileIO<F> {
    pub fn read_buffer(&mut self, buffer: &mut [u8]) -> IoResult<()> {
        self.f.fread_exact(buffer)
    }
}

impl<F: FileExt> SDSSFileIO<F> {
    pub fn file_cursor(&mut self) -> IoResult<u64> {
        self.f.f_cursor()
    }
    pub fn file_length(&self) -> IoResult<u64> {
        self.f.f_len()
    }
    pub fn seek_from_start(&mut self, by: u64) -> IoResult<()> {
        self.f.f_seek_start(by)
    }
}

impl<F: FileRead + FileExt> SDSSFileIO<F> {
    pub fn read_full(&mut self) -> IoResult<Vec<u8>> {
        let len = self.file_length()? - self.file_cursor()?;
        let mut buf = vec![0; len as usize];
        self.read_buffer(&mut buf)?;
        Ok(buf)
    }
}

impl<F: FileWrite + FileWriteExt> SDSSFileIO<F> {
    pub fn fsynced_write(&mut self, data: &[u8]) -> IoResult<()> {
        self.f.fwrite_all(data)?;
        self.f.fsync_all()
    }
}
