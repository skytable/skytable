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
        header_impl::{
            FileScope, FileSpecifier, FileSpecifierVersion, HostRunMode, SDSSHeader, SDSSHeaderRaw,
        },
        SDSSResult,
    },
    crate::{
        engine::storage::{v1::SDSSError, SCrc},
        util::os::SysIOError,
    },
    std::{
        fs::File,
        io::{Read, Seek, SeekFrom, Write},
        ptr, slice,
    },
};

#[derive(Debug)]
/// Log whether
pub enum FileOpen<F> {
    Created(F),
    Existing(F, SDSSHeader),
}

impl<F> FileOpen<F> {
    pub fn into_existing(self) -> Option<(F, SDSSHeader)> {
        match self {
            Self::Existing(f, h) => Some((f, h)),
            Self::Created(_) => None,
        }
    }
    pub fn into_created(self) -> Option<F> {
        match self {
            Self::Created(f) => Some(f),
            Self::Existing(_, _) => None,
        }
    }
}

#[derive(Debug)]
pub enum RawFileOpen<F> {
    Created(F),
    Existing(F),
}

pub trait RawFileIOInterface: Sized {
    /// Indicates that the interface is not a `/dev/null` (or related) implementation
    const NOTNULL: bool = true;
    fn fopen_or_create_rw(file_path: &str) -> SDSSResult<RawFileOpen<Self>>;
    fn fread_exact(&mut self, buf: &mut [u8]) -> SDSSResult<()>;
    fn fwrite_all(&mut self, bytes: &[u8]) -> SDSSResult<()>;
    fn fsync_all(&mut self) -> SDSSResult<()>;
    fn fseek_ahead(&mut self, by: u64) -> SDSSResult<()>;
    fn flen(&self) -> SDSSResult<u64>;
    fn flen_set(&mut self, to: u64) -> SDSSResult<()>;
    fn fcursor(&mut self) -> SDSSResult<u64>;
}

/// This is a kind of file like `/dev/null`. It exists in ... nothing!
pub struct NullZero;

impl RawFileIOInterface for NullZero {
    const NOTNULL: bool = false;
    fn fopen_or_create_rw(_: &str) -> SDSSResult<RawFileOpen<Self>> {
        Ok(RawFileOpen::Created(Self))
    }
    fn fread_exact(&mut self, _: &mut [u8]) -> SDSSResult<()> {
        Ok(())
    }
    fn fwrite_all(&mut self, _: &[u8]) -> SDSSResult<()> {
        Ok(())
    }
    fn fsync_all(&mut self) -> SDSSResult<()> {
        Ok(())
    }
    fn fseek_ahead(&mut self, _: u64) -> SDSSResult<()> {
        Ok(())
    }
    fn flen(&self) -> SDSSResult<u64> {
        Ok(0)
    }
    fn flen_set(&mut self, _: u64) -> SDSSResult<()> {
        Ok(())
    }
    fn fcursor(&mut self) -> SDSSResult<u64> {
        Ok(0)
    }
}

impl RawFileIOInterface for File {
    fn fopen_or_create_rw(file_path: &str) -> SDSSResult<RawFileOpen<Self>> {
        let f = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(file_path)?;
        let md = f.metadata()?;
        if md.len() == 0 {
            Ok(RawFileOpen::Created(f))
        } else {
            Ok(RawFileOpen::Existing(f))
        }
    }
    fn fread_exact(&mut self, buf: &mut [u8]) -> SDSSResult<()> {
        self.read_exact(buf)?;
        Ok(())
    }
    fn fwrite_all(&mut self, bytes: &[u8]) -> SDSSResult<()> {
        self.write_all(bytes)?;
        Ok(())
    }
    fn fsync_all(&mut self) -> SDSSResult<()> {
        self.sync_all()?;
        Ok(())
    }
    fn flen(&self) -> SDSSResult<u64> {
        Ok(self.metadata()?.len())
    }
    fn fseek_ahead(&mut self, by: u64) -> SDSSResult<()> {
        self.seek(SeekFrom::Start(by))?;
        Ok(())
    }
    fn flen_set(&mut self, to: u64) -> SDSSResult<()> {
        self.set_len(to)?;
        Ok(())
    }
    fn fcursor(&mut self) -> SDSSResult<u64> {
        self.stream_position().map_err(From::from)
    }
}

pub struct SDSSFileTrackedWriter<F> {
    f: SDSSFileIO<F>,
    cs: SCrc,
}

impl<F: RawFileIOInterface> SDSSFileTrackedWriter<F> {
    pub fn new(f: SDSSFileIO<F>) -> Self {
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
    pub fn inner_file(&mut self) -> &mut SDSSFileIO<F> {
        &mut self.f
    }
}

/// [`SDSSFileLenTracked`] simply maintains application level length and checksum tracking to avoid frequent syscalls because we
/// do not expect (even though it's very possible) users to randomly modify file lengths while we're reading them
pub struct SDSSFileTrackedReader<F> {
    f: SDSSFileIO<F>,
    len: u64,
    pos: u64,
    cs: SCrc,
}

impl<F: RawFileIOInterface> SDSSFileTrackedReader<F> {
    /// Important: this will only look at the data post the current cursor!
    pub fn new(mut f: SDSSFileIO<F>) -> SDSSResult<Self> {
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
    pub fn inner_file(&mut self) -> &mut SDSSFileIO<F> {
        &mut self.f
    }
    pub fn into_inner_file(self) -> SDSSFileIO<F> {
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
pub struct SDSSFileIO<F> {
    f: F,
}

impl<F: RawFileIOInterface> SDSSFileIO<F> {
    /// **IMPORTANT: File position: end-of-header-section**
    pub fn open_or_create_perm_rw<const REWRITE_MODIFY_COUNTER: bool>(
        file_path: &str,
        file_scope: FileScope,
        file_specifier: FileSpecifier,
        file_specifier_version: FileSpecifierVersion,
        host_setting_version: u32,
        host_run_mode: HostRunMode,
        host_startup_counter: u64,
    ) -> SDSSResult<FileOpen<Self>> {
        let f = F::fopen_or_create_rw(file_path)?;
        match f {
            RawFileOpen::Created(f) => {
                // since this file was just created, we need to append the header
                let data = SDSSHeaderRaw::new_auto(
                    file_scope,
                    file_specifier,
                    file_specifier_version,
                    host_setting_version,
                    host_run_mode,
                    host_startup_counter,
                    0,
                )
                .array();
                let mut f = Self::_new(f);
                f.fsynced_write(&data)?;
                Ok(FileOpen::Created(f))
            }
            RawFileOpen::Existing(mut f) => {
                // this is an existing file. decoded the header
                let mut header_raw = [0u8; SDSSHeaderRaw::header_size()];
                f.fread_exact(&mut header_raw)?;
                let header = SDSSHeaderRaw::decode_noverify(header_raw)
                    .ok_or(SDSSError::HeaderDecodeCorruptedHeader)?;
                // now validate the header
                header.verify(file_scope, file_specifier, file_specifier_version)?;
                let mut f = Self::_new(f);
                if REWRITE_MODIFY_COUNTER {
                    // since we updated this file, let us update the header
                    let mut new_header = header.clone();
                    new_header.dr_rs_mut().bump_modify_count();
                    f.seek_from_start(0)?;
                    f.fsynced_write(new_header.encoded().array().as_ref())?;
                    f.seek_from_start(SDSSHeaderRaw::header_size() as _)?;
                }
                Ok(FileOpen::Existing(f, header))
            }
        }
    }
}

impl<F: RawFileIOInterface> SDSSFileIO<F> {
    fn _new(f: F) -> Self {
        Self { f }
    }
    pub fn unfsynced_write(&mut self, data: &[u8]) -> SDSSResult<()> {
        self.f.fwrite_all(data)
    }
    pub fn fsync_all(&mut self) -> SDSSResult<()> {
        self.f.fsync_all()?;
        Ok(())
    }
    pub fn fsynced_write(&mut self, data: &[u8]) -> SDSSResult<()> {
        self.f.fwrite_all(data)?;
        self.f.fsync_all()
    }
    pub fn read_to_buffer(&mut self, buffer: &mut [u8]) -> SDSSResult<()> {
        self.f.fread_exact(buffer)
    }
    pub fn file_length(&self) -> SDSSResult<u64> {
        self.f.flen()
    }
    pub fn seek_from_start(&mut self, by: u64) -> SDSSResult<()> {
        self.f.fseek_ahead(by)
    }
    pub fn trim_file_to(&mut self, to: u64) -> SDSSResult<()> {
        self.f.flen_set(to)
    }
    pub fn retrieve_cursor(&mut self) -> SDSSResult<u64> {
        self.f.fcursor()
    }
    pub fn read_byte(&mut self) -> SDSSResult<u8> {
        let mut r = [0; 1];
        self.read_to_buffer(&mut r).map(|_| r[0])
    }
}

pub struct BufferedScanner<'a> {
    d: &'a [u8],
    i: usize,
}

impl<'a> BufferedScanner<'a> {
    pub const fn new(d: &'a [u8]) -> Self {
        Self { d, i: 0 }
    }
    pub const fn remaining(&self) -> usize {
        self.d.len() - self.i
    }
    pub const fn consumed(&self) -> usize {
        self.i
    }
    pub const fn cursor(&self) -> usize {
        self.i
    }
    pub(crate) fn has_left(&self, sizeof: usize) -> bool {
        self.remaining() >= sizeof
    }
    unsafe fn _cursor(&self) -> *const u8 {
        self.d.as_ptr().add(self.i)
    }
    pub fn eof(&self) -> bool {
        self.remaining() == 0
    }
    unsafe fn _incr(&mut self, by: usize) {
        self.i += by;
    }
    pub fn current(&self) -> &[u8] {
        &self.d[self.i..]
    }
}

impl<'a> BufferedScanner<'a> {
    pub unsafe fn next_u64_le(&mut self) -> u64 {
        u64::from_le_bytes(self.next_chunk())
    }
    pub unsafe fn next_chunk<const N: usize>(&mut self) -> [u8; N] {
        let mut b = [0u8; N];
        ptr::copy_nonoverlapping(self._cursor(), b.as_mut_ptr(), N);
        self._incr(N);
        b
    }
    pub unsafe fn next_chunk_variable(&mut self, size: usize) -> &[u8] {
        let r = slice::from_raw_parts(self._cursor(), size);
        self._incr(size);
        r
    }
    pub unsafe fn next_byte(&mut self) -> u8 {
        let r = *self._cursor();
        self._incr(1);
        r
    }
}
