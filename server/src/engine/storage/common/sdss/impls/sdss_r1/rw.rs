/*
 * Created on Sat Jan 20 2024
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

use {
    crate::{
        engine::{
            mem::fixed_vec::FixedVec,
            storage::common::{
                checksum::SCrc64,
                interface::fs::{BufferedReader, File, FileExt, FileRead, FileWrite, FileWriteExt},
                sdss::sdss_r1::FileSpecV1,
            },
            RuntimeResult,
        },
        util::os::SysIOError,
        IoResult,
    },
    core::fmt,
    std::mem,
};

/*
    file impl
*/

#[derive(Debug, PartialEq)]
/// A file with it's layout defined by a SDSS file specification
pub struct SdssFile<S: FileSpecV1, F = File> {
    file: F,
    meta: S::Metadata,
}

impl<S: FileSpecV1, F> SdssFile<S, F> {
    fn new(file: F, meta: S::Metadata) -> Self {
        Self { file, meta }
    }
}

impl<S: FileSpecV1> SdssFile<S> {
    /// Open an existing SDSS based file (with no validation arguments)
    pub fn open(path: &str) -> RuntimeResult<Self>
    where
        S: FileSpecV1<DecodeArgs = ()>,
    {
        let mut f = File::open(path)?;
        let md = S::read_metadata(&mut f, ())?;
        Ok(Self::new(f, md))
    }
    /// Create a new SDSS based file (with no initialization arguments)
    pub fn create(path: &str) -> RuntimeResult<Self>
    where
        S: FileSpecV1<EncodeArgs = ()>,
    {
        let mut f = File::create(path)?;
        let md = S::write_metadata(&mut f, ())?;
        Ok(Self::new(f, md))
    }
    pub fn into_buffered_reader(self) -> IoResult<SdssFile<S, BufferedReader>> {
        let Self { file, meta } = self;
        let r = file.into_buffered_reader();
        Ok(SdssFile::new(r, meta))
    }
    pub fn downgrade_reader(SdssFile { file, meta }: SdssFile<S, BufferedReader>) -> Self {
        Self::new(file.into_inner(), meta)
    }
}

impl<S: FileSpecV1, F: FileRead> SdssFile<S, F> {
    /// Attempt to fill the entire buffer from the file
    pub fn read_buffer(&mut self, buffer: &mut [u8]) -> IoResult<()> {
        self.file.fread_exact(buffer)
    }
}

impl<S: FileSpecV1, F: FileRead + FileExt> SdssFile<S, F> {
    /// Read the entire part of the remaining file into memory
    pub fn read_full(&mut self) -> IoResult<Vec<u8>> {
        let len = self.file_length()? - self.file_cursor()?;
        let mut buf = vec![0; len as usize];
        self.read_buffer(&mut buf)?;
        Ok(buf)
    }
}

impl<S: FileSpecV1, F: FileExt> SdssFile<S, F> {
    /// Get the current position of the file
    pub fn file_cursor(&mut self) -> IoResult<u64> {
        self.file.f_cursor()
    }
    /// Get the length of the file
    pub fn file_length(&self) -> IoResult<u64> {
        self.file.f_len()
    }
    /// Move the cursor `n` bytes from the start
    pub fn seek_from_start(&mut self, n: u64) -> IoResult<()> {
        self.file.f_seek_start(n)
    }
}

impl<S: FileSpecV1, F: FileWrite> SdssFile<S, F> {
    /// Attempt to write the entire buffer into the file
    pub fn write_buffer(&mut self, data: &[u8]) -> IoResult<()> {
        self.file.fwrite_all(data)
    }
}

impl<S: FileSpecV1, F: FileWrite + FileWriteExt> SdssFile<S, F> {
    /// Sync all data and metadata permanently
    pub fn fsync_all(&mut self) -> IoResult<()> {
        self.file.fsync_all()?;
        Ok(())
    }
    /// Write a block followed by an explicit fsync call
    pub fn fsynced_write(&mut self, data: &[u8]) -> IoResult<()> {
        self.file.fwrite_all(data)?;
        self.file.fsync_all()
    }
    pub fn truncate(&mut self, new_size: u64) -> IoResult<()> {
        self.file.f_truncate(new_size)
    }
}

/*
    tracked reader impl
*/

/// A [`TrackedReader`] will track various parameters of the file during read operations. By default
/// all reads are buffered
pub struct TrackedReader<S: FileSpecV1> {
    f: SdssFile<S, BufferedReader>,
    len: u64,
    cursor: u64,
    cs: SCrc64,
}

pub struct TrackedReaderContext<'a, S: FileSpecV1> {
    tr: &'a mut TrackedReader<S>,
    p_checksum: SCrc64,
}

impl<'a, S: FileSpecV1> TrackedReaderContext<'a, S> {
    pub fn read(&mut self, buf: &mut [u8]) -> IoResult<()> {
        self.tr
            .tracked_read(buf)
            .map(|_| self.p_checksum.update(buf))
    }
    pub fn read_block<const N: usize>(&mut self) -> IoResult<[u8; N]> {
        let mut block = [0; N];
        self.tr.tracked_read(&mut block).map(|_| {
            self.p_checksum.update(&block);
            block
        })
    }
    pub fn finish(self) -> (u64, &'a mut TrackedReader<S>) {
        let Self { tr, p_checksum } = self;
        (p_checksum.finish(), tr)
    }
    pub fn remaining(&self) -> u64 {
        self.tr.remaining()
    }
}

impl<S: FileSpecV1> TrackedReader<S> {
    /// Create a new [`TrackedReader`]. This needs to retrieve file position and length
    pub fn new(mut f: SdssFile<S, File>) -> IoResult<TrackedReader<S>> {
        f.file_cursor().and_then(|c| Self::with_cursor(f, c))
    }
    pub fn with_cursor(f: SdssFile<S, File>, cursor: u64) -> IoResult<Self> {
        let len = f.file_length()?;
        let f = f.into_buffered_reader()?;
        Ok(TrackedReader {
            f,
            len,
            cursor,
            cs: SCrc64::new(),
        })
    }
}

impl<S: FileSpecV1> TrackedReader<S> {
    pub fn context(&mut self) -> TrackedReaderContext<S> {
        TrackedReaderContext {
            tr: self,
            p_checksum: SCrc64::new(),
        }
    }
    /// Attempt to fill the buffer. This read is tracked.
    pub fn tracked_read(&mut self, buf: &mut [u8]) -> IoResult<()> {
        self.untracked_read(buf).map(|_| self.cs.update(buf))
    }
    /// Attempt to read a byte. This read is also tracked.
    pub fn read_byte(&mut self) -> IoResult<u8> {
        let mut buf = [0u8; 1];
        self.tracked_read(&mut buf).map(|_| buf[0])
    }
    /// Reset the tracked checksum
    pub fn __reset_checksum(&mut self) -> u64 {
        let mut crc = SCrc64::new();
        core::mem::swap(&mut crc, &mut self.cs);
        crc.finish()
    }
    /// Do an untracked read of the file.
    ///
    /// NB: The change in cursor however will still be tracked.
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
            Err(SysIOError::from(std::io::ErrorKind::UnexpectedEof).into_inner())
        }
    }
    /// Tracked read of a given block size. Shorthand for [`Self::tracked_read`]
    pub fn read_block<const N: usize>(&mut self) -> IoResult<[u8; N]> {
        if !self.has_left(N as _) {
            return Err(SysIOError::from(std::io::ErrorKind::UnexpectedEof).into_inner());
        }
        let mut buf = [0; N];
        self.tracked_read(&mut buf)?;
        Ok(buf)
    }
    /// Tracked read of a [`u64`] value
    pub fn read_u64_le(&mut self) -> IoResult<u64> {
        Ok(u64::from_le_bytes(self.read_block()?))
    }
    pub fn current_checksum(&self) -> u64 {
        self.cs.clone().finish()
    }
    pub fn checksum(&self) -> SCrc64 {
        self.cs.clone()
    }
    pub fn cursor(&self) -> u64 {
        self.cursor
    }
    pub fn cached_size(&self) -> u64 {
        self.len
    }
}

impl<S: FileSpecV1> TrackedReader<S> {
    /// Returns the base [`SdssFile`]
    pub fn into_inner(self) -> SdssFile<S> {
        SdssFile::downgrade_reader(self.f)
    }
    /// Returns the number of remaining bytes
    pub fn remaining(&self) -> u64 {
        self.len - self.cursor
    }
    /// Checks if EOF
    pub fn is_eof(&self) -> bool {
        self.len == self.cursor
    }
    /// Check if atleast `v` bytes are left
    pub fn has_left(&self, v: u64) -> bool {
        self.remaining() >= v
    }
}

/*
    tracked writer
*/

/// A [`TrackedWriter`] is an advanced writer primitive that provides a robust abstraction over a writable
/// interface. It tracks the cursor, automatically buffers writes and in case of buffer flush failure,
/// provides methods to robustly handle errors, down to byte-level cursor tracking in case of failure.
pub struct TrackedWriter<
    S: FileSpecV1,
    const SIZE: usize = 8192,
    const PANIC_IF_UNFLUSHED: bool = true,
    const CHECKSUM_WRITTEN_IF_BLOCK_ERROR: bool = true,
> {
    f_d: File,
    f_md: S::Metadata,
    t_cursor: u64,
    t_checksum: SCrc64,
    t_partial_checksum: SCrc64,
    buf: FixedVec<u8, SIZE>,
}

impl<
        S: FileSpecV1,
        const SIZE: usize,
        const PANIC_IF_UNFLUSHED: bool,
        const CHECKSUM_WRITTEN_IF_BLOCK_ERROR: bool,
    > fmt::Debug for TrackedWriter<S, SIZE, PANIC_IF_UNFLUSHED, CHECKSUM_WRITTEN_IF_BLOCK_ERROR>
where
    S::Metadata: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackedWriter")
            .field("f_d", &self.f_d)
            .field("f_md", &self.f_md)
            .field("t_cursor", &self.t_cursor)
            .field("t_checksum", &self.t_checksum)
            .field("t_partial_checksum", &self.t_partial_checksum)
            .field("buf", &self.buf)
            .finish()
    }
}

impl<
        S: FileSpecV1,
        const SIZE: usize,
        const PANIC_IF_UNFLUSHED: bool,
        const CHECKSUM_WRITTEN_IF_BLOCK_ERROR: bool,
    > TrackedWriter<S, SIZE, PANIC_IF_UNFLUSHED, CHECKSUM_WRITTEN_IF_BLOCK_ERROR>
{
    fn available_capacity(&self) -> usize {
        self.buf.remaining_capacity()
    }
    pub fn verify_cursor(&mut self) -> IoResult<()> {
        let cursor = self.f_d.f_cursor()?;
        if self.cursor() == cursor {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "file cursor is out of sync. unreliable file system",
            ))
        }
    }
    pub fn __zero_buffer(&mut self) {
        self.buf.clear()
    }
}

impl<
        S: FileSpecV1,
        const SIZE: usize,
        const PANIC_IF_UNFLUSHED: bool,
        const CHECKSUM_WRITTEN_IF_BLOCK_ERROR: bool,
    > TrackedWriter<S, SIZE, PANIC_IF_UNFLUSHED, CHECKSUM_WRITTEN_IF_BLOCK_ERROR>
{
    fn _new(f_d: File, f_md: S::Metadata, t_cursor: u64, t_checksum: SCrc64) -> Self {
        Self {
            f_d,
            f_md,
            t_cursor,
            t_checksum,
            t_partial_checksum: SCrc64::new(),
            buf: FixedVec::allocate(),
        }
    }
    /// Get the writer tracked cursor
    ///
    /// IMPORTANT: this might not be the real file cursor if the file is externally modified
    pub fn cursor(&self) -> u64 {
        self.t_cursor
    }
    /// Get the cursor (casted to an [`usize`])
    pub fn cursor_usize(&self) -> usize {
        self.cursor() as _
    }
    /// Returns true if not all data has been synced
    pub fn is_dirty(&self) -> bool {
        !self.buf.is_empty()
    }
}

impl<
        S: FileSpecV1,
        const SIZE: usize,
        const PANIC_IF_UNFLUSHED: bool,
        const CHECKSUM_WRITTEN_IF_BLOCK_ERROR: bool,
    > TrackedWriter<S, SIZE, PANIC_IF_UNFLUSHED, CHECKSUM_WRITTEN_IF_BLOCK_ERROR>
{
    /// Create a new tracked writer
    ///
    /// NB: The cursor is fetched. If the cursor is already available, use [`Self::with_cursor`]
    pub fn new(
        mut f: SdssFile<S>,
    ) -> IoResult<TrackedWriter<S, SIZE, PANIC_IF_UNFLUSHED, CHECKSUM_WRITTEN_IF_BLOCK_ERROR>> {
        f.file_cursor().map(|v| TrackedWriter::with_cursor(f, v))
    }
    /// Create a new tracked writer with the provided cursor
    pub fn with_cursor(f: SdssFile<S>, c: u64) -> Self {
        Self::with_cursor_and_checksum(f, c, SCrc64::new())
    }
    /// Create a new tracked writer with the provided checksum and cursor
    pub fn with_cursor_and_checksum(
        SdssFile { file, meta }: SdssFile<S>,
        c: u64,
        ck: SCrc64,
    ) -> Self {
        Self::_new(file, meta, c, ck)
    }
    pub fn current_checksum(&self) -> u64 {
        self.t_checksum.clone().finish()
    }
}

impl<
        S: FileSpecV1,
        const SIZE: usize,
        const PANIC_IF_UNFLUSHED: bool,
        const CHECKSUM_WRITTEN_IF_BLOCK_ERROR: bool,
    > TrackedWriter<S, SIZE, PANIC_IF_UNFLUSHED, CHECKSUM_WRITTEN_IF_BLOCK_ERROR>
{
    /// Same as [`Self::tracked_write_through_buffer`], but the partial state is updated
    pub fn dtrack_write_through_buffer(&mut self, buf: &[u8]) -> IoResult<()> {
        self.tracked_write_through_buffer(buf)
            .map(|_| self.t_partial_checksum.update(buf))
    }
    /// Don't write to the buffer, instead directly write to the file
    ///
    /// NB:
    /// - If errored, the number of bytes written are still tracked
    /// - If errored, the checksum is updated to reflect the number of bytes written (unless otherwise configured)
    pub fn tracked_write_through_buffer(&mut self, buf: &[u8]) -> IoResult<()> {
        debug_assert!(self.buf.is_empty());
        match self.f_d.fwrite_all_count(buf) {
            (cnt, r) => {
                self.t_cursor += cnt;
                if r.is_err() {
                    if CHECKSUM_WRITTEN_IF_BLOCK_ERROR {
                        self.t_checksum.update(&buf[..cnt as usize]);
                    }
                } else {
                    self.t_checksum.update(buf);
                }
                r
            }
        }
    }
    /// Same as [`Self::tracked_write`], but the partial state is updated
    pub fn dtrack_write(&mut self, buf: &[u8]) -> IoResult<()> {
        self.tracked_write(buf)
            .map(|_| self.t_partial_checksum.update(buf))
    }
    /// Reset the partial state
    pub fn reset_partial(&mut self) -> u64 {
        mem::take(&mut self.t_partial_checksum).finish()
    }
    /// Do a tracked write
    ///
    /// On error, if block error checksumming is set then whatever part of the block was written
    /// will be updated in the checksum. If disabled, then the checksum is unchanged.
    pub fn tracked_write(&mut self, buf: &[u8]) -> IoResult<()> {
        let cursor_start = self.cursor_usize();
        match self.untracked_write(buf) {
            Ok(()) => {
                self.t_checksum.update(buf);
                Ok(())
            }
            Err(e) => {
                if CHECKSUM_WRITTEN_IF_BLOCK_ERROR {
                    let cursor_now = self.cursor_usize();
                    self.t_checksum.update(&buf[..cursor_now - cursor_start]);
                }
                Err(e)
            }
        }
    }
    /// Do an untracked write
    pub fn untracked_write(&mut self, buf: &[u8]) -> IoResult<()> {
        if self.available_capacity() >= buf.len() {
            unsafe {
                // UNSAFE(@ohsayan): above branch guarantees that we have sufficient space
                self.buf.extend_from_slice(buf)
            }
            return Ok(());
        }
        self.flush_buf()?;
        // write whatever capacity exceeds the buffer size
        let to_write_cnt = buf.len().saturating_sub(SIZE);
        match self.f_d.fwrite_all_count(&buf[..to_write_cnt]) {
            (cnt, r) => {
                self.t_cursor += cnt;
                r?;
            }
        }
        // store remainder in buffer
        unsafe {
            // UNSAFE(@ohsayan): above branch guarantees that we have sufficient space
            self.buf.extend_from_slice(&buf[to_write_cnt..])
        }
        Ok(())
    }
    /// Flush the buffer and then sync data and metadata
    pub fn flush_sync(&mut self) -> IoResult<()> {
        self.flush_buf().and_then(|_| self.fsync())
    }
    /// Flush the buffer
    pub fn flush_buf(&mut self) -> IoResult<()> {
        match self.f_d.fwrite_all_count(&self.buf) {
            (written, r) => {
                if written as usize == self.buf.len() {
                    // if we wrote the full buffer, simply decrement
                    unsafe {
                        // UNSAFE(@ohsayan): completely safe as no dtor needed (and is a decrement anyways)
                        self.buf.decr_len_by(written as usize)
                    }
                } else {
                    // if we failed to write the whole buffer, only remove what was written and keep
                    // the remaining in the buffer
                    unsafe {
                        // UNSAFE(@ohsayan): written is obviously not larger, so this is fine
                        self.buf.clear_start(written as _)
                    }
                }
                // update the cursor to what was written (atleast what the syscall told us)
                self.t_cursor += written;
                // return
                r
            }
        }
    }
    pub fn fsync(&mut self) -> IoResult<()> {
        self.f_d.fsync_all()
    }
}

impl<
        S: FileSpecV1,
        const SIZE: usize,
        const PANIC_IF_UNFLUSHED: bool,
        const CHECKSUM_WRITTEN_IF_BLOCK_ERROR: bool,
    > Drop for TrackedWriter<S, SIZE, PANIC_IF_UNFLUSHED, CHECKSUM_WRITTEN_IF_BLOCK_ERROR>
{
    fn drop(&mut self) {
        if PANIC_IF_UNFLUSHED && !self.buf.is_empty() {
            panic!("buffer not completely flushed");
        }
    }
}

#[test]
fn check_vfs_buffering() {
    use crate::engine::storage::{
        common::interface::fs::FileSystem,
        v2::raw::spec::{Header, SystemDatabaseV1},
    };
    fn rawfile() -> Vec<u8> {
        FileSystem::read("myfile").unwrap()
    }
    let compiled_header = SystemDatabaseV1::metadata_to_block(()).unwrap();
    let expected_checksum = {
        let mut crc = SCrc64::new();
        crc.update(&vec![0; 8192]);
        crc.update(&[0]);
        crc.update(&vec![0xFF; 8192]);
        crc.finish()
    };
    closure! {
        // init writer
        let mut twriter: TrackedWriter<SystemDatabaseV1> =
            TrackedWriter::new(SdssFile::create("myfile")?)?;
        assert_eq!(twriter.cursor_usize(), Header::SIZE);
        {
            // W8192: write exact bufsize block; nothing is written (except SDSS header)
            twriter.tracked_write(&[0; 8192])?;
            assert_eq!(rawfile(), compiled_header);
            assert_eq!(twriter.cursor_usize(), Header::SIZE);
        }
        {
            // W1: write one more byte; buf should be flushed
            twriter.tracked_write(&[0; 1])?;
            assert_eq!(twriter.cursor_usize(), Header::SIZE + 8192);
            let _raw_file = rawfile();
            assert_eq!(&_raw_file[..Header::SIZE], compiled_header);
            assert_eq!(&_raw_file[Header::SIZE..], vec![0u8; 8192]);
        }
        {
            // FLUSH: flush buffer; 8193 bytes should be on disk (+header)
            twriter.flush_buf()?;
            let _raw_file = rawfile();
            assert_eq!(twriter.cursor_usize(), Header::SIZE + 8192 + 1);
            assert_eq!(&_raw_file[..Header::SIZE], compiled_header);
            assert_eq!(&_raw_file[Header::SIZE..], vec![0u8; 8193]);
        }
        {
            // W1: now write one byte, nothing should happen
            twriter.tracked_write(&[0xFF; 1])?;
            let _raw_file = rawfile();
            assert_eq!(twriter.cursor_usize(), Header::SIZE + 8192 + 1);
            assert_eq!(&_raw_file[..Header::SIZE], compiled_header);
            assert_eq!(&_raw_file[Header::SIZE..], vec![0u8; 8193]);
        }
        {
            // W8191: now write 8191 bytes, nothing should happen
            twriter.tracked_write(&[0xFF; 8191])?;
            let _raw_file = rawfile();
            assert_eq!(twriter.cursor_usize(), Header::SIZE + 8192 + 1);
            assert_eq!(&_raw_file[..Header::SIZE], compiled_header);
            assert_eq!(&_raw_file[Header::SIZE..], vec![0u8; 8193]);
        }
        assert_eq!(expected_checksum, twriter.current_checksum());
        {
            // FLUSH: now flush and we should have header + 8193 bytes with 0x00 + 8192 bytes with 0xFF
            twriter.flush_buf()?;
            let _raw_file = rawfile();
            assert_eq!(twriter.cursor_usize(), Header::SIZE + 8192 + 1 + 8191 + 1);
            assert_eq!(&_raw_file[..Header::SIZE], compiled_header);
            assert_eq!(&_raw_file[Header::SIZE..Header::SIZE + 8192 + 1], vec![0u8; 8193]);
            assert_eq!(&_raw_file[Header::SIZE + 8193..], vec![0xFF; 8192]);
        }
        assert_eq!(expected_checksum, twriter.current_checksum());
        RuntimeResult::Ok(())
    }
    .unwrap()
}
