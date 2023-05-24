/*
 * Created on Fri May 19 2023
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

use crate::engine::storage::versions;

use {
    super::header_impl::SDSSHeaderRaw,
    crate::{
        engine::storage::v1::header_impl::{
            HostRecord, HostRecordRaw, MetadataRecordRaw, SDSSHeader, StaticRecordRaw,
        },
        util::{ByteRepr, NumericRepr},
        IoResult,
    },
    std::{
        fs::{self, File, OpenOptions},
        io::{Error as IoError, ErrorKind, Read, Seek, SeekFrom, Write},
    },
};

#[derive(Debug)]
pub enum SDSSError {
    SRVersionMismatch,
    IoError(IoError),
    CorruptedHeaderSR,
    CorruptedHeaderMDR,
    CorruptedHeaderHR,
}

impl From<IoError> for SDSSError {
    fn from(e: IoError) -> Self {
        Self::IoError(e)
    }
}

pub type SDSSResult<T> = Result<T, SDSSError>;

/*
    Writer interface
*/

pub trait RawWriterInterface: Sized {
    fn fopen_truncated(fname: &str) -> IoResult<Self>;
    fn fopen_create(fname: &str) -> IoResult<Self>;
    fn fwrite_all(&mut self, bytes: &[u8]) -> IoResult<()>;
    fn fsync_all(&mut self) -> IoResult<()>;
}

impl RawWriterInterface for File {
    fn fopen_truncated(fname: &str) -> IoResult<Self> {
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(false)
            .open(fname)
    }

    fn fopen_create(fname: &str) -> IoResult<Self> {
        File::create(fname)
    }

    fn fwrite_all(&mut self, bytes: &[u8]) -> IoResult<()> {
        Write::write_all(self, bytes)
    }

    fn fsync_all(&mut self) -> IoResult<()> {
        // FIXME(@ohsayan): too slow? maybe fdatasync only?
        File::sync_all(self)
    }
}

/*
    Writer
*/

pub struct SDSSWriter<W> {
    writer: W,
}

impl<W: RawWriterInterface> SDSSWriter<W> {
    pub fn open_create_with_header(file: &str, header: SDSSHeaderRaw) -> IoResult<Self> {
        let mut w = W::fopen_create(file)?;
        w.fwrite_all(header.get0_sr())?;
        w.fwrite_all(header.get1_dr_0_mdr())?;
        w.fwrite_all(header.get1_dr_1_hr_0())?;
        w.fwrite_all(header.get1_dr_1_hr_1())?;
        w.fsync_all()?;
        Ok(Self { writer: w })
    }
    pub fn fsync_write<D: ?Sized + ByteRepr>(&mut self, data: &D) -> IoResult<()> {
        self.writer.fwrite_all(data.repr())?;
        self.writer.fsync_all()
    }
    pub fn newrite_numeric(&mut self, num: impl NumericRepr) -> IoResult<()> {
        self.fsync_write(num.repr())
    }
    pub fn lewrite_numeric(&mut self, num: impl NumericRepr) -> IoResult<()> {
        self.fsync_write(num.le().repr())
    }
    pub fn bewrite_numeric(&mut self, num: impl NumericRepr) -> IoResult<()> {
        self.fsync_write(num.be().repr())
    }
}

/*
    Read interface
*/

pub trait RawReaderInterface: Sized {
    fn fopen(fname: &str) -> IoResult<Self>;
    fn fread_exact_seek(&mut self, buf: &mut [u8]) -> IoResult<()>;
    fn fread_to_end(&mut self, buf: &mut Vec<u8>) -> IoResult<()>;
}

impl RawReaderInterface for File {
    fn fopen(fname: &str) -> IoResult<Self> {
        File::open(fname)
    }
    fn fread_exact_seek(&mut self, buf: &mut [u8]) -> IoResult<()> {
        self.read_exact(buf)?;
        let _ = self.seek(SeekFrom::Start(buf.len() as _))?;
        Ok(())
    }
    fn fread_to_end(&mut self, buf: &mut Vec<u8>) -> IoResult<()> {
        match self.read_to_end(buf) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

pub struct FileBuffered {
    cursor: usize,
    base: Vec<u8>,
}

impl RawReaderInterface for FileBuffered {
    fn fopen(name: &str) -> IoResult<Self> {
        Ok(Self {
            base: fs::read(name)?,
            cursor: 0,
        })
    }
    fn fread_exact_seek(&mut self, buf: &mut [u8]) -> IoResult<()> {
        let l = self.base[self.cursor..].len();
        if l >= buf.len() {
            self.cursor += buf.len();
            Ok(buf.copy_from_slice(&self.base[self.cursor..]))
        } else {
            Err(ErrorKind::UnexpectedEof.into())
        }
    }
    fn fread_to_end(&mut self, buf: &mut Vec<u8>) -> IoResult<()> {
        buf.extend_from_slice(&self.base[self.cursor..]);
        Ok(())
    }
}

pub struct SDSSReader<R> {
    reader: R,
    header: SDSSHeader,
}

impl<R: RawReaderInterface> SDSSReader<R> {
    pub fn open(f: &str) -> SDSSResult<Self> {
        let mut r = R::fopen(f)?;
        let mut sr = StaticRecordRaw::empty_buffer();
        let mut mdr = MetadataRecordRaw::empty_buffer();
        let mut hr_0_const = HostRecordRaw::empty_buffer_const_section();
        r.fread_exact_seek(&mut sr)?;
        r.fread_exact_seek(&mut mdr)?;
        r.fread_exact_seek(&mut hr_0_const)?;
        let sr = StaticRecordRaw::decode_from_bytes(sr).ok_or(SDSSError::CorruptedHeaderSR)?;
        let mdr = MetadataRecordRaw::decode_from_bytes(mdr).ok_or(SDSSError::CorruptedHeaderMDR)?;
        let (hr_const, hostname_len) = HostRecordRaw::decode_from_bytes_const_sec(hr_0_const)
            .ok_or(SDSSError::CorruptedHeaderHR)?;
        let mut host_name = vec![0u8; hostname_len].into_boxed_slice();
        r.fread_exact_seek(&mut host_name)?;
        if (sr.sr().header_version() != versions::v1::V1_HEADER_VERSION)
            || (mdr.driver_version() != versions::v1::V1_DRIVER_VERSION)
            || (mdr.server_version() != versions::v1::V1_SERVER_VERSION)
        {
            return Err(SDSSError::SRVersionMismatch);
        }
        Ok(Self {
            reader: r,
            header: SDSSHeader::new(sr, mdr, HostRecord::new(hr_const, host_name)),
        })
    }
}
