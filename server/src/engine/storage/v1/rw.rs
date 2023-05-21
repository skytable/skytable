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

use {
    super::header_impl::SDSSHeader,
    crate::{
        util::{ByteRepr, NumericRepr},
        IoResult,
    },
    std::{
        fs::{File, OpenOptions},
        io::Write,
    },
};

/*
    Writer interface
*/

pub trait RawWriterInterface: Sized {
    fn open_truncated(fname: &str) -> IoResult<Self>;
    fn open_create(fname: &str) -> IoResult<Self>;
    fn fwrite_all(&mut self, bytes: &[u8]) -> IoResult<()>;
    fn fsync_all(&mut self) -> IoResult<()>;
}

impl RawWriterInterface for File {
    fn open_truncated(fname: &str) -> IoResult<Self> {
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(false)
            .open(fname)
    }

    fn open_create(fname: &str) -> IoResult<Self> {
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
    pub fn open_create_with_header(file: &str, header: SDSSHeader) -> IoResult<Self> {
        let mut w = W::open_create(file)?;
        w.fwrite_all(header.get0_sr())?;
        w.fwrite_all(header.get1_dr_0_mdr())?;
        w.fwrite_all(header.get1_dr_1_vhr_0())?;
        w.fwrite_all(header.get1_dr_1_vhr_1())?;
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
