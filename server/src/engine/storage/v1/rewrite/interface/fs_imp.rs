/*
 * Created on Sun Jan 07 2024
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

use {
    super::fs::{
        FSInterface, FileBufWrite, FileInterface, FileInterfaceExt, FileOpen, FileRead, FileWrite,
        FileWriteExt,
    },
    crate::engine::RuntimeResult,
    std::{
        fs::{self, File},
        io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    },
};

/*
    local fs impls
*/

/// A type representing the host's local filesystem (or atleast where our data directory is)
pub struct LocalFS;

fn cvt<T, E1, E2: From<E1>>(r: Result<T, E1>) -> Result<T, E2> {
    r.map_err(Into::into)
}

impl FSInterface for LocalFS {
    type File = File;
    fn fs_remove_file(fpath: &str) -> RuntimeResult<()> {
        cvt(fs::remove_file(fpath))
    }
    fn fs_rename(from: &str, to: &str) -> RuntimeResult<()> {
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
    fn fs_fopen_or_create_rw(fpath: &str) -> RuntimeResult<super::fs::FileOpen<Self::File>> {
        let r = || -> Result<_, std::io::Error> {
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
        };
        cvt(r())
    }
    fn fs_fopen_rw(fpath: &str) -> RuntimeResult<Self::File> {
        let f = File::options().read(true).write(true).open(fpath)?;
        Ok(f)
    }
    fn fs_fcreate_rw(fpath: &str) -> RuntimeResult<Self::File> {
        let f = File::options()
            .create_new(true)
            .read(true)
            .write(true)
            .open(fpath)?;
        Ok(f)
    }
}

/*
    common impls for files
*/

impl<R: Read> FileRead for R {
    fn fread_exact(&mut self, buf: &mut [u8]) -> RuntimeResult<()> {
        cvt(self.read_exact(buf))
    }
}

impl<W: Write> FileWrite for W {
    fn fwrite(&mut self, buf: &[u8]) -> RuntimeResult<u64> {
        cvt(self.write(buf).map(|v| v as _))
    }
}

/*
    local file impls
*/

impl FileInterface for File {
    type BufReader = BufReader<Self>;
    type BufWriter = BufWriter<Self>;
    fn upgrade_to_buffered_reader(self) -> RuntimeResult<Self::BufReader> {
        Ok(BufReader::new(self))
    }
    fn upgrade_to_buffered_writer(self) -> RuntimeResult<Self::BufWriter> {
        Ok(BufWriter::new(self))
    }
    fn downgrade_reader(r: Self::BufReader) -> RuntimeResult<Self> {
        Ok(r.into_inner())
    }
    fn downgrade_writer(mut r: Self::BufWriter) -> RuntimeResult<Self> {
        // TODO(@ohsayan): maybe we'll want to explicitly handle not syncing this?
        r.flush()?;
        let (me, err) = r.into_parts();
        match err {
            Ok(x) if x.is_empty() => Ok(me),
            Ok(_) | Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "failed to flush data from buffer into sink",
                )
                .into())
            }
        }
    }
}

/// A trait for handling wrappers of [`std::fs::File`]
trait AsLocalFile {
    fn file(&self) -> &File;
    fn file_mut(&mut self) -> &mut File;
}

impl AsLocalFile for File {
    fn file(&self) -> &File {
        self
    }
    fn file_mut(&mut self) -> &mut File {
        self
    }
}

impl AsLocalFile for BufReader<File> {
    fn file(&self) -> &File {
        self.get_ref()
    }
    fn file_mut(&mut self) -> &mut File {
        self.get_mut()
    }
}

impl AsLocalFile for BufWriter<File> {
    fn file(&self) -> &File {
        self.get_ref()
    }
    fn file_mut(&mut self) -> &mut File {
        self.get_mut()
    }
}

impl FileBufWrite for BufWriter<File> {
    fn sync_write_cache(&mut self) -> RuntimeResult<()> {
        // TODO(@ohsayan): maybe we'll want to explicitly handle not syncing this?
        cvt(self.flush())
    }
}

impl<F: AsLocalFile> FileInterfaceExt for F {
    fn fext_length(&mut self) -> RuntimeResult<u64> {
        Ok(self.file().metadata()?.len())
    }
    fn fext_cursor(&mut self) -> RuntimeResult<u64> {
        cvt(self.file_mut().stream_position())
    }
    fn fext_seek_ahead_from_start_by(&mut self, by: u64) -> RuntimeResult<()> {
        cvt(self.file_mut().seek(SeekFrom::Start(by)).map(|_| ()))
    }
}

impl FileWriteExt for File {
    fn fwext_truncate_to(&mut self, to: u64) -> RuntimeResult<()> {
        cvt(self.set_len(to))
    }
}
