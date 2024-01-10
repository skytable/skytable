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
    crate::engine::RuntimeResult,
    std::io::{Error as IoError, ErrorKind as IoErrorKind},
};

#[derive(Debug, PartialEq)]
/// Result of opening a file
/// - Created: newly created file
/// - Existing: existing file that was reopened
pub enum FileOpen<CF, EF = CF> {
    /// new file
    Created(CF),
    /// existing file
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
            Self::Existing(_) => None,
            Self::Created(c) => Some(c),
        }
    }
}

#[cfg(test)]
impl<CF> FileOpen<CF> {
    pub fn into_inner(self) -> CF {
        match self {
            Self::Created(f) | Self::Existing(f) => f,
        }
    }
}

pub trait FSInterface {
    // settings
    /// set to false if the file system is a special device like `/dev/null`
    const NOT_NULL: bool = true;
    // types
    /// the file type that is returned by this file system
    type File: FileInterface;
    // functions
    /// Remove a file
    fn fs_remove_file(fpath: &str) -> RuntimeResult<()>;
    /// Rename a file
    fn fs_rename(from: &str, to: &str) -> RuntimeResult<()>;
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

/// File interface definition
pub trait FileInterface:
    FileInterfaceRead + FileInterfaceWrite + FileInterfaceWriteExt + FileInterfaceExt + Sized
{
    /// A buffered reader implementation
    type BufReader: FileInterfaceRead + FileInterfaceExt;
    /// A buffered writer implementation
    type BufWriter: FileInterfaceBufWrite;
    /// Get a buffered reader for this file
    fn upgrade_to_buffered_reader(self) -> RuntimeResult<Self::BufReader>;
    /// Get a buffered writer for this file
    fn upgrade_to_buffered_writer(self) -> RuntimeResult<Self::BufWriter>;
    /// Get the file back from the buffered reader
    fn downgrade_reader(r: Self::BufReader) -> RuntimeResult<Self>;
    /// Get the file back from the buffered writer
    fn downgrade_writer(r: Self::BufWriter) -> RuntimeResult<Self>;
}

pub trait FileInterfaceBufWrite: FileInterfaceWrite + FileInterfaceExt {
    fn sync_write_cache(&mut self) -> RuntimeResult<()>;
}

/// Readable object
pub trait FileInterfaceRead {
    /// Read in a block of the exact given length
    fn fread_exact_block<const N: usize>(&mut self) -> RuntimeResult<[u8; N]> {
        let mut ret = [0u8; N];
        self.fread_exact(&mut ret)?;
        Ok(ret)
    }
    /// Read in `n` bytes to fill the given buffer
    fn fread_exact(&mut self, buf: &mut [u8]) -> RuntimeResult<()>;
}

/// Writable object
pub trait FileInterfaceWrite {
    /// Attempt to write the buffer into this object, returning the number of bytes that were
    /// written. It is **NOT GUARANTEED THAT ALL DATA WILL BE WRITTEN**
    fn fwrite(&mut self, buf: &[u8]) -> RuntimeResult<u64>;
    /// Attempt to write the entire buffer into this object, returning the number of bytes written
    ///
    /// It is guaranteed that if the [`Result`] returned is [`Ok(())`], then the entire buffer was
    /// written to disk.
    fn fwrite_all_count(&mut self, buf: &[u8]) -> (u64, RuntimeResult<()>) {
        let len = buf.len() as u64;
        let mut written = 0;
        while written != len {
            match self.fwrite(buf) {
                Ok(0) => {
                    return (
                        written,
                        Err(IoError::new(
                            IoErrorKind::WriteZero,
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
    /// Attempt to write the entire buffer into this object
    ///
    /// If this return [`Ok(())`] then it is guaranteed that all bytes have been written
    fn fw_write_all(&mut self, buf: &[u8]) -> RuntimeResult<()> {
        self.fwrite_all_count(buf).1
    }
}

/// Advanced write traits
pub trait FileInterfaceWriteExt {
    /// Sync data and metadata for this file
    fn fwext_sync_all(&mut self) -> RuntimeResult<()> {
        Ok(())
    }
    /// Sync data for this file
    fn fwext_sync_data(&mut self) -> RuntimeResult<()> {
        Ok(())
    }
    /// Sync meta for this file
    fn fwext_sync_meta(&mut self) -> RuntimeResult<()> {
        Ok(())
    }
    /// Truncate the size of the file to the given size
    ///
    /// - If `to` > actual file length: the file is zero padded to fill `to - len`
    /// - If `to` < actual file length: the file is trimmed to the size `to`
    fn fwext_truncate_to(&mut self, to: u64) -> RuntimeResult<()>;
}

/// Advanced file access
pub trait FileInterfaceExt {
    /// Returns the length of the file
    fn fext_length(&self) -> RuntimeResult<u64>;
    /// Returns the current cursor position of the file
    fn fext_cursor(&mut self) -> RuntimeResult<u64>;
    /// Seek by `from` bytes from the start of the file
    fn fext_seek_ahead_from_start_by(&mut self, by: u64) -> RuntimeResult<()>;
}
