/*
 * Created on Fri Sep 08 2023
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
    crate::engine::{
        storage::v1::{
            rw::{
                RawFSInterface, RawFileInterface, RawFileInterfaceExt, RawFileInterfaceRead,
                RawFileInterfaceWrite, RawFileInterfaceWriteExt, RawFileOpen,
            },
            SDSSResult,
        },
        sync::cell::Lazy,
    },
    parking_lot::RwLock,
    std::{
        collections::{hash_map::Entry, HashMap},
        io::{Error, ErrorKind},
    },
};

static VFS: Lazy<RwLock<HashMap<Box<str>, VNode>>, fn() -> RwLock<HashMap<Box<str>, VNode>>> =
    Lazy::new(|| Default::default());

type ComponentIter<'a> = std::iter::Take<std::vec::IntoIter<&'a str>>;

/*
    vnode
    ---
    either a vfile or a vdir
*/

#[derive(Debug)]
enum VNode {
    Dir(HashMap<Box<str>, Self>),
    File(VFile),
}

impl VNode {
    const fn is_file(&self) -> bool {
        matches!(self, Self::File(_))
    }
    const fn is_dir(&self) -> bool {
        matches!(self, Self::Dir(_))
    }
    fn as_dir_mut(&mut self) -> Option<&mut HashMap<Box<str>, Self>> {
        match self {
            Self::Dir(d) => Some(d),
            Self::File(_) => None,
        }
    }
}

/*
    vfs impl:
    - nested directory structure
    - make parents
    - make child
*/

fn split_parts(fpath: &str) -> Vec<&str> {
    fpath.split("/").collect()
}

fn split_target_and_components(fpath: &str) -> (&str, ComponentIter) {
    let parts = split_parts(fpath);
    let target = parts.last().unwrap();
    let component_len = parts.len() - 1;
    (target, parts.into_iter().take(component_len))
}

#[derive(Debug)]
pub struct VirtualFS;

impl RawFSInterface for VirtualFS {
    type File = VFileDescriptor;
    fn fs_create_dir(fpath: &str) -> super::SDSSResult<()> {
        // get vfs
        let mut vfs = VFS.write();
        // get root dir
        let mut current = &mut *vfs;
        // process components
        let (target, mut components) = split_target_and_components(fpath);
        while let Some(component) = components.next() {
            match current.get_mut(component) {
                Some(VNode::Dir(d)) => {
                    current = d;
                }
                Some(VNode::File(_)) => {
                    return Err(Error::new(ErrorKind::InvalidInput, "found file in path").into())
                }
                None => {
                    return Err(
                        Error::new(ErrorKind::NotFound, "could not find directory in path").into(),
                    )
                }
            }
        }
        match current.entry(target.into()) {
            Entry::Occupied(_) => return Err(Error::from(ErrorKind::AlreadyExists).into()),
            Entry::Vacant(ve) => {
                ve.insert(VNode::Dir(into_dict!()));
                Ok(())
            }
        }
    }
    fn fs_create_dir_all(fpath: &str) -> super::SDSSResult<()> {
        let mut vfs = VFS.write();
        fn create_ahead(
            mut ahead: &[&str],
            current: &mut HashMap<Box<str>, VNode>,
        ) -> SDSSResult<()> {
            if ahead.is_empty() {
                return Ok(());
            }
            let this = ahead[0];
            ahead = &ahead[1..];
            match current.get_mut(this) {
                Some(VNode::Dir(d)) => {
                    if ahead.is_empty() {
                        // hmm, this was the list dir that was to be created, but it already exists
                        return Err(Error::from(ErrorKind::AlreadyExists).into());
                    }
                    return create_ahead(ahead, d);
                }
                Some(VNode::File(_)) => {
                    return Err(Error::new(ErrorKind::InvalidInput, "found file in path").into())
                }
                None => {
                    let _ = current.insert(this.into(), VNode::Dir(into_dict!()));
                    let dir = current.get_mut(this).unwrap().as_dir_mut().unwrap();
                    return create_ahead(ahead, dir);
                }
            }
        }
        let pieces = split_parts(fpath);
        create_ahead(&pieces, &mut *vfs)
    }
    fn fs_delete_dir(fpath: &str) -> super::SDSSResult<()> {
        delete_dir(fpath, false)
    }
    fn fs_delete_dir_all(fpath: &str) -> super::SDSSResult<()> {
        delete_dir(fpath, true)
    }
    fn fs_fopen_or_create_rw(fpath: &str) -> super::SDSSResult<super::rw::RawFileOpen<Self::File>> {
        let mut vfs = VFS.write();
        // components
        let (target_file, components) = split_target_and_components(fpath);
        let target_dir = find_target_dir_mut(components, &mut vfs)?;
        match target_dir.entry(target_file.into()) {
            Entry::Occupied(mut oe) => match oe.get_mut() {
                VNode::File(f) => {
                    f.read = true;
                    f.write = true;
                    Ok(RawFileOpen::Existing(VFileDescriptor(fpath.into())))
                }
                VNode::Dir(_) => {
                    return Err(
                        Error::new(ErrorKind::InvalidInput, "found directory, not a file").into(),
                    )
                }
            },
            Entry::Vacant(v) => {
                v.insert(VNode::File(VFile::new(true, true, vec![], 0)));
                Ok(RawFileOpen::Created(VFileDescriptor(fpath.into())))
            }
        }
    }
}

fn find_target_dir_mut<'a>(
    components: ComponentIter,
    mut current: &'a mut HashMap<Box<str>, VNode>,
) -> Result<&'a mut HashMap<Box<str>, VNode>, super::SDSSError> {
    for component in components {
        match current.get_mut(component) {
            Some(VNode::Dir(d)) => current = d,
            Some(VNode::File(_)) => {
                return Err(Error::new(ErrorKind::InvalidInput, "found file in path").into())
            }
            None => {
                return Err(
                    Error::new(ErrorKind::NotFound, "could not find directory in path").into(),
                )
            }
        }
    }
    Ok(current)
}

fn find_target_dir<'a>(
    components: ComponentIter,
    mut current: &'a HashMap<Box<str>, VNode>,
) -> Result<&'a HashMap<Box<str>, VNode>, super::SDSSError> {
    for component in components {
        match current.get(component) {
            Some(VNode::Dir(d)) => current = d,
            Some(VNode::File(_)) => {
                return Err(Error::new(ErrorKind::InvalidInput, "found file in path").into())
            }
            None => {
                return Err(
                    Error::new(ErrorKind::NotFound, "could not find directory in path").into(),
                )
            }
        }
    }
    Ok(current)
}

fn delete_dir(fpath: &str, allow_if_non_empty: bool) -> Result<(), super::SDSSError> {
    let mut vfs = VFS.write();
    let mut current = &mut *vfs;
    // process components
    let (target, components) = split_target_and_components(fpath);
    for component in components {
        match current.get_mut(component) {
            Some(VNode::Dir(dir)) => {
                current = dir;
            }
            Some(VNode::File(_)) => {
                return Err(Error::new(ErrorKind::InvalidInput, "found file in path").into())
            }
            None => {
                return Err(
                    Error::new(ErrorKind::NotFound, "could not find directory in path").into(),
                )
            }
        }
    }
    match current.entry(target.into()) {
        Entry::Occupied(dir) => match dir.get() {
            VNode::Dir(d) => {
                if allow_if_non_empty || d.is_empty() {
                    dir.remove();
                    return Ok(());
                }
                return Err(Error::new(ErrorKind::InvalidInput, "directory is not empty").into());
            }
            VNode::File(_) => {
                return Err(Error::new(ErrorKind::InvalidInput, "found file in path").into())
            }
        },
        Entry::Vacant(_) => {
            return Err(Error::new(ErrorKind::NotFound, "could not find directory in path").into())
        }
    }
}

/*
    vfile impl
    ---
    - all r/w operations
    - all seek operations
    - dummy sync operations
*/

#[derive(Debug)]
pub struct VFile {
    read: bool,
    write: bool,
    data: Vec<u8>,
    pos: usize,
}

impl VFile {
    fn new(read: bool, write: bool, data: Vec<u8>, pos: usize) -> Self {
        Self {
            read,
            write,
            data,
            pos,
        }
    }
    fn current(&self) -> &[u8] {
        &self.data[self.pos..]
    }
}

pub struct VFileDescriptor(Box<str>);
impl Drop for VFileDescriptor {
    fn drop(&mut self) {
        let _ = with_file_mut(&self.0, |f| {
            f.read = false;
            f.write = false;
            f.pos = 0;
            Ok(())
        });
    }
}

fn with_file_mut<T>(fpath: &str, mut f: impl FnMut(&mut VFile) -> SDSSResult<T>) -> SDSSResult<T> {
    let mut vfs = VFS.write();
    let (target_file, components) = split_target_and_components(fpath);
    let target_dir = find_target_dir_mut(components, &mut vfs)?;
    match target_dir.get_mut(target_file) {
        Some(VNode::File(file)) => f(file),
        Some(VNode::Dir(_)) => {
            return Err(Error::new(ErrorKind::InvalidInput, "found directory, not a file").into())
        }
        None => return Err(Error::from(ErrorKind::NotFound).into()),
    }
}

fn with_file<T>(fpath: &str, mut f: impl FnMut(&VFile) -> SDSSResult<T>) -> SDSSResult<T> {
    let vfs = VFS.read();
    let (target_file, components) = split_target_and_components(fpath);
    let target_dir = find_target_dir(components, &vfs)?;
    match target_dir.get(target_file) {
        Some(VNode::File(file)) => f(file),
        Some(VNode::Dir(_)) => {
            return Err(Error::new(ErrorKind::InvalidInput, "found directory, not a file").into())
        }
        None => return Err(Error::from(ErrorKind::NotFound).into()),
    }
}

impl RawFileInterface for VFileDescriptor {
    type Reader = Self;
    type Writer = Self;
    fn into_buffered_reader(self) -> super::SDSSResult<Self::Reader> {
        Ok(self)
    }
    fn into_buffered_writer(self) -> super::SDSSResult<Self::Writer> {
        Ok(self)
    }
}

impl RawFileInterfaceRead for VFileDescriptor {
    fn fr_read_exact(&mut self, buf: &mut [u8]) -> super::SDSSResult<()> {
        with_file_mut(&self.0, |file| {
            if !file.read {
                return Err(
                    Error::new(ErrorKind::PermissionDenied, "Read permission denied").into(),
                );
            }
            let available_bytes = file.current().len();
            if available_bytes < buf.len() {
                return Err(Error::from(ErrorKind::UnexpectedEof).into());
            }
            buf.copy_from_slice(&file.data[file.pos..file.pos + buf.len()]);
            file.pos += buf.len();
            Ok(())
        })
    }
}

impl RawFileInterfaceWrite for VFileDescriptor {
    fn fw_write_all(&mut self, bytes: &[u8]) -> super::SDSSResult<()> {
        with_file_mut(&self.0, |file| {
            if !file.write {
                return Err(
                    Error::new(ErrorKind::PermissionDenied, "Write permission denied").into(),
                );
            }
            if file.pos + bytes.len() > file.data.len() {
                file.data.resize(file.pos + bytes.len(), 0);
            }
            file.data[file.pos..file.pos + bytes.len()].copy_from_slice(bytes);
            file.pos += bytes.len();
            Ok(())
        })
    }
}

impl RawFileInterfaceWriteExt for VFileDescriptor {
    fn fw_fsync_all(&mut self) -> super::SDSSResult<()> {
        with_file(&self.0, |_| Ok(()))
    }
    fn fw_truncate_to(&mut self, to: u64) -> super::SDSSResult<()> {
        with_file_mut(&self.0, |file| {
            if !file.write {
                return Err(
                    Error::new(ErrorKind::PermissionDenied, "Write permission denied").into(),
                );
            }
            if to as usize > file.data.len() {
                file.data.resize(to as usize, 0);
            } else {
                file.data.truncate(to as usize);
            }
            if file.pos > file.data.len() {
                file.pos = file.data.len();
            }
            Ok(())
        })
    }
}

impl RawFileInterfaceExt for VFileDescriptor {
    fn fext_file_length(&self) -> super::SDSSResult<u64> {
        with_file(&self.0, |f| Ok(f.data.len() as u64))
    }
    fn fext_cursor(&mut self) -> super::SDSSResult<u64> {
        with_file(&self.0, |f| Ok(f.pos as u64))
    }
    fn fext_seek_ahead_from_start_by(&mut self, by: u64) -> super::SDSSResult<()> {
        with_file_mut(&self.0, |file| {
            if by > file.data.len() as u64 {
                return Err(
                    Error::new(ErrorKind::InvalidInput, "Can't seek beyond file's end").into(),
                );
            }
            file.pos = by as usize;
            Ok(())
        })
    }
}

/*
    nullfs
    ---
    - equivalent of `/dev/null`
    - all calls are no-ops
    - infallible
*/

/// An infallible `/dev/null` implementation. Whatever you run on this, it will always be a no-op since nothing
/// is actually happening
#[derive(Debug)]
pub struct NullFS;
pub struct NullFile;
impl RawFSInterface for NullFS {
    type File = NullFile;
    fn fs_create_dir(_: &str) -> SDSSResult<()> {
        Ok(())
    }
    fn fs_create_dir_all(_: &str) -> SDSSResult<()> {
        Ok(())
    }
    fn fs_delete_dir(_: &str) -> SDSSResult<()> {
        Ok(())
    }
    fn fs_delete_dir_all(_: &str) -> SDSSResult<()> {
        Ok(())
    }
    fn fs_fopen_or_create_rw(_: &str) -> SDSSResult<RawFileOpen<Self::File>> {
        Ok(RawFileOpen::Created(NullFile))
    }
}
impl RawFileInterfaceRead for NullFile {
    fn fr_read_exact(&mut self, _: &mut [u8]) -> SDSSResult<()> {
        Ok(())
    }
}
impl RawFileInterfaceWrite for NullFile {
    fn fw_write_all(&mut self, _: &[u8]) -> SDSSResult<()> {
        Ok(())
    }
}
impl RawFileInterfaceWriteExt for NullFile {
    fn fw_fsync_all(&mut self) -> SDSSResult<()> {
        Ok(())
    }
    fn fw_truncate_to(&mut self, _: u64) -> SDSSResult<()> {
        Ok(())
    }
}
impl RawFileInterfaceExt for NullFile {
    fn fext_file_length(&self) -> SDSSResult<u64> {
        Ok(0)
    }

    fn fext_cursor(&mut self) -> SDSSResult<u64> {
        Ok(0)
    }

    fn fext_seek_ahead_from_start_by(&mut self, _: u64) -> SDSSResult<()> {
        Ok(())
    }
}
impl RawFileInterface for NullFile {
    type Reader = Self;
    type Writer = Self;
    fn into_buffered_reader(self) -> SDSSResult<Self::Reader> {
        Ok(self)
    }
    fn into_buffered_writer(self) -> SDSSResult<Self::Writer> {
        Ok(self)
    }
}
