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
        error::RuntimeResult,
        storage::v1::rw::{
            FileOpen, RawFSInterface, RawFileInterface, RawFileInterfaceBufferedWriter,
            RawFileInterfaceExt, RawFileInterfaceRead, RawFileInterfaceWrite,
            RawFileInterfaceWriteExt,
        },
        sync::cell::Lazy,
    },
    parking_lot::RwLock,
    std::{
        collections::{
            hash_map::{Entry, OccupiedEntry},
            HashMap,
        },
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
pub(super) enum VNode {
    Dir(HashMap<Box<str>, Self>),
    File(VFile),
}

impl VNode {
    fn as_dir_mut(&mut self) -> Option<&mut HashMap<Box<str>, Self>> {
        match self {
            Self::Dir(d) => Some(d),
            Self::File(_) => None,
        }
    }
}

/*
    errors
*/

fn err_item_is_not_file<T>() -> RuntimeResult<T> {
    Err(Error::new(ErrorKind::InvalidInput, "found directory, not a file").into())
}
fn err_file_in_dir_path<T>() -> RuntimeResult<T> {
    Err(Error::new(ErrorKind::InvalidInput, "found file in directory path").into())
}
fn err_dir_missing_in_path<T>() -> RuntimeResult<T> {
    Err(Error::new(ErrorKind::InvalidInput, "could not find directory in path").into())
}
fn err_could_not_find_item<T>() -> RuntimeResult<T> {
    Err(Error::new(ErrorKind::NotFound, "could not find item").into())
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
    fn fs_rename_file(from: &str, to: &str) -> RuntimeResult<()> {
        // get file data
        let data = with_file(from, |f| Ok(f.data.clone()))?;
        // create new file
        let file = VirtualFS::fs_fopen_or_create_rw(to)?;
        match file {
            FileOpen::Created(mut c) => {
                c.fw_write_all(&data)?;
            }
            FileOpen::Existing(mut e) => {
                e.fwext_truncate_to(0)?;
                e.fw_write_all(&data)?;
            }
        }
        // delete old file
        Self::fs_remove_file(from)
    }
    fn fs_remove_file(fpath: &str) -> RuntimeResult<()> {
        handle_item_mut(fpath, |e| match e.get() {
            VNode::File(_) => {
                e.remove();
                Ok(())
            }
            _ => return err_item_is_not_file(),
        })
    }
    fn fs_create_dir(fpath: &str) -> RuntimeResult<()> {
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
                Some(VNode::File(_)) => return err_file_in_dir_path(),
                None => return err_dir_missing_in_path(),
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
    fn fs_create_dir_all(fpath: &str) -> RuntimeResult<()> {
        let mut vfs = VFS.write();
        fn create_ahead(
            mut ahead: &[&str],
            current: &mut HashMap<Box<str>, VNode>,
        ) -> RuntimeResult<()> {
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
                Some(VNode::File(_)) => return err_file_in_dir_path(),
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
    fn fs_delete_dir(fpath: &str) -> RuntimeResult<()> {
        delete_dir(fpath, false)
    }
    fn fs_delete_dir_all(fpath: &str) -> RuntimeResult<()> {
        delete_dir(fpath, true)
    }
    fn fs_fopen_or_create_rw(fpath: &str) -> RuntimeResult<super::rw::FileOpen<Self::File>> {
        let mut vfs = VFS.write();
        // components
        let (target_file, components) = split_target_and_components(fpath);
        let target_dir = find_target_dir_mut(components, &mut vfs)?;
        match target_dir.entry(target_file.into()) {
            Entry::Occupied(mut oe) => match oe.get_mut() {
                VNode::File(f) => {
                    f.read = true;
                    f.write = true;
                    Ok(FileOpen::Existing(VFileDescriptor(fpath.into())))
                }
                VNode::Dir(_) => return err_item_is_not_file(),
            },
            Entry::Vacant(v) => {
                v.insert(VNode::File(VFile::new(true, true, vec![], 0)));
                Ok(FileOpen::Created(VFileDescriptor(fpath.into())))
            }
        }
    }
    fn fs_fcreate_rw(fpath: &str) -> RuntimeResult<Self::File> {
        let mut vfs = VFS.write();
        let (target_file, components) = split_target_and_components(fpath);
        let target_dir = find_target_dir_mut(components, &mut vfs)?;
        match target_dir.entry(target_file.into()) {
            Entry::Occupied(k) => {
                match k.get() {
                    VNode::Dir(_) => {
                        return Err(Error::new(
                            ErrorKind::AlreadyExists,
                            "found directory with same name where file was to be created",
                        )
                        .into());
                    }
                    VNode::File(_) => {
                        // the file already exists
                        return Err(Error::new(
                            ErrorKind::AlreadyExists,
                            "the file already exists",
                        )
                        .into());
                    }
                }
            }
            Entry::Vacant(v) => {
                // no file exists, we can create this
                v.insert(VNode::File(VFile::new(true, true, vec![], 0)));
                Ok(VFileDescriptor(fpath.into()))
            }
        }
    }
    fn fs_fopen_rw(fpath: &str) -> RuntimeResult<Self::File> {
        with_file_mut(fpath, |f| {
            f.read = true;
            f.write = true;
            Ok(VFileDescriptor(fpath.into()))
        })
    }
}

fn find_target_dir_mut<'a>(
    components: ComponentIter,
    mut current: &'a mut HashMap<Box<str>, VNode>,
) -> RuntimeResult<&'a mut HashMap<Box<str>, VNode>> {
    for component in components {
        match current.get_mut(component) {
            Some(VNode::Dir(d)) => current = d,
            Some(VNode::File(_)) => return err_file_in_dir_path(),
            None => return err_dir_missing_in_path(),
        }
    }
    Ok(current)
}

fn find_target_dir<'a>(
    components: ComponentIter,
    mut current: &'a HashMap<Box<str>, VNode>,
) -> RuntimeResult<&'a HashMap<Box<str>, VNode>> {
    for component in components {
        match current.get(component) {
            Some(VNode::Dir(d)) => current = d,
            Some(VNode::File(_)) => return err_file_in_dir_path(),
            None => return err_dir_missing_in_path(),
        }
    }
    Ok(current)
}

fn handle_item_mut<T>(
    fpath: &str,
    f: impl Fn(OccupiedEntry<Box<str>, VNode>) -> RuntimeResult<T>,
) -> RuntimeResult<T> {
    let mut vfs = VFS.write();
    let mut current = &mut *vfs;
    // process components
    let (target, components) = split_target_and_components(fpath);
    for component in components {
        match current.get_mut(component) {
            Some(VNode::Dir(dir)) => {
                current = dir;
            }
            Some(VNode::File(_)) => return err_file_in_dir_path(),
            None => return err_dir_missing_in_path(),
        }
    }
    match current.entry(target.into()) {
        Entry::Occupied(item) => return f(item),
        Entry::Vacant(_) => return err_could_not_find_item(),
    }
}
fn delete_dir(fpath: &str, allow_if_non_empty: bool) -> RuntimeResult<()> {
    handle_item_mut(fpath, |node| match node.get() {
        VNode::Dir(d) => {
            if allow_if_non_empty || d.is_empty() {
                node.remove();
                return Ok(());
            }
            return Err(Error::new(ErrorKind::InvalidInput, "directory is not empty").into());
        }
        VNode::File(_) => return err_file_in_dir_path(),
    })
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

fn with_file_mut<T>(
    fpath: &str,
    mut f: impl FnMut(&mut VFile) -> RuntimeResult<T>,
) -> RuntimeResult<T> {
    let mut vfs = VFS.write();
    let (target_file, components) = split_target_and_components(fpath);
    let target_dir = find_target_dir_mut(components, &mut vfs)?;
    match target_dir.get_mut(target_file) {
        Some(VNode::File(file)) => f(file),
        Some(VNode::Dir(_)) => return err_item_is_not_file(),
        None => return Err(Error::from(ErrorKind::NotFound).into()),
    }
}

fn with_file<T>(fpath: &str, mut f: impl FnMut(&VFile) -> RuntimeResult<T>) -> RuntimeResult<T> {
    let vfs = VFS.read();
    let (target_file, components) = split_target_and_components(fpath);
    let target_dir = find_target_dir(components, &vfs)?;
    match target_dir.get(target_file) {
        Some(VNode::File(file)) => f(file),
        Some(VNode::Dir(_)) => return err_item_is_not_file(),
        None => return Err(Error::from(ErrorKind::NotFound).into()),
    }
}

impl RawFileInterface for VFileDescriptor {
    type BufReader = Self;
    type BufWriter = Self;
    fn into_buffered_reader(self) -> RuntimeResult<Self::BufReader> {
        Ok(self)
    }
    fn downgrade_reader(r: Self::BufReader) -> RuntimeResult<Self> {
        Ok(r)
    }
    fn into_buffered_writer(self) -> RuntimeResult<Self::BufWriter> {
        Ok(self)
    }
    fn downgrade_writer(w: Self::BufWriter) -> RuntimeResult<Self> {
        Ok(w)
    }
}

impl RawFileInterfaceBufferedWriter for VFileDescriptor {}

impl RawFileInterfaceRead for VFileDescriptor {
    fn fr_read_exact(&mut self, buf: &mut [u8]) -> RuntimeResult<()> {
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
    fn fw_write_all(&mut self, bytes: &[u8]) -> RuntimeResult<()> {
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
    fn fwext_fsync_all(&mut self) -> RuntimeResult<()> {
        with_file(&self.0, |_| Ok(()))
    }
    fn fwext_truncate_to(&mut self, to: u64) -> RuntimeResult<()> {
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
    fn fext_file_length(&self) -> RuntimeResult<u64> {
        with_file(&self.0, |f| Ok(f.data.len() as u64))
    }
    fn fext_cursor(&mut self) -> RuntimeResult<u64> {
        with_file(&self.0, |f| Ok(f.pos as u64))
    }
    fn fext_seek_ahead_from_start_by(&mut self, by: u64) -> RuntimeResult<()> {
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
    const NOT_NULL: bool = false;
    type File = NullFile;
    fn fs_rename_file(_: &str, _: &str) -> RuntimeResult<()> {
        Ok(())
    }
    fn fs_remove_file(_: &str) -> RuntimeResult<()> {
        Ok(())
    }
    fn fs_create_dir(_: &str) -> RuntimeResult<()> {
        Ok(())
    }
    fn fs_create_dir_all(_: &str) -> RuntimeResult<()> {
        Ok(())
    }
    fn fs_delete_dir(_: &str) -> RuntimeResult<()> {
        Ok(())
    }
    fn fs_delete_dir_all(_: &str) -> RuntimeResult<()> {
        Ok(())
    }
    fn fs_fopen_or_create_rw(_: &str) -> RuntimeResult<FileOpen<Self::File>> {
        Ok(FileOpen::Created(NullFile))
    }
    fn fs_fopen_rw(_: &str) -> RuntimeResult<Self::File> {
        Ok(NullFile)
    }
    fn fs_fcreate_rw(_: &str) -> RuntimeResult<Self::File> {
        Ok(NullFile)
    }
}
impl RawFileInterfaceRead for NullFile {
    fn fr_read_exact(&mut self, _: &mut [u8]) -> RuntimeResult<()> {
        Ok(())
    }
}
impl RawFileInterfaceWrite for NullFile {
    fn fw_write_all(&mut self, _: &[u8]) -> RuntimeResult<()> {
        Ok(())
    }
}
impl RawFileInterfaceWriteExt for NullFile {
    fn fwext_fsync_all(&mut self) -> RuntimeResult<()> {
        Ok(())
    }
    fn fwext_truncate_to(&mut self, _: u64) -> RuntimeResult<()> {
        Ok(())
    }
}
impl RawFileInterfaceExt for NullFile {
    fn fext_file_length(&self) -> RuntimeResult<u64> {
        Ok(0)
    }

    fn fext_cursor(&mut self) -> RuntimeResult<u64> {
        Ok(0)
    }

    fn fext_seek_ahead_from_start_by(&mut self, _: u64) -> RuntimeResult<()> {
        Ok(())
    }
}
impl RawFileInterface for NullFile {
    type BufReader = Self;
    type BufWriter = Self;
    fn into_buffered_reader(self) -> RuntimeResult<Self::BufReader> {
        Ok(self)
    }
    fn downgrade_reader(r: Self::BufReader) -> RuntimeResult<Self> {
        Ok(r)
    }
    fn into_buffered_writer(self) -> RuntimeResult<Self::BufWriter> {
        Ok(self)
    }
    fn downgrade_writer(w: Self::BufWriter) -> RuntimeResult<Self> {
        Ok(w)
    }
}

impl RawFileInterfaceBufferedWriter for NullFile {}
