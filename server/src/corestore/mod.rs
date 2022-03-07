/*
 * Created on Tue Jul 20 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

use crate::actions::ActionResult;
use crate::corestore::{
    memstore::{DdlError, Keyspace, Memstore, ObjectID, DEFAULT},
    table::{DescribeTable, Table},
};
use crate::registry;
use crate::storage;
use crate::storage::v1::sengine::SnapshotEngine;
use crate::util::Unwrappable;
use crate::IoResult;
use core::borrow::Borrow;
use core::fmt;
use core::hash::Hash;
pub use htable::Data;
use std::sync::Arc;
pub mod array;
pub mod backoff;
pub mod booltable;
pub mod buffers;
pub mod heap_array;
pub mod htable;
pub mod iarray;
pub mod lazy;
pub mod lock;
pub mod map;
pub mod memstore;
pub mod table;
#[cfg(test)]
mod tests;

pub(super) type KeyspaceResult<T> = Result<T, DdlError>;
type OptionTuple<T> = (Option<T>, Option<T>);
/// An owned entity group
pub type OwnedEntityGroup = OptionTuple<ObjectID>;
/// A raw borrowed entity (not the struct, but in a tuple form)
type BorrowedEntityGroupRaw<'a> = OptionTuple<&'a [u8]>;

#[derive(PartialEq)]
/// An entity group borrowed from a byte slice
pub struct BorrowedEntityGroup<'a> {
    va: Option<&'a [u8]>,
    vb: Option<&'a [u8]>,
}

impl<'a> fmt::Debug for BorrowedEntityGroup<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_if_some(v: Option<&'_ [u8]>) -> String {
            if let Some(v) = v {
                format!("{:?}", String::from_utf8_lossy(&v))
            } else {
                "None".to_owned()
            }
        }
        f.debug_struct("BorrowedEntityGroup")
            .field("va", &write_if_some(self.va))
            .field("vb", &write_if_some(self.vb))
            .finish()
    }
}

impl<'a> BorrowedEntityGroup<'a> {
    pub unsafe fn into_owned(self) -> OwnedEntityGroup {
        match self {
            BorrowedEntityGroup {
                va: Some(a),
                vb: Some(b),
            } => (Some(ObjectID::from_slice(a)), Some(ObjectID::from_slice(b))),
            BorrowedEntityGroup {
                va: Some(a),
                vb: None,
            } => (Some(ObjectID::from_slice(a)), None),
            _ => impossible!(),
        }
    }
}

impl<'a> From<BorrowedEntityGroupRaw<'a>> for BorrowedEntityGroup<'a> {
    fn from(oth: BorrowedEntityGroupRaw<'a>) -> Self {
        let (va, vb) = oth;
        Self { va, vb }
    }
}

#[derive(Debug, Clone)]
struct ConnectionEntityState {
    /// the current table for a connection
    table: Option<(ObjectID, Arc<Table>)>,
    /// the current keyspace for a connection
    ks: Option<(ObjectID, Arc<Keyspace>)>,
}

impl ConnectionEntityState {
    fn default(ks: Arc<Keyspace>, tbl: Arc<Table>) -> Self {
        Self {
            table: Some((DEFAULT.clone(), tbl)),
            ks: Some((DEFAULT.clone(), ks)),
        }
    }
    fn set_ks(&mut self, ks: Arc<Keyspace>, ksid: ObjectID) {
        self.ks = Some((ksid, ks));
        self.table = None;
    }
    fn set_table(&mut self, ks: Arc<Keyspace>, ksid: ObjectID, tbl: Arc<Table>, tblid: ObjectID) {
        self.ks = Some((ksid, ks));
        self.table = Some((tblid, tbl));
    }
    fn get_id_pack(&self) -> (Option<&ObjectID>, Option<&ObjectID>) {
        (
            self.ks.as_ref().map(|(id, _)| id),
            self.table.as_ref().map(|(id, _)| id),
        )
    }
}

/// The top level abstraction for the in-memory store. This is free to be shared across
/// threads, cloned and well, whatever. Most importantly, clones have an independent container
/// state that is the state of one connection and its container state preferences are never
/// synced across instances. This is important (see the impl for more info)
#[derive(Debug, Clone)]
pub struct Corestore {
    estate: ConnectionEntityState,
    /// an atomic reference to the actual backing storage
    store: Arc<Memstore>,
    /// the snapshot engine
    sengine: Arc<SnapshotEngine>,
}

impl Corestore {
    /// This is the only function you'll ever need to either create a new database instance
    /// or restore from an earlier instance
    pub fn init_with_snapcfg(sengine: Arc<SnapshotEngine>) -> IoResult<Self> {
        let store = storage::unflush::read_full()?;
        Ok(Self::default_with_store(store, sengine))
    }
    pub fn clone_store(&self) -> Arc<Memstore> {
        self.store.clone()
    }
    pub fn default_with_store(store: Memstore, sengine: Arc<SnapshotEngine>) -> Self {
        let cks = unsafe { store.get_keyspace_atomic_ref(&DEFAULT).unsafe_unwrap() };
        let ctable = unsafe { cks.get_table_atomic_ref(&DEFAULT).unsafe_unwrap() };
        Self {
            estate: ConnectionEntityState::default(cks, ctable),
            store: Arc::new(store),
            sengine,
        }
    }
    pub fn get_engine(&self) -> &SnapshotEngine {
        &self.sengine
    }
    pub fn get_store(&self) -> &Memstore {
        &self.store
    }
    /// Swap out the current table with a different one
    ///
    /// If the table is non-existent or the default keyspace was unset, then
    /// false is returned. Else true is returned
    pub fn swap_entity(&mut self, entity: BorrowedEntityGroup) -> KeyspaceResult<()> {
        match entity {
            // Switch to the provided keyspace
            BorrowedEntityGroup {
                va: Some(ks),
                vb: None,
            } => match self.store.get_keyspace_atomic_ref(ks) {
                Some(ksref) => self
                    .estate
                    .set_ks(ksref, unsafe { ObjectID::from_slice(ks) }),
                None => return Err(DdlError::ObjectNotFound),
            },
            // Switch to the provided table in the given keyspace
            BorrowedEntityGroup {
                va: Some(ks),
                vb: Some(tbl),
            } => match self.store.get_keyspace_atomic_ref(ks) {
                Some(kspace) => match kspace.get_table_atomic_ref(tbl) {
                    Some(tblref) => unsafe {
                        self.estate.set_table(
                            kspace,
                            ObjectID::from_slice(ks),
                            tblref,
                            ObjectID::from_slice(tbl),
                        )
                    },
                    None => return Err(DdlError::ObjectNotFound),
                },
                None => return Err(DdlError::ObjectNotFound),
            },
            _ => unsafe { impossible!() },
        }
        Ok(())
    }
    pub fn get_keyspace<Q>(&self, ksid: &Q) -> Option<Arc<Keyspace>>
    where
        ObjectID: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.store.get_keyspace_atomic_ref(ksid)
    }
    /// Get an atomic reference to a table
    pub fn get_table(&self, entity: BorrowedEntityGroup) -> KeyspaceResult<Arc<Table>> {
        match entity {
            BorrowedEntityGroup {
                va: Some(ksid),
                vb: Some(table),
            } => match self.store.get_keyspace_atomic_ref(ksid) {
                Some(ks) => match ks.get_table_atomic_ref(table) {
                    Some(tbl) => Ok(tbl),
                    None => Err(DdlError::ObjectNotFound),
                },
                None => Err(DdlError::ObjectNotFound),
            },
            BorrowedEntityGroup {
                va: Some(tbl),
                vb: None,
            } => match &self.estate.ks {
                Some((_, ks)) => match ks.get_table_atomic_ref(tbl) {
                    Some(tbl) => Ok(tbl),
                    None => Err(DdlError::ObjectNotFound),
                },
                None => Err(DdlError::DefaultNotFound),
            },
            _ => unsafe { impossible!() },
        }
    }
    pub fn get_ctable(&self) -> Option<Arc<Table>> {
        self.estate.table.as_ref().map(|(_, tbl)| tbl.clone())
    }
    pub fn get_ctable_ref(&self) -> Option<&Table> {
        self.estate.table.as_ref().map(|(_, tbl)| tbl.as_ref())
    }
    /// Returns a table with the provided specification
    pub fn get_table_with<T: DescribeTable>(&self) -> ActionResult<&T::Table> {
        T::get(self)
    }
    /// Create a table: in-memory; **no transactional guarantees**. Two tables can be created
    /// simultaneously, but are never flushed unless we are very lucky. If the global flush
    /// system is close to a flush cycle -- then we are in luck: we pause the flush cycle
    /// through a global flush lock and then allow it to resume once we're done adding the table.
    /// This enables the flush routine to permanently write the table to disk. But it's all about
    /// luck -- the next mutual access may be yielded to the next `create table` command
    ///
    /// **Trip switch handled:** Yes
    pub fn create_table(
        &self,
        entity: OwnedEntityGroup,
        modelcode: u8,
        volatile: bool,
    ) -> KeyspaceResult<()> {
        // first lock the global flush state
        let flush_lock = registry::lock_flush_state();
        let ret;
        match entity {
            // Important: create table <tblname> is only ks
            (Some(tblid), None) => {
                ret = match &self.estate.ks {
                    Some((_, ks)) => {
                        let tbl = Table::from_model_code(modelcode, volatile);
                        if let Some(tbl) = tbl {
                            if ks.create_table(tblid, tbl) {
                                // we need to re-init tree; so trip
                                registry::get_preload_tripswitch().trip();
                                Ok(())
                            } else {
                                Err(DdlError::AlreadyExists)
                            }
                        } else {
                            Err(DdlError::WrongModel)
                        }
                    }
                    None => Err(DdlError::DefaultNotFound),
                };
            }
            (Some(ksid), Some(tblid)) => {
                ret = match self.store.get_keyspace_atomic_ref(&ksid) {
                    Some(kspace) => {
                        let tbl = Table::from_model_code(modelcode, volatile);
                        if let Some(tbl) = tbl {
                            if kspace.create_table(tblid, tbl) {
                                // trip the preload switch
                                registry::get_preload_tripswitch().trip();
                                Ok(())
                            } else {
                                Err(DdlError::AlreadyExists)
                            }
                        } else {
                            Err(DdlError::WrongModel)
                        }
                    }
                    None => Err(DdlError::ObjectNotFound),
                }
            }
            _ => unsafe { impossible!() },
        }
        // free the global flush lock
        drop(flush_lock);
        ret
    }

    /// Drop a table
    pub fn drop_table(&self, entity: BorrowedEntityGroup) -> KeyspaceResult<()> {
        match entity {
            BorrowedEntityGroup {
                va: Some(tblid),
                vb: None,
            } => match &self.estate.ks {
                Some((_, ks)) => ks.drop_table(tblid),
                None => Err(DdlError::DefaultNotFound),
            },
            BorrowedEntityGroup {
                va: Some(ksid),
                vb: Some(tblid),
            } => match self.store.get_keyspace_atomic_ref(ksid) {
                Some(ks) => ks.drop_table(tblid),
                None => Err(DdlError::ObjectNotFound),
            },
            _ => unsafe { impossible!() },
        }
    }

    /// Create a keyspace **without any transactional guarantees**
    ///
    /// **Trip switch handled:** Yes
    pub fn create_keyspace(&self, ksid: ObjectID) -> KeyspaceResult<()> {
        // lock the global flush lock (see comment in create_table to know why)
        let flush_lock = registry::lock_flush_state();
        let ret = if self.store.create_keyspace(ksid) {
            // woo, created
            // trip the preload switch
            registry::get_preload_tripswitch().trip();
            Ok(())
        } else {
            // ugh, already exists
            Err(DdlError::AlreadyExists)
        };
        drop(flush_lock);
        ret
    }

    /// Drop a keyspace
    pub fn drop_keyspace(&self, ksid: ObjectID) -> KeyspaceResult<()> {
        // trip switch is handled by memstore here
        self.store.drop_keyspace(ksid)
    }

    /// Force drop a keyspace
    pub fn force_drop_keyspace(&self, ksid: ObjectID) -> KeyspaceResult<()> {
        // trip switch is handled by memstore here
        self.store.force_drop_keyspace(ksid)
    }
    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.store)
    }
    pub fn get_ids(&self) -> (Option<&ObjectID>, Option<&ObjectID>) {
        self.estate.get_id_pack()
    }
}
