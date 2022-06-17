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

use {
    crate::{
        actions::{translate_ddl_error, ActionResult},
        corestore::{
            memstore::{DdlError, Keyspace, Memstore, ObjectID, DEFAULT},
            table::{DescribeTable, Table},
        },
        protocol::interface::ProtocolSpec,
        queryengine::parser::{Entity, OwnedEntity},
        registry,
        storage::{
            self,
            v1::{error::StorageEngineResult, sengine::SnapshotEngine},
        },
        util::{self, Unwrappable},
    },
    core::{borrow::Borrow, hash::Hash},
    std::sync::Arc,
};

pub use htable::Data;
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
            table: Some((DEFAULT, tbl)),
            ks: Some((DEFAULT, ks)),
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
    pub fn init_with_snapcfg(sengine: Arc<SnapshotEngine>) -> StorageEngineResult<Self> {
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
    pub fn swap_entity(&mut self, entity: Entity<'_>) -> KeyspaceResult<()> {
        match entity {
            // Switch to the provided keyspace
            Entity::Single(ks) => match self.store.get_keyspace_atomic_ref(ks) {
                Some(ksref) => self
                    .estate
                    .set_ks(ksref, unsafe { ObjectID::from_slice(ks) }),
                None => return Err(DdlError::ObjectNotFound),
            },
            // Switch to the provided table in the given keyspace
            Entity::Full(ks, tbl) => match self.store.get_keyspace_atomic_ref(ks) {
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
            Entity::Partial(tbl) => match &self.estate.ks {
                Some((_, ks)) => match ks.get_table_atomic_ref(tbl) {
                    Some(tblref) => {
                        self.estate.table = Some((unsafe { ObjectID::from_slice(tbl) }, tblref));
                    }
                    None => return Err(DdlError::ObjectNotFound),
                },
                None => return Err(DdlError::DefaultNotFound),
            },
        }
        Ok(())
    }
    /// Returns the current keyspace, if set
    pub fn get_cks(&self) -> KeyspaceResult<&Keyspace> {
        match self.estate.ks {
            Some((_, ref cks)) => Ok(cks),
            _ => Err(DdlError::DefaultNotFound),
        }
    }
    pub fn get_keyspace<Q>(&self, ksid: &Q) -> Option<Arc<Keyspace>>
    where
        ObjectID: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.store.get_keyspace_atomic_ref(ksid)
    }
    /// Get an atomic reference to a table
    pub fn get_table(&self, entity: Entity<'_>) -> KeyspaceResult<Arc<Table>> {
        match entity {
            Entity::Full(ksid, table) => match self.store.get_keyspace_atomic_ref(ksid) {
                Some(ks) => match ks.get_table_atomic_ref(table) {
                    Some(tbl) => Ok(tbl),
                    None => Err(DdlError::ObjectNotFound),
                },
                None => Err(DdlError::ObjectNotFound),
            },
            Entity::Single(tbl) | Entity::Partial(tbl) => match &self.estate.ks {
                Some((_, ks)) => match ks.get_table_atomic_ref(tbl) {
                    Some(tbl) => Ok(tbl),
                    None => Err(DdlError::ObjectNotFound),
                },
                None => Err(DdlError::DefaultNotFound),
            },
        }
    }
    pub fn get_ctable(&self) -> Option<Arc<Table>> {
        self.estate.table.as_ref().map(|(_, tbl)| tbl.clone())
    }
    pub fn get_table_result(&self) -> KeyspaceResult<&Table> {
        match self.estate.table {
            Some((_, ref table)) => Ok(table),
            _ => Err(DdlError::DefaultNotFound),
        }
    }
    pub fn get_ctable_ref(&self) -> Option<&Table> {
        self.estate.table.as_ref().map(|(_, tbl)| tbl.as_ref())
    }
    /// Returns a table with the provided specification
    pub fn get_table_with<P: ProtocolSpec, T: DescribeTable>(&self) -> ActionResult<&T::Table> {
        T::get::<P>(self)
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
        entity: Entity<'_>,
        modelcode: u8,
        volatile: bool,
    ) -> KeyspaceResult<()> {
        let entity = entity.into_owned();
        // first lock the global flush state
        let flush_lock = registry::lock_flush_state();
        let ret = match entity {
            // Important: create table <tblname> is only ks
            OwnedEntity::Single(tblid) | OwnedEntity::Partial(tblid) => {
                match &self.estate.ks {
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
                }
            }
            OwnedEntity::Full(ksid, tblid) => {
                match self.store.get_keyspace_atomic_ref(&ksid) {
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
        };
        // free the global flush lock
        drop(flush_lock);
        ret
    }

    /// Drop a table
    pub fn drop_table(&self, entity: Entity<'_>, force: bool) -> KeyspaceResult<()> {
        match entity {
            Entity::Single(tblid) | Entity::Partial(tblid) => match &self.estate.ks {
                Some((_, ks)) => ks.drop_table(tblid, force),
                None => Err(DdlError::DefaultNotFound),
            },
            Entity::Full(ksid, tblid) => match self.store.get_keyspace_atomic_ref(ksid) {
                Some(ks) => ks.drop_table(tblid, force),
                None => Err(DdlError::ObjectNotFound),
            },
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
    pub fn list_tables<P: ProtocolSpec>(&self, ksid: Option<&[u8]>) -> ActionResult<Vec<ObjectID>> {
        Ok(match ksid {
            Some(keyspace_name) => {
                // inspect the provided keyspace
                let ksid = if keyspace_name.len() > 64 {
                    return util::err(P::RSTRING_BAD_CONTAINER_NAME);
                } else {
                    keyspace_name
                };
                let ks = match self.get_keyspace(ksid) {
                    Some(kspace) => kspace,
                    None => return util::err(P::RSTRING_CONTAINER_NOT_FOUND),
                };
                ks.tables.iter().map(|kv| kv.key().clone()).collect()
            }
            None => {
                // inspect the current keyspace
                let cks = translate_ddl_error::<P, &Keyspace>(self.get_cks())?;
                cks.tables.iter().map(|kv| kv.key().clone()).collect()
            }
        })
    }
}
