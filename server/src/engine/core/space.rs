/*
 * Created on Mon Feb 06 2023
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
    super::EntityIDRef,
    crate::engine::{
        data::{dict, uuid::Uuid, DictEntryGeneric, DictGeneric},
        error::{QueryError, QueryResult},
        fractal::{GenericTask, GlobalInstanceLike, Task},
        idx::STIndex,
        mem::unsafe_apis::BoxStr,
        ql::ddl::{alt::AlterSpace, crt::CreateSpace, drop::DropSpace},
        txn::{self, SpaceIDRef},
    },
    std::collections::HashSet,
};

#[derive(Debug, PartialEq)]
pub struct Space {
    uuid: Uuid,
    models: HashSet<BoxStr>,
    props: DictGeneric,
}

#[derive(Debug, PartialEq)]
/// Procedure for `create space`
struct ProcedureCreate {
    space_name: BoxStr,
    space: Space,
    if_not_exists: bool,
}

impl Space {
    pub fn new(uuid: Uuid, models: HashSet<BoxStr>, props: DictGeneric) -> Self {
        Self {
            uuid,
            models,
            props,
        }
    }
    #[cfg(test)]
    pub fn new_auto_all() -> Self {
        Self::new_auto(Default::default(), Default::default())
    }
    pub fn get_uuid(&self) -> Uuid {
        self.uuid
    }
    pub fn new_restore_empty(uuid: Uuid, props: DictGeneric) -> Self {
        Self::new(uuid, Default::default(), props)
    }
    pub fn new_empty_auto(props: DictGeneric) -> Self {
        Self::new_auto(Default::default(), props)
    }
    pub fn new_auto(models: HashSet<BoxStr>, props: DictGeneric) -> Self {
        Self::new(Uuid::new(), models, props)
    }
    pub fn models(&self) -> &HashSet<BoxStr> {
        &self.models
    }
    pub fn models_mut(&mut self) -> &mut HashSet<BoxStr> {
        &mut self.models
    }
    pub fn props(&self) -> &DictGeneric {
        &self.props
    }
    pub fn props_mut(&mut self) -> &mut DictGeneric {
        &mut self.props
    }
    #[cfg(test)]
    pub fn env(&self) -> &DictGeneric {
        match self.props().get(Self::KEY_ENV).unwrap() {
            DictEntryGeneric::Map(m) => m,
            _ => panic!(),
        }
    }
}

impl Space {
    const KEY_ENV: &'static str = "env";
    #[inline]
    /// Validate a `create` stmt
    fn process_create(
        CreateSpace {
            space_name,
            mut props,
            if_not_exists,
        }: CreateSpace,
    ) -> QueryResult<ProcedureCreate> {
        let space_name = BoxStr::new(space_name.as_str());
        // now let's check our props
        match props.get(Self::KEY_ENV) {
            Some(d) if props.len() == 1 => {
                match d {
                    DictEntryGeneric::Data(d) if d.is_init() => {
                        // not the right type for a dict
                        return Err(QueryError::QExecDdlInvalidProperties);
                    }
                    DictEntryGeneric::Data(_) => {
                        // a null? make it empty
                        let _ = props.insert(
                            BoxStr::new(Self::KEY_ENV),
                            DictEntryGeneric::Map(into_dict!()),
                        );
                    }
                    DictEntryGeneric::Map(_) => {}
                }
            }
            None if props.is_empty() => {
                let _ = props.st_insert(
                    BoxStr::new(Self::KEY_ENV),
                    DictEntryGeneric::Map(into_dict!()),
                );
            }
            _ => {
                // in all the other cases, we have illegal properties
                // not the right type for a dict
                return Err(QueryError::QExecDdlInvalidProperties);
            }
        }
        Ok(ProcedureCreate {
            space_name,
            space: Space::new_empty_auto(dict::rflatten_metadata(props)),
            if_not_exists,
        })
    }
}

impl Space {
    pub fn transactional_exec_create<G: GlobalInstanceLike>(
        global: &G,
        space: CreateSpace,
    ) -> QueryResult<Option<bool>> {
        // process create
        let ProcedureCreate {
            space_name,
            space,
            if_not_exists,
        } = Self::process_create(space)?;
        // lock the global namespace
        global.state().namespace().ddl_with_spaces_write(|spaces| {
            if spaces.st_contains(&space_name) {
                if if_not_exists {
                    return Ok(Some(false));
                } else {
                    return Err(QueryError::QExecDdlObjectAlreadyExists);
                }
            }
            // commit txn
            // prepare txn
            let txn = txn::gns::space::CreateSpaceTxn::new(space.props(), &space_name, &space);
            // try to create space for...the space
            global.initialize_space(&space_name, space.get_uuid())?;
            // commit txn
            global.state().gns_driver().driver_context(
                global,
                |drv| drv.commit_event(txn),
                || {
                    global.taskmgr_post_standard_priority(Task::new(GenericTask::delete_space_dir(
                        &space_name,
                        space.get_uuid(),
                    )))
                },
            )?;
            // update global state
            let _ = spaces.st_insert(space_name, space);
            if if_not_exists {
                Ok(Some(true))
            } else {
                Ok(None)
            }
        })
    }
    #[allow(unused)]
    pub fn transactional_exec_alter<G: GlobalInstanceLike>(
        global: &G,
        AlterSpace {
            space_name,
            updated_props,
        }: AlterSpace,
    ) -> QueryResult<()> {
        global
            .state()
            .namespace()
            .ddl_with_space_mut(&space_name, |space| {
                match updated_props.get(Self::KEY_ENV) {
                    Some(DictEntryGeneric::Map(_)) if updated_props.len() == 1 => {}
                    Some(DictEntryGeneric::Data(l)) if updated_props.len() == 1 && l.is_null() => {}
                    None if updated_props.is_empty() => return Ok(()),
                    _ => return Err(QueryError::QExecDdlInvalidProperties),
                }
                // create patch
                let patch = match dict::rprepare_metadata_patch(space.props(), updated_props) {
                    Some(patch) => patch,
                    None => return Err(QueryError::QExecDdlInvalidProperties),
                };
                // prepare txn
                let txn = txn::gns::space::AlterSpaceTxn::new(
                    SpaceIDRef::new(&space_name, space),
                    &patch,
                );
                // commit
                // commit txn
                global.state().gns_driver().driver_context(
                    global,
                    |drv| drv.commit_event(txn),
                    || {},
                )?;
                // merge
                dict::rmerge_data_with_patch(space.props_mut(), patch);
                // the `env` key may have been popped, so put it back (setting `env: null` removes the env key and we don't want to waste time enforcing this in the
                // merge algorithm)
                let _ = space.props_mut().st_insert(
                    BoxStr::new(Self::KEY_ENV),
                    DictEntryGeneric::Map(into_dict!()),
                );
                Ok(())
            })
    }
    pub fn transactional_exec_drop<G: GlobalInstanceLike>(
        global: &G,
        DropSpace {
            space: space_name,
            force,
            if_exists,
        }: DropSpace,
    ) -> QueryResult<Option<bool>> {
        if force {
            global
                .state()
                .namespace()
                .ddl_with_all_mut(|spaces, models| {
                    let Some(space) = spaces.remove(space_name.as_str()) else {
                        if if_exists {
                            return Ok(Some(false));
                        } else {
                            return Err(QueryError::QExecObjectNotFound);
                        }
                    };
                    // commit drop
                    // prepare txn
                    let txn =
                        txn::gns::space::DropSpaceTxn::new(SpaceIDRef::new(&space_name, &space));
                    // commit txn
                    global.state().gns_driver().driver_context(
                        global,
                        |drv| drv.commit_event(txn),
                        || {},
                    )?;
                    // request cleanup
                    global.taskmgr_post_standard_priority(Task::new(
                        GenericTask::delete_space_dir(&space_name, space.get_uuid()),
                    ));
                    for model in space.models.into_iter() {
                        let e: EntityIDRef<'static> = unsafe {
                            // UNSAFE(@ohsayan): I want to try what the borrow checker has been trying
                            core::mem::transmute(EntityIDRef::new(space_name.as_str(), &model))
                        };
                        let mdl = models.st_delete_return(&e).unwrap();
                        // no need to purge model drive since the dir itself is deleted. our work here is to just
                        // remove this from the linked models from the model ns. but we should update the global state
                        if mdl.driver().status().is_iffy() {
                            // yes this driver had a fault but it's being purged anyway so update global status
                            global.health().report_removal_of_faulty_source();
                        }
                    }
                    let _ = spaces.st_delete(space_name.as_str());
                    if if_exists {
                        Ok(Some(true))
                    } else {
                        Ok(None)
                    }
                })
        } else {
            global.state().namespace().ddl_with_spaces_write(|spaces| {
                let Some(space) = spaces.get(space_name.as_str()) else {
                    if if_exists {
                        return Ok(Some(false));
                    } else {
                        return Err(QueryError::QExecObjectNotFound);
                    }
                };
                if !space.models.is_empty() {
                    // nonempty, we can't do anything
                    return Err(QueryError::QExecDdlNotEmpty);
                }
                // okay, it's empty; good riddance
                // prepare txn
                let txn = txn::gns::space::DropSpaceTxn::new(SpaceIDRef::new(&space_name, &space));
                // commit txn
                global.state().gns_driver().driver_context(
                    global,
                    |drv| drv.commit_event(txn),
                    || {},
                )?;
                // request cleanup
                global.taskmgr_post_standard_priority(Task::new(GenericTask::delete_space_dir(
                    &space_name,
                    space.get_uuid(),
                )));
                let _ = spaces.st_delete(space_name.as_str());
                if if_exists {
                    Ok(Some(true))
                } else {
                    Ok(None)
                }
            })
        }
    }
}
