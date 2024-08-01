/*
 * Created on Sun Mar 05 2023
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
    super::{Field, Layer, ModelData},
    crate::{
        engine::{
            core::EntityIDRef,
            data::{
                tag::{DataTag, TagClass},
                DictEntryGeneric,
            },
            error::{QueryError, QueryResult},
            fractal::GlobalInstanceLike,
            idx::{IndexST, IndexSTSeqCns, STIndex, STIndexSeq},
            mem::unsafe_apis::BoxStr,
            ql::{
                ddl::{
                    alt::{AlterKind, AlterModel},
                    syn::{ExpandedField, LayerSpec},
                },
                lex::Ident,
            },
            txn::{gns, ModelIDRef},
        },
        util,
    },
    std::collections::{HashMap, HashSet},
};

#[derive(Debug, PartialEq)]
pub(in crate::engine::core) struct AlterPlan<'a> {
    pub(in crate::engine::core) model: EntityIDRef<'a>,
    pub(in crate::engine::core) no_lock: bool,
    pub(in crate::engine::core) action: AlterAction<'a>,
}

#[derive(Debug, PartialEq)]
pub(in crate::engine::core) enum AlterAction<'a> {
    Ignore,
    Add(IndexSTSeqCns<BoxStr, Field>),
    Update(IndexST<BoxStr, Field>),
    Remove(Box<[Ident<'a>]>),
}

macro_rules! can_ignore {
    (AlterAction::$variant:ident($expr:expr)) => {
        if crate::engine::mem::StatelessLen::stateless_empty(&$expr) {
            AlterAction::Ignore
        } else {
            AlterAction::$variant($expr)
        }
    };
}

#[inline(always)]
fn no_field(mr: &ModelData, new: &str) -> bool {
    !mr.fields().st_contains(new)
}

fn check_nullable(props: &mut HashMap<BoxStr, DictEntryGeneric>) -> QueryResult<bool> {
    match props.remove("nullable") {
        Some(DictEntryGeneric::Data(b)) if b.kind() == TagClass::Bool => Ok(b.bool()),
        Some(_) => Err(QueryError::QExecDdlInvalidProperties),
        None => Ok(false),
    }
}

impl<'a> AlterPlan<'a> {
    pub fn fdeltas(
        mdl: &ModelData,
        AlterModel { model, kind }: AlterModel<'a>,
    ) -> QueryResult<AlterPlan<'a>> {
        let mut no_lock = true;
        let mut okay = true;
        let action = match kind {
            AlterKind::Remove(r) => {
                let mut x = HashSet::new();
                if !r.iter().all(|id| x.insert(id.as_str())) {
                    return Err(QueryError::QExecDdlModelAlterIllegal);
                }
                let mut not_found = false;
                if r.iter().all(|id| {
                    let not_pk = mdl.not_pk(id);
                    let exists = !no_field(mdl, id.as_str());
                    not_found = !exists;
                    not_pk & exists
                }) {
                    can_ignore!(AlterAction::Remove(r))
                } else if not_found {
                    return Err(QueryError::QExecUnknownField);
                } else {
                    return Err(QueryError::QExecDdlModelAlterIllegal);
                }
            }
            AlterKind::Add(new_fields) => {
                let mut fields = util::bx_to_vec(new_fields).into_iter();
                let mut add = IndexSTSeqCns::with_capacity(fields.len());
                while (fields.len() != 0) & okay {
                    let ExpandedField {
                        field_name,
                        layers,
                        mut props,
                    } = fields.next().unwrap();
                    okay &= no_field(mdl, &field_name) & mdl.not_pk(&field_name);
                    let is_nullable = check_nullable(&mut props)?;
                    let layers = Field::parse_layers(layers, is_nullable)?;
                    okay &= add.st_insert(BoxStr::new(field_name.as_str()), layers);
                }
                can_ignore!(AlterAction::Add(add))
            }
            AlterKind::Update(updated_fields) => {
                let updated_fields = util::bx_to_vec::<ExpandedField<'a>>(updated_fields);
                let mut updated_fields = updated_fields.into_iter();
                let mut any_delta = 0;
                let mut new_fields = IndexST::new();
                while (updated_fields.len() != 0) & okay {
                    let ExpandedField {
                        field_name,
                        layers,
                        mut props,
                    } = updated_fields.next().unwrap();
                    // enforce pk
                    mdl.guard_pk(&field_name)?;
                    // get the current field
                    let Some(current_field) = mdl.fields().st_get(field_name.as_str()) else {
                        return Err(QueryError::QExecUnknownField);
                    };
                    // check props
                    let is_nullable = check_nullable(&mut props)?;
                    okay &= props.is_empty();
                    // check layers
                    let (anydelta, new_field) =
                        Self::ldeltas(current_field, layers, is_nullable, &mut no_lock, &mut okay)?;
                    any_delta += anydelta as usize;
                    okay &= new_fields.st_insert(BoxStr::new(field_name.as_str()), new_field);
                }
                if any_delta == 0 {
                    AlterAction::Ignore
                } else {
                    AlterAction::Update(new_fields)
                }
            }
        };
        if okay {
            Ok(Self {
                model,
                action,
                no_lock,
            })
        } else {
            Err(QueryError::QExecDdlModelAlterIllegal)
        }
    }
    fn ldeltas(
        current: &Field,
        layers: Vec<LayerSpec<'a>>,
        nullable: bool,
        super_nlck: &mut bool,
        super_okay: &mut bool,
    ) -> QueryResult<(bool, Field)> {
        #[inline(always)]
        fn classeq(current: &Layer, new: &Layer, class: TagClass) -> bool {
            // KIDDOS, LEARN SOME RELATIONS BEFORE WRITING CODE
            (current.tag.tag_class() == new.tag.tag_class()) & (current.tag.tag_class() == class)
        }
        #[inline(always)]
        fn interop(current: &Layer, new: &Layer) -> bool {
            classeq(current, new, TagClass::UnsignedInt)
                | classeq(current, new, TagClass::SignedInt)
                | classeq(current, new, TagClass::Float)
        }
        if layers.len() > current.layers().len() {
            // simply a dumb tomato; ELIMINATE THESE DUMB TOMATOES
            return Err(QueryError::QExecDdlModelAlterIllegal);
        }
        let mut no_lock = !(current.is_nullable() & !nullable);
        let mut deltasize = (current.is_nullable() ^ nullable) as usize;
        let mut okay = true;
        let mut new_field = current.clone();
        new_field.nullable = nullable;
        let mut zipped_layers = layers
            .into_iter()
            .rev()
            .zip(current.layers())
            .zip(new_field.layers.iter_mut());
        // check all layers
        while (zipped_layers.len() != 0) & okay {
            let ((LayerSpec { ty, props }, current_layer), new_layer) =
                zipped_layers.next().unwrap();
            // actually parse the new layer
            okay &= props.is_empty();
            let Some(new_parsed_layer) = Layer::get_layer(&ty) else {
                return Err(QueryError::QExecDdlInvalidTypeDefinition);
            };
            match (
                current_layer.tag.tag_selector(),
                new_parsed_layer.tag.tag_selector(),
            ) {
                (current_tag, new_tag) if current_tag == new_tag => {
                    // no delta
                }
                (current_selector, new_selector) if interop(current_layer, &new_parsed_layer) => {
                    // now, we're not sure if we can run this
                    // FIXME(@ohsayan): look, should we be explicit about this?
                    no_lock &= new_selector >= current_selector;
                    deltasize += (new_selector != current_selector) as usize;
                }
                _ => {
                    // can't cast this directly
                    return Err(QueryError::QExecDdlInvalidTypeDefinition);
                }
            }
            *new_layer = new_parsed_layer;
        }
        *super_nlck &= no_lock;
        *super_okay &= okay;
        if okay {
            Ok((deltasize != 0, new_field))
        } else {
            Err(QueryError::QExecDdlModelAlterIllegal)
        }
    }
}

impl ModelData {
    pub fn transactional_exec_alter<G: GlobalInstanceLike>(
        global: &G,
        alter: AlterModel,
    ) -> QueryResult<()> {
        let (space_name, model_name) = (alter.model.space(), alter.model.entity());
        global
            .state()
            .namespace()
            .with_model_space_mut_for_ddl(alter.model, |space, model| {
                // prepare plan
                let plan = AlterPlan::fdeltas(model, alter)?;
                // we have a legal plan; acquire exclusive if we need it
                if !plan.no_lock {
                    // TODO(@ohsayan): allow this later on, once we define the syntax
                    return Err(QueryError::QExecNeedLock);
                }
                // fine, we're good
                match plan.action {
                    AlterAction::Ignore => {}
                    AlterAction::Add(new_fields) => {
                        // TODO(@ohsayan): this impacts lockdown duration; fix it

                        // prepare txn
                        let txn = gns::model::AlterModelAddTxn::new(
                            ModelIDRef::new_ref(&space_name, &space, &model_name, model),
                            &new_fields,
                        );
                        // commit txn
                        global.state().gns_driver().driver_context(
                            global,
                            |drv| drv.commit_event(txn),
                            || {},
                        )?;
                        let mut mutator = model.model_mutator();
                        new_fields
                            .stseq_ord_kv()
                            .map(|(x, y)| (x.clone(), y.clone()))
                            .for_each(|(field_id, field)| {
                                mutator.add_field(field_id, field);
                            });
                    }
                    AlterAction::Remove(removed) => {
                        // prepare txn
                        let txn = gns::model::AlterModelRemoveTxn::new(
                            ModelIDRef::new_ref(&space_name, space, &model_name, model),
                            &removed,
                        );
                        // commit txn
                        global.state().gns_driver().driver_context(
                            global,
                            |drv| drv.commit_event(txn),
                            || {},
                        )?;
                        let mut mutator = model.model_mutator();
                        removed.iter().for_each(|field_id| {
                            mutator.remove_field(field_id.as_str());
                        });
                    }
                    AlterAction::Update(updated) => {
                        // prepare txn
                        let txn = gns::model::AlterModelUpdateTxn::new(
                            ModelIDRef::new_ref(&space_name, space, &model_name, model),
                            &updated,
                        );
                        // commit txn
                        global.state().gns_driver().driver_context(
                            global,
                            |drv| drv.commit_event(txn),
                            || {},
                        )?;
                        let mut mutator = model.model_mutator();
                        updated.into_iter().for_each(|(field_id, field)| {
                            mutator.update_field(field_id.as_ref(), field);
                        });
                    }
                }
                Ok(())
            })
    }
}
