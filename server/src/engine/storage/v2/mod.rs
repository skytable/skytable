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
    self::impls::mdl_journal::FullModel,
    super::{
        common::interface::{fs_imp::LocalFS, fs_traits::FSInterface},
        v1, SELoaded,
    },
    crate::engine::{
        config::Configuration,
        core::{
            system_db::{SystemDatabase, VerifyUser},
            GlobalNS,
        },
        fractal::{context, ModelDrivers, ModelUniqueID},
        storage::common::paths_v1,
        txn::{
            gns::{
                model::CreateModelTxn,
                space::CreateSpaceTxn,
                sysctl::{AlterUserTxn, CreateUserTxn},
            },
            SpaceIDRef,
        },
        RuntimeResult,
    },
    impls::mdl_journal::ModelDriver,
};

pub(super) mod impls;
pub(super) mod raw;

pub const GNS_PATH: &str = v1::GNS_PATH;
pub const DATA_DIR: &str = v1::DATA_DIR;

pub fn recreate(gns: GlobalNS) -> RuntimeResult<SELoaded> {
    let model_drivers = ModelDrivers::empty();
    context::set_dmsg("creating gns");
    let mut gns_driver = impls::gns_log::GNSDriver::create_gns()?;
    // create all spaces
    context::set_dmsg("creating all spaces");
    for (space_name, space) in gns.idx().read().iter() {
        LocalFS::fs_create_dir_all(&paths_v1::space_dir(space_name, space.get_uuid()))?;
        gns_driver.commit_event(CreateSpaceTxn::new(space.props(), &space_name, space))?;
    }
    // create all models
    context::set_dmsg("creating all models");
    for (model_id, model) in gns.idx_models().read().iter() {
        let space_uuid = gns.idx().read().get(model_id.space()).unwrap().get_uuid();
        LocalFS::fs_create_dir_all(&paths_v1::model_dir(
            model_id.space(),
            space_uuid,
            model_id.entity(),
            model.get_uuid(),
        ))?;
        let mut model_driver = ModelDriver::create_model_driver(&paths_v1::model_path(
            model_id.space(),
            space_uuid,
            model_id.entity(),
            model.get_uuid(),
        ))?;
        gns_driver.commit_event(CreateModelTxn::new(
            SpaceIDRef::with_uuid(model_id.space(), space_uuid),
            model_id.entity(),
            model,
        ))?;
        model_driver.commit_event(FullModel::new(model))?;
        model_drivers.add_driver(
            ModelUniqueID::new(model_id.space(), model_id.entity(), model.get_uuid()),
            model_driver,
        );
    }
    Ok(SELoaded {
        gns,
        gns_driver,
        model_drivers,
    })
}

pub fn initialize_new(config: &Configuration) -> RuntimeResult<SELoaded> {
    LocalFS::fs_create_dir_all(DATA_DIR)?;
    let mut gns_driver = impls::gns_log::GNSDriver::create_gns()?;
    let gns = GlobalNS::empty();
    let password_hash = rcrypt::hash(&config.auth.root_key, rcrypt::DEFAULT_COST).unwrap();
    // now go ahead and initialize our root user
    gns_driver.commit_event(CreateUserTxn::new(
        SystemDatabase::ROOT_ACCOUNT,
        &password_hash,
    ))?;
    assert!(gns.sys_db().__raw_create_user(
        SystemDatabase::ROOT_ACCOUNT.to_owned().into_boxed_str(),
        password_hash.into_boxed_slice(),
    ));
    Ok(SELoaded {
        gns,
        gns_driver,
        model_drivers: ModelDrivers::empty(),
    })
}

pub fn restore(cfg: &Configuration) -> RuntimeResult<SELoaded> {
    let gns = GlobalNS::empty();
    context::set_dmsg("loading gns");
    let mut gns_driver = impls::gns_log::GNSDriver::open_gns(&gns)?;
    let model_drivers = ModelDrivers::empty();
    for (id, model) in gns.idx_models().write().iter_mut() {
        let space_uuid = gns.idx().read().get(id.space()).unwrap().get_uuid();
        let model_data_file_path =
            paths_v1::model_path(id.space(), space_uuid, id.entity(), model.get_uuid());
        context::set_dmsg(format!("loading model driver in {model_data_file_path}"));
        let model_driver =
            impls::mdl_journal::ModelDriver::open_model_driver(model, &model_data_file_path)?;
        model_drivers.add_driver(
            ModelUniqueID::new(id.space(), id.entity(), model.get_uuid()),
            model_driver,
        );
        unsafe {
            // UNSAFE(@ohsayan): all pieces of data are upgraded by now, so vacuum
            model.model_mutator().vacuum_stashed();
        }
    }
    // check if password has changed
    if gns
        .sys_db()
        .verify_user(SystemDatabase::ROOT_ACCOUNT, cfg.auth.root_key.as_bytes())
        == VerifyUser::IncorrectPassword
    {
        // the password was changed
        warn!("root password changed via configuration");
        context::set_dmsg("updating password to system database from configuration");
        let phash = rcrypt::hash(&cfg.auth.root_key, rcrypt::DEFAULT_COST).unwrap();
        gns_driver.commit_event(AlterUserTxn::new(SystemDatabase::ROOT_ACCOUNT, &phash))?;
        gns.sys_db()
            .__raw_alter_user(SystemDatabase::ROOT_ACCOUNT, phash.into_boxed_slice());
    }
    Ok(SELoaded {
        gns,
        gns_driver,
        model_drivers,
    })
}
