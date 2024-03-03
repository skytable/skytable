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
    self::impls::mdl_journal::{BatchStats, FullModel},
    super::{common::interface::fs::FileSystem, v1, SELoaded},
    crate::engine::{
        config::Configuration,
        core::{
            system_db::{SystemDatabase, VerifyUser},
            GNSData, GlobalNS,
        },
        fractal::{context, FractalGNSDriver},
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

pub fn recreate(gns: GNSData) -> RuntimeResult<SELoaded> {
    context::set_dmsg("creating gns");
    let mut gns_driver = impls::gns_log::GNSDriver::create_gns()?;
    // create all spaces
    context::set_dmsg("creating all spaces");
    for (space_name, space) in gns.idx().read().iter() {
        FileSystem::create_dir_all(&paths_v1::space_dir(space_name, space.get_uuid()))?;
        gns_driver.commit_event(CreateSpaceTxn::new(space.props(), &space_name, space))?;
    }
    // create all models
    context::set_dmsg("creating all models");
    for (model_id, model) in gns.idx_models().read().iter() {
        let model_data = model.data();
        let space_uuid = gns.idx().read().get(model_id.space()).unwrap().get_uuid();
        FileSystem::create_dir_all(&paths_v1::model_dir(
            model_id.space(),
            space_uuid,
            model_id.entity(),
            model_data.get_uuid(),
        ))?;
        let mut model_driver = ModelDriver::create_model_driver(&paths_v1::model_path(
            model_id.space(),
            space_uuid,
            model_id.entity(),
            model_data.get_uuid(),
        ))?;
        gns_driver.commit_event(CreateModelTxn::new(
            SpaceIDRef::with_uuid(model_id.space(), space_uuid),
            model_id.entity(),
            model_data,
        ))?;
        model_driver.commit_with_ctx(FullModel::new(model_data), BatchStats::new())?;
        model.driver().initialize_model_driver(model_driver);
    }
    // create all users
    context::set_dmsg("creating all users");
    for (user_name, user) in gns.sys_db().users().read().iter() {
        gns_driver.commit_event(CreateUserTxn::new(&user_name, user.hash()))?;
    }
    Ok(SELoaded {
        gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
    })
}

pub fn initialize_new(config: &Configuration) -> RuntimeResult<SELoaded> {
    FileSystem::create_dir_all(DATA_DIR)?;
    let mut gns_driver = impls::gns_log::GNSDriver::create_gns()?;
    let gns = GNSData::empty();
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
        gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
    })
}

pub fn restore(cfg: &Configuration) -> RuntimeResult<SELoaded> {
    let gns = GNSData::empty();
    context::set_dmsg("loading gns");
    let mut gns_driver = impls::gns_log::GNSDriver::open_gns(&gns)?;
    for (id, model) in gns.idx_models().write().iter_mut() {
        let model_data = model.data();
        let space_uuid = gns.idx().read().get(id.space()).unwrap().get_uuid();
        let model_data_file_path =
            paths_v1::model_path(id.space(), space_uuid, id.entity(), model_data.get_uuid());
        context::set_dmsg(format!("loading model driver in {model_data_file_path}"));
        let model_driver =
            impls::mdl_journal::ModelDriver::open_model_driver(model_data, &model_data_file_path)?;
        model.driver().initialize_model_driver(model_driver);
        unsafe {
            // UNSAFE(@ohsayan): all pieces of data are upgraded by now, so vacuum
            model.data_mut().model_mutator().vacuum_stashed();
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
        gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
    })
}
