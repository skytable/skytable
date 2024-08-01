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
    self::{
        impls::{
            gns_log::{self, GNSEventLog},
            mdl_journal::{BatchStats, FullModel, ModelDataAdapter},
        },
        raw::journal::{BatchAdapter, EventLogAdapter, JournalSettings, RepairResult},
    },
    crate::{
        engine::{
            config::{BackupSettings, BackupType, Configuration, RestoreSettings},
            core::{
                system_db::{SystemDatabase, VerifyUser},
                EntityIDRef, GNSData, GlobalNS,
            },
            error::StorageError,
            fractal::{context, FractalGNSDriver},
            mem::unsafe_apis::BoxStr,
            storage::{
                common::{interface::fs::FileSystem, paths_v1, sdss::sdss_r1::rw::SdssFile},
                v1,
                v2::{
                    impls::{gns_log::GNSAdapter, mdl_journal::ModelAdapter},
                    raw::journal::{self, JournalRepairMode},
                },
                SELoaded,
            },
            txn::gns::sysctl::{AlterUserTxn, CreateUserTxn},
            RuntimeResult,
        },
        util::{
            self,
            os::{self, FileLocks},
        },
    },
    impls::{
        backup_manifest::{BackupContext, BackupManifest},
        gns_log::{FSpecSystemDatabaseV1, GNSDriver},
        mdl_journal::ModelDriver,
    },
    std::path::Path,
};

pub(super) mod impls;
pub(super) mod raw;

pub const GNS_PATH: &str = v1::GNS_PATH;
pub const DATA_DIR: &str = v1::DATA_DIR;

/*
    upgrade
*/

pub fn recreate(gns: GNSData) -> RuntimeResult<SELoaded> {
    context::set_dmsg("creating gns");
    let mut gns_driver = impls::gns_log::GNSDriver::create_gns()?;
    gns_log::reinit_full::<true>(&mut gns_driver, &gns, |model_id, model| {
        // re-initialize model
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
        model_driver.commit_with_ctx(FullModel::new(model_data), BatchStats::new())?;
        model.driver().initialize_model_driver(model_driver);
        Ok(())
    })?;
    Ok(SELoaded {
        gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
    })
}

/*
    initialize
*/

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
        BoxStr::new(SystemDatabase::ROOT_ACCOUNT),
        password_hash.into_boxed_slice(),
    ));
    Ok(SELoaded {
        gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
    })
}

/*
    restore
*/

pub fn load(cfg: &Configuration) -> RuntimeResult<SELoaded> {
    let gns = GNSData::empty();
    context::set_dmsg("loading gns");
    let mut did_backup = false;
    let (mut gns_driver, gns_driver_stats) =
        impls::gns_log::GNSDriver::open_gns(&gns, JournalSettings::default())?;
    if gns_driver_stats.recommended_action().needs_compaction() {
        full_backup("before-startup-compaction", BackupContext::BeforeCompaction)?;
        did_backup = true;
        gns_driver = journal::compact_journal::<true, EventLogAdapter<GNSEventLog>>(
            GNS_PATH, gns_driver, &gns,
        )?;
    }
    let mut initialize_drivers = || {
        for (id, model) in gns.idx_models().write().iter_mut() {
            let model_data = model.data();
            let space_uuid = gns.idx().read().get(id.space()).unwrap().get_uuid();
            let model_data_file_path =
                paths_v1::model_path(id.space(), space_uuid, id.entity(), model_data.get_uuid());
            context::set_dmsg(format!("loading model driver in {model_data_file_path}"));
            let (mut model_driver, mdl_stats) = impls::mdl_journal::ModelDriver::open_model_driver(
                model_data,
                &model_data_file_path,
                JournalSettings::default(),
            )?;
            if mdl_stats.recommended_action().needs_compaction() {
                info!(
                    "{}.{} needs compaction due to {}",
                    id.space(),
                    id.entity(),
                    mdl_stats.recommended_action().reason_str()
                );
                if !did_backup {
                    full_backup(
                        &format!("before-compaction-of-model-{}.{}", id.entity(), id.space()),
                        BackupContext::BeforeCompaction,
                    )?;
                    did_backup = true;
                }
                model_driver = journal::compact_journal::<true, BatchAdapter<ModelDataAdapter>>(
                    &model_data_file_path,
                    model_driver,
                    model.data(),
                )?;
            }
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
        // all done, so now verify presence of data directory
        if !Path::new(DATA_DIR).is_dir() {
            context::set_dmsg("data directory missing");
            return Err(StorageError::RuntimeEngineLoadError.into());
        }
        RuntimeResult::Ok(())
    };
    match initialize_drivers() {
        Ok(()) => Ok(SELoaded {
            gns: GlobalNS::new(gns, FractalGNSDriver::new(gns_driver)),
        }),
        Err(e) => {
            error!("failed to load all storage drivers and/or data");
            info!("safely shutting down loaded drivers");
            for (id, model) in gns.idx_models().read().iter() {
                let mut batch_driver = model.driver().batch_driver().lock();
                let Some(mdl_driver) = batch_driver.as_mut() else {
                    continue;
                };
                if let Err(e) = ModelDriver::close_driver(mdl_driver) {
                    error!(
                        "failed to close model driver {}:{} due to error: {e}",
                        id.space(),
                        id.entity()
                    );
                }
            }
            if let Err(e) = GNSDriver::close_driver(&mut gns_driver) {
                error!("failed to close GNS driver due to error: {e}");
            }
            Err(e)
        }
    }
}

/*
    invoke repair
*/

pub fn repair() -> RuntimeResult<()> {
    full_backup("before-recovery-process", BackupContext::BeforeRepair)?;
    // check and attempt repair: GNS
    let gns = GNSData::empty();
    context::set_dmsg("repair GNS");
    print_repair_info(
        journal::repair_journal::<raw::journal::EventLogAdapter<impls::gns_log::GNSEventLog>>(
            GNS_PATH,
            &gns,
            JournalSettings::default(),
            JournalRepairMode::Simple,
        )?,
        "GNS",
    );
    // check and attempt repair: models
    let models = gns.idx_models().read();
    for (space_id, space) in gns.idx().read().iter() {
        for model_id in space.models().iter() {
            let model = models.get(&EntityIDRef::new(&space_id, &model_id)).unwrap();
            let model_data_file_path = paths_v1::model_path(
                &space_id,
                space.get_uuid(),
                &model_id,
                model.data().get_uuid(),
            );
            context::set_dmsg(format!("repairing {model_data_file_path}"));
            print_repair_info(
                journal::repair_journal::<
                    raw::journal::BatchAdapter<impls::mdl_journal::ModelDataAdapter>,
                >(
                    &model_data_file_path,
                    model.data(),
                    JournalSettings::default(),
                    JournalRepairMode::Simple,
                )?,
                &model_data_file_path,
            )
        }
    }
    Ok(())
}

fn full_backup(name: &str, context: BackupContext) -> RuntimeResult<()> {
    _full_backup(
        &format!("backups/{}-{name}", util::time_now_string()),
        true,
        context,
        None,
    )
}

fn _full_backup(
    backup_dir: &str,
    create_backup_dir: bool,
    context: BackupContext,
    description: Option<String>,
) -> RuntimeResult<()> {
    if create_backup_dir {
        context::set_dmsg("creating backup directory");
        FileSystem::create_dir_all(&backup_dir)?;
    }
    context::set_dmsg("creating backup manifest");
    BackupManifest::create(
        pathbuf!(&backup_dir, BACKUP_MANIFEST_FILE),
        context,
        description,
    )?;
    context::set_dmsg("backing up GNS");
    FileSystem::copy(GNS_PATH, pathbuf!(&backup_dir, GNS_PATH))?;
    context::set_dmsg("backing up data directory");
    FileSystem::copy_directory(DATA_DIR, pathbuf!(&backup_dir, DATA_DIR))?;
    info!("backup: All data backed up in {backup_dir}");
    Ok(())
}

fn print_repair_info(result: RepairResult, id: &str) {
    match result {
        RepairResult::NoErrors => info!("repair: no errors detected in {id}"),
        RepairResult::UnspecifiedLoss(definitely_lost) => {
            if definitely_lost == 0 {
                warn!("repair: LOST DATA. repaired {id} but lost an unspecified amount of data")
            } else {
                warn!("repair: LOST DATA. repaired {id} but lost atleast {definitely_lost} trailing bytes")
            }
        }
    }
}

pub fn compact() -> RuntimeResult<()> {
    full_backup("before-compaction", BackupContext::BeforeCompaction)?;
    let gns = GNSData::empty();
    context::set_dmsg("reading GNS");
    let stats = journal::read_journal::<GNSAdapter>(GNS_PATH, &gns, JournalSettings::default())?;
    if !stats.recommended_action().needs_compaction() {
        warn!("compact: GNS does not need compaction");
    }
    journal::compact_journal_direct::<true, GNSAdapter, _>(GNS_PATH, None, &gns, true, |_| Ok(()))?;
    for (id, model) in gns.idx_models().write().iter_mut() {
        let model_data = model.data();
        let space_uuid = gns.idx().read().get(id.space()).unwrap().get_uuid();
        let model_data_file_path =
            paths_v1::model_path(id.space(), space_uuid, id.entity(), model_data.get_uuid());
        context::set_dmsg(format!("loading model driver in {model_data_file_path}"));
        if !journal::read_journal::<ModelAdapter>(
            &model_data_file_path,
            model.data(),
            JournalSettings::default(),
        )?
        .recommended_action()
        .needs_compaction()
        {
            warn!(
                "compact: model {}.{} does not need compaction",
                id.space(),
                id.entity()
            );
        }
        journal::compact_journal_direct::<true, ModelAdapter, _>(
            &model_data_file_path,
            None,
            model.data(),
            true,
            |_| Ok(()),
        )?;
    }
    Ok(())
}

/*
    backup
*/

const BACKUP_MANIFEST_FILE: &str = "backup.manifest";

pub fn backup(settings: BackupSettings) -> RuntimeResult<()> {
    match settings.kind {
        BackupType::Direct => {}
    }
    // first lock directory
    let mut locks = FileLocks::new();
    if settings.allow_dirty {
        warn!("backup: potentially unsafe backup operation has been started (dirty read allowed)");
    } else {
        context::set_dmsg("locking data directory for backup");
        match settings.from.as_ref() {
            Some(from) => locks.lock(pathbuf!(from, crate::SKY_PID_FILE)),
            None => locks.lock(crate::SKY_PID_FILE),
        }?
    }
    // create backup directory
    context::set_dmsg("creating backup directory");
    FileSystem::create_dir_all(&settings.to)?;
    // lock backup directory
    context::set_dmsg("locking backup directory");
    locks.lock(pathbuf!(&settings.to, crate::SKY_PID_FILE))?;
    // initiate backup
    _full_backup(
        &settings.to,
        false,
        BackupContext::Manual,
        settings.description,
    )?;
    // release locks
    context::set_dmsg("releasing directory locks");
    locks.release()?;
    Ok(())
}

/*
    restore
*/

pub fn restore(settings: RestoreSettings) -> RuntimeResult<()> {
    context::set_dmsg("opening backup manifest");
    let (backup_manifest, backup_md) =
        BackupManifest::open(&format!("{}/{BACKUP_MANIFEST_FILE}", &settings.from))?;
    let this_host_name = &os::get_hostname();
    // verify if this backup should be restored
    if !settings.flag_skip_compatibility_check {
        // the compat check is not skipped which means that we can assume that the GNS is readable
        context::set_dmsg("loading metadata from current installation");
        let real_md = SdssFile::<FSpecSystemDatabaseV1>::open(
            if let Some(path) = settings.to.as_deref() {
                pathbuf!(path, GNS_PATH)
            } else {
                pathbuf!(GNS_PATH)
            }
            .to_str()
            .unwrap(),
            true,
            false,
        )?
        .into_meta();
        if real_md.driver_version() != backup_md.driver_version()
            || real_md.server_version() != backup_md.server_version()
            || real_md.header_version() != backup_md.header_version()
        {
            if settings.flag_allow_incompatible {
                warn!("restore: incompatible backup detected, but incompatible restore is enabled")
            } else {
                context::set_dmsg(
                    "incompatible backup detected, but incompatible restore is enabled",
                );
                return Err(StorageError::RuntimeRestoreValidationFailure.into());
            }
        }
    }
    if backup_manifest.hostname() != this_host_name.as_str() {
        if settings.flag_allow_different_host {
            warn!(
                "restore: this backup is from a different host ({})",
                backup_manifest.hostname()
            )
        } else {
            context::set_dmsg(format!(
                "expected backup to be from host {} but backup is from {}",
                this_host_name.as_str(),
                backup_manifest.hostname()
            ));
            return Err(StorageError::RuntimeRestoreValidationFailure.into());
        }
    }
    if backup_manifest.date() >= chrono::Utc::now().naive_utc() {
        if settings.flag_allow_invalid_date {
            warn!(
                "restore: the date of this backup is in the future ({})",
                backup_manifest.date()
            )
        } else {
            context::set_dmsg(format!(
                "the date of this backup is in the future ({})",
                backup_manifest.date()
            ));
            return Err(StorageError::RuntimeRestoreValidationFailure.into());
        }
    }
    // output backup information
    let mut backup_info_fmt = format!(
        "restore: this backup was created {}",
        backup_manifest.context().context_str()
    );
    if let Some(description) = backup_manifest.description() {
        backup_info_fmt.push_str(" with description ");
        backup_info_fmt.push_str(&format!("{description:?}"));
    }
    backup_info_fmt.push_str(&format!(" on {} (UTC)", backup_manifest.date()));
    info!("{backup_info_fmt}");
    // now restore the files
    // restore gns
    context::set_dmsg("restoring GNS");
    FileSystem::copy(
        pathbuf!(&settings.from, GNS_PATH).to_str().unwrap(),
        if let Some(to) = settings.to.as_deref() {
            pathbuf!(to, GNS_PATH)
        } else {
            pathbuf!(GNS_PATH)
        }
        .to_str()
        .unwrap(),
    )?;
    // restore data dir
    let data_dir_path = if let Some(to) = settings.to.as_deref() {
        pathbuf!(to, DATA_DIR)
    } else {
        pathbuf!(DATA_DIR)
    };
    context::set_dmsg("creating data directory"); // if data dir is absent (can be if some data loss happened), create it
    FileSystem::create_dir(&data_dir_path)?;
    context::set_dmsg("restoring data directory");
    FileSystem::copy_directory(
        pathbuf!(&settings.from, DATA_DIR).to_str().unwrap(),
        data_dir_path.to_str().unwrap(),
    )?;
    if settings.flag_delete_on_restore_completion {
        context::set_dmsg("removing backup directory that was recently restored");
        FileSystem::remove_dir_all(&settings.from)?;
        info!("restore: successfully removed the backup directory that was used for the restore process");
    }
    info!("restore: completed successfully");
    Ok(())
}
