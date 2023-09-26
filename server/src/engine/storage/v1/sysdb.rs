/*
 * Created on Fri Sep 22 2023
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

use crate::engine::{
    data::{cell::Datacell, DictEntryGeneric, DictGeneric},
    fractal::SysConfig,
    storage::v1::{spec, RawFSInterface, SDSSError, SDSSFileIO, SDSSResult},
};

const SYSDB_PATH: &str = "sys.db";
const SYSDB_COW_PATH: &str = "sys.db.cow";

pub fn sync_system_database<Fs: RawFSInterface>(cfg: &SysConfig) -> SDSSResult<()> {
    // get auth data
    let auth_data = cfg.auth_data().read();
    // prepare our flat file
    let mut map: DictGeneric = into_dict!(
        "host" => DictEntryGeneric::Map(into_dict!(
            "settings_version" => Datacell::new_uint(cfg.host_data().settings_version() as _),
            "startup_counter" => Datacell::new_uint(cfg.host_data().startup_counter() as _),
        )),
        "auth" => DictGeneric::new(),
    );
    let auth_key = map.get_mut("auth").unwrap();
    match &*auth_data {
        None => *auth_key = Datacell::null().into(),
        Some(auth) => {
            let auth_key = auth_key.as_dict_mut().unwrap();
            auth_key.insert(
                "root".into(),
                DictEntryGeneric::Data(Datacell::new_bin(auth.root_key().into())),
            );
            auth_key.insert(
                "users".into(),
                DictEntryGeneric::Map(
                    auth.users()
                        .iter()
                        .map(|(username, user)| {
                            (
                                username.to_owned(),
                                DictEntryGeneric::Data(Datacell::new_list(vec![
                                    Datacell::new_bin(user.key().into()),
                                ])),
                            )
                        })
                        .collect(),
                ),
            );
        }
    }
    // open file
    let mut file = SDSSFileIO::<Fs>::open_or_create_perm_rw::<spec::SysDBV1>(SYSDB_COW_PATH)?
        .into_created()
        .ok_or(SDSSError::OtherError(
            "sys.db.cow already exists. please remove this file.",
        ))?;
    // write
    let buf = super::inf::enc::enc_dict_full::<super::inf::map::GenericDictSpec>(&map);
    file.fsynced_write(&buf)?;
    // replace
    Fs::fs_rename_file(SYSDB_COW_PATH, SYSDB_PATH)
}
