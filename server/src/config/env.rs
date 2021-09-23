/*
 * Created on Thu Sep 23 2021
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

use std::collections::HashMap;
use std::env as std_env;
use std_env::VarError;

type ConfigMap = HashMap<&'static str, Option<String>>;

#[derive(Debug)]
pub enum ConfigStatus {
    ParseFailure,
    None,
    Config(ConfigMap),
}

macro_rules! to_const {
    ($($(#[$attr:meta])* $v:ident),* $(,)?) => {
        $(
            $(#[$attr])*
            const $v: &str = stringify!($v);
        )*
        pub fn has_env_config() -> ConfigStatus {
            let mut hm = ConfigMap::new();
            let mut has_env = false;
            $(
                match std_env::var($v) {
                    Ok(var) => {
                        hm.insert($v, Some(var));
                        has_env = true;
                    },
                    Err(e) => {
                        match e {
                            VarError::NotPresent => {},
                            VarError::NotUnicode {..} => {
                                return ConfigStatus::ParseFailure;
                            }
                        }
                    }
                }
            )*
            if has_env {
                ConfigStatus::Config(hm)
            } else {
                ConfigStatus::None
            }
        }
    };
}

to_const! {
    // system config
    /// host addr
    SKY_SYSTEM_HOST,
    /// port
    SKY_SYSTEM_PORT,
    /// noart configuration for secure environments
    SKY_SYSTEM_NOART,
    /// the maximum number of connections
    SKY_SYSTEM_MAXCON,
    // bgsave
    /// enabled/disabled flag for bgsave
    SKY_BGSAVE_ENABLED,
    /// bgsave interval
    SKY_BGSAVE_EVERY,
    // snapshot
    /// snapshot interval
    SKY_SNAPSHOT_EVERY,
    /// maximum number of snapshots
    SKY_SNAPSHOT_ATMOST,
    /// flag to disable writes if snapshot fails
    SKY_SNAPSHOT_FAILSAFE,
    // TLS
    /// the tls private key
    SKY_TLS_KEY,
    /// the tls cert
    SKY_TLS_CERT,
    /// the tls port
    SKY_TLS_PORT,
    /// the tls-only flag
    SKY_TLS_ONLY,
    /// the tls password stream
    SKY_TLS_PASSIN
}
