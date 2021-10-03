/*
 * Created on Sat Oct 02 2021
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

use super::{
    BGSave, ConfigurationSet, PortConfig, SnapshotConfig, SnapshotPref, SslOpts, DEFAULT_IPV4,
    DEFAULT_PORT, DEFAULT_SSL_PORT,
};
use std::env::{self, VarError};
use std::net::IpAddr;

pub(super) enum EnvError {
    CfgError(&'static str),
    ParseError(String),
}

pub(super) fn get_env_config() -> Result<Option<ConfigurationSet>, EnvError> {
    let mut defset = ConfigurationSet::default();
    let mut is_set = false;
    macro_rules! getenv {
        ($var:ident) => {{
            let var = stringify!($var);
            match env::var(var) {
                Ok(v) => {
                    // set flag to true
                    is_set = true;
                    Some(v)
                }
                Err(e) => match e {
                    VarError::NotPresent => None,
                    VarError::NotUnicode(..) => {
                        return Err(EnvError::ParseError(format!(
                            "Bad value for {var}. The value is not unicode",
                            var = var
                        )));
                    }
                },
            }
        }};
        ($var:ident, $ty:ty) => {{
            match getenv!($var).map(|v| v.parse::<$ty>()) {
                Some(Ok(v)) => Some(v),
                Some(Err(e)) => {
                    return Err(EnvError::ParseError(format!(
                        "Bad value for {var}. {e}",
                        var = stringify!($var),
                        e = e
                    )))
                }
                None => None,
            }
        }};
    }
    // get system settings
    let noart = getenv!(SKY_SYSTEM_NOART, bool);
    let maxcon = getenv!(SKY_SYSTEM_MAXCON, usize);
    set_if_exists!(noart, defset.noart);
    set_if_exists!(maxcon, defset.maxcon);

    // now get port config
    let port = getenv!(SKY_SYSTEM_PORT, u16).unwrap_or(DEFAULT_PORT);
    let host = getenv!(SKY_SYSTEM_HOST, IpAddr).unwrap_or(DEFAULT_IPV4);
    let tlsport = getenv!(SKY_TLS_PORT, u16);
    let tlsonly = getenv!(SKY_TLS_ONLY, bool).unwrap_or_default();
    let tlscert = getenv!(SKY_TLS_CERT, String);
    let tlskey = getenv!(SKY_TLS_KEY, String);
    let tls_passin = getenv!(SKY_TLS_PASSIN, String);
    let portcfg = match (tlscert, tlskey) {
        (Some(cert), Some(key)) => {
            let sslopts = SslOpts::new(key, cert, tlsport.unwrap_or(DEFAULT_SSL_PORT), tls_passin);
            if tlsonly {
                PortConfig::new_secure_only(host, sslopts)
            } else {
                PortConfig::new_multi(host, port, sslopts)
            }
        }
        (None, None) => {
            // no TLS
            if tlsonly {
                log::warn!("Ignoring value for SKY_TLS_ONLY because TLS was disabled");
            }
            if tlsport.is_some() {
                log::warn!("Ignoring value for SKY_TLS_PORT because TLS was disabled");
            }
            PortConfig::new_insecure_only(host, port)
        }
        _ => {
            return Err(EnvError::CfgError(
                "To use TLS, pass values for both SKY_TLS_CERT and SKY_TLS_KEY",
            ))
        }
    };
    defset.ports = portcfg;

    // now get bgsave
    let bgsave_enabled = getenv!(SKY_BGSAVE_ENABLED, bool).unwrap_or(true);
    let bgsave_duration = getenv!(SKY_BGSAVE_DURATION, u64).unwrap_or(120);
    let bgsave = BGSave::new(bgsave_enabled, bgsave_duration);
    defset.bgsave = bgsave;
    // now get snapshot config
    let snapshot_enabled = getenv!(SKY_SNAPSHOT_ENABLED, bool).unwrap_or_default();
    let snapshot_duration = getenv!(SKY_SNAPSHOT_DURATION, u64);
    let snapshot_keep = getenv!(SKY_SNAPSHOT_KEEP, usize);
    let snapshot_failsafe = getenv!(SKY_SNAPSHOT_FAILSAFE, bool).unwrap_or(true);
    let snapcfg = {
        if snapshot_enabled {
            match (snapshot_duration, snapshot_keep) {
                (Some(duration), Some(keep)) => {
                    SnapshotConfig::Enabled(SnapshotPref::new(duration, keep, snapshot_failsafe))
                },
                _ => return Err(EnvError::CfgError("To use snapshots, you must pass values for both SKY_SNAPSHOT_DURATION and SKY_SNAPSHOT_KEEP")),
            }
        } else {
            SnapshotConfig::default()
        }
    };
    defset.snapshot = snapcfg;
    if is_set {
        Ok(Some(defset))
    } else {
        Ok(None)
    }
}
