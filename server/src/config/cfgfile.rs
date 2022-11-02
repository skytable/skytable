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

use {
    super::{
        AuthSettings, ConfigSourceParseResult, Configset, Modeset, OptString, ProtocolVersion,
        TryFromConfigSource,
    },
    serde::Deserialize,
    std::net::IpAddr,
};

/// This struct is an _object representation_ used for parsing the TOML file
#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct Config {
    /// The `server` key
    pub(super) server: ConfigKeyServer,
    /// The `bgsave` key
    pub(super) bgsave: Option<ConfigKeyBGSAVE>,
    /// The snapshot key
    pub(super) snapshot: Option<ConfigKeySnapshot>,
    /// SSL configuration
    pub(super) ssl: Option<KeySslOpts>,
    /// auth settings
    pub(super) auth: Option<AuthSettings>,
}

/// This struct represents the `server` key in the TOML file
#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct ConfigKeyServer {
    /// The host key is any valid IPv4/IPv6 address
    pub(super) host: IpAddr,
    /// The port key is any valid port
    pub(super) port: u16,
    /// The noart key is an `Option`al boolean value which is set to true
    /// for secure environments to disable terminal artwork
    pub(super) noart: Option<bool>,
    /// The maximum number of clients
    pub(super) maxclient: Option<usize>,
    /// The deployment mode
    pub(super) mode: Option<Modeset>,
    pub(super) protocol: Option<ProtocolVersion>,
}

/// The BGSAVE section in the config file
#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct ConfigKeyBGSAVE {
    /// Whether BGSAVE is enabled or not
    ///
    /// If this key is missing, then we can assume that BGSAVE is enabled
    pub(super) enabled: Option<bool>,
    /// The duration after which BGSAVE should start
    ///
    /// If this is the only key specified, then it is clear that BGSAVE is enabled
    /// and the duration is `every`
    pub(super) every: Option<u64>,
}

/// The snapshot section in the TOML file
#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct ConfigKeySnapshot {
    /// After how many seconds should the snapshot be created
    pub(super) every: u64,
    /// The maximum number of snapshots to keep
    ///
    /// If atmost is set to `0`, then all the snapshots will be kept
    pub(super) atmost: usize,
    /// Prevent writes to the database if snapshotting fails
    pub(super) failsafe: Option<bool>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct KeySslOpts {
    pub(super) key: String,
    pub(super) chain: String,
    pub(super) port: u16,
    pub(super) only: Option<bool>,
    pub(super) passin: Option<String>,
}

/// A custom non-null type for config files
pub struct NonNull<T> {
    val: T,
}

impl<T> From<T> for NonNull<T> {
    fn from(val: T) -> Self {
        Self { val }
    }
}

impl<T> TryFromConfigSource<T> for NonNull<T> {
    fn is_present(&self) -> bool {
        true
    }
    fn mutate_failed(self, target: &mut T, trip: &mut bool) -> bool {
        *target = self.val;
        *trip = true;
        false
    }
    fn try_parse(self) -> ConfigSourceParseResult<T> {
        ConfigSourceParseResult::Okay(self.val)
    }
}

pub struct Optional<T> {
    base: Option<T>,
}

impl<T> Optional<T> {
    pub const fn some(val: T) -> Self {
        Self { base: Some(val) }
    }
}

impl<T> From<Option<T>> for Optional<T> {
    fn from(base: Option<T>) -> Self {
        Self { base }
    }
}

impl<T> TryFromConfigSource<T> for Optional<T> {
    fn is_present(&self) -> bool {
        self.base.is_some()
    }
    fn mutate_failed(self, target: &mut T, trip: &mut bool) -> bool {
        if let Some(v) = self.base {
            *trip = true;
            *target = v;
        }
        false
    }
    fn try_parse(self) -> ConfigSourceParseResult<T> {
        match self.base {
            Some(v) => ConfigSourceParseResult::Okay(v),
            None => ConfigSourceParseResult::Absent,
        }
    }
}

type ConfigFile = Config;

pub fn from_file(file: ConfigFile) -> Configset {
    let mut set = Configset::new_file();
    let ConfigFile {
        server,
        bgsave,
        snapshot,
        ssl,
        auth,
    } = file;
    // server settings
    set.server_tcp(
        Optional::some(server.host),
        "server.host",
        Optional::some(server.port),
        "server.port",
    );
    set.protocol_settings(server.protocol, "server.protocol");
    set.server_maxcon(Optional::from(server.maxclient), "server.maxcon");
    set.server_noart(Optional::from(server.noart), "server.noart");
    set.server_mode(Optional::from(server.mode), "server.mode");
    // bgsave settings
    if let Some(bgsave) = bgsave {
        let ConfigKeyBGSAVE { enabled, every } = bgsave;
        set.bgsave_settings(
            Optional::from(enabled),
            "bgsave.enabled",
            Optional::from(every),
            "bgsave.every",
        );
    }
    // snapshot settings
    if let Some(snapshot) = snapshot {
        let ConfigKeySnapshot {
            every,
            atmost,
            failsafe,
        } = snapshot;
        set.snapshot_settings(
            NonNull::from(every),
            "snapshot.every",
            NonNull::from(atmost),
            "snapshot.atmost",
            Optional::from(failsafe),
            "snapshot.failsafe",
        );
    }
    // TLS settings
    if let Some(tls) = ssl {
        let KeySslOpts {
            key,
            chain,
            port,
            only,
            passin,
        } = tls;
        set.tls_settings(
            NonNull::from(key),
            "ssl.key",
            NonNull::from(chain),
            "ssl.chain",
            NonNull::from(port),
            "ssl.port",
            Optional::from(only),
            "ssl.only",
            OptString::from(passin),
            "ssl.passin",
        );
    }
    if let Some(auth) = auth {
        let AuthSettings { origin_key } = auth;
        set.auth_settings(Optional::from(origin_key), "auth.origin")
    }
    set
}
