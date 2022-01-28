/*
 * Created on Fri Jan 28 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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

use super::cfg2::{ConfigSourceParseResult, Configset, OptString, TryFromConfigSource};
use super::cfgfile::{Config as ConfigFile, ConfigKeyBGSAVE, ConfigKeySnapshot, KeySslOpts};

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
    pub const fn none() -> Self {
        Self { base: None }
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

pub fn from_file(file: ConfigFile) -> Configset {
    let mut set = Configset::new_file();
    let ConfigFile {
        server,
        bgsave,
        snapshot,
        ssl,
    } = file;
    // server settings
    set.server_tcp(
        Optional::some(server.host),
        "server.host",
        Optional::some(server.port),
        "server.port",
    );
    set.server_maxcon(Optional::from(server.maxclient), "server.maxcon");
    set.server_noart(Optional::from(server.noart), "server.noart");
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
    set
}
