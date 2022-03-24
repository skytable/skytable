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

use super::{feedback::WarningStack, DEFAULT_IPV4, DEFAULT_PORT};
use crate::config::AuthkeyWrapper;
use crate::dbnet::MAXIMUM_CONNECTION_LIMIT;
use core::fmt;
use core::str::FromStr;
use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize,
};
use std::net::IpAddr;

/// The BGSAVE configuration
///
/// If BGSAVE is enabled, then the duration (corresponding to `every`) is wrapped in the `Enabled`
/// variant. Otherwise, the `Disabled` variant is to be used
#[derive(PartialEq, Debug)]
pub enum BGSave {
    Enabled(u64),
    Disabled,
}

impl BGSave {
    /// Create a new BGSAVE configuration with all the fields
    pub const fn new(enabled: bool, every: u64) -> Self {
        if enabled {
            BGSave::Enabled(every)
        } else {
            BGSave::Disabled
        }
    }
    /// The default BGSAVE configuration
    ///
    /// Defaults:
    /// - `enabled`: true
    /// - `every`: 120
    pub const fn default() -> Self {
        BGSave::new(true, 120)
    }
    /// Check if BGSAVE is disabled
    pub const fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }
}

/// A `ConfigurationSet` which can be used by main::check_args_or_connect() to bind
/// to a `TcpListener` and show the corresponding terminal output for the given
/// configuration
#[derive(Debug, PartialEq)]
pub struct ConfigurationSet {
    /// If `noart` is set to true, no terminal artwork should be displayed
    pub noart: bool,
    /// The BGSAVE configuration
    pub bgsave: BGSave,
    /// The snapshot configuration
    pub snapshot: SnapshotConfig,
    /// Port configuration
    pub ports: PortConfig,
    /// The maximum number of connections
    pub maxcon: usize,
    /// The deployment mode
    pub mode: Modeset,
    /// The auth settings
    pub auth: AuthSettings,
}

impl ConfigurationSet {
    pub const fn new(
        noart: bool,
        bgsave: BGSave,
        snapshot: SnapshotConfig,
        ports: PortConfig,
        maxcon: usize,
        mode: Modeset,
        auth: AuthSettings,
    ) -> Self {
        Self {
            noart,
            bgsave,
            snapshot,
            ports,
            maxcon,
            mode,
            auth,
        }
    }
    /// Create a default `ConfigurationSet` with the following setup defaults:
    /// - `host`: 127.0.0.1
    /// - `port` : 2003
    /// - `noart` : false
    /// - `bgsave_enabled` : true
    /// - `bgsave_duration` : 120
    /// - `ssl` : disabled
    pub const fn default() -> Self {
        Self::new(
            false,
            BGSave::default(),
            SnapshotConfig::default(),
            PortConfig::new_insecure_only(DEFAULT_IPV4, 2003),
            MAXIMUM_CONNECTION_LIMIT,
            Modeset::Dev,
            AuthSettings::default(),
        )
    }
    /// Returns `false` if `noart` is enabled. Otherwise it returns `true`
    pub const fn is_artful(&self) -> bool {
        !self.noart
    }
}

/// Port configuration
///
/// This enumeration determines whether the ports are:
/// - `Multi`: This means that the database server will be listening to both
/// SSL **and** non-SSL requests
/// - `SecureOnly` : This means that the database server will only accept SSL requests
/// and will not even activate the non-SSL socket
/// - `InsecureOnly` : This indicates that the server would only accept non-SSL connections
/// and will not even activate the SSL socket
#[derive(Debug, PartialEq)]
pub enum PortConfig {
    SecureOnly {
        host: IpAddr,
        ssl: SslOpts,
    },
    Multi {
        host: IpAddr,
        port: u16,
        ssl: SslOpts,
    },
    InsecureOnly {
        host: IpAddr,
        port: u16,
    },
}

impl Default for PortConfig {
    fn default() -> PortConfig {
        PortConfig::InsecureOnly {
            host: DEFAULT_IPV4,
            port: DEFAULT_PORT,
        }
    }
}

impl PortConfig {
    pub const fn new_secure_only(host: IpAddr, ssl: SslOpts) -> Self {
        PortConfig::SecureOnly { host, ssl }
    }
    pub const fn new_insecure_only(host: IpAddr, port: u16) -> Self {
        PortConfig::InsecureOnly { host, port }
    }
    pub fn get_host(&self) -> IpAddr {
        match self {
            Self::InsecureOnly { host, .. }
            | Self::SecureOnly { host, .. }
            | Self::Multi { host, .. } => *host,
        }
    }
    pub fn upgrade_to_tls(&mut self, ssl: SslOpts) {
        match self {
            Self::InsecureOnly { host, port } => {
                *self = Self::Multi {
                    host: *host,
                    port: *port,
                    ssl,
                }
            }
            Self::SecureOnly { .. } | Self::Multi { .. } => {
                panic!("Port config is already upgraded to TLS")
            }
        }
    }
    pub const fn insecure_only(&self) -> bool {
        matches!(self, Self::InsecureOnly { .. })
    }
    pub const fn secure_only(&self) -> bool {
        matches!(self, Self::SecureOnly { .. })
    }
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct SslOpts {
    pub key: String,
    pub chain: String,
    pub port: u16,
    pub passfile: Option<String>,
}

impl SslOpts {
    pub const fn new(key: String, chain: String, port: u16, passfile: Option<String>) -> Self {
        SslOpts {
            key,
            chain,
            port,
            passfile,
        }
    }
}

#[derive(Debug, PartialEq)]
/// The snapshot configuration
///
pub struct SnapshotPref {
    /// Capture a snapshot `every` seconds
    pub every: u64,
    /// The maximum numeber of snapshots to be kept
    pub atmost: usize,
    /// Lock writes if snapshotting fails
    pub poison: bool,
}

impl SnapshotPref {
    /// Create a new a new `SnapshotPref` instance
    pub const fn new(every: u64, atmost: usize, poison: bool) -> Self {
        SnapshotPref {
            every,
            atmost,
            poison,
        }
    }
    /// Returns `every,almost` as a tuple for pattern matching
    pub const fn decompose(self) -> (u64, usize, bool) {
        (self.every, self.atmost, self.poison)
    }
}

#[derive(Debug, PartialEq)]
/// Snapshotting configuration
///
/// The variant `Enabled` directly carries a `ConfigKeySnapshot` object that
/// is parsed from the configuration file, The variant `Disabled` is a ZST, and doesn't
/// hold any data
pub enum SnapshotConfig {
    /// Snapshotting is enabled: this variant wraps around a `SnapshotPref`
    /// object
    Enabled(SnapshotPref),
    /// Snapshotting is disabled
    Disabled,
}

impl SnapshotConfig {
    /// Snapshots are disabled by default, so `SnapshotConfig::Disabled` is the
    /// default configuration
    pub const fn default() -> Self {
        SnapshotConfig::Disabled
    }
}

type RestoreFile = Option<String>;

#[derive(Debug, PartialEq)]
/// The type of configuration:
/// - The default configuration
/// - A custom supplied configuration
pub struct ConfigType {
    pub(super) config: ConfigurationSet,
    restore: RestoreFile,
    is_custom: bool,
    warnings: Option<WarningStack>,
}

impl ConfigType {
    fn _new(
        config: ConfigurationSet,
        restore: RestoreFile,
        is_custom: bool,
        warnings: Option<WarningStack>,
    ) -> Self {
        Self {
            config,
            restore,
            is_custom,
            warnings,
        }
    }
    pub fn print_warnings(&self) {
        if let Some(warnings) = self.warnings.as_ref() {
            warnings.print_warnings()
        }
    }
    pub fn finish(self) -> (ConfigurationSet, Option<String>) {
        (self.config, self.restore)
    }
    pub fn is_custom(&self) -> bool {
        self.is_custom
    }
    pub fn is_artful(&self) -> bool {
        self.config.is_artful()
    }
    pub fn new_custom(
        config: ConfigurationSet,
        restore: RestoreFile,
        warnings: WarningStack,
    ) -> Self {
        Self::_new(config, restore, true, Some(warnings))
    }
    pub fn new_default(restore: RestoreFile) -> Self {
        Self::_new(ConfigurationSet::default(), restore, false, None)
    }
    /// Check if the current deploy mode is prod
    pub const fn is_prod_mode(&self) -> bool {
        matches!(self.config.mode, Modeset::Prod)
    }
    pub fn wpush(&mut self, w: impl ToString) {
        match self.warnings.as_mut() {
            Some(stack) => stack.push(w),
            None => {
                self.warnings = {
                    let mut wstack = WarningStack::new("");
                    wstack.push(w);
                    Some(wstack)
                };
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Modeset {
    Dev,
    Prod,
}

impl FromStr for Modeset {
    type Err = ();
    fn from_str(st: &str) -> Result<Modeset, Self::Err> {
        match st {
            "dev" => Ok(Modeset::Dev),
            "prod" => Ok(Modeset::Prod),
            _ => Err(()),
        }
    }
}

struct ModesetVisitor;

impl<'de> Visitor<'de> for ModesetVisitor {
    type Value = Modeset;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Expecting a string with the deployment mode")
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match value {
            "dev" => Ok(Modeset::Dev),
            "prod" => Ok(Modeset::Prod),
            _ => return Err(E::custom(format!("Bad value `{value}` for modeset"))),
        }
    }
}

impl<'de> Deserialize<'de> for Modeset {
    fn deserialize<D>(deserializer: D) -> Result<Modeset, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ModesetVisitor)
    }
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct AuthSettings {
    pub origin_key: Option<AuthkeyWrapper>,
}

impl AuthSettings {
    pub const fn default() -> Self {
        Self { origin_key: None }
    }
    #[cfg(test)]
    pub fn new(origin: AuthkeyWrapper) -> Self {
        Self {
            origin_key: Some(origin),
        }
    }
}

struct AuthSettingsVisitor;

impl<'de> Visitor<'de> for AuthSettingsVisitor {
    type Value = AuthkeyWrapper;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a 40 character ASCII string")
    }
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        AuthkeyWrapper::try_new(value).ok_or_else(|| {
            E::custom(
                "Invalid value for authkey. must be 40 ASCII characters with nonzero first char",
            )
        })
    }
}

impl<'de> Deserialize<'de> for AuthkeyWrapper {
    fn deserialize<D>(deserializer: D) -> Result<AuthkeyWrapper, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(AuthSettingsVisitor)
    }
}
