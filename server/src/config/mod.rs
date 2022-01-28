/*
 * Created on Thu Jan 27 2022
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

// external imports
use clap::{load_yaml, App};
// std imports
use core::str::FromStr;
use std::env::VarError;
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
// internal modules
mod cfgcli;
mod cfgenv;
mod cfgfile;
mod definitions;
mod feedback;
#[cfg(test)]
mod tests;
// internal imports
use self::cfgfile::Config as ConfigFile;
pub use self::definitions::*;
use self::feedback::{ConfigError, ErrorStack, WarningStack};
use crate::dbnet::MAXIMUM_CONNECTION_LIMIT;

// server defaults
const DEFAULT_IPV4: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const DEFAULT_PORT: u16 = 2003;
// bgsave defaults
const DEFAULT_BGSAVE_DURATION: u64 = 120;
// snapshot defaults
const DEFAULT_SNAPSHOT_FAILSAFE: bool = true;
// TLS defaults
const DEFAULT_SSL_PORT: u16 = 2004;

type StaticStr = &'static str;

#[derive(Debug)]
/// An enum representing the outcome of a parse operation for a specific configuration item from a
/// specific configuration source
pub enum ConfigSourceParseResult<T> {
    Okay(T),
    Absent,
    ParseFailure,
}

/// A trait for configuration sources. Any type implementing this trait is considered to be a valid
/// source for configuration
pub trait TryFromConfigSource<T: Sized>: Sized {
    /// Check if the value is present
    fn is_present(&self) -> bool;
    /// Attempt to mutate the value if present. If any error occurs
    /// while parsing the value, return true. Else return false if all went well.
    /// Here:
    /// - `target_value`: is a mutable reference to the target var
    /// - `trip`: is a mutable ref to a bool that will be set to true if a value is present
    /// (whether parseable or not)
    fn mutate_failed(self, target_value: &mut T, trip: &mut bool) -> bool;
    /// Attempt to parse the value into the target type
    fn try_parse(self) -> ConfigSourceParseResult<T>;
}

impl<'a, T: FromStr + 'a> TryFromConfigSource<T> for Option<&'a str> {
    fn is_present(&self) -> bool {
        self.is_some()
    }
    fn mutate_failed(self, target_value: &mut T, trip: &mut bool) -> bool {
        self.map(|slf| {
            *trip = true;
            match slf.parse() {
                Ok(p) => {
                    *target_value = p;
                    false
                }
                Err(_) => true,
            }
        })
        .unwrap_or(false)
    }
    fn try_parse(self) -> ConfigSourceParseResult<T> {
        self.map(|s| {
            s.parse()
                .map(|ret| ConfigSourceParseResult::Okay(ret))
                .unwrap_or(ConfigSourceParseResult::ParseFailure)
        })
        .unwrap_or(ConfigSourceParseResult::Absent)
    }
}

impl<'a, T: FromStr + 'a> TryFromConfigSource<T> for Result<String, VarError> {
    fn is_present(&self) -> bool {
        !matches!(self, Err(VarError::NotPresent))
    }
    fn mutate_failed(self, target_value: &mut T, trip: &mut bool) -> bool {
        match self {
            Ok(s) => s
                .parse()
                .map(|v| {
                    *trip = true;
                    *target_value = v;
                    false
                })
                .unwrap_or(true),
            Err(e) => {
                if matches!(e, VarError::NotPresent) {
                    false
                } else {
                    // yes, we got the var but failed to parse it into unicode; so trip
                    *trip = true;
                    true
                }
            }
        }
    }
    fn try_parse(self) -> ConfigSourceParseResult<T> {
        match self {
            Ok(s) => s
                .parse()
                .map(|v| ConfigSourceParseResult::Okay(v))
                .unwrap_or(ConfigSourceParseResult::ParseFailure),
            Err(e) => match e {
                VarError::NotPresent => ConfigSourceParseResult::Absent,
                VarError::NotUnicode(_) => ConfigSourceParseResult::ParseFailure,
            },
        }
    }
}

#[derive(Debug)]
/// Since we have conflicting trait implementations, we define a custom `Option<String>` type
pub struct OptString {
    base: Option<String>,
}

impl OptString {
    pub const fn new_null() -> Self {
        Self { base: None }
    }
}

impl From<Option<String>> for OptString {
    fn from(base: Option<String>) -> Self {
        Self { base }
    }
}

impl FromStr for OptString {
    type Err = ();
    fn from_str(st: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            base: Some(st.to_string()),
        })
    }
}

impl TryFromConfigSource<OptString> for OptString {
    fn is_present(&self) -> bool {
        self.base.is_some()
    }
    fn mutate_failed(self, target: &mut OptString, trip: &mut bool) -> bool {
        if let Some(v) = self.base {
            target.base = Some(v);
            *trip = true;
        }
        false
    }
    fn try_parse(self) -> ConfigSourceParseResult<OptString> {
        self.base
            .map(|v| ConfigSourceParseResult::Okay(OptString { base: Some(v) }))
            .unwrap_or(ConfigSourceParseResult::Okay(OptString::new_null()))
    }
}

#[derive(Debug)]
/// A high-level configuration set that automatically handles errors, warnings and provides a convenient [`Result`]
/// type that can be used
pub struct Configset {
    did_mutate: bool,
    cfg: ConfigurationSet,
    estack: ErrorStack,
    wstack: WarningStack,
}

impl Configset {
    const EMSG_ENV: StaticStr = "Environment";
    const EMSG_CLI: StaticStr = "CLI arguments";
    const EMSG_FILE: StaticStr = "Configuration file";

    /// Internal ctor for a given feedback source. We do not want to expose this to avoid
    /// erroneous feedback source names
    fn _new(feedback_source: StaticStr) -> Self {
        Self {
            did_mutate: false,
            cfg: ConfigurationSet::default(),
            estack: ErrorStack::new(feedback_source),
            wstack: WarningStack::new(feedback_source),
        }
    }
    /// Create a new configset for environment variables
    pub fn new_env() -> Self {
        Self::_new(Self::EMSG_ENV)
    }
    /// Create a new configset for CLI args
    pub fn new_cli() -> Self {
        Self::_new(Self::EMSG_CLI)
    }
    /// Create a new configset for config files
    pub fn new_file() -> Self {
        Self {
            did_mutate: true,
            cfg: ConfigurationSet::default(),
            estack: ErrorStack::new(Self::EMSG_FILE),
            wstack: WarningStack::new(Self::EMSG_FILE),
        }
    }
    /// Mark the configset mutated
    fn mutated(&mut self) {
        self.did_mutate = true;
    }
    /// Push an error onto the error stack
    fn epush(&mut self, field_key: StaticStr, expected: StaticStr) {
        self.estack.push(format!(
            "Bad value for `${field_key}`. Expected ${expected}",
        ))
    }
    /// Check if no errors have occurred
    pub fn is_okay(&self) -> bool {
        self.estack.is_empty()
    }
    /// Check if the configset was mutated
    pub fn is_mutated(&self) -> bool {
        self.did_mutate
    }
    /// Attempt to mutate with a target `TryFromConfigSource` type, and push in any error that occurs
    /// using the given diagnostic info
    fn try_mutate<T>(
        &mut self,
        new: impl TryFromConfigSource<T>,
        target: &mut T,
        field_key: StaticStr,
        expected: StaticStr,
    ) {
        if new.mutate_failed(target, &mut self.did_mutate) {
            self.epush(field_key, expected)
        }
    }
    /// Attempt to mutate with a target `TryFromConfigSource` type, and push in any error that occurs
    /// using the given diagnostic info while checking the correctly parsed type using the provided validation
    /// closure for any additional validation check that goes beyond type correctness
    fn try_mutate_with_condcheck<T, F>(
        &mut self,
        new: impl TryFromConfigSource<T>,
        target: &mut T,
        field_key: StaticStr,
        expected: StaticStr,
        validation_fn: F,
    ) where
        F: Fn(&T) -> bool,
    {
        let mut needs_error = false;
        match new.try_parse() {
            ConfigSourceParseResult::Okay(ok) => {
                self.mutated();
                needs_error = !validation_fn(&ok);
                *target = ok;
            }
            ConfigSourceParseResult::ParseFailure => needs_error = true,
            ConfigSourceParseResult::Absent => {}
        }
        if needs_error {
            self.epush(field_key, expected)
        }
    }
    /// This method can be used to chain configurations to ultimately return the first modified configuration
    /// that occurs. For example: `cfg_file.and_then(cfg_cli).and_then(cfg_env)`; it will return the first
    /// modified Configset
    ///
    /// ## Panics
    /// This method will panic if both the provided sets are mutated. Hence, you need to check beforehand that
    /// there is no conflict
    pub fn and_then(self, other: Self) -> Self {
        if self.is_mutated() {
            if other.is_mutated() {
                panic!(
                    "Double mutation: {env_a} and {env_b}",
                    env_a = self.estack.source(),
                    env_b = other.estack.source()
                );
            }
            self
        } else {
            other
        }
    }
}

// server settings
impl Configset {
    pub fn server_tcp(
        &mut self,
        nhost: impl TryFromConfigSource<IpAddr>,
        nhost_key: StaticStr,
        nport: impl TryFromConfigSource<u16>,
        nport_key: StaticStr,
    ) {
        let mut host = DEFAULT_IPV4;
        let mut port = DEFAULT_PORT;
        self.try_mutate(nhost, &mut host, nhost_key, "an IPv4/IPv6 address");
        self.try_mutate(nport, &mut port, nport_key, "a 16-bit positive integer");
        self.cfg.ports = PortConfig::new_insecure_only(host, port);
    }
    pub fn server_noart(&mut self, nart: impl TryFromConfigSource<bool>, nart_key: StaticStr) {
        let mut noart = false;
        self.try_mutate(nart, &mut noart, nart_key, "true/false");
        self.cfg.noart = noart;
    }
    pub fn server_maxcon(
        &mut self,
        nmaxcon: impl TryFromConfigSource<usize>,
        nmaxcon_key: StaticStr,
    ) {
        let mut maxcon = MAXIMUM_CONNECTION_LIMIT;
        self.try_mutate_with_condcheck(
            nmaxcon,
            &mut maxcon,
            nmaxcon_key,
            "a positive integer greater than zero",
            |max| *max > 0,
        );
        self.cfg.maxcon = maxcon;
    }
}

// bgsave settings
impl Configset {
    pub fn bgsave_settings(
        &mut self,
        nenabled: impl TryFromConfigSource<bool>,
        nenabled_key: StaticStr,
        nduration: impl TryFromConfigSource<u64>,
        nduration_key: StaticStr,
    ) {
        let mut enabled = true;
        let mut duration = DEFAULT_BGSAVE_DURATION;
        let has_custom_duration = nduration.is_present();
        self.try_mutate(nenabled, &mut enabled, nenabled_key, "true/false");
        self.try_mutate_with_condcheck(
            nduration,
            &mut duration,
            nduration_key,
            "a positive integer greater than zero",
            |dur| *dur > 0,
        );
        if enabled {
            self.cfg.bgsave = BGSave::Enabled(duration);
        } else {
            if has_custom_duration {
                self.wstack.push(format!(
                    "Specifying `${nduration_key}` is useless when BGSAVE is disabled"
                ));
            }
            self.wstack
                .push("BGSAVE is disabled. You may lose data if the host crashes");
        }
    }
}

// snapshot settings
impl Configset {
    pub fn snapshot_settings(
        &mut self,
        nevery: impl TryFromConfigSource<u64>,
        nevery_key: StaticStr,
        natmost: impl TryFromConfigSource<usize>,
        natmost_key: StaticStr,
        nfailsafe: impl TryFromConfigSource<bool>,
        nfailsafe_key: StaticStr,
    ) {
        match (nevery.is_present(), natmost.is_present()) {
            (false, false) => {
                // noice, disabled
                if nfailsafe.is_present() {
                    // this mutation is pointless, but it is just for the sake of making sure
                    // that the `failsafe` key has a proper boolean, no matter if it is pointless
                    let mut _failsafe = DEFAULT_SNAPSHOT_FAILSAFE;
                    self.try_mutate(nfailsafe, &mut _failsafe, nfailsafe_key, "true/false");
                    self.wstack.push(format!(
                        "Specifying `${nfailsafe_key}` is usless when snapshots are disabled"
                    ));
                }
            }
            (true, true) => {
                let mut every = 0;
                let mut atmost = 0;
                let mut failsafe = DEFAULT_SNAPSHOT_FAILSAFE;
                self.try_mutate_with_condcheck(
                    nevery,
                    &mut every,
                    nevery_key,
                    "an integer greater than 0",
                    |dur| *dur > 0,
                );
                self.try_mutate(
                    natmost,
                    &mut atmost,
                    natmost_key,
                    "a positive integer. 0 indicates that all snapshots will be kept",
                );
                self.try_mutate(nfailsafe, &mut failsafe, nfailsafe_key, "true/false");
                self.cfg.snapshot =
                    SnapshotConfig::Enabled(SnapshotPref::new(every, atmost, failsafe));
            }
            (false, true) | (true, false) => self.estack.push(format!(
                "To use snapshots, pass values for both `${nevery_key}` and `${natmost_key}`"
            )),
        }
    }
}

// TLS settings
impl Configset {
    pub fn tls_settings(
        &mut self,
        nkey: impl TryFromConfigSource<String>,
        nkey_key: StaticStr,
        ncert: impl TryFromConfigSource<String>,
        ncert_key: StaticStr,
        nport: impl TryFromConfigSource<u16>,
        nport_key: StaticStr,
        nonly: impl TryFromConfigSource<bool>,
        nonly_key: StaticStr,
        npass: impl TryFromConfigSource<OptString>,
        npass_key: StaticStr,
    ) {
        match (nkey.is_present(), ncert.is_present()) {
            (true, true) => {
                // get the cert details
                let mut key = String::new();
                let mut cert = String::new();
                self.try_mutate(nkey, &mut key, nkey_key, "path to private key file");
                self.try_mutate(ncert, &mut cert, ncert_key, "path to TLS certificate file");

                // now get port info
                let mut port = DEFAULT_SSL_PORT;
                self.try_mutate(nport, &mut port, nport_key, "a positive 16-bit integer");

                // now check if TLS only
                let mut tls_only = false;
                self.try_mutate(nonly, &mut tls_only, nonly_key, "true/false");

                // check if we have a TLS cert
                let mut tls_pass = OptString::new_null();
                self.try_mutate(
                    npass,
                    &mut tls_pass,
                    npass_key,
                    "path to TLS cert passphrase",
                );

                let sslopts = SslOpts::new(key, cert, port, tls_pass.base);
                // now check if TLS only
                if tls_only {
                    let host = self.cfg.ports.get_host();
                    self.cfg.ports = PortConfig::new_secure_only(host, sslopts)
                } else {
                    // multi. go and upgrade existing
                    self.cfg.ports.upgrade_to_tls(sslopts);
                }
            }
            (true, false) | (false, true) => {
                self.estack.push(format!(
                    "To use TLS, pass values for both `${nkey_key}` and `${ncert_key}`"
                ));
            }
            (false, false) => {
                if nport.is_present() {
                    self.wstack
                        .push("Specifying `${nport_key}` is pointless when TLS is disabled");
                }
                if nonly.is_present() {
                    self.wstack
                        .push("Specifying `${nonly_key}` is pointless when TLS is disabled");
                }
                if npass.is_present() {
                    self.wstack
                        .push("Specifying `${npass_key}` is pointless when TLS is disabled");
                }
            }
        }
    }
}

pub fn get_config() -> Result<ConfigType, ConfigError> {
    // initialize clap because that will let us check for CLI/file configs
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let restore_file = matches.value_of("restore").map(|v| v.to_string());

    // get config from file
    let cfg_from_file = if let Some(file) = matches.value_of("config") {
        let file = match fs::read(file) {
            Ok(f) => f,
            Err(e) => return Err(ConfigError::OSError(e)),
        };
        let cfg_file: ConfigFile = match toml::from_slice(&file) {
            Ok(cfg) => cfg,
            Err(e) => return Err(ConfigError::ConfigFileParseError(e)),
        };
        Some(cfgfile::from_file(cfg_file))
    } else {
        None
    };

    // get config from CLI
    let cfg_from_cli = cfgcli::parse_cli_args(matches);
    // get config from env
    let cfg_from_env = cfgenv::parse_env_config();
    // calculate the number of config sources
    let cfg_degree = cfg_from_cli.is_mutated() as u8
        + cfg_from_env.is_mutated() as u8
        + cfg_from_file.is_some() as u8;
    // if degree is more than 1, there is a conflict
    let has_conflict = cfg_degree > 1;
    if has_conflict {
        return Err(ConfigError::Conflict);
    }
    if cfg_degree == 0 {
        // no configuration, use default
        Ok(ConfigType::new_default(restore_file))
    } else {
        let final_config = if let Some(cfg) = cfg_from_file {
            cfg
        } else {
            cfg_from_env.and_then(cfg_from_cli)
        };
        if final_config.is_okay() {
            let Configset { cfg, wstack, .. } = final_config;
            return Ok(ConfigType::new_custom(cfg, restore_file, wstack));
        } else {
            return Err(ConfigError::CfgError(final_config.estack));
        }
    }
}
