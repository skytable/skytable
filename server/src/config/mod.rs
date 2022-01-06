/*
 * Created on Tue Sep 01 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

//! This module provides tools to handle configuration files and settings

use crate::dbnet::MAXIMUM_CONNECTION_LIMIT;
use clap::ArgMatches;
use clap::{load_yaml, App};
use serde::Deserialize;
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
// modules
#[macro_use]
mod macros;
mod cfgenv;
mod cfgerr;
mod cfgfile;
#[cfg(test)]
mod tests;
// self imports
use self::cfgerr::{ConfigError, ERR_CONFLICT};

const DEFAULT_IPV4: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const DEFAULT_SSL_PORT: u16 = 2004;
const DEFAULT_PORT: u16 = 2003;

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
    /// If `self` is a `Disabled` variant, then BGSAVE has been disabled by the user
    pub const fn is_disabled(&self) -> bool {
        matches!(self, BGSave::Disabled)
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

#[cfg(test)]
impl PortConfig {
    pub const fn default() -> PortConfig {
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
    pub const fn new_multi(host: IpAddr, port: u16, ssl: SslOpts) -> Self {
        PortConfig::Multi { host, port, ssl }
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

/// A `ConfigurationSet` which can be used by main::check_args_or_connect() to bind
/// to a `TcpListener` and show the corresponding terminal output for the given
/// configuration
#[derive(Debug, PartialEq)]
pub struct ConfigurationSet {
    /// If `noart` is set to true, no terminal artwork should be displayed
    noart: bool,
    /// The BGSAVE configuration
    pub bgsave: BGSave,
    /// The snapshot configuration
    pub snapshot: SnapshotConfig,
    /// Port configuration
    pub ports: PortConfig,
    /// The maximum number of connections
    pub maxcon: usize,
}

impl ConfigurationSet {
    /// Create a new `ConfigurationSet` from a given file in `location`
    pub fn new_from_file(location: String) -> Result<Self, ConfigError> {
        let file = fs::read_to_string(location)?;
        let r = toml::from_str(&file).map(ConfigurationSet::from_config)?;
        Ok(r)
    }
    /// Create a `ConfigurationSet` instance from a `Config` object, which is a parsed
    /// TOML file (represented as an object)
    fn from_config(cfg_info: cfgfile::Config) -> Self {
        let mut cfg = Self::default();
        set_if_exists!(cfg_info.server.noart, cfg.noart);
        if let Some(bgsave) = cfg_info.bgsave {
            let bgsave_ret = match (bgsave.enabled, bgsave.every) {
                // TODO: Show a warning that there are unused keys
                (Some(enabled), Some(every)) => BGSave::new(enabled, every),
                (Some(enabled), None) => BGSave::new(enabled, 120),
                (None, Some(every)) => BGSave::new(true, every),
                (None, None) => BGSave::default(),
            };
            cfg.bgsave = bgsave_ret;
        }
        if let Some(snapshot) = cfg_info.snapshot {
            cfg.snapshot = SnapshotConfig::Enabled(SnapshotPref::new(
                snapshot.every,
                snapshot.atmost,
                option_unwrap_or!(snapshot.failsafe, true),
            ));
        }
        if let Some(sslopts) = cfg_info.ssl {
            let portcfg = if sslopts.only.unwrap_or_default() {
                PortConfig::SecureOnly {
                    ssl: SslOpts {
                        key: sslopts.key,
                        chain: sslopts.chain,
                        port: sslopts.port,
                        passfile: sslopts.passin,
                    },
                    host: cfg_info.server.host,
                }
            } else {
                PortConfig::Multi {
                    ssl: SslOpts {
                        key: sslopts.key,
                        chain: sslopts.chain,
                        port: sslopts.port,
                        passfile: sslopts.passin,
                    },
                    host: cfg_info.server.host,
                    port: cfg_info.server.port,
                }
            };
            cfg.ports = portcfg;
        } else {
            // make sure we check for portcfg for non-TLS connections
            cfg.ports = PortConfig::new_insecure_only(cfg_info.server.host, cfg_info.server.port);
        }
        set_if_exists!(cfg_info.server.maxclient, cfg.maxcon);
        cfg
    }
    #[cfg(test)]
    /// Create a new `ConfigurationSet` from a `TOML` string
    pub fn new_from_toml_str(tomlstr: String) -> tests::TResult<Self> {
        Ok(ConfigurationSet::from_config(toml::from_str(&tomlstr)?))
    }
    #[cfg(test)]
    /// Create a new `ConfigurationSet` with all the fields
    pub const fn new(
        noart: bool,
        bgsave: BGSave,
        snapshot: SnapshotConfig,
        ports: PortConfig,
        maxcon: usize,
    ) -> Self {
        ConfigurationSet {
            noart,
            bgsave,
            snapshot,
            ports,
            maxcon,
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
        ConfigurationSet {
            noart: false,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
            ports: PortConfig::new_insecure_only(DEFAULT_IPV4, 2003),
            maxcon: MAXIMUM_CONNECTION_LIMIT,
        }
    }
    /// Returns `false` if `noart` is enabled. Otherwise it returns `true`
    pub const fn is_artful(&self) -> bool {
        !self.noart
    }
}

/// The type of configuration:
/// - We either used a custom configuration file given to us by the user (`Custom`) OR
/// - We used the default configuration (`Def`)
///
/// The second field in the tuple is for the restore file, if there was any
#[derive(Debug)]
pub enum ConfigType {
    Def(ConfigurationSet, Option<String>),
    Custom(ConfigurationSet, Option<String>),
}

/// This function returns a  `ConfigType<ConfigurationSet>`
///
/// This parses a configuration file if it is supplied as a command line argument
/// or it returns the default configuration. **If** the configuration file
/// contains an error, then this returns it as an `Err` variant
pub fn get_config_file_or_return_cfg() -> Result<ConfigType, ConfigError> {
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    self::get_config_file_or_return_cfg_from_matches(matches)
}

// this method simply allows us to simplify tests for conflicts
fn get_config_file_or_return_cfg_from_matches(
    matches: ArgMatches,
) -> Result<ConfigType, ConfigError> {
    let no_cli_args = matches.args.is_empty(); // check cli args
    let env_args = cfgenv::get_env_config()?;
    if no_cli_args && env_args.is_none() {
        // that means we need to use the default config
        return Ok(ConfigType::Def(ConfigurationSet::default(), None));
    }

    // Check if there is a config file
    let filename = matches.value_of("config");
    let restorefile = matches.value_of("restore").map(|v| v.to_string());
    let cfg = if let Some(filename) = filename {
        // so we have a config file; let's confirm that we don't have any other arguments
        // either no restore file and len greater than 1; or restore file is some, and args greater
        // than 2
        let is_conflict = (restorefile.is_none()
            && (matches.args.len() > 1 || matches.subcommand.is_some()))
            || (restorefile.is_some() && (matches.args.len() > 2 || matches.subcommand.is_some()));
        let is_conflict = is_conflict || env_args.is_some();
        if is_conflict {
            // nope, more args were passed; error
            return Err(ConfigError::CfgError(ERR_CONFLICT));
        }
        ConfigurationSet::new_from_file(filename.to_owned())
    } else {
        if env_args.is_some() && !matches.args.is_empty() {
            // so we have env args and some CLI args? that's a conflict
            return Err(ConfigError::CfgError(ERR_CONFLICT));
        }
        if let Some(env_args) = env_args {
            // we are sure that we just have env args
            Ok(env_args)
        } else {
            // we are sure that we just have CLI args
            parse_cli_args(matches)
        }
    }?;
    // now validate
    if cfg.bgsave.is_disabled() {
        log::warn!("BGSAVE is disabled: If this system crashes unexpectedly, it may lead to the loss of data");
    }
    if let SnapshotConfig::Enabled(e) = &cfg.snapshot {
        if e.every == 0 {
            return Err(ConfigError::CfgError(
                "The snapshot duration has to be greater than 0!",
            ));
        }
    }
    if let BGSave::Enabled(dur) = &cfg.bgsave {
        if *dur == 0 {
            return Err(ConfigError::CfgError(
                "The BGSAVE duration has to be greater than 0!",
            ));
        }
    }
    Ok(ConfigType::Custom(cfg, restorefile))
}

fn parse_cli_args(matches: ArgMatches) -> Result<ConfigurationSet, ConfigError> {
    let mut cfg = ConfigurationSet::default();
    // Check flags
    let sslonly = matches.is_present("sslonly");
    let noart = matches.is_present("noart");
    let nosave = matches.is_present("nosave");
    // check options
    let host = matches.value_of("host");
    let port = matches.value_of("port");
    let sslport = matches.value_of("sslport");
    let custom_ssl_port = sslport.is_some();
    let snapevery = matches.value_of("snapevery");
    let snapkeep = matches.value_of("snapkeep");
    let saveduration = matches.value_of("saveduration");
    let sslkey = matches.value_of("sslkey");
    let sslchain = matches.value_of("sslchain");
    let maxcon = matches.value_of("maxcon");
    let passfile = matches.value_of("tlspassin");

    cfg.noart = noart;
    let port: u16 = cli_parse_or_default_or_err!(
        port,
        2003,
        "Invalid value for `--port`. Expected an unsigned 16-bit integer"
    );
    let host: IpAddr = cli_parse_or_default_or_err!(
        host,
        DEFAULT_IPV4,
        "Invalid value for `--host`. Expected a valid IPv4 or IPv6 address"
    );
    let sslport: u16 = cli_parse_or_default_or_err!(
        sslport,
        DEFAULT_SSL_PORT,
        "Invalid value for `--sslport`. Expected a valid unsigned 16-bit integer"
    );
    cli_setparse_or_err!(
        cfg.maxcon,
        maxcon,
        "Invalid value for `--maxcon`. Expected a valid positive integer"
    );
    if nosave {
        if saveduration.is_some() {
            // If there is both `nosave` and `saveduration` - the arguments aren't logically correct!
            // How would we run BGSAVE in a given `saveduration` if it is disabled? Return an error
            ret_cli_err!("Invalid options for BGSAVE. Either supply `--nosave` or `--saveduration` or nothing");
        }
        // It is our responsibility to keep the user aware of bad settings, so we'll send a warning
        log::warn!("BGSAVE is disabled. You might lose data if the host crashes");
        cfg.bgsave = BGSave::Disabled;
    } else if let Some(dur) = saveduration {
        // A duration is specified for BGSAVE, so use it
        cfg.bgsave = match dur.parse() {
            Ok(duration) => BGSave::new(true, duration),
            Err(_) => {
                ret_cli_err!(
                    "Invalid value for `--saveduration`. Expected an unsigned 64-bit integer"
                )
            }
        };
    }
    // check snapshot configuration
    let snapevery: Option<u64> = match snapevery {
        Some(dur) => match dur.parse() {
            Ok(dur) => Some(dur),
            Err(_) => {
                ret_cli_err!("Invalid value for `--snapevery`. Expected an unsigned 64-bit integer")
            }
        },
        None => None,
    };
    let snapkeep: Option<usize> = match snapkeep {
        Some(maxtop) => match maxtop.parse() {
            Ok(maxtop) => Some(maxtop),
            Err(_) => {
                ret_cli_err!("Invalid value for `--snapkeep`. Expected an unsigned 64-bit integer")
            }
        },
        None => None,
    };
    let failsafe = if let Ok(failsafe) = option_unwrap_or!(
        matches
            .value_of("stop-write-on-fail")
            .map(|val| val.parse::<bool>()),
        Ok(true)
    ) {
        failsafe
    } else {
        ret_cli_err!("Please provide a boolean `true` or `false` value to --stop-write-on-fail");
    };
    cfg.snapshot = match (snapevery, snapkeep) {
        (Some(every), Some(keep)) => {
            SnapshotConfig::Enabled(SnapshotPref::new(every, keep, failsafe))
        }
        (None, None) => SnapshotConfig::Disabled,
        _ => {
            ret_cli_err!(
                "No value supplied for `--snapevery`. When you supply `--snapkeep`, you also need to specify `--snapevery`"
            )
        }
    };
    // check port config
    let portcfg = match (
        sslkey.map(|val| val.to_owned()),
        sslchain.map(|val| val.to_owned()),
    ) {
        (None, None) => {
            if sslonly {
                ret_cli_err!(
                    "You mast pass values for both --sslkey and --sslchain to use the --sslonly flag"
                );
            } else {
                if custom_ssl_port {
                    log::warn!("Ignoring value for `--sslport` as TLS was not enabled");
                }
                PortConfig::new_insecure_only(host, port)
            }
        }
        (Some(key), Some(chain)) => {
            if sslonly {
                PortConfig::new_secure_only(
                    host,
                    SslOpts::new(key, chain, sslport, passfile.map(|v| v.to_string())),
                )
            } else {
                PortConfig::new_multi(
                    host,
                    port,
                    SslOpts::new(key, chain, sslport, passfile.map(|v| v.to_string())),
                )
            }
        }
        _ => {
            ret_cli_err!("To use TLS, pass values for both --sslkey and --sslchain");
        }
    };
    cfg.ports = portcfg;
    Ok(cfg)
}
