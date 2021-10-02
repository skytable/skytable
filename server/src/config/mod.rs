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
use serde::Deserialize;
use std::fmt;
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
// modules
mod cfgfile;
#[cfg(test)]
mod tests;

const DEFAULT_IPV4: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
const DEFAULT_SSL_PORT: u16 = 2004;

macro_rules! cli_parse_or_default_or_err {
    ($parsewhat:expr, $default:expr, $except:expr $(,)?) => {
        match $parsewhat.map(|v| v.parse()) {
            Some(Ok(v)) => v,
            Some(Err(_)) => return Err(ConfigError::CliArgErr($except)),
            None => $default,
        }
    };
}

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
            port: tests::DEFAULT_PORT,
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

/// A `ParsedConfig` which can be used by main::check_args_or_connect() to bind
/// to a `TcpListener` and show the corresponding terminal output for the given
/// configuration
#[derive(Debug, PartialEq)]
pub struct ParsedConfig {
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

impl ParsedConfig {
    /// Create a new `ParsedConfig` from a given file in `location`
    pub fn new_from_file(location: String) -> Result<Self, ConfigError> {
        let file = match fs::read_to_string(location) {
            Ok(f) => f,
            Err(e) => return Err(ConfigError::OSError(e)),
        };
        match toml::from_str(&file) {
            Ok(cfgfile) => Ok(ParsedConfig::from_config(cfgfile)),
            Err(e) => Err(ConfigError::SyntaxError(e)),
        }
    }
    /// Create a `ParsedConfig` instance from a `Config` object, which is a parsed
    /// TOML file (represented as an object)
    fn from_config(cfg_info: cfgfile::Config) -> Self {
        ParsedConfig {
            noart: option_unwrap_or!(cfg_info.server.noart, false),
            bgsave: if let Some(bgsave) = cfg_info.bgsave {
                match (bgsave.enabled, bgsave.every) {
                    // TODO: Show a warning that there are unused keys
                    (Some(enabled), Some(every)) => BGSave::new(enabled, every),
                    (Some(enabled), None) => BGSave::new(enabled, 120),
                    (None, Some(every)) => BGSave::new(true, every),
                    (None, None) => BGSave::default(),
                }
            } else {
                BGSave::default()
            },
            snapshot: cfg_info
                .snapshot
                .map(|snapshot| {
                    SnapshotConfig::Enabled(SnapshotPref::new(
                        snapshot.every,
                        snapshot.atmost,
                        option_unwrap_or!(snapshot.failsafe, true),
                    ))
                })
                .unwrap_or_else(SnapshotConfig::default),
            ports: if let Some(sslopts) = cfg_info.ssl {
                if option_unwrap_or!(sslopts.only, false) {
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
                }
            } else {
                PortConfig::InsecureOnly {
                    host: cfg_info.server.host,
                    port: cfg_info.server.port,
                }
            },
            maxcon: option_unwrap_or!(cfg_info.server.maxclient, MAXIMUM_CONNECTION_LIMIT),
        }
    }
    #[cfg(test)]
    /// Create a new `ParsedConfig` from a `TOML` string
    pub fn new_from_toml_str(tomlstr: String) -> tests::TResult<Self> {
        Ok(ParsedConfig::from_config(toml::from_str(&tomlstr)?))
    }
    /// Create a new `ParsedConfig` with all the fields
    pub const fn new(
        noart: bool,
        bgsave: BGSave,
        snapshot: SnapshotConfig,
        ports: PortConfig,
        maxcon: usize,
    ) -> Self {
        ParsedConfig {
            noart,
            bgsave,
            snapshot,
            ports,
            maxcon,
        }
    }
    /// Create a default `ParsedConfig` with the following setup defaults:
    /// - `host`: 127.0.0.1
    /// - `port` : 2003
    /// - `noart` : false
    /// - `bgsave_enabled` : true
    /// - `bgsave_duration` : 120
    /// - `ssl` : disabled
    pub const fn default() -> Self {
        ParsedConfig {
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

use clap::{load_yaml, App};

/// The type of configuration:
/// - We either used a custom configuration file given to us by the user (`Custom`) OR
/// - We used the default configuration (`Def`)
pub enum ConfigType<T, U> {
    Def(T, Option<U>),
    Custom(T, Option<U>),
}

#[derive(Debug)]
/// Type of configuration error:
/// - The config file was not found (`OSError`)
/// - The config file was invalid (`SyntaxError`)
/// - The config file has an invalid value, which is syntatically correct
/// but logically incorrect (`CfgError`)
/// - The command line arguments have an invalid value/invalid values (`CliArgError`)
pub enum ConfigError {
    OSError(std::io::Error),
    SyntaxError(toml::de::Error),
    CfgError(&'static str),
    CliArgErr(&'static str),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::OSError(e) => writeln!(f, "error: {}", e),
            ConfigError::SyntaxError(e) => writeln!(f, "syntax error in configuration file: {}", e),
            ConfigError::CfgError(e) => write!(f, "Configuration error: {}", e),
            ConfigError::CliArgErr(e) => write!(f, "Argument error: {}", e),
        }
    }
}

/// This function returns a  `ConfigType<ParsedConfig>`
///
/// This parses a configuration file if it is supplied as a command line argument
/// or it returns the default configuration. **If** the configuration file
/// contains an error, then this returns it as an `Err` variant
pub fn get_config_file_or_return_cfg() -> Result<ConfigType<ParsedConfig, String>, ConfigError> {
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let restorefile = matches.value_of("restore").map(|v| v.to_string());
    // Check flags
    let sslonly = matches.is_present("sslonly");
    let noart = matches.is_present("noart");
    let nosave = matches.is_present("nosave");
    // Check options
    let filename = matches.value_of("config");
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
    let cli_has_overrideable_args = host.is_some()
        || port.is_some()
        || noart
        || nosave
        || snapevery.is_some()
        || snapkeep.is_some()
        || saveduration.is_some()
        || sslchain.is_some()
        || sslkey.is_some()
        || maxcon.is_some()
        || custom_ssl_port
        || passfile.is_some()
        || sslonly;
    if filename.is_some() && cli_has_overrideable_args {
        return Err(ConfigError::CfgError(
            "Either use command line arguments or use a configuration file",
        ));
    }
    // At this point we're sure that either a configuration file or command-line arguments
    // were supplied
    if cli_has_overrideable_args {
        // This means that there are some command-line args that we need to parse
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
        let maxcon: usize = cli_parse_or_default_or_err!(
            maxcon,
            MAXIMUM_CONNECTION_LIMIT,
            "Invalid value for `--maxcon`. Expected a valid positive integer"
        );
        let bgsave = if nosave {
            if saveduration.is_some() {
                // If there is both `nosave` and `saveduration` - the arguments aren't logically correct!
                // How would we run BGSAVE in a given `saveduration` if it is disabled? Return an error
                return Err(ConfigError::CliArgErr("Invalid options for BGSAVE. Either supply `--nosave` or `--saveduration` or nothing"));
            }
            // It is our responsibility to keep the user aware of bad settings, so we'll send a warning
            log::warn!("BGSAVE is disabled. You might lose data if the host crashes");
            BGSave::Disabled
        } else if let Some(duration) = saveduration.map(|dur| dur.parse()) {
            // A duration is specified for BGSAVE, so use it
            match duration {
                Ok(duration) => BGSave::new(true, duration),
                Err(_) => {
                    return Err(ConfigError::CliArgErr(
                        "Invalid value for `--saveduration`. Expected an unsigned 64-bit integer",
                    ))
                }
            }
        } else {
            // There's no `nosave` and no `saveduration` - cool; we'll use the default configuration
            BGSave::default()
        };
        let snapevery: Option<u64> = match snapevery {
            Some(dur) => match dur.parse() {
                Ok(dur) => Some(dur),
                Err(_) => {
                    return Err(ConfigError::CliArgErr(
                        "Invalid value for `--snapevery`. Expected an unsigned 64-bit integer",
                    ))
                }
            },
            None => None,
        };
        let snapkeep: Option<usize> = match snapkeep {
            Some(maxtop) => match maxtop.parse() {
                Ok(maxtop) => Some(maxtop),
                Err(_) => {
                    return Err(ConfigError::CliArgErr(
                        "Invalid value for `--snapkeep`. Expected an unsigned 64-bit integer",
                    ))
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
            return Err(ConfigError::CliArgErr(
                "Please provide a boolean `true` or `false` value to --stop-write-on-fail",
            ));
        };
        let snapcfg = match (snapevery, snapkeep) {
            (Some(every), Some(keep)) => {
                SnapshotConfig::Enabled(SnapshotPref::new(every, keep, failsafe))
            }
            (Some(_), None) => {
                return Err(ConfigError::CliArgErr(
                    "No value supplied for `--snapkeep`. When you supply `--snapevery`, you also need to specify `--snapkeep`"
                ));
            }
            (None, Some(_)) => {
                return Err(ConfigError::CliArgErr(
                    "No value supplied for `--snapevery`. When you supply `--snapkeep`, you also need to specify `--snapevery`"
                ));
            }
            (None, None) => SnapshotConfig::Disabled,
        };
        let portcfg = match (
            sslkey.map(|val| val.to_owned()),
            sslchain.map(|val| val.to_owned()),
        ) {
            (None, None) => {
                if sslonly {
                    return Err(ConfigError::CliArgErr(
                        "You mast pass values for both --sslkey and --sslchain to use the --sslonly flag"
                    ));
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
                return Err(ConfigError::CliArgErr(
                    "To use SSL, pass values for both --sslkey and --sslchain",
                ));
            }
        };
        let cfg = ParsedConfig::new(noart, bgsave, snapcfg, portcfg, maxcon);
        return Ok(ConfigType::Custom(cfg, restorefile));
    }
    if let Some(filename) = filename {
        match ParsedConfig::new_from_file(filename.to_owned()) {
            Ok(cfg) => {
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
            Err(e) => Err(e),
        }
    } else {
        Ok(ConfigType::Def(ParsedConfig::default(), restorefile))
    }
}
