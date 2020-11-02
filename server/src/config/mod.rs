/*
 * Created on Tue Sep 01 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! This module provides tools to handle configuration files and settings

#[cfg(test)]
use libtdb::TResult;
use serde::Deserialize;
use std::error::Error;
use std::fmt;
use std::fs;
#[cfg(test)]
use std::net::Ipv6Addr;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use tokio::net::ToSocketAddrs;
use toml;

/// This struct is an _object representation_ used for parsing the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    /// The `server` key
    server: ConfigKeyServer,
    /// The `bgsave` key
    /* TODO(@ohsayan): As of now, we will keep this optional, but post 0.5.0,
     * we will make it compulsory (so that we don't break semver)
     * See the link below for more details:
     * https://github.com/terrabasedb/terrabasedb/issues/21#issuecomment-693217709
     */
    bgsave: Option<ConfigKeyBGSAVE>,
    /// The snapshot key
    snapshot: Option<ConfigKeySnapshot>,
}

/// The BGSAVE section in the config file
#[derive(Deserialize, Debug, PartialEq)]
pub struct ConfigKeyBGSAVE {
    /// Whether BGSAVE is enabled or not
    ///
    /// If this key is missing, then we can assume that BGSAVE is enabled
    enabled: Option<bool>,
    /// The duration after which BGSAVE should start
    ///
    /// If this is the only key specified, then it is clear that BGSAVE is enabled
    /// and the duration is `every`
    every: Option<u64>,
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
        if let BGSave::Disabled = self {
            true
        } else {
            false
        }
    }
}

/// This struct represents the `server` key in the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct ConfigKeyServer {
    /// The host key is any valid IPv4/IPv6 address
    host: IpAddr,
    /// The port key is any valid port
    port: u16,
    /// The noart key is an `Option`al boolean value which is set to true
    /// for secure environments to disable terminal artwork
    noart: Option<bool>,
}

/// The snapshot section in the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct ConfigKeySnapshot {
    /// Whether snapshotting is enabled or not
    enabled: bool,
    /// After how many seconds should the snapshot be created
    every: u64,
    /// The maximum number of snapshots to keep
    ///
    /// If atmost is set to `0`, then all the snapshots will be kept
    atmost: usize,
}

#[derive(Debug, PartialEq)]
/// The snapshot configuration
///
pub struct SnapshotPref {
    /// Capture a snapshot `every` seconds
    pub every: u64,
    /// The maximum numeber of snapshots to be kept
    pub atmost: usize,
}

impl SnapshotPref {
    /// Create a new a new `SnapshotPref` instance
    pub const fn new(every: u64, atmost: usize) -> Self {
        SnapshotPref { every, atmost }
    }
    /// Returns `every,almost` as a tuple for pattern matching
    pub const fn decompose(self) -> (u64, usize) {
        (self.every, self.atmost)
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
    /// Check if snapshotting is enabled
    pub const fn is_enabled(&self) -> bool {
        if let SnapshotConfig::Disabled = self {
            false
        } else {
            true
        }
    }
}

/// A `ParsedConfig` which can be used by main::check_args_or_connect() to bind
/// to a `TcpListener` and show the corresponding terminal output for the given
/// configuration
#[derive(Debug, PartialEq)]
pub struct ParsedConfig {
    /// A valid IPv4/IPv6 address
    host: IpAddr,
    /// A valid port
    port: u16,
    /// If `noart` is set to true, no terminal artwork should be displayed
    noart: bool,
    /// The BGSAVE configuration
    pub bgsave: BGSave,
    /// The snapshot configuration
    pub snapshot: SnapshotConfig,
}

impl ParsedConfig {
    /// Create a new `ParsedConfig` from a given file in `location`
    pub fn new_from_file(location: String) -> Result<Self, ConfigError> {
        let file = match fs::read_to_string(location) {
            Ok(f) => f,
            Err(e) => return Err(ConfigError::OSError(e.into())),
        };
        match toml::from_str(&file) {
            Ok(cfgfile) => Ok(ParsedConfig::from_config(cfgfile)),
            Err(e) => return Err(ConfigError::SyntaxError(e.into())),
        }
    }
    /// Create a `ParsedConfig` instance from a `Config` object, which is a parsed
    /// TOML file (represented as an object)
    const fn from_config(cfg: Config) -> Self {
        ParsedConfig {
            host: cfg.server.host,
            port: cfg.server.port,
            noart: if let Some(noart) = cfg.server.noart {
                noart
            } else {
                false
            },
            bgsave: if let Some(bgsave) = cfg.bgsave {
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
            snapshot: if let Some(snapshot) = cfg.snapshot {
                if snapshot.enabled {
                    SnapshotConfig::Enabled(SnapshotPref::new(snapshot.every, snapshot.atmost))
                } else {
                    // TODO: Show a warning that there are unused keys
                    SnapshotConfig::Disabled
                }
            } else {
                SnapshotConfig::default()
            },
        }
    }
    #[cfg(test)]
    /// Create a new `ParsedConfig` from a `TOML` string
    pub fn new_from_toml_str(tomlstr: String) -> TResult<Self> {
        Ok(ParsedConfig::from_config(toml::from_str(&tomlstr)?))
    }
    /// Create a new `ParsedConfig` with the default `host` and `noart` settngs
    /// and a supplied `port`
    pub const fn default_with_port(port: u16) -> Self {
        ParsedConfig {
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
            noart: false,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
        }
    }
    /// Create a new `ParsedConfig` with the default `port` and `noart` settngs
    /// and a supplied `host`
    pub const fn default_with_host(host: IpAddr) -> Self {
        ParsedConfig::new(
            host,
            2003,
            false,
            BGSave::default(),
            SnapshotConfig::default(),
        )
    }
    /// Create a new `ParsedConfig` with all the fields
    pub const fn new(
        host: IpAddr,
        port: u16,
        noart: bool,
        bgsave: BGSave,
        snapshot: SnapshotConfig,
    ) -> Self {
        ParsedConfig {
            host,
            port,
            noart,
            bgsave,
            snapshot,
        }
    }
    /// Create a default `ParsedConfig` with the following setup defaults:
    /// - `host`: 127.0.0.1
    /// - `port` : 2003
    /// - `noart` : false
    /// - `bgsave_enabled` : true
    /// - `bgsave_duration` : 120
    pub const fn default() -> Self {
        ParsedConfig {
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 2003,
            noart: false,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
        }
    }
    /// Return a (host, port) tuple which can be bound to with `TcpListener`
    pub fn get_host_port_tuple(&self) -> impl ToSocketAddrs {
        ((self.host), self.port)
    }
    /// Returns `false` if `noart` is enabled. Otherwise it returns `true`
    pub const fn is_artful(&self) -> bool {
        !self.noart
    }
}

#[test]
#[cfg(test)]
fn test_config_toml_okayport() {
    let file = r#"
        [server]
        host = "127.0.0.1"
        port = 2003
    "#
    .to_owned();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(cfg, ParsedConfig::default(),);
}
/// Gets a `toml` file from `WORKSPACEROOT/examples/config-files`
#[cfg(test)]
fn get_toml_from_examples_dir(filename: String) -> TResult<String> {
    use std::path;
    let curdir = std::env::current_dir().unwrap();
    let workspaceroot = curdir.ancestors().nth(1).unwrap();
    let mut fileloc = path::PathBuf::from(workspaceroot);
    fileloc.push("examples");
    fileloc.push("config-files");
    fileloc.push(filename);
    Ok(fs::read_to_string(fileloc)?)
}

#[test]
#[cfg(test)]
fn test_config_toml_badport() {
    let file = r#"
        [server]
        port = 20033002
    "#
    .to_owned();
    let cfg = ParsedConfig::new_from_toml_str(file);
    assert!(cfg.is_err());
}

#[test]
#[cfg(test)]
fn test_config_file_ok() {
    let file = get_toml_from_examples_dir("tdb.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(cfg, ParsedConfig::default());
}

#[test]
#[cfg(test)]
fn test_config_file_err() {
    let file = get_toml_from_examples_dir("tdb.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_file(file);
    assert!(cfg.is_err());
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
    OSError(Box<dyn Error>),
    SyntaxError(Box<dyn Error>),
    CfgError(&'static str),
    CliArgErr(&'static str),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::OSError(e) => write!(f, "error: {}\n", e),
            ConfigError::SyntaxError(e) => write!(f, "syntax error in configuration file: {}\n", e),
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
pub fn get_config_file_or_return_cfg() -> Result<ConfigType<ParsedConfig, PathBuf>, ConfigError> {
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let restorefile = matches.value_of("restore").map(|val| {
        let mut path = PathBuf::from("snapshots/");
        path.push(val);
        path
    });
    let filename = matches.value_of("config");
    let host = matches.value_of("host");
    let port = matches.value_of("port");
    let noart = matches.is_present("noart");
    let nosave = matches.is_present("nosave");
    let snapevery = matches.value_of("snapevery");
    let snapkeep = matches.value_of("snapkeep");
    let saveduration = matches.value_of("saveduration");
    let cli_has_overrideable_args = host.is_some()
        || port.is_some()
        || noart
        || nosave
        || snapevery.is_some()
        || snapkeep.is_some()
        || saveduration.is_some();
    if filename.is_some() && cli_has_overrideable_args {
        return Err(ConfigError::CfgError(
            "Either use command line arguments or use a configuration file",
        ));
    }
    // At this point we're sure that either a configuration file or command-line arguments
    // were supplied
    if cli_has_overrideable_args {
        // This means that there are some command-line args that we need to parse
        let port: u16 = match port {
            Some(p) => match p.parse() {
                Ok(parsed) => parsed,
                Err(_) => {
                    return Err(ConfigError::CliArgErr(
                        "Invalid value for `--port`. Expected an unsigned 16-bit integer",
                    ))
                }
            },
            None => 2003,
        };
        let host: IpAddr = match host {
            Some(h) => match h.parse() {
                Ok(h) => h,
                Err(_) => {
                    return Err(ConfigError::CliArgErr(
                        "Invalid value for `--host`. Expected a valid IPv4 or IPv6 address",
                    ));
                }
            },
            None => "127.0.0.1".parse().unwrap(),
        };
        let bgsave = if nosave {
            if saveduration.is_some() {
                // If there is both `nosave` and `saveduration` - the arguments aren't logically correct!
                // How would we run BGSAVE in a given `saveduration` if it is disabled? Return an error
                return Err(ConfigError::CliArgErr("Invalid options for BGSAVE. Either supply `--nosave` or `--saveduration` or nothing"));
            }
            // It is our responsibility to keep the user aware of bad settings, so we'll send a warning
            log::warn!("BGSAVE is disabled. You might lose data if the host crashes");
            BGSave::Disabled
        } else {
            if let Some(duration) = saveduration.map(|dur| dur.parse()) {
                // A duration is specified for BGSAVE, so use it
                match duration {
                    Ok(duration) => BGSave::new(true, duration),
                    Err(_) => return Err(ConfigError::CliArgErr(
                        "Invalid value for `--saveduration`. Expected an unsigned 64-bit integer",
                    )),
                }
            } else {
                // There's no `nosave` and no `saveduration` - cool; we'll use the default configuration
                BGSave::default()
            }
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
        let snapcfg = match (snapevery, snapkeep) {
            (Some(every), Some(keep)) => SnapshotConfig::Enabled(SnapshotPref::new(every, keep)),
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
        let cfg = ParsedConfig::new(host, port, noart, bgsave, snapcfg);
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
                return Ok(ConfigType::Custom(cfg, restorefile));
            }
            Err(e) => return Err(e),
        }
    } else {
        Ok(ConfigType::Def(ParsedConfig::default(), restorefile))
    }
}

#[test]
#[cfg(test)]
fn test_args() {
    let cmdlineargs = vec!["tdb", "--withconfig", "../examples/config-files/tdb.toml"];
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches_from(cmdlineargs);
    let filename = matches.value_of("config").unwrap();
    assert_eq!("../examples/config-files/tdb.toml", filename);
    let cfg = ParsedConfig::new_from_toml_str(std::fs::read_to_string(filename).unwrap()).unwrap();
    assert_eq!(cfg, ParsedConfig::default());
}

#[test]
#[cfg(test)]
fn test_config_file_noart() {
    let file = get_toml_from_examples_dir("secure-noart.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            port: 2003,
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            noart: true,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
        }
    );
}

#[test]
#[cfg(test)]
fn test_config_file_ipv6() {
    let file = get_toml_from_examples_dir("ipv6.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            port: 2003,
            host: IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)),
            noart: false,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
        }
    );
}

#[test]
#[cfg(test)]
fn test_config_file_template() {
    let file = get_toml_from_examples_dir("template.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(cfg, ParsedConfig::default());
}

#[test]
#[cfg(test)]
fn test_config_file_bad_bgsave_section() {
    let file = get_toml_from_examples_dir("badcfg2.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file);
    assert!(cfg.is_err());
}

#[test]
#[cfg(test)]
fn test_config_file_custom_bgsave() {
    let file = get_toml_from_examples_dir("withcustombgsave.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 2003,
            noart: false,
            bgsave: BGSave::new(true, 600),
            snapshot: SnapshotConfig::default()
        }
    );
}

#[test]
fn test_config_file_bgsave_enabled_only() {
    /*
     * This test demonstrates a case where the user just said that BGSAVE is enabled.
     * In that case, we will default to the 120 second duration
     */
    let file = get_toml_from_examples_dir("bgsave-justenabled.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 2003,
            noart: false,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
        }
    )
}

#[test]
fn test_config_file_bgsave_every_only() {
    /*
     * This test demonstrates a case where the user just gave the value for every
     * In that case, it means BGSAVE is enabled and set to `every` seconds
     */
    let file = get_toml_from_examples_dir("bgsave-justevery.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 2003,
            noart: false,
            bgsave: BGSave::new(true, 600),
            snapshot: SnapshotConfig::default()
        }
    )
}

#[test]
fn test_config_file_snapshot() {
    let file = get_toml_from_examples_dir("snapshot.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            snapshot: SnapshotConfig::Enabled(SnapshotPref::new(3600, 4)),
            bgsave: BGSave::default(),
            host: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 2003,
            noart: false,
        }
    );
}
