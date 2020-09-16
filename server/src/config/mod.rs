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

use libtdb::TResult;
use serde::Deserialize;
use std::error::Error;
use std::fmt;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tokio::net::ToSocketAddrs;
use toml;

/// This struct is an _object representation_ used for parsing the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    /// The `server` key
    server: ServerConfig,
    /// The `bgsave` key
    bgsave: Option<BGSave>,
}

/// This struct represents the `bgsave` key in the TOML file
///
/* TODO(@ohsayan): As of now, we will keep this optional, but post 0.5.0,
 * we will make it compulsory (so that we don't break semver)
 * See the link below for more details:
 * https://github.com/terrabasedb/terrabasedb/issues/21#issuecomment-693217709
 */
#[derive(Deserialize, Debug, PartialEq)]
pub struct BGSave {
    /// Whether BGSAVE is enabled or not
    ///
    /// If `enabled` is set to true, and an `every` field is also present, we will
    /// display a warning, that the `every` key is unused
    enabled: bool,
    /// Every 'n' seconds
    every: u64,
}

/// This struct represents the `server` key in the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct ServerConfig {
    /// The host key is any valid IPv4/IPv6 address
    host: IpAddr,
    /// The port key is any valid port
    port: u16,
    /// The noart key is an `Option`al boolean value which is set to true
    /// for secure environments to disable terminal artwork
    noart: Option<bool>,
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
    /// Whether BGSAVE is enabled or not
    bgsave_enabled: bool,
    /// Run `BGSAVE` every _n_ seconds
    bgsave_duration: u64,
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
        let (bgsave_enabled, bgsave_duration) = if let Some(bgsave) = cfg.bgsave {
            (bgsave.enabled, bgsave.every)
        } else {
            (true, 120)
        };
        ParsedConfig {
            host: cfg.server.host,
            port: cfg.server.port,
            noart: if let Some(noart) = cfg.server.noart {
                noart
            } else {
                false
            },
            bgsave_enabled,
            bgsave_duration,
        }
    }
    /// Create a new file
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
            bgsave_enabled: true,
            bgsave_duration: 120,
        }
    }
    /// Create a new `ParsedConfig` with the default `port` and `noart` settngs
    /// and a supplied `host`
    pub const fn default_with_host(host: IpAddr) -> Self {
        ParsedConfig::new(host, 2003, false, true, 120)
    }
    /// Create a new `ParsedConfig` with all the fields
    pub const fn new(
        host: IpAddr,
        port: u16,
        noart: bool,
        bgsave_enabled: bool,
        bgsave_duration: u64,
    ) -> Self {
        ParsedConfig {
            host,
            port,
            noart,
            bgsave_enabled,
            bgsave_duration,
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
            bgsave_enabled: true,
            bgsave_duration: 120,
        }
    }
    /// Return a (host, port) tuple which can be bound to with `TcpListener`
    pub fn get_host_port_tuple(self) -> impl ToSocketAddrs {
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
pub enum ConfigType<T> {
    Def(T),
    Custom(T),
}

/// Type of configuration error:
/// - The config file was not found (`OSError`)
/// - THe config file was invalid (`SyntaxError`)
pub enum ConfigError {
    OSError(Box<dyn Error>),
    SyntaxError(Box<dyn Error>),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::OSError(e) => write!(f, "error: {}\n", e),
            ConfigError::SyntaxError(e) => write!(f, "syntax error in configuration file: {}\n", e),
        }
    }
}

/// This function returns a  `ConfigType<ParsedConfig>`
///
/// This parses a configuration file if it is supplied as a command line argument
/// or it returns the default configuration. **If** the configuration file
/// contains an error, then this returns it as an `Err` variant
pub fn get_config_file_or_return_cfg() -> Result<ConfigType<ParsedConfig>, ConfigError> {
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let filename = matches.value_of("config");
    if let Some(filename) = filename {
        match ParsedConfig::new_from_file(filename.to_owned()) {
            Ok(cfg) => return Ok(ConfigType::Custom(cfg)),
            Err(e) => return Err(e),
        }
    } else {
        Ok(ConfigType::Def(ParsedConfig::default()))
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
            bgsave_enabled: true,
            bgsave_duration: 120,
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
            bgsave_enabled: true,
            bgsave_duration: 120,
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
