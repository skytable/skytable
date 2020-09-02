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
use toml;

#[derive(Deserialize, Debug, PartialEq)]
struct Config {
    server: ServerConfig,
}

#[derive(Deserialize, Debug, PartialEq)]
struct ServerConfig {
    host: String,
    port: u16,
    noart: Option<bool>,
}

impl Config {
    pub fn new(file: String) -> TResult<Self> {
        let res: Config = toml::from_str(&file)?;
        Ok(res)
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
    let cfg = Config::new(file).unwrap();
    assert_eq!(
        cfg,
        Config {
            server: ServerConfig {
                port: 2003,
                host: "127.0.0.1".to_owned(),
                noart: None,
            }
        }
    );
}

#[test]
#[cfg(test)]
fn test_config_toml_badport() {
    let file = r#"
        [server]
        port = 20033002
    "#
    .to_owned();
    let cfg = Config::new(file);
    assert!(cfg.is_err());
}

#[test]
#[cfg(test)]
fn test_config_file_ok() {
    let fileloc = "../examples/config-files/tdb.toml";
    let file = std::fs::read_to_string(fileloc).unwrap();
    let cfg: Config = Config::new(file).unwrap();
    assert_eq!(
        cfg,
        Config {
            server: ServerConfig {
                port: 2003,
                host: "127.0.0.1".to_owned(),
                noart: None,
            }
        }
    );
}

#[test]
#[cfg(test)]
fn test_config_file_err() {
    let fileloc = "../examples/config-files/badcfg.toml";
    let file = std::fs::read_to_string(fileloc).unwrap();
    let cfg = Config::new(file);
    assert!(cfg.is_err());
}
use clap::{load_yaml, App};

/// Get the command line arguments
pub fn get_config_file() -> Option<String> {
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches();
    let filename = matches.value_of("config");
    filename.map(|fname| fname.to_owned())
}

#[test]
#[cfg(test)]
fn test_args() {
    let cmdlineargs = vec!["tdb", "--withconfig", "../examples/config-files/tdb.toml"];
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches_from(cmdlineargs);
    let filename = matches.value_of("config").unwrap();
    assert_eq!("../examples/config-files/tdb.toml", filename);
    let cfg = Config::new(std::fs::read_to_string(filename).unwrap()).unwrap();
    assert_eq!(
        cfg,
        Config {
            server: ServerConfig {
                port: 2003,
                host: "127.0.0.1".to_owned(),
                noart: None,
            }
        }
    );
}

#[test]
#[cfg(test)]
fn test_config_file_noart() {
    let fileloc = "../examples/config-files/secure-noart.toml";
    let file = std::fs::read_to_string(fileloc).unwrap();
    let cfg: Config = Config::new(file).unwrap();
    assert_eq!(
        cfg,
        Config {
            server: ServerConfig {
                port: 2003,
                host: "127.0.0.1".to_owned(),
                noart: Some(true),
            }
        }
    );
}
