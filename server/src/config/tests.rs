/*
 * Created on Thu Sep 23 2021
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

use super::cfgerr::{ConfigError, ERR_CONFLICT};
use super::{
    BGSave, IpAddr, ParsedConfig, PortConfig, SnapshotConfig, SnapshotPref, SslOpts, DEFAULT_IPV4,
    MAXIMUM_CONNECTION_LIMIT,
};
use clap::{load_yaml, App};
pub(super) use libsky::TResult;
use std::fs;
use std::net::Ipv6Addr;

pub(super) const DEFAULT_PORT: u16 = 2003;

#[test]
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
fn test_config_file_ok() {
    let file = get_toml_from_examples_dir("skyd.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(cfg, ParsedConfig::default());
}

#[test]
fn test_config_file_err() {
    let file = get_toml_from_examples_dir("skyd.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_file(file);
    assert!(cfg.is_err());
}
#[test]
fn test_args() {
    let cmdlineargs = vec!["skyd", "--withconfig", "../examples/config-files/skyd.toml"];
    let cfg_layout = load_yaml!("../cli.yml");
    let matches = App::from_yaml(cfg_layout).get_matches_from(cmdlineargs);
    let filename = matches.value_of("config").unwrap();
    assert_eq!("../examples/config-files/skyd.toml", filename);
    let cfg = ParsedConfig::new_from_toml_str(std::fs::read_to_string(filename).unwrap()).unwrap();
    assert_eq!(cfg, ParsedConfig::default());
}

#[test]
fn test_config_file_noart() {
    let file = get_toml_from_examples_dir("secure-noart.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            noart: true,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
            ports: PortConfig::default(),
            maxcon: MAXIMUM_CONNECTION_LIMIT
        }
    );
}

#[test]
fn test_config_file_ipv6() {
    let file = get_toml_from_examples_dir("ipv6.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            noart: false,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
            ports: PortConfig::new_insecure_only(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0x1)),
                DEFAULT_PORT
            ),
            maxcon: MAXIMUM_CONNECTION_LIMIT
        }
    );
}

#[test]
fn test_config_file_template() {
    let file = get_toml_from_examples_dir("template.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig::new(
            false,
            BGSave::default(),
            SnapshotConfig::Enabled(SnapshotPref::new(3600, 4, true)),
            PortConfig::new_secure_only(
                DEFAULT_IPV4,
                SslOpts::new(
                    "/path/to/keyfile.pem".into(),
                    "/path/to/chain.pem".into(),
                    2004,
                    Some("/path/to/cert/passphrase.txt".to_owned())
                )
            ),
            MAXIMUM_CONNECTION_LIMIT
        )
    );
}

#[test]
fn test_config_file_bad_bgsave_section() {
    let file = get_toml_from_examples_dir("badcfg2.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file);
    assert!(cfg.is_err());
}

#[test]
fn test_config_file_custom_bgsave() {
    let file = get_toml_from_examples_dir("withcustombgsave.toml".to_owned()).unwrap();
    let cfg = ParsedConfig::new_from_toml_str(file).unwrap();
    assert_eq!(
        cfg,
        ParsedConfig {
            noart: false,
            bgsave: BGSave::new(true, 600),
            snapshot: SnapshotConfig::default(),
            ports: PortConfig::default(),
            maxcon: MAXIMUM_CONNECTION_LIMIT
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
            noart: false,
            bgsave: BGSave::default(),
            snapshot: SnapshotConfig::default(),
            ports: PortConfig::default(),
            maxcon: MAXIMUM_CONNECTION_LIMIT
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
            noart: false,
            bgsave: BGSave::new(true, 600),
            snapshot: SnapshotConfig::default(),
            ports: PortConfig::default(),
            maxcon: MAXIMUM_CONNECTION_LIMIT
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
            snapshot: SnapshotConfig::Enabled(SnapshotPref::new(3600, 4, true)),
            bgsave: BGSave::default(),
            noart: false,
            ports: PortConfig::default(),
            maxcon: MAXIMUM_CONNECTION_LIMIT
        }
    );
}

#[test]
fn test_cli_args_conflict() {
    let cfg_layout = load_yaml!("../cli.yml");
    let cli_args = ["--sslonly", "-c config.toml"];
    let matches = App::from_yaml(cfg_layout).get_matches_from(&cli_args);
    let err = super::get_config_file_or_return_cfg_from_matches(matches).unwrap_err();
    assert_eq!(err, ConfigError::CfgError(ERR_CONFLICT));
}

#[test]
fn test_cli_args_conflict_with_restore_file_okay() {
    let cfg_layout = load_yaml!("../cli.yml");
    let cli_args = ["--restore somedir", "-c config.toml"];
    let matches = App::from_yaml(cfg_layout).get_matches_from(&cli_args);
    let ret = super::get_config_file_or_return_cfg_from_matches(matches).unwrap_err();
    // this should only compain about the missing dir but not about conflict
    assert_eq!(
        ret,
        ConfigError::OSError(std::io::Error::from(std::io::ErrorKind::NotFound))
    );
}

#[test]
fn test_cli_args_conflict_with_restore_file_fail() {
    let cfg_layout = load_yaml!("../cli.yml");
    let cli_args = ["--restore somedir", "-c config.toml", "--nosave"];
    let matches = App::from_yaml(cfg_layout).get_matches_from(&cli_args);
    let ret = super::get_config_file_or_return_cfg_from_matches(matches).unwrap_err();
    // this should only compain about the missing dir but not about conflict
    assert_eq!(ret, ConfigError::CfgError(ERR_CONFLICT));
}
