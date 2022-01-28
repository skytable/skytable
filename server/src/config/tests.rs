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

use super::{Configset, PortConfig, DEFAULT_IPV4};
pub(super) use libsky::TResult;
use std::fs;

#[test]
fn server_tcp() {
    let mut cfgset = Configset::new_env();
    cfgset.server_tcp(
        Some("127.0.0.1"),
        "SKY_SERVER_HOST",
        Some("2004"),
        "SKY_SERVER_PORT",
    );
    assert_eq!(
        cfgset.cfg.ports,
        PortConfig::new_insecure_only(DEFAULT_IPV4, 2004)
    );
    assert!(cfgset.is_mutated());
    assert!(cfgset.is_okay());
}
#[test]
fn server_tcp_fail_host() {
    let mut cfgset = Configset::new_env();
    cfgset.server_tcp(
        Some("?127.0.0.1"),
        "SKY_SERVER_HOST",
        Some("2004"),
        "SKY_SERVER_PORT",
    );
    assert_eq!(
        cfgset.cfg.ports,
        PortConfig::new_insecure_only(DEFAULT_IPV4, 2004)
    );
    assert!(cfgset.is_mutated());
    assert!(!cfgset.is_okay());
}
#[test]
fn server_tcp_fail_port() {
    let mut cfgset = Configset::new_env();
    cfgset.server_tcp(
        Some("127.0.0.1"),
        "SKY_SERVER_HOST",
        Some("65537"),
        "SKY_SERVER_PORT",
    );
    assert_eq!(
        cfgset.cfg.ports,
        PortConfig::new_insecure_only(DEFAULT_IPV4, 2003)
    );
    assert!(cfgset.is_mutated());
    assert!(!cfgset.is_okay());
}
#[test]
fn server_tcp_fail_both() {
    let mut cfgset = Configset::new_env();
    cfgset.server_tcp(
        Some("?127.0.0.1"),
        "SKY_SERVER_HOST",
        Some("65537"),
        "SKY_SERVER_PORT",
    );
    assert_eq!(
        cfgset.cfg.ports,
        PortConfig::new_insecure_only(DEFAULT_IPV4, 2003)
    );
    assert!(cfgset.is_mutated());
    assert!(!cfgset.is_okay());
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
