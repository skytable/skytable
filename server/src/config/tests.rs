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

use super::{BGSave, Configset, PortConfig, SnapshotConfig, SnapshotPref, SslOpts, DEFAULT_IPV4};
pub(super) use libsky::TResult;
use std::fs;

// server tests
// TCP
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
// noart
#[test]
fn server_noart_okay() {
    let mut cfgset = Configset::new_env();
    cfgset.server_noart(Some("true"), "SKY_SYSTEM_NOART");
    assert!(!cfgset.cfg.is_artful());
    assert!(cfgset.is_okay());
    assert!(cfgset.is_mutated());
}
#[test]
fn server_noart_fail() {
    let mut cfgset = Configset::new_env();
    cfgset.server_noart(Some("truee"), "SKY_SYSTEM_NOART");
    assert!(cfgset.cfg.is_artful());
    assert!(!cfgset.is_okay());
    assert!(cfgset.is_mutated());
}
#[test]
fn server_maxcon_okay() {
    let mut cfgset = Configset::new_env();
    cfgset.server_maxcon(Some("12345"), "SKY_SYSTEM_MAXCON");
    assert!(cfgset.is_mutated());
    assert!(cfgset.is_okay());
    assert_eq!(cfgset.cfg.maxcon, 12345);
}
#[test]
fn server_maxcon_fail() {
    let mut cfgset = Configset::new_env();
    cfgset.server_maxcon(Some("12345A"), "SKY_SYSTEM_MAXCON");
    assert!(cfgset.is_mutated());
    assert!(!cfgset.is_okay());
    assert_eq!(cfgset.cfg.maxcon, 50000);
}

// bgsave settings
#[test]
fn bgsave_okay() {
    let mut cfgset = Configset::new_env();
    cfgset.bgsave_settings(
        Some("true"),
        "SKY_BGSAVE_ENABLED",
        Some("128"),
        "SKY_BGSAVE_DURATION",
    );
    assert!(cfgset.is_mutated());
    assert!(cfgset.is_okay());
    assert_eq!(cfgset.cfg.bgsave, BGSave::Enabled(128));
}
#[test]
fn bgsave_fail() {
    let mut cfgset = Configset::new_env();
    cfgset.bgsave_settings(
        Some("truee"),
        "SKY_BGSAVE_ENABLED",
        Some("128"),
        "SKY_BGSAVE_DURATION",
    );
    assert!(cfgset.is_mutated());
    assert!(!cfgset.is_okay());
    assert_eq!(cfgset.cfg.bgsave, BGSave::Enabled(128));
}

// snapshot settings
#[test]
fn snapshot_okay() {
    let mut cfgset = Configset::new_env();
    cfgset.snapshot_settings(
        Some("3600"),
        "SKY_SNAPSHOT_EVERY",
        Some("0"),
        "SKY_SNAPSHOT_ATMOST",
        Some("false"),
        "SKY_SNAPSHOT_FAILSAFE",
    );
    assert!(cfgset.is_mutated());
    assert!(cfgset.is_okay());
    assert_eq!(
        cfgset.cfg.snapshot,
        SnapshotConfig::Enabled(SnapshotPref::new(3600, 0, false))
    );
}
#[test]
fn snapshot_fail() {
    let mut cfgset = Configset::new_env();
    cfgset.snapshot_settings(
        Some("3600"),
        "SKY_SNAPSHOT_EVERY",
        Some("0"),
        "SKY_SNAPSHOT_ATMOST",
        Some("falsee"),
        "SKY_SNAPSHOT_FAILSAFE",
    );
    assert!(cfgset.is_mutated());
    assert!(!cfgset.is_okay());
    assert_eq!(
        cfgset.cfg.snapshot,
        SnapshotConfig::Enabled(SnapshotPref::new(3600, 0, true))
    );
}
#[test]
fn snapshot_fail_with_missing_required_values() {
    let mut cfgset = Configset::new_env();
    cfgset.snapshot_settings(
        Some("3600"),
        "SKY_SNAPSHOT_EVERY",
        None,
        "SKY_SNAPSHOT_ATMOST",
        None,
        "SKY_SNAPSHOT_FAILSAFE",
    );
    assert!(cfgset.is_mutated());
    assert!(!cfgset.is_okay());
    assert_eq!(cfgset.cfg.snapshot, SnapshotConfig::Disabled);
}

// TLS settings
#[test]
fn tls_settings_okay() {
    let mut cfg = Configset::new_env();
    cfg.tls_settings(
        Some("key.pem"),
        "SKY_TLS_KEY",
        Some("cert.pem"),
        "SKY_TLS_CERT",
        Some("2005"),
        "SKY_TLS_PORT",
        Some("false"),
        "SKY_TLS_ONLY",
        None,
        "SKY_TLS_PASSIN",
    );
    assert!(cfg.is_mutated());
    assert!(cfg.is_okay());
    assert_eq!(cfg.cfg.ports, {
        let mut pf = PortConfig::default();
        pf.upgrade_to_tls(SslOpts::new(
            "key.pem".to_owned(),
            "cert.pem".to_owned(),
            2005,
            None,
        ));
        pf
    });
}
#[test]
fn tls_settings_fail() {
    let mut cfg = Configset::new_env();
    cfg.tls_settings(
        Some("key.pem"),
        "SKY_TLS_KEY",
        Some("cert.pem"),
        "SKY_TLS_CERT",
        Some("A2005"),
        "SKY_TLS_PORT",
        Some("false"),
        "SKY_TLS_ONLY",
        None,
        "SKY_TLS_PASSIN",
    );
    assert!(cfg.is_mutated());
    assert!(!cfg.is_okay());
    assert_eq!(cfg.cfg.ports, {
        let mut pf = PortConfig::default();
        pf.upgrade_to_tls(SslOpts::new(
            "key.pem".to_owned(),
            "cert.pem".to_owned(),
            2004,
            None,
        ));
        pf
    });
}
#[test]
fn tls_settings_fail_with_missing_required_values() {
    let mut cfg = Configset::new_env();
    cfg.tls_settings(
        Some("key.pem"),
        "SKY_TLS_KEY",
        None,
        "SKY_TLS_CERT",
        Some("2005"),
        "SKY_TLS_PORT",
        Some("false"),
        "SKY_TLS_ONLY",
        None,
        "SKY_TLS_PASSIN",
    );
    assert!(cfg.is_mutated());
    assert!(!cfg.is_okay());
    assert_eq!(cfg.cfg.ports, PortConfig::default());
}

/// Gets a `toml` file from `WORKSPACEROOT/examples/config-files`
fn get_toml_from_examples_dir(filename: String) -> TResult<String> {
    use std::path;
    let curdir = path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspaceroot = curdir.ancestors().nth(1).unwrap();
    let mut fileloc = path::PathBuf::from(workspaceroot);
    fileloc.push("examples");
    fileloc.push("config-files");
    fileloc.push(filename);
    Ok(fs::read_to_string(fileloc)?)
}
