/*
 * Created on Sat Oct 02 2021
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

use serde::Deserialize;
use std::net::IpAddr;

/// This struct is an _object representation_ used for parsing the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct Config {
    /// The `server` key
    pub(super) server: ConfigKeyServer,
    /// The `bgsave` key
    pub(super) bgsave: Option<ConfigKeyBGSAVE>,
    /// The snapshot key
    pub(super) snapshot: Option<ConfigKeySnapshot>,
    /// SSL configuration
    pub(super) ssl: Option<KeySslOpts>,
}

/// This struct represents the `server` key in the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct ConfigKeyServer {
    /// The host key is any valid IPv4/IPv6 address
    pub(super) host: IpAddr,
    /// The port key is any valid port
    pub(super) port: u16,
    /// The noart key is an `Option`al boolean value which is set to true
    /// for secure environments to disable terminal artwork
    pub(super) noart: Option<bool>,
    /// The maximum number of clients
    pub(super) maxclient: Option<usize>,
}

/// The BGSAVE section in the config file
#[derive(Deserialize, Debug, PartialEq)]
pub struct ConfigKeyBGSAVE {
    /// Whether BGSAVE is enabled or not
    ///
    /// If this key is missing, then we can assume that BGSAVE is enabled
    pub(super) enabled: Option<bool>,
    /// The duration after which BGSAVE should start
    ///
    /// If this is the only key specified, then it is clear that BGSAVE is enabled
    /// and the duration is `every`
    pub(super) every: Option<u64>,
}

/// The snapshot section in the TOML file
#[derive(Deserialize, Debug, PartialEq)]
pub struct ConfigKeySnapshot {
    /// After how many seconds should the snapshot be created
    pub(super) every: u64,
    /// The maximum number of snapshots to keep
    ///
    /// If atmost is set to `0`, then all the snapshots will be kept
    pub(super) atmost: usize,
    /// Prevent writes to the database if snapshotting fails
    pub(super) failsafe: Option<bool>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct KeySslOpts {
    pub(super) key: String,
    pub(super) chain: String,
    pub(super) port: u16,
    pub(super) only: Option<bool>,
    pub(super) passin: Option<String>,
}
