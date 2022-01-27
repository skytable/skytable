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

use super::eval::{ErrorStack, WarningStack};
use super::{ConfigurationSet, PortConfig};
use core::str::FromStr;
use std::env::VarError;
use std::net::IpAddr;

type StaticStr = &'static str;

#[derive(Debug)]
pub enum ConfigSourceParseResult<T> {
    Okay(T),
    Absent,
    ParseFailure,
}

pub trait TryFromConfigSource<T: Sized>: Sized {
    /// Check if the value is present
    fn is_present(&self) -> bool;
    /// Attempt to mutate the value if present. If any error occurs
    /// while parsing the value, return true. Else return false if all went well
    fn mutate_failed(self, target_value: &mut T, trip: &mut bool) -> bool;
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
                    // yes, we failed to get the var but failed to parse it into unicode
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

    fn _new(feedback_source: StaticStr) -> Self {
        Self {
            did_mutate: false,
            cfg: ConfigurationSet::default(),
            estack: ErrorStack::new(feedback_source),
            wstack: WarningStack::new(feedback_source),
        }
    }
    pub fn new_env() -> Self {
        Self::_new(Self::EMSG_ENV)
    }
    pub fn new_cli() -> Self {
        Self::_new(Self::EMSG_CLI)
    }
    pub fn new_file() -> Self {
        Self::_new(Self::EMSG_FILE)
    }
    fn mutated(&mut self) {
        self.did_mutate = true;
    }
    fn epush(&mut self, field_key: StaticStr, expected: StaticStr) {
        self.estack.push(format!(
            "Bad value for `${field_key}`. Expected ${expected}",
        ))
    }
    pub fn is_okay(&self) -> bool {
        self.estack.is_empty()
    }
    pub fn is_mutated(&self) -> bool {
        self.did_mutate
    }
    pub fn into_result(self) -> Result<Option<ConfigurationSet>, ErrorStack> {
        let Self {
            wstack,
            estack,
            cfg,
            did_mutate,
        } = self;
        log::warn!("{}", wstack);
        if did_mutate {
            if estack.is_empty() {
                Ok(Some(cfg))
            } else {
                Err(estack)
            }
        } else {
            Ok(None)
        }
    }
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
        let mut host = super::DEFAULT_IPV4;
        let mut port = super::DEFAULT_PORT;
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
        let mut maxcon = super::MAXIMUM_CONNECTION_LIMIT;
        self.try_mutate_with_condcheck(
            nmaxcon,
            &mut maxcon,
            nmaxcon_key,
            "a positive integer greater than one",
            |max| *max > 0,
        );
        self.cfg.maxcon = maxcon;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_IPV4;
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
}
