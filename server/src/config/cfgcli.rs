/*
 * Created on Fri Jan 28 2022
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

use super::{ConfigSourceParseResult, Configset, TryFromConfigSource};
use clap::ArgMatches;

/// A flag. The flag is said to be set if `self.set` is true and unset if `self.set` is false. However,
/// if the flag is set, the value of SWITCH determines what value it is set to
#[derive(Copy, Clone)]
pub(super) struct Flag<const SWITCH: bool> {
    set: bool,
}

impl<const SWITCH: bool> Flag<SWITCH> {
    pub(super) fn new(set: bool) -> Self {
        Self { set }
    }
}

impl<const SWITCH: bool> TryFromConfigSource<bool> for Flag<SWITCH> {
    fn is_present(&self) -> bool {
        self.set
    }
    fn mutate_failed(self, target: &mut bool, trip: &mut bool) -> bool {
        if self.set {
            *trip = true;
            *target = SWITCH;
        }
        false
    }
    fn try_parse(self) -> ConfigSourceParseResult<bool> {
        if self.set {
            ConfigSourceParseResult::Okay(SWITCH)
        } else {
            ConfigSourceParseResult::Absent
        }
    }
}

pub(super) fn parse_cli_args(matches: ArgMatches) -> Configset {
    let mut defset = Configset::new_cli();
    macro_rules! fcli {
        ($fn:ident, $($source:expr, $key:literal),*) => {
            defset.$fn(
                $(
                    $source,
                    $key,
                )*
            )
        };
    }
    // server settings
    fcli!(
        server_tcp,
        matches.value_of("host"),
        "--host",
        matches.value_of("port"),
        "--port"
    );
    fcli!(
        server_noart,
        Flag::<true>::new(matches.is_present("noart")),
        "--noart"
    );
    fcli!(server_mode, matches.value_of("mode"), "--mode");
    fcli!(server_maxcon, matches.value_of("maxcon"), "--maxcon");
    // bgsave settings
    fcli!(
        bgsave_settings,
        Flag::<false>::new(matches.is_present("nosave")),
        "--nosave",
        matches.value_of("saveduration"),
        "--saveduration"
    );
    // snapshot settings
    fcli!(
        snapshot_settings,
        matches.value_of("snapevery"),
        "--snapevery",
        matches.value_of("snapkeep"),
        "--snapkeep",
        matches.value_of("stop-write-on-fail"),
        "--stop-write-on-fail"
    );
    // TLS settings
    fcli!(
        tls_settings,
        matches.value_of("sslkey"),
        "--sslkey",
        matches.value_of("sslchain"),
        "--sslchain",
        matches.value_of("sslport"),
        "--sslport",
        Flag::<true>::new(matches.is_present("sslonly")),
        "--sslonly",
        matches.value_of("tlspass"),
        "--tlspassin"
    );
    // auth settings
    fcli!(
        auth_settings,
        matches.value_of("authkey"),
        "--auth-origin-key"
    );
    defset
}
