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

use super::Configset;

/// Returns the environment configuration
pub(super) fn parse_env_config() -> Configset {
    let mut defset = Configset::new_env();
    macro_rules! fenv {
        (
            $fn:ident,
            $(
                $field:ident
            ),*
        ) => {
            defset.$fn(
                $(
                    ::std::env::var(stringify!($field)),
                    stringify!($field),
                )*
            );
        };
    }
    // server settings
    fenv!(server_tcp, SKY_SYSTEM_HOST, SKY_SYSTEM_PORT);
    fenv!(server_noart, SKY_SYSTEM_NOART);
    fenv!(server_maxcon, SKY_SYSTEM_MAXCON);
    fenv!(server_mode, SKY_DEPLOY_MODE);
    // bgsave settings
    fenv!(bgsave_settings, SKY_BGSAVE_ENABLED, SKY_BGSAVE_DURATION);
    // snapshot settings
    fenv!(
        snapshot_settings,
        SKY_SNAPSHOT_DURATION,
        SKY_SNAPSHOT_KEEP,
        SKY_SNAPSHOT_FAILSAFE
    );
    // TLS settings
    fenv!(
        tls_settings,
        SKY_TLS_KEY,
        SKY_TLS_CERT,
        SKY_TLS_PORT,
        SKY_TLS_ONLY,
        SKY_TLS_PASSIN
    );
    fenv!(auth_settings, SKY_AUTH_ORIGIN_KEY);
    defset
}
