/*
 * This file is a part of Skytable
 *
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use {
    super::variables,
    std::{collections::HashMap, env, path::PathBuf},
};

pub fn format(body: &str, arguments: &HashMap<&str, &str>, auto: bool) -> String {
    use regex::Regex;
    let pattern = r"\{[a-zA-Z_][a-zA-Z_0-9]*\}|\{\}";
    let re = Regex::new(pattern).unwrap();
    re.replace_all(body.as_ref(), |caps: &regex::Captures| {
        let capture: &str = &caps[0];
        let capture = &capture[1..capture.len() - 1];
        match capture {
            "" => {
                panic!("found an empty format")
            }
            "default_tcp_endpoint" if auto => "tcp@127.0.0.1:2003".to_owned(),
            "default_tls_endpoint" if auto => "tls@127.0.0.1:2004".to_owned(),
            "password_env_var" if auto => variables::env_vars::SKYDB_PASSWORD.into(),
            "version" if auto => format!("v{}", variables::VERSION),
            "further_assistance" if auto => "For further assistance, refer to the official documentation here: https://docs.skytable.org".to_owned(),
            arbitrary => arguments
                .get(arbitrary)
                .unwrap_or_else(|| panic!("could not find value for argument {}", arbitrary))
                .to_string(),
        }
    })
    .to_string()
}

pub fn get_home_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        env::var("USERPROFILE").map(PathBuf::from).ok()
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        env::var("HOME").map(PathBuf::from).ok()
    }
}
