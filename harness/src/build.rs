/*
 * Created on Thu Mar 17 2022
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

use crate::util;
use crate::HarnessResult;
use std::{
    path::{Path, PathBuf},
    process::Command,
};

/// The binaries that will be present in a bundle
pub const BINARIES: [&str; 4] = ["skyd", "sky-bench", "skysh", "sky-migrate"];

/// The build mode
pub enum BuildMode {
    Debug,
    Release,
}

impl BuildMode {
    /// Get the build mode as an argument to pass to `cargo`
    pub const fn get_arg(&self) -> Option<&'static str> {
        match self {
            BuildMode::Debug => None,
            BuildMode::Release => Some("--release"),
        }
    }
}

impl ToString for BuildMode {
    fn to_string(&self) -> String {
        match self {
            BuildMode::Debug => "debug".to_owned(),
            BuildMode::Release => "release".to_owned(),
        }
    }
}

/// Returns `{body}/{binary_name}(.exe if on windows)`
fn concat_path(binary_name: &str, body: impl AsRef<Path>) -> PathBuf {
    let mut pb = PathBuf::from(body.as_ref());
    #[cfg(windows)]
    let binary_name = format!("{}.exe", binary_name);
    pb.push(binary_name);
    pb
}

/// Returns the paths of the files for the given target folder
pub fn get_files_index(target_folder: &PathBuf) -> Vec<PathBuf> {
    let mut paths = Vec::with_capacity(3);
    for binary in BINARIES {
        paths.push(concat_path(binary, target_folder));
    }
    paths
}

/// Runs `cargo build` with the provided mode. `TARGET` is handled automatically
pub fn build(mode: BuildMode) -> HarnessResult<PathBuf> {
    let mut build_args = vec!["build".to_owned()];
    let mut target_folder = PathBuf::from("target");
    match util::get_var(util::VAR_TARGET) {
        Some(t) => {
            build_args.push("--target".to_owned());
            build_args.push(t.to_string());
            target_folder.push(&t);
        }
        None => {}
    };
    target_folder.push(mode.to_string());

    // assemble build args
    for binary in BINARIES {
        build_args.extend(["-p".to_owned(), binary.to_owned()])
    }
    if let Some(arg) = mode.get_arg() {
        build_args.push(arg.to_owned());
    }
    let mut cmd = Command::new("cargo");
    cmd.args(&build_args);
    util::handle_child(
        &format!("build {mode} binaries", mode = mode.to_string()),
        cmd,
    )?;
    Ok(target_folder)
}
