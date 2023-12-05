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

use {
    crate::{util, HarnessResult},
    std::{
        path::{Path, PathBuf},
        process::Command,
    },
    zip::CompressionMethod,
};

/// The binaries that will be present in a bundle
pub const BINARIES: [&str; 3] = ["skyd", "sky-bench", "skysh"];

/// The build mode
#[derive(Copy, Clone, PartialEq, Eq)]
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
    /// Returns the compression method for the build mode
    pub const fn get_compression_method(&self) -> CompressionMethod {
        match self {
            BuildMode::Debug => CompressionMethod::Stored,
            BuildMode::Release => CompressionMethod::Deflated,
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

/// Returns the paths of the files for the given target folder
pub fn get_files_index(target_folder: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::with_capacity(BINARIES.len());
    for binary in BINARIES {
        let binary = util::add_extension(binary);
        paths.push(util::concat_path(&binary, target_folder));
    }
    paths
}

/// Runs `cargo build` with the provided mode. `TARGET` is handled automatically
pub fn build(mode: BuildMode) -> HarnessResult<PathBuf> {
    let mut build_args = vec!["build".to_owned()];
    let target_folder = util::get_target_folder(mode);
    if let Some(t) = util::get_var(util::VAR_TARGET) {
        build_args.push("--target".to_owned());
        build_args.push(t);
    };

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
