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

use crate::build::{self, BuildMode};
use crate::{util, HarnessResult};
use libsky::VERSION;
use std::process::Command;

/// The Linux package type
#[derive(Copy, Clone)]
pub enum LinuxPackageType {
    /// Debian packages
    Deb,
}

impl LinuxPackageType {
    /// Returns the extension
    fn get_extension(&self) -> String {
        match self {
            Self::Deb => ".deb".to_owned(),
        }
    }
    /// Returns the file name for the package
    fn get_file_name(&self) -> String {
        let mut filename = format!("skytable-v{VERSION}");
        match util::get_var(util::VAR_ARTIFACT) {
            Some(artifact) => {
                filename.push('-');
                filename.push_str(&artifact);
            }
            None => {}
        }
        filename.push_str(&self.get_extension());
        filename
    }
}

impl ToString for LinuxPackageType {
    fn to_string(&self) -> String {
        match self {
            Self::Deb => "deb".to_owned(),
        }
    }
}

/// Creates a Linux package for the provided Linux package type
pub fn create_linuxpkg(package_type: LinuxPackageType) -> HarnessResult<()> {
    info!("Building binaries for Linux package");
    let _ = build::build(BuildMode::Release)?;
    info!("Creating Linux package");
    let filename = package_type.get_file_name();
    match package_type {
        LinuxPackageType::Deb => {
            // install cargo-deb
            util::handle_child("install cargo-deb", cmd!("cargo", "install", "cargo-deb"))?;
            // assemble the command
            let mut build_args = vec!["deb".to_owned()];
            match util::get_var(util::VAR_TARGET) {
                Some(t) => {
                    build_args.push("--target".to_string());
                    build_args.push(t);
                }
                None => {}
            }
            build_args.extend([
                "--no-build".to_owned(),
                "--manifest-path=server/Cargo.toml".to_owned(),
                "--output".to_owned(),
                filename.to_owned(),
            ]);
            let mut command = Command::new("cargo");
            command.args(build_args);
            util::handle_child("build dpkg", command)?;
        }
    }
    info!("Done building Linux package: {filename}");
    Ok(())
}
