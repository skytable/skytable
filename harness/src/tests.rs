/*
 * Created on Mon Mar 21 2022
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
    crate::{
        build::{self, BuildMode},
        bundle, linuxpkg, util,
        util::WORKSPACE_ROOT,
    },
    libsky::VERSION,
    std::{env, path::PathBuf},
};

#[test]
fn file_names() {
    const ARTIFACT: &str = "x86_64-linux-gnu";
    const TARGET: &str = "x86_64-unknown-linux-gnu";
    // without a target
    // check bundle name
    let bundle_name = bundle::get_bundle_name();
    assert_eq!(bundle_name, format!("sky-bundle-v{VERSION}.zip"));
    // check target folder
    let target_folder_debug = util::get_target_folder(BuildMode::Debug);
    assert_eq!(
        target_folder_debug.to_string_lossy(),
        format!("{WORKSPACE_ROOT}target/debug")
    );
    let target_folder_release = util::get_target_folder(BuildMode::Release);
    assert_eq!(
        target_folder_release.to_string_lossy(),
        format!("{WORKSPACE_ROOT}target/release")
    );
    // check files index
    // files index for debug
    let files_index = build::get_files_index(&target_folder_debug);
    let expected_files_index: Vec<PathBuf> = build::BINARIES
        .iter()
        .map(|bin| {
            {
                let bin = util::add_extension(bin);
                format!("{WORKSPACE_ROOT}target/debug/{bin}")
            }
            .into()
        })
        .collect();
    assert_eq!(files_index, expected_files_index);
    // files index for release
    let files_index = build::get_files_index(&target_folder_release);
    let expected_files_index: Vec<PathBuf> = build::BINARIES
        .iter()
        .map(|bin| {
            {
                let bin = util::add_extension(bin);
                format!("{WORKSPACE_ROOT}target/release/{bin}")
            }
            .into()
        })
        .collect();
    assert_eq!(files_index, expected_files_index);
    // linux package name
    let name = linuxpkg::LinuxPackageType::Deb.get_file_name();
    assert_eq!(name, format!("skytable-v{VERSION}.deb"));

    // with a target
    env::set_var(util::VAR_ARTIFACT, ARTIFACT); // check bundle name
    env::set_var(util::VAR_TARGET, TARGET);
    let bundle_name = bundle::get_bundle_name();
    assert_eq!(bundle_name, format!("sky-bundle-v{VERSION}-{ARTIFACT}.zip"));
    // check target folder
    let target_folder_debug = util::get_target_folder(BuildMode::Debug);
    assert_eq!(
        target_folder_debug.to_string_lossy(),
        format!("{WORKSPACE_ROOT}target/{TARGET}/debug")
    );
    let target_folder_release = util::get_target_folder(BuildMode::Release);
    assert_eq!(
        target_folder_release.to_string_lossy(),
        format!("{WORKSPACE_ROOT}target/{TARGET}/release")
    );
    // check files index
    // files index for debug
    let files_index = build::get_files_index(&target_folder_debug);
    let expected_files_index: Vec<PathBuf> = build::BINARIES
        .iter()
        .map(|bin| {
            format!(
                "{WORKSPACE_ROOT}target/{TARGET}/debug/{bin}",
                bin = util::add_extension(bin)
            )
            .into()
        })
        .collect();
    assert_eq!(files_index, expected_files_index);
    // files index for release
    let files_index = build::get_files_index(&target_folder_release);
    let expected_files_index: Vec<PathBuf> = build::BINARIES
        .iter()
        .map(|bin| {
            format!(
                "{WORKSPACE_ROOT}target/{TARGET}/release/{bin}",
                bin = util::add_extension(bin)
            )
            .into()
        })
        .collect();
    assert_eq!(files_index, expected_files_index);
    // linux package name
    let name = linuxpkg::LinuxPackageType::Deb.get_file_name();
    assert_eq!(name, format!("skytable-v{VERSION}-{ARTIFACT}.deb"));
}
