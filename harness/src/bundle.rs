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

use crate::{util, HarnessError, HarnessResult};
use libsky::VERSION;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use zip::{write::FileOptions, ZipWriter};

const BINARIES: [&str; 4] = ["skyd", "sky-bench", "skysh", "sky-migrate"];

fn concat_path(binary_name: &str, body: impl AsRef<Path>) -> PathBuf {
    let mut pb = PathBuf::from(body.as_ref());
    #[cfg(windows)]
    let binary_name = format!("{}.exe", binary_name);
    pb.push(binary_name);
    pb
}

fn get_files_index(target_folder: &PathBuf) -> Vec<PathBuf> {
    let mut paths = Vec::with_capacity(3);
    for binary in BINARIES {
        paths.push(concat_path(binary, target_folder));
    }
    paths
}

fn get_bundle_name() -> String {
    let mut filename = format!("sky-bundle-v{VERSION}");
    match util::get_var(util::VAR_ARTIFACT) {
        Some(artifact) => {
            filename.push('-');
            filename.push_str(&artifact);
        }
        None => {}
    }
    filename.push_str(".zip");
    filename
}

pub fn run_bundle() -> HarnessResult<()> {
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
    target_folder.push("release");

    // assemble build args
    build_args.extend([
        "-p".into(),
        "skyd".into(),
        "-p".into(),
        "sky-bench".into(),
        "-p".into(),
        "skysh".into(),
        "-p".into(),
        "sky-migrate".into(),
        "--release".into(),
    ]);
    let mut cmd = Command::new("cargo");
    cmd.args(&build_args);
    util::handle_child("build release binaries", cmd)?;

    // now package
    package_binaries(target_folder)?;
    Ok(())
}

fn package_binaries(target_folder: PathBuf) -> HarnessResult<()> {
    // get the file index
    let file_index = get_files_index(&target_folder);
    // get the bundle file name
    let bundle_file_name = get_bundle_name();
    // create the bundle file
    let bundle_file = fs::File::create(&bundle_file_name)
        .map_err(|e| HarnessError::Other(format!("Failed to create ZIP file with error: {e}")))?;
    // init zip writer
    let mut zip = ZipWriter::new(bundle_file);
    // create a temp buffer
    let mut buffer = Vec::new();
    // ZIP settings
    let options = FileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o755);
    for file in file_index {
        let path = file.as_path();
        let name = path.strip_prefix(Path::new(&target_folder)).unwrap();
        #[allow(deprecated)]
        zip.start_file_from_path(name, options).unwrap();
        let mut f = fs::File::open(path).map_err(|e| {
            HarnessError::Other(format!(
                "Failed to add file `{}` to ZIP with error: {e}",
                path.to_string_lossy()
            ))
        })?;
        f.read_to_end(&mut buffer).unwrap();
        zip.write_all(&*buffer).unwrap();
        buffer.clear();
    }
    zip.finish().unwrap();
    Ok(())
}
