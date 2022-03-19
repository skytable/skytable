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

use crate::{
    build::{self, BuildMode},
    util, HarnessError, HarnessResult,
};
use libsky::VERSION;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use zip::{write::FileOptions, ZipWriter};

/// Returns the bundle name
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

/// Create a bundle using the provided mode
pub fn bundle(mode: BuildMode) -> HarnessResult<()> {
    let target_folder = build::build(mode)?;
    // now package
    package_binaries(target_folder, mode)?;
    Ok(())
}

/// Package the binaries into a ZIP file
fn package_binaries(target_folder: PathBuf, mode: BuildMode) -> HarnessResult<()> {
    // get the file index
    let file_index = build::get_files_index(&target_folder);
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
    let mut options = FileOptions::default().unix_permissions(0o755);
    if mode == BuildMode::Debug {
        // avoid compressing in debug since the binaries will be huge, so it's
        // better to avoid wasting CI time
        options = options.compression_method(zip::CompressionMethod::Stored);
    } else {
        options = options.compression_method(zip::CompressionMethod::Deflated);
    }
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
