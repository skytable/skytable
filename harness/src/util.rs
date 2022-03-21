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

use crate::build::BuildMode;
use crate::process::ExitStatus;
use crate::{HarnessError, HarnessResult};
use std::env;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::process::Command;
pub type ExitCode = Option<i32>;

#[cfg(not(test))]
pub const VAR_TARGET: &str = "TARGET";
#[cfg(test)]
pub const VAR_TARGET: &str = "TARGET_TESTSUITE";
#[cfg(not(test))]
pub const VAR_ARTIFACT: &str = "ARTIFACT";
#[cfg(test)]
pub const VAR_ARTIFACT: &str = "ARTIFACT_TESTSUITE";
pub const WORKSPACE_ROOT: &str = env!("ROOT_DIR");
pub const SLEEP_FOR_STARTUP: u64 = 15;
pub const SLEEP_FOR_TERMINATION: u64 = 20;

pub fn get_var(var: &str) -> Option<String> {
    env::var_os(var).map(|v| v.to_string_lossy().to_string())
}

pub fn handle_exitstatus(desc: &str, status: IoResult<ExitStatus>) -> HarnessResult<()> {
    match status {
        Ok(status) => {
            if status.success() {
                Ok(())
            } else {
                Err(HarnessError::ChildError(desc.to_owned(), status.code()))
            }
        }
        Err(e) => Err(HarnessError::Other(format!(
            "Failed to get exitcode while running `{desc}`. this error happened: {e}"
        ))),
    }
}

pub fn get_child(desc: impl ToString, mut input: Command) -> HarnessResult<Child> {
    let desc = desc.to_string();
    match input.spawn() {
        Ok(child) => Ok(child),
        Err(e) => Err(HarnessError::Other(format!(
            "Failed to spawn process for `{desc}` with error: {e}"
        ))),
    }
}

pub fn handle_child(desc: &str, input: Command) -> HarnessResult<()> {
    self::handle_exitstatus(desc, self::get_child(desc, input)?.wait())
}

pub fn sleep_sec(secs: u64) {
    std::thread::sleep(std::time::Duration::from_secs(secs))
}

pub fn get_target_folder(mode: BuildMode) -> PathBuf {
    match env::var_os(VAR_TARGET).map(|v| v.to_string_lossy().to_string()) {
        Some(target) => format!("{WORKSPACE_ROOT}target/{target}/{}", mode.to_string()).into(),
        None => format!("{WORKSPACE_ROOT}target/{}", mode.to_string()).into(),
    }
}

/// Get the extension
pub fn add_extension(binary_name: &str) -> String {
    if cfg!(windows) {
        format!("{binary_name}.exe")
    } else {
        binary_name.to_owned()
    }
}

/// Returns `{body}/{binary_name}`
pub fn concat_path(binary_name: &str, body: impl AsRef<Path>) -> PathBuf {
    let mut pb = PathBuf::from(body.as_ref());
    pb.push(binary_name);
    pb
}

#[macro_export]
macro_rules! cmd {
    ($base:expr, $($cmd:expr),*) => {{
        let mut cmd = ::std::process::Command::new($base);
        $(
            cmd.arg($cmd);
        )*
        cmd
    }};
}
