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
use std::fs;
use std::process::Child;
use std::process::Command;

const WORKSPACE_ROOT: &str = env!("ROOT_DIR");
#[cfg(windows)]
const POWERSHELL_SCRIPT: &str = include_str!("../../ci/windows/stop.ps1");

pub fn get_run_server_cmd(server_id: &'static str, cmd_payload: &[String]) -> Command {
    let mut cmd = Command::new("cargo");
    cmd.args(cmd_payload);
    cmd.arg("--");
    cmd.arg("--withconfig");
    cmd.arg(format!("{WORKSPACE_ROOT}ci/{server_id}.toml"));
    cmd.current_dir(server_id);
    cmd
}

pub fn start_servers(s1_cmd: Command, s2_cmd: Command) -> HarnessResult<(Child, Child)> {
    info!("Starting server1 ...");
    let s1 = util::get_child("start server1", s1_cmd)?;
    util::sleep_sec(10);
    info!("Starting server2 ...");
    let s2 = util::get_child("start server2", s2_cmd)?;
    util::sleep_sec(10);
    Ok((s1, s2))
}

#[cfg(not(windows))]
fn kill_servers() -> HarnessResult<()> {
    util::handle_child("kill servers", cmd!("pkill", "skyd"))?;
    // sleep
    util::sleep_sec(10);
    Ok(())
}

#[cfg(windows)]
fn kill_servers() -> HarnessResult<()> {
    match powershell_script::run(POWERSHELL_SCRIPT, false) {
        Ok(_) => Ok(()),
        Err(e) => Err(HarnessError::Other(format!(
            "Failed to run powershell script with error: {e}"
        ))),
    }
}

pub fn run_test() -> HarnessResult<()> {
    let ret = run_test_inner();
    kill_servers()?;

    // clean up
    fs::remove_dir_all("server1").map_err(|e| {
        HarnessError::Other(format!("Failed to remove dir `server1` with error: {e}"))
    })?;
    fs::remove_dir_all("server2").map_err(|e| {
        HarnessError::Other(format!("Failed to remove dir `server1` with error: {e}"))
    })?;
    ret
}

pub fn run_test_inner() -> HarnessResult<()> {
    // first create the TLS keys
    info!("Creating TLS key+cert");
    util::handle_child("generate TLS key+cert", cmd!("bash", "ci/ssl.sh"))?;
    util::handle_child(
        "create server1 directory",
        cmd!("mkdir", "-p", "server1", "server2"),
    )?;

    // assemble commands
    let mut cmd: Vec<String> = vec!["run".to_string(), "-p".to_string(), "skyd".to_string()];
    let standard_test_suite;
    let persist_test_suite;
    let build_cmd;
    match util::get_var(util::VAR_TARGET) {
        Some(target) => {
            cmd.push("--target".into());
            cmd.push(target.to_string());
            standard_test_suite = cmd!("cargo", "test", "--target", &target);
            persist_test_suite = cmd!(
                "cargo",
                "test",
                "--target",
                &target,
                "--features",
                "persist-suite"
            );
            build_cmd = cmd!("cargo", "build", "-p", "skyd", "--target", &target);
        }
        None => {
            standard_test_suite = cmd!("cargo", "test");
            persist_test_suite = cmd!("cargo", "test", "--features", "persist-suite");
            build_cmd = cmd!("cargo", "build", "-p", "skyd");
        }
    }

    // build skyd
    info!("Building server binary ...");
    util::handle_child("build skyd", build_cmd)?;

    // start the servers, run tests and kill
    info!("Starting servers ...");
    let s1_cmd = get_run_server_cmd("server1", &cmd);
    let s2_cmd = get_run_server_cmd("server2", &cmd);
    let (_s1, _s2) = start_servers(s1_cmd, s2_cmd)?;
    info!("All servers started. Now running standard test suite ...");
    util::handle_child("standard test suite", standard_test_suite)?;
    kill_servers()?;

    // start server up again, run tests and kill
    info!("Starting servers ...");
    let s1_cmd = get_run_server_cmd("server1", &cmd);
    let s2_cmd = get_run_server_cmd("server2", &cmd);
    let (_s1, _s2) = start_servers(s1_cmd, s2_cmd)?;
    info!("All servers started. Now running persistence test suite ...");
    util::handle_child("standard test suite", persist_test_suite)?;

    Ok(())
}
