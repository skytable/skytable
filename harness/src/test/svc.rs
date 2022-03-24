/*
 * Created on Thu Mar 24 2022
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
    util::{self, SLEEP_FOR_TERMINATION},
    HarnessError, HarnessResult,
};
use skytable::Connection;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::{
    io::{Error as IoError, ErrorKind},
    path::Path,
    process::{Child, Command},
};
#[cfg(windows)]
/// The powershell script hack to send CTRL+C using kernel32
const POWERSHELL_SCRIPT: &str = include_str!("../../../ci/windows/stop.ps1");
#[cfg(windows)]
/// Flag for new console Window
const CREATE_NEW_CONSOLE: u32 = 0x00000010;
/// The test suite server host
const TESTSUITE_SERVER_HOST: &str = "127.0.0.1";
/// The test suite server ports
const TESTSUITE_SERVER_PORTS: [u16; 4] = [2003, 2004, 2005, 2006];
/// The server IDs matching with the configuration files
const SERVER_IDS: [&str; 2] = ["server1", "server2"];
/// The workspace root
const WORKSPACE_ROOT: &str = env!("ROOT_DIR");

/// Get the command to start the provided server1
pub fn get_run_server_cmd(server_id: &'static str, target_folder: impl AsRef<Path>) -> Command {
    let args = vec![
        // binary
        util::concat_path("skyd", target_folder)
            .to_string_lossy()
            .to_string(),
        // config
        "--withconfig".to_owned(),
        format!("{WORKSPACE_ROOT}ci/{server_id}.toml"),
    ];
    let mut cmd = util::assemble_command_from_slice(&args);
    cmd.current_dir(server_id);
    #[cfg(windows)]
    cmd.creation_flags(CREATE_NEW_CONSOLE);
    cmd
}

fn connection_refused<T>(input: Result<T, IoError>) -> HarnessResult<bool> {
    match input {
        Ok(_) => Ok(false),
        Err(e)
            if matches!(
                e.kind(),
                ErrorKind::ConnectionRefused | ErrorKind::ConnectionReset
            ) =>
        {
            Ok(true)
        }
        Err(e) => Err(HarnessError::Other(format!(
            "Expected ConnectionRefused while checking for startup. Got error {e} instead"
        ))),
    }
}

/// Waits for the servers to start up or errors if something unexpected happened
fn wait_for_startup() -> HarnessResult<()> {
    info!("Waiting for servers to start up");
    for port in TESTSUITE_SERVER_PORTS {
        let connection_string = format!("{TESTSUITE_SERVER_HOST}:{port}");
        let mut backoff = 1;
        let mut con = Connection::new(TESTSUITE_SERVER_HOST, port);
        while connection_refused(con)? {
            if backoff > 64 {
                // enough sleeping, return an error
                error!("Server didn't respond in {backoff} seconds. Something is wrong");
                return Err(HarnessError::Other(format!(
                    "Startup backoff elapsed. Server at {connection_string} did not respond."
                )));
            }
            info!(
                "Server at {connection_string} not started. Sleeping for {backoff} second(s) ..."
            );
            util::sleep_sec(backoff);
            con = Connection::new(TESTSUITE_SERVER_HOST, port);
            backoff *= 2;
        }
        info!("Server at {connection_string} has started");
    }
    info!("All servers started up");
    Ok(())
}

/// Wait for the servers to shutdown, returning an error if something unexpected happens
fn wait_for_shutdown() -> HarnessResult<()> {
    info!("Waiting for servers to shut down");
    for port in TESTSUITE_SERVER_PORTS {
        let connection_string = format!("{TESTSUITE_SERVER_HOST}:{port}");
        let mut backoff = 1;
        let mut con = Connection::new(TESTSUITE_SERVER_HOST, port);
        while !connection_refused(con)? {
            if backoff > 64 {
                // enough sleeping, return an error
                error!("Server didn't shut down within {backoff} seconds. Something is wrong");
                return Err(HarnessError::Other(format!(
                    "Shutdown backoff elapsed. Server at {connection_string} did not shut down."
                )));
            }
            info!(
                "Server at {connection_string} still active. Sleeping for {backoff} second(s) ..."
            );
            util::sleep_sec(backoff);
            con = Connection::new(TESTSUITE_SERVER_HOST, port);
            backoff *= 2;
        }
        info!("Server at {connection_string} has stopped accepting connections");
    }
    info!("All servers have stopped accepting connections. Allowing {SLEEP_FOR_TERMINATION} seconds for them to exit");
    util::sleep_sec(SLEEP_FOR_TERMINATION);
    info!("All servers have shutdown");
    Ok(())
}

/// Start the servers returning handles to the child processes
fn start_servers(target_folder: impl AsRef<Path>) -> HarnessResult<Vec<Child>> {
    let mut ret = Vec::with_capacity(SERVER_IDS.len());
    for server_id in SERVER_IDS {
        let cmd = get_run_server_cmd(server_id, target_folder.as_ref());
        info!("Starting {server_id} ...");
        ret.push(util::get_child(format!("start {server_id}"), cmd)?);
    }
    wait_for_startup()?;
    Ok(ret)
}

pub(super) fn run_with_servers(
    target_folder: impl AsRef<Path>,
    kill_servers_when_done: bool,
    run_what: impl FnOnce() -> HarnessResult<()>,
) -> HarnessResult<()> {
    info!("Starting servers ...");
    let children = start_servers(target_folder.as_ref())?;
    run_what()?;
    if kill_servers_when_done {
        kill_servers()?;
        wait_for_shutdown()?;
    }
    // just use this to avoid ignoring the children vector
    assert_eq!(children.len(), SERVER_IDS.len());
    Ok(())
}

/// Send termination signal to the servers
pub(super) fn kill_servers() -> HarnessResult<()> {
    info!("Terminating server instances ...");
    kill_servers_inner()?;
    info!("Sent termination signals");
    Ok(())
}

#[cfg(not(windows))]
/// Kill the servers using `pkill` (send SIGTERM)
fn kill_servers_inner() -> HarnessResult<()> {
    util::handle_child("kill servers", cmd!("pkill", "skyd"))?;
    Ok(())
}

#[cfg(windows)]
/// HACK(@ohsayan): Kill the servers using a powershell hack
fn kill_servers_inner() -> HarnessResult<()> {
    match powershell_script::run(POWERSHELL_SCRIPT, false) {
        Ok(_) => Ok(()),
        Err(e) => Err(HarnessError::Other(format!(
            "Failed to run powershell script with error: {e}"
        ))),
    }
}
