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

use crate::{build::BuildMode, util, HarnessError, HarnessResult};
use openssl::{
    asn1::Asn1Time,
    bn::{BigNum, MsbOption},
    error::ErrorStack,
    hash::MessageDigest,
    pkey::{PKey, Private},
    rsa::Rsa,
    x509::{
        extension::{BasicConstraints, KeyUsage, SubjectKeyIdentifier},
        X509NameBuilder, X509,
    },
};
use std::fs;
use std::io::Write;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::process::Command;

/// The workspace root
const WORKSPACE_ROOT: &str = env!("ROOT_DIR");
#[cfg(windows)]
/// The powershell script hack to send CTRL+C using kernel32
const POWERSHELL_SCRIPT: &str = include_str!("../../ci/windows/stop.ps1");
#[cfg(windows)]
/// Flag for new console Window
const CREATE_NEW_CONSOLE: u32 = 0x00000010;

/// Get the command to start the provided server1
pub fn get_run_server_cmd(server_id: &'static str, target_folder: impl AsRef<Path>) -> Command {
    let cfg_file_path = PathBuf::from(format!("{WORKSPACE_ROOT}ci/{server_id}.toml"));
    let binpath = util::concat_path("skyd", target_folder)
        .to_string_lossy()
        .to_string();
    let mut cmd = Command::new(binpath);
    cmd.arg("--withconfig");
    cmd.arg(cfg_file_path);
    cmd.current_dir(server_id);
    #[cfg(windows)]
    cmd.creation_flags(CREATE_NEW_CONSOLE);
    cmd
}

/// Start the servers returning handles to the child processes
pub fn start_servers(s1_cmd: Command, s2_cmd: Command) -> HarnessResult<(Child, Child)> {
    info!("Starting server1 ...");
    let s1 = util::get_child("start server1", s1_cmd)?;
    util::sleep_sec(10);
    info!("Starting server2 ...");
    let s2 = util::get_child("start server2", s2_cmd)?;
    util::sleep_sec(10);
    Ok((s1, s2))
}

/// Kill the servers (run the command and then sleep for 10s)
fn kill_servers() -> HarnessResult<()> {
    kill_servers_inner()?;
    // sleep
    util::sleep_sec(20);
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

/// Run the test suite
pub fn run_test() -> HarnessResult<()> {
    let ret = run_test_inner();
    if let Err(e) = kill_servers() {
        error!("Failed to kill servers with error: {e}");
    }

    // clean up
    fs::remove_dir_all("server1").map_err(|e| {
        HarnessError::Other(format!("Failed to remove dir `server1` with error: {e}"))
    })?;
    fs::remove_dir_all("server2").map_err(|e| {
        HarnessError::Other(format!("Failed to remove dir `server1` with error: {e}"))
    })?;
    ret
}

/// Actually run the tests. This will run:
/// - The standard test suite
/// - The persistence test suite
fn run_test_inner() -> HarnessResult<()> {
    // first create the TLS keys
    info!("Creating TLS key+cert");
    let (cert, pkey) = mk_ca_cert().expect("Failed to create cert");
    let mut certfile = fs::File::create("cert.pem").expect("failed to create cert.pem");
    certfile.write_all(&cert.to_pem().unwrap()).unwrap();
    let mut pkeyfile = fs::File::create("key.pem").expect("failed to create key.pem");
    pkeyfile
        .write_all(&pkey.private_key_to_pem_pkcs8().unwrap())
        .unwrap();
    fs::create_dir_all("server1").map_err(|e| {
        HarnessError::Other(format!("Failed to create `server1` dir with error: {e}"))
    })?;
    fs::create_dir_all("server2").map_err(|e| {
        HarnessError::Other(format!("Failed to create `server2` dir with error: {e}"))
    })?;

    // assemble commands
    let target_folder = util::get_target_folder(BuildMode::Debug);
    let standard_test_suite;
    let persist_test_suite;
    let build_cmd;
    match util::get_var(util::VAR_TARGET) {
        Some(target) => {
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
    let s1_cmd = get_run_server_cmd("server1", &target_folder);
    let s2_cmd = get_run_server_cmd("server2", &target_folder);
    let (_s1, _s2) = start_servers(s1_cmd, s2_cmd)?;
    info!("All servers started. Now running standard test suite ...");
    util::handle_child("standard test suite", standard_test_suite)?;
    kill_servers()?;

    // start server up again, run tests and kill
    info!("Starting servers ...");
    let s1_cmd = get_run_server_cmd("server1", &target_folder);
    let s2_cmd = get_run_server_cmd("server2", &target_folder);
    let (_s1, _s2) = start_servers(s1_cmd, s2_cmd)?;
    info!("All servers started. Now running persistence test suite ...");
    util::handle_child("standard test suite", persist_test_suite)?;

    Ok(())
}

/// Generate certificates
fn mk_ca_cert() -> Result<(X509, PKey<Private>), ErrorStack> {
    let rsa = Rsa::generate(2048)?;
    let key_pair = PKey::from_rsa(rsa)?;

    let mut x509_name = X509NameBuilder::new()?;
    x509_name.append_entry_by_text("C", "US")?;
    x509_name.append_entry_by_text("ST", "CA")?;
    x509_name.append_entry_by_text("O", "Skytable")?;
    x509_name.append_entry_by_text("CN", "sky-harness")?;
    let x509_name = x509_name.build();

    let mut cert_builder = X509::builder()?;
    cert_builder.set_version(2)?;
    let serial_number = {
        let mut serial = BigNum::new()?;
        serial.rand(159, MsbOption::MAYBE_ZERO, false)?;
        serial.to_asn1_integer()?
    };
    cert_builder.set_serial_number(&serial_number)?;
    cert_builder.set_subject_name(&x509_name)?;
    cert_builder.set_issuer_name(&x509_name)?;
    cert_builder.set_pubkey(&key_pair)?;
    let not_before = Asn1Time::days_from_now(0)?;
    cert_builder.set_not_before(&not_before)?;
    let not_after = Asn1Time::days_from_now(365)?;
    cert_builder.set_not_after(&not_after)?;

    cert_builder.append_extension(BasicConstraints::new().critical().ca().build()?)?;
    cert_builder.append_extension(
        KeyUsage::new()
            .critical()
            .key_cert_sign()
            .crl_sign()
            .build()?,
    )?;

    let subject_key_identifier =
        SubjectKeyIdentifier::new().build(&cert_builder.x509v3_context(None, None))?;
    cert_builder.append_extension(subject_key_identifier)?;

    cert_builder.sign(&key_pair, MessageDigest::sha256())?;
    let cert = cert_builder.build();

    Ok((cert, key_pair))
}
