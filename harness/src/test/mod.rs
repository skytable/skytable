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
    build::BuildMode,
    util::{self, SLEEP_FOR_TERMINATION},
    HarnessError, HarnessResult,
};
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
use std::{fs, io::Write};
mod svc;

/// Run the test suite
pub fn run_test() -> HarnessResult<()> {
    let ret = run_test_inner();
    let kill_check = svc::kill_servers();
    util::sleep_sec(SLEEP_FOR_TERMINATION);
    if let Err(e) = kill_check {
        error!("Failed to kill servers with error: {e}");
    }

    // clean up
    info!("Cleaning up test directories ...");
    fs::remove_dir_all("server1").map_err(|e| {
        HarnessError::Other(format!("Failed to remove dir `server1` with error: {e}"))
    })?;
    fs::remove_dir_all("server2").map_err(|e| {
        HarnessError::Other(format!("Failed to remove dir `server1` with error: {e}"))
    })?;
    ret
}

fn append_target(args: &mut Vec<String>) {
    match util::get_var(util::VAR_TARGET) {
        Some(target) => {
            args.push("--target".into());
            args.push(target);
        }
        None => {}
    }
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
    let mut standard_test_suite_args = vec!["cargo".to_owned(), "test".into()];
    let mut build_cmd_args = vec![
        "cargo".to_owned(),
        "build".into(),
        "-p".to_owned(),
        "skyd".into(),
    ];
    append_target(&mut build_cmd_args);
    append_target(&mut standard_test_suite_args);
    let persist_test_suite_args = [
        standard_test_suite_args.as_slice(),
        &["--features".to_owned(), "persist-suite".into()],
    ]
    .concat();
    // get cmd
    let build_cmd = util::assemble_command_from_slice(build_cmd_args);
    let standard_test_suite = util::assemble_command_from_slice(standard_test_suite_args);
    let persist_test_suite = util::assemble_command_from_slice(persist_test_suite_args);

    // build skyd
    info!("Building server binary ...");
    util::handle_child("build skyd", build_cmd)?;

    // run standard test suite
    svc::run_with_servers(&target_folder, true, move || {
        info!("Running standard test suite ...");
        util::handle_child("standard test suite", standard_test_suite)?;
        Ok(())
    })?;

    // run persistence tests; don't kill the servers because if either of the tests fail
    // then we can ensure that they will be killed by `run_test`
    svc::run_with_servers(&target_folder, false, move || {
        info!("Running persistence test suite ...");
        util::handle_child("standard test suite", persist_test_suite)?;
        Ok(())
    })?;

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
