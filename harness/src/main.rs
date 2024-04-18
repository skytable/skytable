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

#[macro_use]
extern crate log;
#[macro_use]
mod util;
mod build;
mod bundle;
mod cli;
mod diagnostics;
mod error;
mod linuxpkg;
mod presetup;
mod test;
#[cfg(test)]
mod tests;
use std::{env, process};
use {
    crate::{
        cli::HarnessWhat,
        error::{HarnessError, HarnessResult},
    },
    env_logger::Builder,
};

const ROOT_DIR: &str = env!("ROOT_DIR");

fn main() {
    Builder::new()
        .parse_filters(&env::var("SKYHARNESS_LOG").unwrap_or_else(|_| "info".to_owned()))
        .init();
    env::set_var("SKY_LOG", "trace");
    if let Err(e) = runner() {
        error!("harness failed with: {}", e);
        error!("fetching logs from server processes");
        for ret in test::get_children() {
            ret.print_logs();
        }
        process::exit(0x01);
    }
}

fn runner() -> HarnessResult<()> {
    let harness = cli::HarnessWhat::from_env()?;
    diagnostics::report_if_not_official_target();
    presetup::install_deps()?;
    match harness {
        HarnessWhat::Test => test::run_test()?,
        HarnessWhat::Bundle(bundle_mode) => bundle::bundle(bundle_mode)?,
        HarnessWhat::LinuxPackage(pkg) => linuxpkg::create_linuxpkg(pkg)?,
    }
    info!(
        "Successfully finished running harness for {}",
        harness.description()
    );
    Ok(())
}
