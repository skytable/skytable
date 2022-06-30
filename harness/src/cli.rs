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

use {
    crate::{build::BuildMode, linuxpkg::LinuxPackageType, HarnessError, HarnessResult},
    std::{env, process},
};

const HELP: &str = "\
harness
A harness for Skytable's test suite

OPTIONS:
    harness [SUBCOMMAND]

SUBCOMMANDS:
    test       Run the full test suite
    bundle     Build the bundle
    bundle-dbg Build the debug bundle \
";

#[derive(Copy, Clone)]
pub enum HarnessWhat {
    Test,
    Bundle(BuildMode),
    LinuxPackage(LinuxPackageType),
}

impl HarnessWhat {
    const CLI_TEST: &'static str = "test";
    const CLI_BUNDLE: &'static str = "bundle";
    const CLI_BUNDLE_DEBUG: &'static str = "bundle-dbg";
    const CLI_DEB: &'static str = "deb";
    const CLI_ARG_HELP: &'static str = "--help";
    const CLI_ARG_HELP_SHORT: &'static str = "-h";
    /// Returns the target _harness mode_ from env
    pub fn from_env() -> HarnessResult<Self> {
        let args: Vec<String> = env::args().skip(1).collect();
        if args.is_empty() {
            display_help();
        } else if args.len() != 1 {
            return Err(HarnessError::BadArguments(format!(
                "expected one argument. found {} args",
                args.len()
            )));
        }
        let ret = match args[0].as_str() {
            Self::CLI_TEST => HarnessWhat::Test,
            Self::CLI_BUNDLE => HarnessWhat::Bundle(BuildMode::Release),
            Self::CLI_BUNDLE_DEBUG => HarnessWhat::Bundle(BuildMode::Debug),
            Self::CLI_ARG_HELP_SHORT | Self::CLI_ARG_HELP => display_help(),
            Self::CLI_DEB => HarnessWhat::LinuxPackage(LinuxPackageType::Deb),
            unknown_arg => return Err(HarnessError::UnknownCommand(unknown_arg.to_string())),
        };
        Ok(ret)
    }
    pub fn description(&self) -> String {
        match self {
            HarnessWhat::Test => "test suite".to_owned(),
            HarnessWhat::Bundle(mode) => format!("{} bundle", mode.to_string()),
            HarnessWhat::LinuxPackage(pkg) => format!("Linux package {}", pkg.to_string()),
        }
    }
}

fn display_help() -> ! {
    println!("{}", HELP);
    process::exit(0x00)
}
