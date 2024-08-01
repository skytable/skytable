/*
 * This file is a part of Skytable
 *
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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
    crate::{error::HarnessResult, util},
    std::process::Command,
};

pub fn audit() -> HarnessResult<()> {
    let mut miri_args = vec!["miri".to_owned(), "test".to_owned()];
    if let Some(t) = util::get_var(util::VAR_TARGET) {
        miri_args.push("--target".to_owned());
        miri_args.push(t);
    }
    miri_args.push("-p".to_owned());
    miri_args.push("skyd".to_owned());
    let mut cmd = Command::new("cargo");
    cmd.args(&miri_args)
        .env(
            "RUSTFLAGS",
            "-A dead_code -A unused_imports -A unused_macros",
        )
        .env("MIRIFLAGS", "-Zmiri-tree-borrows");
    util::handle_child(&format!("audit skyd using miri"), cmd)?;
    info!("successfully completed audit of skyd (miri)");
    Ok(())
}
