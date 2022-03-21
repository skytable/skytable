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

use crate::{util, HarnessResult};

const TARGET_I686_GNU_LINUX: &str = "i686-unknown-linux-gnu";
const TARGET_X86_64_MUSL_LINUX: &str = "x86_64-unknown-linux-musl";

/// Install system deps
pub fn install_deps() -> HarnessResult<()> {
    info!("Installing additional deps for this platform ...");
    let install = match util::get_var(util::VAR_TARGET) {
        Some(t) => match t.as_str() {
            TARGET_I686_GNU_LINUX => cmd!(
                "bash",
                "-c",
                "sudo apt-get update && sudo apt install gcc-multilib -y"
            ),
            TARGET_X86_64_MUSL_LINUX => cmd!(
                "bash",
                "-c",
                "sudo apt-get update && sudo apt install musl-tools -y"
            ),
            _ => {
                info!("No additional dependencies required on this platform");
                return Ok(());
            }
        },
        None => {
            warn!("No target specified so not attempting to install any dependencies");
            return Ok(());
        }
    };
    util::handle_child("install system deps", install)?;
    Ok(())
}
