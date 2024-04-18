/*
 * Created on Thu Apr 18 2024
 *
 * This file is a part of Skytable
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

use crate::util;

const OFFICIAL_TARGETS: [&str; 4] = [
    "i686-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
];

const THIS_TARGET: &str = env!("RUSTC_TARGET");
const THIS_HOST: &str = env!("RUSTC_HOST");

pub fn report_if_not_official_target() {
    let Some(set_target) = util::get_var(util::VAR_TARGET) else {
        check_target(THIS_TARGET);
        return;
    };
    check_target(&set_target)
}

fn check_target(target: &str) {
    if OFFICIAL_TARGETS.contains(&target) {
        info!("target `{target}` is offically supported. If you come across any build errors, please report them on the issue tracker")
    } else {
        warn!("target `{target}` is not an officially supported target. please use with caution");
    }
    if target != THIS_HOST {
        warn!("you are cross compiling from `{THIS_HOST}` to `{target}`")
    }
}
