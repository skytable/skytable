/*
 * Created on Sun Sep 13 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! Objects for handling background services
//!

use std::path;
use std::process;

/// A `BackgroundTask` starts `tdb` in a background
pub struct BackGroundTask {
    child: process::Child,
}
impl BackGroundTask {
    /// Start a new background database server
    ///
    /// **Note**: This function expects that you're running it from the workspace
    /// root. It is **test-only**, and this must be kept in mind.
    pub fn new() -> Self {
        if !path::Path::new("../target/debug/tdb").exists() {
            panic!("The `tdb` binary could not be found");
        }
        let cmd = process::Command::new("../target/debug/tdb")
            .spawn()
            .unwrap();
        BackGroundTask { child: cmd }
    }
    /// Execute a function block
    pub fn execute(&self, body: fn() -> ()) {
        body()
    }
}
impl Drop for BackGroundTask {
    fn drop(&mut self) {
        // Terminate the background server
        self.child.kill().unwrap();
    }
}
