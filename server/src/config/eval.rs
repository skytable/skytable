/*
 * Created on Tue Jan 25 2022
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

use std::fmt;

const EMSG_ENV: &str = "Environment";
const EMSG_FILE: &str = "Configuration file";
const EMSG_CLI: &str = "Command line arguments";
const TAB: &str = "    ";

#[derive(Debug, PartialEq)]
pub struct ErrorStack {
    stack: Vec<String>,
    init: &'static str,
}

impl ErrorStack {
    pub fn new(init: &'static str) -> Self {
        Self {
            init,
            stack: Vec::new(),
        }
    }
    pub fn epush(&mut self, e: impl ToString) {
        self.stack.push(e.to_string())
    }
}

impl fmt::Display for ErrorStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} errors:\n", self.init)?;
        for err in self.stack.iter() {
            write!(f, "{}- {}", TAB, err)?;
        }
        Ok(())
    }
}

#[test]
fn errorstact_fmt() {
    const EXPECTED: &str = "\
Environment errors:
    - Invalid value for `SKY_SYSTEM_PORT`. Expected a 16-bit integer
";
    let mut estk = ErrorStack::new(EMSG_ENV);
    estk.epush("Invalid value for `SKY_SYSTEM_PORT`. Expected a 16-bit integer");
    let fmt = format!("{}\n", estk);
    assert_eq!(fmt, EXPECTED);
}
