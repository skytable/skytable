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

#[derive(Debug)]
struct FeedbackStack {
    stack: Vec<String>,
    feedback_type: &'static str,
    feedback_source: &'static str,
}

impl FeedbackStack {
    fn new(feedback_source: &'static str, feedback_type: &'static str) -> Self {
        Self {
            stack: Vec::new(),
            feedback_type,
            feedback_source,
        }
    }
}

impl fmt::Display for FeedbackStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}:\n", self.feedback_source, self.feedback_type)?;
        for err in self.stack.iter() {
            writeln!(f, "{}- {}", TAB, err)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ErrorStack {
    feedback: FeedbackStack,
}

impl ErrorStack {
    pub fn new(err_source: &'static str) -> Self {
        Self {
            feedback: FeedbackStack::new(err_source, "errors"),
        }
    }
    pub fn epush(&mut self, e: impl ToString) {
        self.feedback.stack.push(e.to_string())
    }
}

impl fmt::Display for ErrorStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.feedback)
    }
}

#[test]
fn errorstack_fmt() {
    const EXPECTED: &str = "\
Environment errors:
    - Invalid value for `SKY_SYSTEM_PORT`. Expected a 16-bit integer
";
    let mut estk = ErrorStack::new(EMSG_ENV);
    estk.epush("Invalid value for `SKY_SYSTEM_PORT`. Expected a 16-bit integer");
    let fmt = format!("{}", estk);
    assert_eq!(fmt, EXPECTED);
}

#[derive(Debug)]
pub struct WarningStack {
    feedback: FeedbackStack,
}

impl WarningStack {
    pub fn new(warning_source: &'static str) -> Self {
        Self {
            feedback: FeedbackStack::new(warning_source, "warnings"),
        }
    }
    pub fn wpush(&mut self, w: impl ToString) {
        self.feedback.stack.push(w.to_string())
    }
}

impl fmt::Display for WarningStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.feedback)
    }
}

#[test]
fn warningstack_fmt() {
    const EXPECTED: &str = "\
Environment warnings:
    - BGSAVE is disabled. You may lose data if the host crashes
    - The setting for `maxcon` is too high
";
    let mut wstk = WarningStack::new(EMSG_ENV);
    wstk.wpush("BGSAVE is disabled. You may lose data if the host crashes");
    wstk.wpush("The setting for `maxcon` is too high");
    let fmt = format!("{}", wstk);
    assert_eq!(fmt, EXPECTED);
}
