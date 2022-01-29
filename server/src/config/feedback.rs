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

// external imports
use toml::de::Error as TomlError;
// std imports
use core::fmt;
use core::ops;
use std::io::Error as IoError;

#[cfg(test)]
const EMSG_ENV: &str = "Environment";
const TAB: &str = "    ";

#[derive(Debug, PartialEq)]
pub struct FeedbackStack {
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
    pub fn source(&self) -> &'static str {
        self.feedback_source
    }
    pub fn push(&mut self, f: impl ToString) {
        self.stack.push(f.to_string())
    }
    pub fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }
}

impl fmt::Display for FeedbackStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.is_empty() {
            write!(f, "{} {}:", self.feedback_source, self.feedback_type)?;
            for err in self.stack.iter() {
                write!(f, "\n{}- {}", TAB, err)?;
            }
        }
        Ok(())
    }
}

impl ops::Deref for FeedbackStack {
    type Target = Vec<String>;
    fn deref(&self) -> &Self::Target {
        &self.stack
    }
}
impl ops::DerefMut for FeedbackStack {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stack
    }
}

#[derive(Debug, PartialEq)]
pub struct ErrorStack {
    feedback: FeedbackStack,
}

impl ErrorStack {
    pub fn new(err_source: &'static str) -> Self {
        Self {
            feedback: FeedbackStack::new(err_source, "errors"),
        }
    }
}

impl fmt::Display for ErrorStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.feedback)
    }
}

impl ops::Deref for ErrorStack {
    type Target = FeedbackStack;
    fn deref(&self) -> &Self::Target {
        &self.feedback
    }
}

impl ops::DerefMut for ErrorStack {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.feedback
    }
}

#[test]
fn errorstack_fmt() {
    const EXPECTED: &str = "\
Environment errors:
    - Invalid value for `SKY_SYSTEM_PORT`. Expected a 16-bit integer\
";
    let mut estk = ErrorStack::new(EMSG_ENV);
    estk.push("Invalid value for `SKY_SYSTEM_PORT`. Expected a 16-bit integer");
    let fmt = format!("{}", estk);
    assert_eq!(fmt, EXPECTED);
}

#[derive(Debug, PartialEq)]
pub struct WarningStack {
    feedback: FeedbackStack,
}

impl WarningStack {
    pub fn new(warning_source: &'static str) -> Self {
        Self {
            feedback: FeedbackStack::new(warning_source, "warnings"),
        }
    }
    pub fn print_warnings(&self) {
        if !self.feedback.is_empty() {
            log::warn!("{}", self);
        }
    }
}

impl ops::Deref for WarningStack {
    type Target = FeedbackStack;
    fn deref(&self) -> &Self::Target {
        &self.feedback
    }
}

impl ops::DerefMut for WarningStack {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.feedback
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
    - The setting for `maxcon` is too high\
";
    let mut wstk = WarningStack::new(EMSG_ENV);
    wstk.push("BGSAVE is disabled. You may lose data if the host crashes");
    wstk.push("The setting for `maxcon` is too high");
    let fmt = format!("{}", wstk);
    assert_eq!(fmt, EXPECTED);
}

#[derive(Debug)]
pub enum ConfigError {
    OSError(IoError),
    CfgError(ErrorStack),
    ConfigFileParseError(TomlError),
    Conflict,
}

impl PartialEq for ConfigError {
    fn eq(&self, oth: &Self) -> bool {
        match (self, oth) {
            (Self::OSError(lhs), Self::OSError(rhs)) => lhs.to_string() == rhs.to_string(),
            (Self::CfgError(lhs), Self::CfgError(rhs)) => lhs == rhs,
            (Self::ConfigFileParseError(lhs), Self::ConfigFileParseError(rhs)) => lhs == rhs,
            (Self::Conflict, Self::Conflict) => true,
            _ => false,
        }
    }
}

impl From<IoError> for ConfigError {
    fn from(e: IoError) -> Self {
        Self::OSError(e)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(e: toml::de::Error) -> Self {
        Self::ConfigFileParseError(e)
    }
}

impl From<ErrorStack> for ConfigError {
    fn from(e: ErrorStack) -> Self {
        Self::CfgError(e)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigFileParseError(e) => write!(f, "Configuration file parse failed: {}", e),
            Self::OSError(e) => write!(f, "OS Error: {}", e),
            Self::CfgError(e) => write!(f, "{}", e),
            Self::Conflict => write!(
                f,
                "Conflict error: Either provide CLI args, environment variables or a config file for configuration"
            ),
        }
    }
}

#[cfg(unix)]
/// Returns the number of open files
fn get_ulimit() -> Result<usize, IoError> {
    use libc::rlimit;
    use libc::RLIMIT_NOFILE;
    unsafe {
        let rlim = rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let ret = libc::getrlimit(RLIMIT_NOFILE, &rlim as *const _ as *mut _);
        if ret != 0 {
            Err(IoError::last_os_error())
        } else {
            Ok(rlim.rlim_cur as usize)
        }
    }
}

#[test]
#[cfg(unix)]
fn test_ulimit() {
    get_ulimit().unwrap();
}
