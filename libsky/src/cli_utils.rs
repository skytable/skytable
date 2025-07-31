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
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    error::Error,
    fmt,
    str::FromStr,
};

/*
    cli args traits & types
*/

pub type CliResult<T> = Result<T, CliArgsError>;
pub type SingleOption = HashMap<String, String>;
pub type MultipleOptions = HashMap<String, Vec<String>>;

#[derive(Debug)]
pub enum CliArgsError {
    ArgFmtError(String),
    DuplicateFlag(String),
    DuplicateOption(String),
    SubcommandDisallowed,
    ArgParseError(String),
    Other(String),
}

impl fmt::Display for CliArgsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArgFmtError(arg) => write!(f, "the argument `--{arg}` is formatted incorrectly"),
            Self::DuplicateFlag(flag) => {
                write!(f, "found duplicate flag `--{flag}` which is not allowed")
            }
            Self::DuplicateOption(opt) => {
                write!(f, "found duplicate option `--{opt}` which is not allowed")
            }
            Self::SubcommandDisallowed => write!(f, "subcommands are disallowed in this context"),
            Self::ArgParseError(arg) => write!(f, "failed to parse value assigned to `--{arg}`"),
            Self::Other(e) => write!(f, "{e}"),
        }
    }
}

impl Error for CliArgsError {}

pub trait CliArgsDecode: Sized {
    type Data;
    fn initialize<const SWITCH: bool>(iter: &mut impl Iterator<Item = impl ArgItem>) -> Self::Data;
    fn push_flag(data: &mut Self::Data, flag: String) -> CliResult<()>;
    fn push_option(
        data: &mut Self::Data,
        option_name: String,
        option_value: String,
    ) -> CliResult<()>;
    fn yield_subcommand(
        data: Self::Data,
        subcommand: String,
        args: impl IntoIterator<Item = impl ArgItem>,
    ) -> CliResult<Self>;
    fn yield_command(data: Self::Data) -> CliResult<Self>;
    fn yield_help(data: Self::Data) -> CliResult<Self>;
    fn yield_version(data: Self::Data) -> CliResult<Self>;
}

pub trait CommandLineArgs: Sized + CliArgsDecode {
    fn parse(src: impl IntoIterator<Item = impl ArgItem>) -> CliResult<Self> {
        decode_args::<Self, true>(src)
    }
    fn from_cli() -> CliResult<Self> {
        Self::parse(std::env::args())
    }
}

impl<T: Sized + CliArgsDecode> CommandLineArgs for T {}

/*
    helper traits
*/

pub trait ArgItem {
    fn as_str(&self) -> &str;
    fn boxed_str(self) -> String;
}

impl<'a> ArgItem for &'a str {
    fn as_str(&self) -> &str {
        self
    }
    fn boxed_str(self) -> String {
        self.to_owned()
    }
}

impl ArgItem for String {
    fn as_str(&self) -> &str {
        self
    }
    fn boxed_str(self) -> String {
        self
    }
}

pub trait CliArgsOptions: Default {
    type Value;
    fn is_unset(&self) -> bool;
    fn push_option(&mut self, option: String, value: String) -> CliResult<()>;
    fn take_option(&mut self, option: &str) -> Option<Self::Value>;
    fn contains(&self, option: &str) -> bool;
}

impl CliArgsOptions for SingleOption {
    type Value = String;
    fn is_unset(&self) -> bool {
        self.is_empty()
    }
    fn contains(&self, option: &str) -> bool {
        self.contains_key(option)
    }
    fn push_option(&mut self, option: String, value: String) -> CliResult<()> {
        match self.entry(option) {
            Entry::Vacant(ve) => {
                ve.insert(value);
                Ok(())
            }
            Entry::Occupied(oe) => return Err(CliArgsError::DuplicateOption(oe.key().to_string())),
        }
    }
    fn take_option(&mut self, option: &str) -> Option<Self::Value> {
        self.remove(option)
    }
}

impl CliArgsOptions for MultipleOptions {
    type Value = Vec<String>;
    fn is_unset(&self) -> bool {
        self.is_empty()
    }
    fn contains(&self, option: &str) -> bool {
        self.contains_key(option)
    }
    fn push_option(&mut self, option: String, value: String) -> CliResult<()> {
        match self.entry(option) {
            Entry::Occupied(mut oe) => oe.get_mut().push(value),
            Entry::Vacant(ve) => {
                ve.insert(vec![value]);
            }
        }
        Ok(())
    }
    fn take_option(&mut self, option: &str) -> Option<Self::Value> {
        self.remove(option)
    }
}

/*
    args decoder
*/

fn decode_args<C: CliArgsDecode, const HAS_BINARY_NAME: bool>(
    src: impl IntoIterator<Item = impl ArgItem>,
) -> CliResult<C> {
    let mut args = src.into_iter().peekable();
    if HAS_BINARY_NAME {
        // must not be empty
        if args.peek().is_none() {
            return Err(CliArgsError::Other(
                "expected arguments but found none".to_owned(),
            ));
        }
    }
    let mut cli_data = C::initialize::<HAS_BINARY_NAME>(&mut args);
    while let Some(arg) = args.next() {
        let arg = arg.as_str();
        let arg = if arg == "-h" || arg == "--help" {
            return C::yield_help(cli_data);
        } else if arg == "-v" || arg == "--version" {
            return C::yield_version(cli_data);
        } else if arg.starts_with("--") {
            // option or flag
            &arg[2..]
        } else if arg.starts_with('-') {
            if arg.len() != 2 {
                // invalid shorthand
                return Err(CliArgsError::Other(format!(
                    "the argument `{arg}` is formatted incorrectly"
                )));
            }
            // option or flag
            &arg[1..]
        } else {
            // this is subcommand
            return C::yield_subcommand(cli_data, arg.boxed_str(), args);
        };
        if arg.is_empty() {
            return Err(CliArgsError::ArgFmtError("invalid argument".to_string()));
        }
        // is this arg in the --x=y format?
        let mut arg_split = arg.split('=');
        let (arg_split_name_, arg_split_value_) = (arg_split.next(), arg_split.next());
        match (arg_split_name_, arg_split_value_) {
            (Some(name_), Some(value_)) => {
                if name_.is_empty() || value_.is_empty() {
                    return Err(CliArgsError::ArgFmtError(arg.to_string()));
                }
                // yes, it was formatted this way
                C::push_option(&mut cli_data, name_.boxed_str(), value_.boxed_str())?;
                continue;
            }
            (Some(_), None) => {}
            _ => unreachable!(),
        }
        // no, probably in the --x y format
        match args.peek() {
            Some(arg_) => {
                if arg_.as_str().starts_with("--") || arg_.as_str().starts_with('-') {
                    // flag
                    C::push_flag(&mut cli_data, arg.boxed_str())?;
                } else {
                    // option
                    C::push_option(
                        &mut cli_data,
                        arg.boxed_str(),
                        args.next().unwrap().boxed_str(),
                    )?;
                }
            }
            None => {
                // flag
                C::push_flag(&mut cli_data, arg.boxed_str())?;
            }
        }
    }
    C::yield_command(cli_data)
}

/*
    cli arg impl: CliCommand (simple, subcommand-less)
*/

#[derive(Debug, PartialEq)]
pub enum CliCommand<Opt: CliArgsOptions> {
    Help(CliCommandData<Opt>),
    Run(CliCommandData<Opt>),
    Version(CliCommandData<Opt>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CliCommandData<Opt: CliArgsOptions> {
    options: Opt,
    flags: HashSet<String>,
}

impl<Opt: CliArgsOptions> CliCommandData<Opt> {
    pub fn take_flag(&mut self, flag: &str) -> CliResult<bool> {
        if self.flags.remove(flag) {
            Ok(true)
        } else if self.options.contains(flag) {
            Err(CliArgsError::Other(format!(
                "expected `--{flag}` to be a flag but found an option"
            )))
        } else {
            Ok(false)
        }
    }
    pub fn into_options_only(self) -> CliResult<Opt> {
        if self.flags.is_empty() {
            Ok(self.options)
        } else {
            Err(CliArgsError::Other(
                "no flags were expected in this context".to_string(),
            ))
        }
    }
    pub fn is_empty(&self) -> bool {
        self.options.is_unset() && self.flags.is_empty()
    }
    pub fn ensure_empty(&self) -> CliResult<()> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(CliArgsError::Other(
                "found unknown flags or options".to_string(),
            ))
        }
    }
    pub fn take_option(&mut self, option: &str) -> CliResult<Option<Opt::Value>> {
        match self.options.take_option(option) {
            Some(opt) => Ok(Some(opt)),
            None => {
                if self.flags.contains(option) {
                    Err(CliArgsError::Other(format!(
                        "expected option `--{option}` but instead found flag"
                    )))
                } else {
                    Ok(None)
                }
            }
        }
    }
    pub fn option(&mut self, option: &str) -> CliResult<Opt::Value> {
        match self.take_option(option)? {
            Some(opt) => Ok(opt),
            None => Err(CliArgsError::Other(format!(
                "option `--{option}` is required"
            ))),
        }
    }
}

impl CliCommandData<SingleOption> {
    pub fn parse_take_option<T: FromStr>(&mut self, option: &str) -> CliResult<Option<T>> {
        match self.options.remove(option) {
            Some(opt) => match opt.parse() {
                Ok(opt) => Ok(Some(opt)),
                Err(_) => Err(CliArgsError::ArgParseError(option.to_owned())),
            },
            None => Ok(None),
        }
    }
}

impl<Opt: CliArgsOptions> CliArgsDecode for CliCommand<Opt> {
    type Data = CliCommandData<Opt>;
    fn initialize<const SWITCH: bool>(
        iter: &mut impl Iterator<Item = impl ArgItem>,
    ) -> CliCommandData<Opt> {
        if SWITCH {
            let _binary_name = iter.next();
        }
        CliCommandData {
            options: Default::default(),
            flags: Default::default(),
        }
    }
    fn push_flag(data: &mut Self::Data, flag: String) -> CliResult<()> {
        if !data.flags.insert(flag.to_owned()) {
            return Err(CliArgsError::DuplicateFlag(flag.to_string()));
        }
        Ok(())
    }
    fn push_option(
        data: &mut Self::Data,
        option_name: String,
        option_value: String,
    ) -> CliResult<()> {
        data.options.push_option(option_name, option_value)
    }
    fn yield_subcommand(
        _: Self::Data,
        _: String,
        _: impl IntoIterator<Item = impl ArgItem>,
    ) -> CliResult<Self> {
        Err(CliArgsError::SubcommandDisallowed)
    }
    fn yield_command(data: Self::Data) -> CliResult<Self> {
        Ok(CliCommand::Run(data))
    }
    fn yield_help(data: Self::Data) -> CliResult<Self> {
        Ok(CliCommand::Help(data))
    }
    fn yield_version(data: Self::Data) -> CliResult<Self> {
        Ok(CliCommand::Version(data))
    }
}

/*
    cli arg impl: multi command (subcommand)
*/

#[derive(Debug, PartialEq)]
pub enum CliMultiCommand<OptR: CliArgsOptions, OptS: CliArgsOptions> {
    Run(CliCommandData<OptR>),
    Help(CliCommandData<OptR>),
    Version(CliCommandData<OptR>),
    Subcommand(CliCommandData<OptR>, Subcommand<OptS>),
    SubcommandHelp(CliCommandData<OptR>, Subcommand<OptS>),
    SubcommandVersion(CliCommandData<OptR>, Subcommand<OptS>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Subcommand<Opt: CliArgsOptions> {
    name: String,
    settings: CliCommandData<Opt>,
}

impl<Opt: CliArgsOptions> Subcommand<Opt> {
    fn new(name: String, settings: CliCommandData<Opt>) -> Self {
        Self { name, settings }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn settings(&self) -> &CliCommandData<Opt> {
        &self.settings
    }
    pub fn settings_mut(&mut self) -> &mut CliCommandData<Opt> {
        &mut self.settings
    }
}

impl<OptR: CliArgsOptions, OptS: CliArgsOptions> CliArgsDecode for CliMultiCommand<OptR, OptS> {
    type Data = CliCommandData<OptR>;
    fn initialize<const SWITCH: bool>(iter: &mut impl Iterator<Item = impl ArgItem>) -> Self::Data {
        <CliCommand<OptR>>::initialize::<SWITCH>(iter)
    }
    fn push_flag(data: &mut Self::Data, flag: String) -> CliResult<()> {
        <CliCommand<OptR>>::push_flag(data, flag)
    }
    fn push_option(
        data: &mut Self::Data,
        option_name: String,
        option_value: String,
    ) -> CliResult<()> {
        <CliCommand<OptR>>::push_option(data, option_name, option_value)
    }
    fn yield_command(data: Self::Data) -> CliResult<Self> {
        Ok(Self::Run(data))
    }
    fn yield_help(data: Self::Data) -> CliResult<Self> {
        Ok(Self::Help(data))
    }
    fn yield_subcommand(
        data: Self::Data,
        subcommand: String,
        args: impl IntoIterator<Item = impl ArgItem>,
    ) -> CliResult<Self> {
        let subcommand_args = decode_args::<CliCommand<OptS>, false>(args)?;
        match subcommand_args {
            CliCommand::Run(subcommand_data) => Ok(CliMultiCommand::Subcommand(
                data,
                Subcommand::new(subcommand, subcommand_data),
            )),
            CliCommand::Help(subcommand_data) => Ok(CliMultiCommand::SubcommandHelp(
                data,
                Subcommand::new(subcommand, subcommand_data),
            )),
            CliCommand::Version(subcommand_data) => Ok(CliMultiCommand::SubcommandVersion(
                data,
                Subcommand::new(subcommand, subcommand_data),
            )),
        }
    }
    fn yield_version(data: Self::Data) -> CliResult<Self> {
        Ok(Self::Version(data))
    }
}

/*
    tests
*/

#[test]
fn command() {
    let cli = CliCommand::<SingleOption>::parse([
        "skyd",
        "--verify-cluster-seed-membership",
        "--auth-root-password",
        "mypassword12345678",
        "--tls-only",
        "--auth-plugin=pwd",
    ])
    .unwrap();
    assert_eq!(
        cli,
        CliCommand::Run(CliCommandData {
            options: [
                ("auth-root-password", "mypassword12345678"),
                ("auth-plugin", "pwd")
            ]
            .into_iter()
            .map(|(x, y)| (x.to_owned(), y.to_owned()))
            .collect(),
            flags: ["tls-only", "verify-cluster-seed-membership"]
                .into_iter()
                .map(|f| f.to_owned())
                .collect()
        })
    )
}

#[test]
fn command_multi() {
    let cli = CliCommand::<MultipleOptions>::parse([
        "skyd",
        "--verify-cluster-seed-membership",
        "--auth-root-password",
        "mypassword12345678",
        "--tls-only",
        "--auth-plugin=pwd",
        "--endpoint=tcp@localhost:2003",
        "--endpoint=tls@localhost:2004",
    ])
    .unwrap();
    assert_eq!(
        cli,
        CliCommand::Run(CliCommandData {
            options: [
                ("auth-root-password", &["mypassword12345678"][..]),
                ("auth-plugin", &["pwd"]),
                ("endpoint", &["tcp@localhost:2003", "tls@localhost:2004"])
            ]
            .into_iter()
            .map(|(x, y)| (x.to_owned(), y.iter().map(|x| x.to_string()).collect()))
            .collect(),
            flags: ["tls-only", "verify-cluster-seed-membership"]
                .into_iter()
                .map(|f| f.to_owned())
                .collect()
        })
    )
}

#[test]
fn subcommand() {
    let cli_input = [
        "skyd",
        "--verify-cluster-membership",
        "--compat-driver=v1",
        "restore",
        "--driver=v2",
        "--name",
        "myoldbackup",
        "--allow-different-host",
    ];
    let base_settings = CliCommandData {
        options: [("compat-driver", "v1")]
            .into_iter()
            .map(|(x, y)| (x.to_owned(), y.to_owned()))
            .collect(),
        flags: ["verify-cluster-membership"]
            .into_iter()
            .map(|f| f.to_owned())
            .collect(),
    };
    let expected_subcommand = Subcommand::new(
        "restore".to_owned(),
        CliCommandData {
            options: [("driver", "v2"), ("name", "myoldbackup")]
                .into_iter()
                .map(|(x, y)| (x.to_owned(), y.to_owned()))
                .collect(),
            flags: ["allow-different-host"]
                .into_iter()
                .map(|f| f.to_owned())
                .collect(),
        },
    );
    assert_eq!(
        CliMultiCommand::<SingleOption, SingleOption>::parse(cli_input).unwrap(),
        CliMultiCommand::Subcommand(base_settings.clone(), expected_subcommand.clone())
    );
    let cli_input = {
        let mut v = Vec::from(cli_input);
        v.push("-h");
        v
    };
    assert_eq!(
        CliMultiCommand::<SingleOption, SingleOption>::parse(cli_input).unwrap(),
        CliMultiCommand::SubcommandHelp(base_settings, expected_subcommand)
    )
}
