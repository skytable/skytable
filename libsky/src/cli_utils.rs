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

/// typedef for errors from CLI arg parse
pub type CliResult<T> = Result<T, CliArgsError>;
/// allow single option for this type
pub type SingleOption = HashMap<String, String>;
/// allow multiple options for this type
pub type MultipleOptions = HashMap<String, Vec<String>>;

#[derive(Debug)]
/// errors from cli arg parse
pub enum CliArgsError {
    /// incorrectly formatted argument
    ArgFmtError(String),
    /// duplicate flag (when it is not allowed)
    DuplicateFlag(String),
    /// duplicate option (when it is not allowed)
    DuplicateOption(String),
    /// subcommand when it is not allowed
    SubcommandDisallowed,
    /// parse error (such as resolving to a type)
    ArgParseError(String),
    /// other custom error
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

/// a type of cli store
pub trait CliStore: Sized {
    /// the object containing the cli args data (flags, args, etc.)
    type CliArgsStoreBase;
    /// initialize an empty instance of this cli store
    fn initialize(iter: &mut impl Iterator<Item = impl AsArgItem>) -> Self::CliArgsStoreBase;
    /// add a flag to this store
    fn push_flag(data: &mut Self::CliArgsStoreBase, flag: String) -> CliResult<()>;
    /// add an option to this store
    fn push_option(
        data: &mut Self::CliArgsStoreBase,
        option_name: String,
        option_value: String,
    ) -> CliResult<()>;
    /// using the given base args store and subcommand name, return a subcommand instance for this cli store type
    fn yield_subcommand(
        data: Self::CliArgsStoreBase,
        subcommand: String,
        args: impl IntoIterator<Item = impl AsArgItem>,
    ) -> CliResult<Self>;
    /// using the given base args store, return a command instance for this cli store type
    fn yield_command(data: Self::CliArgsStoreBase) -> CliResult<Self>;
    /// using the given base args store, return a help instance for this cli store type
    fn yield_help(data: Self::CliArgsStoreBase) -> CliResult<Self>;
    /// using the given base args store, return a version instance for this cli store type
    fn yield_version(data: Self::CliArgsStoreBase) -> CliResult<Self>;
}

/// the top-level trait of [`CliStore`] which can load arguments from a given source
pub trait CommandLineArgs: Sized + CliStore {
    /// parse with the first argument skipped (if using direct-command line, for example)
    fn parse_skip(src: impl IntoIterator<Item = impl AsArgItem>) -> CliResult<Self> {
        let mut src = src.into_iter();
        let _ = src.next();
        Self::parse(src)
    }
    /// parse
    fn parse(src: impl IntoIterator<Item = impl AsArgItem>) -> CliResult<Self> {
        decode_args(src)
    }
    /// load and parse from env
    fn from_cli() -> CliResult<Self> {
        Self::parse(std::env::args())
    }
}

impl<T: Sized + CliStore> CommandLineArgs for T {}

/*
    helper traits
*/

/// any type representing an argument item (primarily used for testing)
pub trait AsArgItem {
    /// get this argument item as a borrowed string
    fn as_str(&self) -> &str;
    /// get this argument as an owned string
    fn boxed_str(self) -> String;
}

impl<'a> AsArgItem for &'a str {
    fn as_str(&self) -> &str {
        self
    }
    fn boxed_str(self) -> String {
        self.to_owned()
    }
}

impl AsArgItem for String {
    fn as_str(&self) -> &str {
        self
    }
    fn boxed_str(self) -> String {
        self
    }
}

/// the type of allowed CLI args (single, double, multiple, etc)
pub trait CliArgMap: Default {
    /// the value type (either a single type, or multiple, etc.)
    type Value;
    /// returns true if no options has been in this cli config map
    fn is_unset(&self) -> bool;
    /// add an option to this cli config map
    fn push_option(&mut self, option: String, value: String) -> CliResult<()>;
    /// remove an option from this
    fn take_option(&mut self, option: &str) -> Option<Self::Value>;
    /// check if a config item is present
    fn contains(&self, option: &str) -> bool;
}

impl CliArgMap for SingleOption {
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

impl CliArgMap for MultipleOptions {
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

/// decode args from an iterator
fn decode_args<C: CliStore>(src: impl IntoIterator<Item = impl AsArgItem>) -> CliResult<C> {
    let mut args = src.into_iter().peekable();
    let mut cli_data = C::initialize(&mut args);
    while let Some(arg) = args.next() {
        let arg = arg.as_str();
        let arg = if arg == "-h" || arg == "--help" {
            return C::yield_help(cli_data);
        } else if arg == "-v" || arg == "--version" {
            return C::yield_version(cli_data);
        } else {
            if arg.starts_with("--") {
                // option or flag
                &arg[2..]
            } else if arg.starts_with("-") {
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
            }
        };
        if arg.is_empty() {
            return Err(CliArgsError::ArgFmtError(format!("invalid argument")));
        }
        // is this arg in the --x=y format?
        let mut arg_split = arg.split("=");
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
                if arg_.as_str().starts_with("--") || arg_.as_str().starts_with("-") {
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
/// A simple cli command
pub enum CliExecSimple<Opt: CliArgMap> {
    /// help (`--help` or `-h`)
    Help(CliCommandData<Opt>),
    /// version (`--version` or `-v``)
    Version(CliCommandData<Opt>),
    /// run (any case excluding help/version)
    Run(CliCommandData<Opt>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CliCommandData<Opt: CliArgMap> {
    options: Opt,
    flags: HashSet<String>,
}

impl<Opt: CliArgMap> CliCommandData<Opt> {
    pub fn take_flag(&mut self, flag: &str) -> CliResult<bool> {
        if self.flags.remove(flag) {
            Ok(true)
        } else {
            if self.options.contains(flag) {
                Err(CliArgsError::Other(format!(
                    "expected `--{flag}` to be a flag but found an option"
                )))
            } else {
                Ok(false)
            }
        }
    }
    pub fn into_options_only(self) -> CliResult<Opt> {
        if self.flags.is_empty() {
            Ok(self.options)
        } else {
            Err(CliArgsError::Other(format!(
                "no flags were expected in this context"
            )))
        }
    }
    pub fn is_empty(&self) -> bool {
        self.options.is_unset() && self.flags.is_empty()
    }
    pub fn ensure_empty(&self) -> CliResult<()> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(CliArgsError::Other(format!(
                "found unknown flags or options",
            )))
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

impl<Opt: CliArgMap> CliStore for CliExecSimple<Opt> {
    type CliArgsStoreBase = CliCommandData<Opt>;
    fn initialize(_: &mut impl Iterator<Item = impl AsArgItem>) -> CliCommandData<Opt> {
        CliCommandData {
            options: Default::default(),
            flags: Default::default(),
        }
    }
    fn push_flag(data: &mut Self::CliArgsStoreBase, flag: String) -> CliResult<()> {
        if !data.flags.insert(flag.to_owned()) {
            return Err(CliArgsError::DuplicateFlag(flag.to_string()));
        }
        Ok(())
    }
    fn push_option(
        data: &mut Self::CliArgsStoreBase,
        option_name: String,
        option_value: String,
    ) -> CliResult<()> {
        data.options.push_option(option_name, option_value)
    }
    fn yield_subcommand(
        _: Self::CliArgsStoreBase,
        _: String,
        _: impl IntoIterator<Item = impl AsArgItem>,
    ) -> CliResult<Self> {
        return Err(CliArgsError::SubcommandDisallowed);
    }
    fn yield_command(data: Self::CliArgsStoreBase) -> CliResult<Self> {
        Ok(CliExecSimple::Run(data))
    }
    fn yield_help(data: Self::CliArgsStoreBase) -> CliResult<Self> {
        Ok(CliExecSimple::Help(data))
    }
    fn yield_version(data: Self::CliArgsStoreBase) -> CliResult<Self> {
        Ok(CliExecSimple::Version(data))
    }
}

/*
    cli arg impl: multi command (subcommand)
*/

#[derive(Debug, PartialEq)]
pub enum CliExecMulti<OptR: CliArgMap, OptS: CliArgMap> {
    Run(CliCommandData<OptR>),
    Help(CliCommandData<OptR>),
    Version(CliCommandData<OptR>),
    Subcommand(CliCommandData<OptR>, Subcommand<OptS>),
    SubcommandHelp(CliCommandData<OptR>, Subcommand<OptS>),
    SubcommandVersion(CliCommandData<OptR>, Subcommand<OptS>),
}

#[derive(Debug, PartialEq, Clone)]
/// a subcommands
pub struct Subcommand<Opt: CliArgMap> {
    /// name of the subcommand
    name: String,
    /// subcommand data
    settings: CliCommandData<Opt>,
}

impl<Opt: CliArgMap> Subcommand<Opt> {
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

impl<OptR: CliArgMap, OptS: CliArgMap> CliStore for CliExecMulti<OptR, OptS> {
    type CliArgsStoreBase = CliCommandData<OptR>;
    fn initialize(iter: &mut impl Iterator<Item = impl AsArgItem>) -> Self::CliArgsStoreBase {
        <CliExecSimple<OptR>>::initialize(iter)
    }
    fn push_flag(data: &mut Self::CliArgsStoreBase, flag: String) -> CliResult<()> {
        <CliExecSimple<OptR>>::push_flag(data, flag)
    }
    fn push_option(
        data: &mut Self::CliArgsStoreBase,
        option_name: String,
        option_value: String,
    ) -> CliResult<()> {
        <CliExecSimple<OptR>>::push_option(data, option_name, option_value)
    }
    fn yield_command(data: Self::CliArgsStoreBase) -> CliResult<Self> {
        Ok(Self::Run(data))
    }
    fn yield_help(data: Self::CliArgsStoreBase) -> CliResult<Self> {
        Ok(Self::Help(data))
    }
    fn yield_subcommand(
        data: Self::CliArgsStoreBase,
        subcommand: String,
        args: impl IntoIterator<Item = impl AsArgItem>,
    ) -> CliResult<Self> {
        let subcommand_args = decode_args::<CliExecSimple<OptS>>(args)?;
        match subcommand_args {
            CliExecSimple::Run(subcommand_data) => Ok(CliExecMulti::Subcommand(
                data,
                Subcommand::new(subcommand, subcommand_data),
            )),
            CliExecSimple::Help(subcommand_data) => Ok(CliExecMulti::SubcommandHelp(
                data,
                Subcommand::new(subcommand, subcommand_data),
            )),
            CliExecSimple::Version(subcommand_data) => Ok(CliExecMulti::SubcommandVersion(
                data,
                Subcommand::new(subcommand, subcommand_data),
            )),
        }
    }
    fn yield_version(data: Self::CliArgsStoreBase) -> CliResult<Self> {
        Ok(Self::Version(data))
    }
}

/*
    tests
*/

#[test]
fn command() {
    let cli = CliExecSimple::<SingleOption>::parse_skip([
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
        CliExecSimple::Run(CliCommandData {
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
    let cli = CliExecSimple::<MultipleOptions>::parse_skip([
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
        CliExecSimple::Run(CliCommandData {
            options: [
                ("auth-root-password", &["mypassword12345678"][..]),
                ("auth-plugin", &["pwd"]),
                ("endpoint", &["tcp@localhost:2003", "tls@localhost:2004"])
            ]
            .into_iter()
            .map(|(x, y)| (x.to_owned(), y.into_iter().map(|x| x.to_string()).collect()))
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
        CliExecMulti::<SingleOption, SingleOption>::parse_skip(cli_input).unwrap(),
        CliExecMulti::Subcommand(base_settings.clone(), expected_subcommand.clone())
    );
    let cli_input = {
        let mut v = Vec::from(cli_input);
        v.push("-h");
        v
    };
    assert_eq!(
        CliExecMulti::<SingleOption, SingleOption>::parse_skip(cli_input).unwrap(),
        CliExecMulti::SubcommandHelp(base_settings, expected_subcommand)
    )
}
