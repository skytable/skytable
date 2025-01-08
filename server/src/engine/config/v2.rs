/*
 * This file is a part of Skytable
 *
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2025, Sayan Nandan <nandansayan@outlook.com>
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

#![allow(dead_code)]

use {
    crate::util::os::SysIOError,
    std::{
        collections::HashMap,
        env::{self, VarError},
        fmt, fs, io,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        ops,
        str::FromStr,
    },
};

type ConfigResult<T> = Result<T, ConfigError>;

const DEFAULT_EP: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 2003));

/*
    configuration group
*/

sky_macros::config_group! {
    #[derive(Debug, PartialEq)]
    /// full configuration
    pub struct Configuration {
        /// client-server settings
        server:
            #[derive(Debug, PartialEq)]
            struct ServerConfig {
                /// client-server comm endpoint
                override impl endpoint: ServerEndpoint,
            }
    }
}

/*
    config items
*/

#[derive(Debug, PartialEq)]
/// tcp server endpoint
pub struct ServerEndpointTcp {
    pub sock: SocketAddr,
}

#[derive(Debug, PartialEq)]
/// tls server endpoint
pub struct ServerEndpointTls {
    pub sock: SocketAddr,
    pub cert: Box<str>,
    pub key: Box<str>,
    pub pass: Box<str>,
}

#[derive(Debug, PartialEq)]
/// client-server communication endpoint configuration
///
/// defined in:
/// - `SKYD_SERVER_ENDPOINT` and/or `SKYD_SERVER_ENDPOINT_TLS` OR
/// - `--server-endpoint` and/or `--server-endpoint-tls`
pub enum ServerEndpoint {
    /// insecure only (TCP)
    Insecure(ServerEndpointTcp),
    /// secure only (TLS)
    Secure(ServerEndpointTls),
    /// multi (TCP+TLS)
    Multi(ServerEndpointTcp, ServerEndpointTls),
}

impl ServerEndpoint {
    fn decode(ep_tcp: Option<String>, ep_tls: Option<String>) -> ConfigResult<ConfigReturn<Self>> {
        let tls_dec_err = || {
            Err(ConfigError::ParseError(format!(
                "invalid protocol definition for TLS socket"
            )))
        };
        let tcp;
        match ep_tcp {
            Some(ep) => {
                // tcp@sockaddr
                if !ep.starts_with("tcp@") {
                    return Err(ConfigError::ParseError(format!(
                        "invalid protocol definition for TCP socket"
                    )));
                }
                tcp = Some(ServerEndpointTcp {
                    sock: (&ep[4..]).parse().map_err(|e| {
                        ConfigError::ParseError(format!("invalid address for TCP socket - {e}"))
                    })?,
                });
            }
            None => tcp = None,
        }
        let tls;
        match ep_tls {
            Some(ep) => {
                if !ep.starts_with("tls:[") {
                    return tls_dec_err();
                }
                let ep = &ep[5..];
                let Some((tls_settings, tls_sockaddr)) = ep.split_once('@') else {
                    return tls_dec_err();
                };
                if !tls_settings.ends_with("]") {
                    return tls_dec_err();
                }
                let tls_settings = &tls_settings[..tls_settings.len() - 1];
                // now tls_settings should look like cert,key,pass and tls_sockaddr should just have the socket address
                let tls_settings: Vec<_> = tls_settings.split(',').collect();
                if tls_settings.len() != 3 {
                    return tls_dec_err();
                }
                let (cert, key, pass) = (tls_settings[0], tls_settings[1], tls_settings[2]);
                let cert = fs::read_to_string(cert)
                    .map_err(|e| ConfigError::io_error(e, "TLS certificate"))?;
                let key =
                    fs::read_to_string(key).map_err(|e| ConfigError::io_error(e, "TLS key"))?;
                let pass = fs::read_to_string(pass)
                    .map_err(|e| ConfigError::io_error(e, "TLS key passphrase"))?;
                tls = Some(ServerEndpointTls {
                    sock: tls_sockaddr.parse().map_err(|e| {
                        ConfigError::ParseError(format!("invalid address for TLS socket - {e}"))
                    })?,
                    cert: cert.into_boxed_str(),
                    key: key.into_boxed_str(),
                    pass: pass.into_boxed_str(),
                });
            }
            None => {
                tls = None;
            }
        }
        Ok(match tcp {
            Some(tcp) => match tls {
                Some(tls) => ConfigReturn::Modified(Self::Multi(tcp, tls)),
                None => ConfigReturn::Modified(Self::Insecure(tcp)),
            },
            None => match tls {
                Some(tls) => ConfigReturn::Modified(Self::Secure(tls)),
                None => {
                    ConfigReturn::Unmodified(Self::Insecure(ServerEndpointTcp { sock: DEFAULT_EP }))
                }
            },
        })
    }
}

impl ConfigGroupOverride for ServerEndpoint {
    fn from_cli(args: &mut ConfigMap, cpath: &str) -> ConfigResult<ConfigReturn<Self>> {
        let ep_tcp_key = format!("{cpath}-endpoint");
        let ep_tls_key = format!("{cpath}-endpoint-tls");
        let ep_tcp = args.take_opt(&ep_tcp_key)?;
        let ep_tls = args.take_opt(&ep_tls_key)?;
        Self::decode(ep_tcp, ep_tls)
    }
    fn from_env(cpath: &str) -> ConfigResult<ConfigReturn<Self>> {
        let ep_tcp_key = format!("{cpath}_ENDPOINT");
        let ep_tls_key = format!("{cpath}_ENDPOINT_TLS");
        let ep_tcp = get_var(&ep_tcp_key)?;
        let ep_tls = get_var(&ep_tls_key)?;
        Self::decode(ep_tcp, ep_tls)
    }
    fn from_env_test(args: &mut ConfigMap, cpath: &str) -> ConfigResult<ConfigReturn<Self>> {
        let ep_tcp_key = format!("{cpath}_ENDPOINT");
        let ep_tls_key = format!("{cpath}_ENDPOINT_TLS");
        let ep_tcp = args.take_opt(&ep_tcp_key)?;
        let ep_tls = args.take_opt(&ep_tls_key)?;
        Self::decode(ep_tcp, ep_tls)
    }
}

/*
    config core traits and objects
*/

/// a configuration group
trait ConfigGroup: Sized {
    /// parse this configuration group using the provided CLI args
    fn from_cli(args: &mut ConfigMap) -> ConfigResult<ConfigReturn<Self>>;
    /// parse this configuration group using env vars
    fn from_env() -> ConfigResult<ConfigReturn<Self>>;
    /// parse this configuration group using the provided vars
    fn from_env_test(args: &mut ConfigMap) -> ConfigResult<ConfigReturn<Self>>;
}

/// similar to [`ConfigGroup`], this trait is to be used when a config group is more complex to decode
/// and needs to access, for example, multiple variables
trait ConfigGroupOverride: Sized {
    /// parse from cli args
    fn from_cli(args: &mut ConfigMap, cpath: &str) -> ConfigResult<ConfigReturn<Self>>;
    /// parse from env vars
    fn from_env(cpath: &str) -> ConfigResult<ConfigReturn<Self>>;
    /// parse from provided vars
    fn from_env_test(args: &mut ConfigMap, cpath: &str) -> ConfigResult<ConfigReturn<Self>>;
}

/// a map of key value pairs of configuration options
struct ConfigMap(HashMap<String, String>);
impl ConfigMap {
    /// take this config option (required key)
    fn take<T: FromStr>(&mut self, key: &str) -> ConfigResult<T>
    where
        T::Err: fmt::Display,
    {
        match self.0.remove(key) {
            Some(val) => match val.parse() {
                Ok(val) => Ok(val),
                Err(e) => Err(ConfigError::parse_error(key, e)),
            },
            None => Err(ConfigError::Required(key.to_owned())),
        }
    }
    /// take this config option (optional key)
    fn take_opt<T: FromStr>(&mut self, key: &str) -> ConfigResult<Option<T>>
    where
        T::Err: fmt::Display,
    {
        match self.0.remove(key) {
            Some(val) => match val.parse() {
                Ok(val) => Ok(Some(val)),
                Err(e) => Err(ConfigError::parse_error(key, e)),
            },
            None => Ok(None),
        }
    }
}

impl Drop for ConfigMap {
    fn drop(&mut self) {
        assert!(self.0.is_empty(), "all args not checked")
    }
}

/*
    errors
*/

#[derive(Debug)]
/// errors resulting from parsing and evaluating configuration options
pub enum ConfigError {
    /// the configuration item was required but is missing
    Required(String),
    /// failed to parse the configuration item
    ParseError(String),
    /// I/O error while fetching configuration resource,
    IoError(SysIOError, String),
}

impl ConfigError {
    fn parse_error(item: impl AsRef<str>, err: impl fmt::Display) -> Self {
        Self::ParseError(format!(
            "failed to parse value for `{}` - {}",
            item.as_ref(),
            err
        ))
    }
    fn io_error(err: io::Error, description: impl AsRef<str>) -> Self {
        Self::IoError(err.into(), description.as_ref().to_owned())
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Required(reqd_val) => {
                write!(f, "the value `{reqd_val}` is required but was not provided")
            }
            Self::ParseError(pe) => write!(f, "{pe}"),
            Self::IoError(ioe, dscr) => {
                write!(f, "i/o error while fetching resource {dscr} - {ioe}")
            }
        }
    }
}

/*
    utils
*/

fn get_var(v: &str) -> ConfigResult<Option<String>> {
    Ok(match env::var(v) {
        Ok(v) => Some(v),
        Err(VarError::NotPresent) => None,
        Err(VarError::NotUnicode(e)) => {
            return Err(ConfigError::ParseError(format!(
                "failed to parse value for env var `{v}` - {e}",
                e = e.to_string_lossy()
            )))
        }
    })
}

#[derive(Debug, PartialEq)]
/// Modified or unmodified [`ConfigItem`]
enum ConfigReturn<T> {
    Modified(T),
    Unmodified(T),
}

impl<T> ops::Deref for ConfigReturn<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Modified(m) | Self::Unmodified(m) => m,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            engine::config::v2::{
                ConfigReturn, ServerEndpoint, ServerEndpointTcp, ServerEndpointTls, DEFAULT_EP,
            },
            util::test_utils,
        },
        std::net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    };

    #[test]
    fn server_ep_decode_default() {
        assert_eq!(
            ServerEndpoint::decode(None, None).unwrap(),
            ConfigReturn::Unmodified(ServerEndpoint::Insecure(ServerEndpointTcp {
                sock: DEFAULT_EP
            }))
        )
    }
    #[test]
    fn server_ep_decode_insecure() {
        assert_eq!(
            ServerEndpoint::decode(Some("tcp@0.0.0.0:1600".to_owned()), None).unwrap(),
            ConfigReturn::Modified(ServerEndpoint::Insecure(ServerEndpointTcp {
                sock: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 1600))
            }))
        )
    }
    #[test]
    fn server_ep_decode_secure() {
        test_utils::with_files(["cert.pem", "key.pem", "pass.txt"], |_| {
            assert_eq!(
                ServerEndpoint::decode(
                    None,
                    Some("tls:[cert.pem,key.pem,pass.txt]@127.0.0.1:2002".to_owned())
                )
                .unwrap(),
                ConfigReturn::Modified(ServerEndpoint::Secure(ServerEndpointTls {
                    sock: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 2002)),
                    cert: "".into(),
                    key: "".into(),
                    pass: "".into(),
                }))
            )
        })
    }
    #[test]
    fn serve_ep_decode_multi() {
        test_utils::with_files(["cert.pem", "key.pem", "pass.txt"], |_| {
            assert_eq!(
                ServerEndpoint::decode(
                    Some("tcp@0.0.0.0:1600".to_owned()),
                    Some("tls:[cert.pem,key.pem,pass.txt]@127.0.0.1:2002".to_owned())
                )
                .unwrap(),
                ConfigReturn::Modified(ServerEndpoint::Multi(
                    ServerEndpointTcp {
                        sock: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 1600))
                    },
                    ServerEndpointTls {
                        sock: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 2002)),
                        cert: "".into(),
                        key: "".into(),
                        pass: "".into(),
                    }
                ))
            )
        })
    }
}
