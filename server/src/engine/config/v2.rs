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
    crate::{engine::mem::AStr, util::os::SysIOError},
    serde::de,
    std::{
        collections::HashMap,
        env::{self, VarError},
        fmt, fs, io,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        ops,
        str::FromStr,
    },
};

pub type ConfigResult<T> = Result<T, ConfigError>;

const DEFAULT_SERVER_EP_INSECURE: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 2003));

/*
    configuration group
*/

sky_macros::config_group! {
    #[derive(Debug, PartialEq)]
    /// full configuration
    pub struct Configuration {
        /// system configuration
        pub system: #[derive(Debug, PartialEq)] pub struct SystemConfig {
            /// (reqd) default root password (unless modified) (--system-auth-default-root-password)
            pub auth_default_root_password: String,
            /// auth plugin (--system-auth-plugin)
            pub auth_plugin: #[derive(Debug, PartialEq)] pub enum AuthPlugin { Pwd } = AuthPlugin::Pwd,
            /// deploy mode (--system-deploy-mode)
            pub deploy_mode: #[derive(Debug, PartialEq)] pub enum SystemDeployMode { Dev, Prod } = SystemDeployMode::Dev,
            /// maximum transaction commit delay (--system-storage-max-commit-delay-ms)
            pub storage_max_commit_delay_ms: u64 = 300,
        }
        /// client-server settings
        pub server: #[derive(Debug, PartialEq)] pub struct ServerConfig {
            /// (reqd) client-server comm endpoint (--server-endpoint and/or --server-endpoint-tls)
            override impl pub endpoint: ServerEndpoint,
            /// maximum number of live connections until queuing begins
            pub max_connections: usize = 10000,
        }
        /// cluster settings
        pub cluster: #[derive(Debug, PartialEq, serde::Deserialize)] pub struct ClusterConfig {
            /// (reqd) the cluster communication port (--cluster-endpoint)
            pub endpoint: Endpoint,
            /// (reqd) the shared cluster secret (--cluster-shared-secret)
            pub shared_secret: ClusterSecret,
            /// (reqd) cluster seed peers (only used during initial bootstrap) (--cluster-seed-peers)
            pub seed_peers: ClusterSeedPeers,
        }
    }
}

/*
    impls for anonymous definitions
*/

impl FromStr for AuthPlugin {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pwd" => Ok(Self::Pwd),
            plugin => Err(format!("unknown auth plugin {plugin}")),
        }
    }
}
impl<'de> de::Deserialize<'de> for AuthPlugin {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct AuthPluginVisitor;
        impl<'de> de::Visitor<'de> for AuthPluginVisitor {
            type Value = AuthPlugin;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "auth plugin name")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                v.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_str(AuthPluginVisitor)
    }
}

impl FromStr for SystemDeployMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "dev" => Self::Dev,
            "prod" => Self::Prod,
            unknown_mode => return Err(format!("unknown deploy mode {unknown_mode}")),
        })
    }
}
impl<'de> de::Deserialize<'de> for SystemDeployMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct DeployModeVisitor;
        impl<'de> de::Visitor<'de> for DeployModeVisitor {
            type Value = SystemDeployMode;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "auth plugin name")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                v.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_str(DeployModeVisitor)
    }
}

/*
    config items
*/

#[derive(Debug, PartialEq)]
/// an endpoint type
pub enum Endpoint {
    /// insecure (tcp) endpoint
    Insecure(EndpointTcp),
    /// secure (tls) endpoint
    Secure(EndpointTls),
}
impl<'de> de::Deserialize<'de> for Endpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct EpVisitor;
        impl<'de> de::Visitor<'de> for EpVisitor {
            type Value = Endpoint;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a tcp or tls endpoint definition")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Endpoint::from_str(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(EpVisitor)
    }
}
impl FromStr for Endpoint {
    type Err = String;
    fn from_str(v: &str) -> Result<Self, Self::Err> {
        if v.starts_with("tcp@") {
            EndpointTcp::parse(v)
                .map(Endpoint::Insecure)
                .map_err(|e| e.to_string())
        } else if v.starts_with("tls") {
            EndpointTls::parse(v)
                .map(Endpoint::Secure)
                .map_err(|e| e.to_string())
        } else {
            Err(format!("unknown endpoint definition '{v:?}'"))
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ClusterSeedPeers(Vec<SocketAddr>);
impl FromStr for ClusterSeedPeers {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let peers: Vec<&str> = s.split(',').collect();
        let mut seed_peer_list = Vec::with_capacity(peers.len());
        for peer in peers {
            match peer.parse() {
                Ok(peer) => seed_peer_list.push(peer),
                Err(e) => return Err(format!("failed to parse peer socket address - {e}")),
            }
        }
        Ok(Self(seed_peer_list))
    }
}
impl<'de> de::Deserialize<'de> for ClusterSeedPeers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ClusterSeedPeerVisitor;
        impl<'de> de::Visitor<'de> for ClusterSeedPeerVisitor {
            type Value = ClusterSeedPeers;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a list of socket addresses")
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut peers = vec![];
                while let Some(peer) = seq.next_element::<SocketAddr>()? {
                    peers.push(peer);
                }
                Ok(ClusterSeedPeers(peers))
            }
        }
        deserializer.deserialize_seq(ClusterSeedPeerVisitor)
    }
}

#[derive(Debug, PartialEq)]
pub struct ClusterSecret(AStr<128>);
impl FromStr for ClusterSecret {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() == 128 {
            let astr = unsafe {
                // UNSAFE(@ohsayan): verified length above
                AStr::from_len_unchecked(s)
            };
            Ok(Self(astr))
        } else {
            Err(format!(
                "expected cluster secret length 128 but found length {}",
                s.len()
            ))
        }
    }
}
impl<'de> de::Deserialize<'de> for ClusterSecret {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ClusterSecretVisitor;
        impl<'de> de::Visitor<'de> for ClusterSecretVisitor {
            type Value = ClusterSecret;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("cluster secret of length 128 bytes")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.len() == 128 {
                    Ok(ClusterSecret(unsafe {
                        // UNSAFE(@ohsayan): verified length above
                        AStr::from_len_unchecked(v)
                    }))
                } else {
                    Err(E::invalid_length(
                        v.len(),
                        &"expected a string of length 128",
                    ))
                }
            }
        }
        let d = deserializer.deserialize_str(ClusterSecretVisitor)?;
        Ok(d)
    }
}

#[derive(Debug, PartialEq)]
/// client-server communication endpoint configuration
///
/// defined in:
/// - `SKYD_SERVER_ENDPOINT` and/or `SKYD_SERVER_ENDPOINT_TLS` OR
/// - `--server-endpoint` and/or `--server-endpoint-tls`
pub enum ServerEndpoint {
    /// insecure only (TCP)
    Insecure(EndpointTcp),
    /// secure only (TLS)
    Secure(EndpointTls),
    /// multi (TCP+TLS)
    Multi(EndpointTcp, EndpointTls),
}
impl ServerEndpoint {
    fn decode<T, U>(ep_tcp: Option<T>, ep_tls: Option<U>) -> ConfigResult<ConfigReturn<Self>>
    where
        T: AsRef<str>,
        U: AsRef<str>,
    {
        let tcp = match ep_tcp {
            Some(ep) => EndpointTcp::parse(ep.as_ref()).map(Some)?,
            None => None,
        };
        let tls = match ep_tls {
            Some(ep) => EndpointTls::parse(ep.as_ref()).map(Some)?,
            None => None,
        };
        Ok(match tcp {
            Some(tcp) => match tls {
                Some(tls) => ConfigReturn::Modified(Self::Multi(tcp, tls)),
                None => ConfigReturn::Modified(Self::Insecure(tcp)),
            },
            None => match tls {
                Some(tls) => ConfigReturn::Modified(Self::Secure(tls)),
                None => ConfigReturn::Unmodified(Self::Insecure(EndpointTcp {
                    sock: DEFAULT_SERVER_EP_INSECURE,
                })),
            },
        })
    }
}
impl ConfigGroupOverride for ServerEndpoint {
    fn from_cli(args: &mut ConfigMap, cpath: &str) -> ConfigResult<ConfigReturn<Self>> {
        let ep_tcp_key = format!("{cpath}-endpoint");
        let ep_tls_key = format!("{cpath}-endpoint-tls");
        let ep_tcp = args.take_opt::<String>(&ep_tcp_key)?;
        let ep_tls = args.take_opt::<String>(&ep_tls_key)?;
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
        let ep_tcp = args.take_opt::<String>(&ep_tcp_key)?;
        let ep_tls = args.take_opt::<String>(&ep_tls_key)?;
        Self::decode(ep_tcp, ep_tls)
    }
}

#[derive(Debug, PartialEq)]
/// tcp server endpoint
pub struct EndpointTcp {
    pub sock: SocketAddr,
}
impl EndpointTcp {
    fn parse(ep: &str) -> ConfigResult<Self> {
        // tcp@sockaddr
        if !ep.starts_with("tcp@") {
            return Err(ConfigError::ParseError(format!(
                "invalid protocol definition for TCP socket"
            )));
        }
        Ok(EndpointTcp {
            sock: (&ep[4..]).parse().map_err(|e| {
                ConfigError::ParseError(format!("invalid address for TCP socket - {e}"))
            })?,
        })
    }
}
impl<'de> de::Deserialize<'de> for EndpointTcp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ServerEndpointTcpVisitor;
        impl<'de> de::Visitor<'de> for ServerEndpointTcpVisitor {
            type Value = EndpointTcp;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "socket address")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                EndpointTcp::parse(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(ServerEndpointTcpVisitor)
    }
}

#[derive(Debug, PartialEq)]
/// tls server endpoint
pub struct EndpointTls {
    pub sock: SocketAddr,
    pub cert: Box<str>,
    pub key: Box<str>,
    pub pass: Box<str>,
}
impl EndpointTls {
    fn tls_dec_err<T>() -> ConfigResult<T> {
        Err(ConfigError::ParseError(format!(
            "invalid protocol definition for TLS socket"
        )))
    }
    fn parse(ep: &str) -> ConfigResult<Self> {
        if !ep.starts_with("tls:[") {
            return Self::tls_dec_err();
        }
        let ep = &ep[5..];
        let Some((tls_settings, tls_sockaddr)) = ep.split_once('@') else {
            return Self::tls_dec_err();
        };
        if !tls_settings.ends_with("]") {
            return Self::tls_dec_err();
        }
        let tls_settings = &tls_settings[..tls_settings.len() - 1];
        // now tls_settings should look like cert,key,pass and tls_sockaddr should just have the socket address
        let tls_settings: Vec<_> = tls_settings.split(',').collect();
        if tls_settings.len() != 3 {
            return Self::tls_dec_err();
        }
        let (cert, key, pass) = (tls_settings[0], tls_settings[1], tls_settings[2]);
        let cert =
            fs::read_to_string(cert).map_err(|e| ConfigError::io_error(e, "TLS certificate"))?;
        let key = fs::read_to_string(key).map_err(|e| ConfigError::io_error(e, "TLS key"))?;
        let pass =
            fs::read_to_string(pass).map_err(|e| ConfigError::io_error(e, "TLS key passphrase"))?;
        Ok(EndpointTls {
            sock: tls_sockaddr.parse().map_err(|e| {
                ConfigError::ParseError(format!("invalid address for TLS socket - {e}"))
            })?,
            cert: cert.into_boxed_str(),
            key: key.into_boxed_str(),
            pass: pass.into_boxed_str(),
        })
    }
}
impl<'de> de::Deserialize<'de> for EndpointTls {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct ServerEndpointTlsVisitor;
        impl<'de> de::Visitor<'de> for ServerEndpointTlsVisitor {
            type Value = EndpointTls;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "socket address")
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                EndpointTls::parse(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(ServerEndpointTlsVisitor)
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
impl<T> ConfigReturn<T> {
    fn into_inner(self) -> T {
        match self {
            Self::Modified(m) | Self::Unmodified(m) => m,
        }
    }
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
        super::Endpoint,
        crate::{
            engine::config::v2::{
                ConfigReturn, EndpointTcp, EndpointTls, ServerEndpoint, DEFAULT_SERVER_EP_INSECURE,
            },
            util::test_utils,
        },
        std::net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    };

    #[test]
    fn server_ep_decode_default() {
        assert_eq!(
            ServerEndpoint::decode(None::<&str>, None::<&str>).unwrap(),
            ConfigReturn::Unmodified(ServerEndpoint::Insecure(EndpointTcp {
                sock: DEFAULT_SERVER_EP_INSECURE
            }))
        )
    }
    #[test]
    fn server_ep_decode_insecure() {
        assert_eq!(
            ServerEndpoint::decode(Some("tcp@0.0.0.0:1600"), None::<&str>).unwrap(),
            ConfigReturn::Modified(ServerEndpoint::Insecure(EndpointTcp {
                sock: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 1600))
            }))
        )
    }
    #[test]
    fn server_ep_decode_secure() {
        test_utils::with_files(
            [
                "server_ep_decode_secure_cert",
                "server_ep_decode_secure_key",
                "server_ep_decode_secure_pass",
            ],
            |[cert_file, key_file, pass_file]| {
                assert_eq!(
                    ServerEndpoint::decode(
                        None::<&str>,
                        Some(format!(
                            "tls:[{cert_file},{key_file},{pass_file}]@127.0.0.1:2002"
                        ))
                    )
                    .unwrap(),
                    ConfigReturn::Modified(ServerEndpoint::Secure(EndpointTls {
                        sock: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 2002)),
                        cert: "".into(),
                        key: "".into(),
                        pass: "".into(),
                    }))
                )
            },
        )
    }
    #[test]
    fn serve_ep_decode_multi() {
        test_utils::with_files(
            [
                "server_ep_decode_multi_cert",
                "server_ep_decode_multi_key",
                "server_ep_decode_multi_pass",
            ],
            |[cert_file, key_file, pass_file]| {
                assert_eq!(
                    ServerEndpoint::decode(
                        Some("tcp@0.0.0.0:1600".to_owned()),
                        Some(format!(
                            "tls:[{cert_file},{key_file},{pass_file}]@127.0.0.1:2002"
                        ))
                    )
                    .unwrap(),
                    ConfigReturn::Modified(ServerEndpoint::Multi(
                        EndpointTcp {
                            sock: SocketAddr::V4(SocketAddrV4::new(
                                Ipv4Addr::new(0, 0, 0, 0),
                                1600
                            ))
                        },
                        EndpointTls {
                            sock: SocketAddr::V4(SocketAddrV4::new(
                                Ipv4Addr::new(127, 0, 0, 1),
                                2002
                            )),
                            cert: "".into(),
                            key: "".into(),
                            pass: "".into(),
                        }
                    ))
                )
            },
        )
    }
    #[test]
    fn t_ep_serde_de() {
        test_utils::with_files(
            [
                "t_ep_serde_de_key",
                "t_ep_serde_de_cert",
                "t_ep_serde_de_pass",
            ],
            |[certfile, keyfile, passfile]| {
                let x = format!("endpoint: tls:[{certfile},{keyfile},{passfile}]@127.0.0.1:2002");
                #[derive(Debug, PartialEq, serde::Deserialize)]
                struct Ep {
                    endpoint: Endpoint,
                }
                assert_eq!(
                    serde_yaml::from_str::<Ep>(&x).unwrap(),
                    Ep {
                        endpoint: Endpoint::Secure(EndpointTls {
                            sock: SocketAddr::V4(SocketAddrV4::new(
                                Ipv4Addr::new(127, 0, 0, 1),
                                2002
                            )),
                            cert: "".into(),
                            key: "".into(),
                            pass: "".into(),
                        })
                    }
                )
            },
        )
    }
}
