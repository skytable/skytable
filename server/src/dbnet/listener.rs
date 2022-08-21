/*
 * Created on Sun Aug 21 2022
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

use {
    super::{
        tcp::{Listener, ListenerV1},
        tls::{SslListener, SslListenerV1},
    },
    crate::{
        auth::AuthProvider,
        config::{PortConfig, ProtocolVersion, SslOpts},
        corestore::Corestore,
        util::error::{Error, SkyResult},
        IoResult,
    },
    core::future::Future,
    std::{net::IpAddr, sync::Arc},
    tokio::{
        net::TcpListener,
        sync::{broadcast, mpsc, Semaphore},
    },
};

/// The base TCP listener
pub struct BaseListener {
    /// An atomic reference to the coretable
    pub db: Corestore,
    /// The auth provider
    pub auth: AuthProvider,
    /// The incoming connection listener (binding)
    pub listener: TcpListener,
    /// The maximum number of connections
    pub climit: Arc<Semaphore>,
    /// The shutdown broadcaster
    pub signal: broadcast::Sender<()>,
    // When all `Sender`s are dropped - the `Receiver` gets a `None` value
    // We send a clone of `terminate_tx` to each `CHandler`
    pub terminate_tx: mpsc::Sender<()>,
    pub terminate_rx: mpsc::Receiver<()>,
}

impl BaseListener {
    pub async fn init(
        db: &Corestore,
        auth: AuthProvider,
        host: IpAddr,
        port: u16,
        semaphore: Arc<Semaphore>,
        signal: broadcast::Sender<()>,
    ) -> SkyResult<Self> {
        let (terminate_tx, terminate_rx) = mpsc::channel(1);
        let listener = TcpListener::bind((host, port))
            .await
            .map_err(|e| Error::ioerror_extra(e, format!("binding to port {port}")))?;
        Ok(Self {
            db: db.clone(),
            auth,
            listener,
            climit: semaphore,
            signal,
            terminate_tx,
            terminate_rx,
        })
    }
    pub async fn release_self(self) {
        let Self {
            mut terminate_rx,
            terminate_tx,
            signal,
            ..
        } = self;
        drop(signal);
        drop(terminate_tx);
        let _ = terminate_rx.recv().await;
    }
}

/// Multiple Listener Interface
///
/// A `MultiListener` is an abstraction over an `SslListener` or a `Listener` to facilitate
/// easier asynchronous listening on multiple ports.
///
/// - The `SecureOnly` variant holds an `SslListener`
/// - The `InsecureOnly` variant holds a `Listener`
/// - The `Multi` variant holds both an `SslListener` and a `Listener`
///     This variant enables listening to both secure and insecure sockets at the same time
///     asynchronously
#[allow(clippy::large_enum_variant)]
pub enum MultiListener {
    SecureOnly(SslListener),
    SecureOnlyV1(SslListenerV1),
    InsecureOnly(Listener),
    InsecureOnlyV1(ListenerV1),
    Multi(Listener, SslListener),
    MultiV1(ListenerV1, SslListenerV1),
}

async fn wait_on_port_futures(
    a: impl Future<Output = IoResult<()>>,
    b: impl Future<Output = IoResult<()>>,
) -> IoResult<()> {
    let (e1, e2) = tokio::join!(a, b);
    if let Err(e) = e1 {
        log::error!("Insecure listener failed with: {}", e);
    }
    if let Err(e) = e2 {
        log::error!("Secure listener failed with: {}", e);
    }
    Ok(())
}

impl MultiListener {
    /// Create a new `InsecureOnly` listener
    pub fn new_insecure_only(base: BaseListener, protocol: ProtocolVersion) -> Self {
        match protocol {
            ProtocolVersion::V2 => MultiListener::InsecureOnly(Listener::new(base)),
            ProtocolVersion::V1 => MultiListener::InsecureOnlyV1(ListenerV1::new(base)),
        }
    }
    /// Create a new `SecureOnly` listener
    pub fn new_secure_only(
        base: BaseListener,
        ssl: SslOpts,
        protocol: ProtocolVersion,
    ) -> SkyResult<Self> {
        let listener = match protocol {
            ProtocolVersion::V2 => {
                let listener = SslListener::new_pem_based_ssl_connection(
                    ssl.key,
                    ssl.chain,
                    base,
                    ssl.passfile,
                )?;
                MultiListener::SecureOnly(listener)
            }
            ProtocolVersion::V1 => {
                let listener = SslListenerV1::new_pem_based_ssl_connection(
                    ssl.key,
                    ssl.chain,
                    base,
                    ssl.passfile,
                )?;
                MultiListener::SecureOnlyV1(listener)
            }
        };
        Ok(listener)
    }
    /// Create a new `Multi` listener that has both a secure and an insecure listener
    pub async fn new_multi(
        ssl_base_listener: BaseListener,
        tcp_base_listener: BaseListener,
        ssl: SslOpts,
        protocol: ProtocolVersion,
    ) -> SkyResult<Self> {
        let mls = match protocol {
            ProtocolVersion::V2 => {
                let secure_listener = SslListener::new_pem_based_ssl_connection(
                    ssl.key,
                    ssl.chain,
                    ssl_base_listener,
                    ssl.passfile,
                )?;
                let insecure_listener = Listener::new(tcp_base_listener);
                MultiListener::Multi(insecure_listener, secure_listener)
            }
            ProtocolVersion::V1 => {
                let secure_listener = SslListenerV1::new_pem_based_ssl_connection(
                    ssl.key,
                    ssl.chain,
                    ssl_base_listener,
                    ssl.passfile,
                )?;
                let insecure_listener = ListenerV1::new(tcp_base_listener);
                MultiListener::MultiV1(insecure_listener, secure_listener)
            }
        };
        Ok(mls)
    }
    /// Start the server
    ///
    /// The running of single and/or parallel listeners is handled by this function by
    /// exploiting the working of async functions
    pub async fn run_server(&mut self) -> IoResult<()> {
        match self {
            MultiListener::SecureOnly(secure_listener) => secure_listener.run().await,
            MultiListener::SecureOnlyV1(secure_listener) => secure_listener.run().await,
            MultiListener::InsecureOnly(insecure_listener) => insecure_listener.run().await,
            MultiListener::InsecureOnlyV1(insecure_listener) => insecure_listener.run().await,
            MultiListener::Multi(insecure_listener, secure_listener) => {
                wait_on_port_futures(insecure_listener.run(), secure_listener.run()).await
            }
            MultiListener::MultiV1(insecure_listener, secure_listener) => {
                wait_on_port_futures(insecure_listener.run(), secure_listener.run()).await
            }
        }
    }
    /// Signal the ports to shut down and only return after they have shut down
    ///
    /// **Do note:** This function doesn't flush the `Corestore` object! The **caller has to
    /// make sure that the data is saved!**
    pub async fn finish_with_termsig(self) {
        match self {
            MultiListener::InsecureOnly(Listener { base, .. })
            | MultiListener::SecureOnly(SslListener { base, .. })
            | MultiListener::InsecureOnlyV1(ListenerV1 { base, .. })
            | MultiListener::SecureOnlyV1(SslListenerV1 { base, .. }) => base.release_self().await,
            MultiListener::Multi(insecure, secure) => {
                insecure.base.release_self().await;
                secure.base.release_self().await;
            }
            MultiListener::MultiV1(insecure, secure) => {
                insecure.base.release_self().await;
                secure.base.release_self().await;
            }
        }
    }
}

/// Initialize the database networking
pub async fn connect(
    ports: PortConfig,
    protocol: ProtocolVersion,
    maxcon: usize,
    db: Corestore,
    auth: AuthProvider,
    signal: broadcast::Sender<()>,
) -> SkyResult<MultiListener> {
    let climit = Arc::new(Semaphore::new(maxcon));
    let base_listener_init = |host, port| {
        BaseListener::init(
            &db,
            auth.clone(),
            host,
            port,
            climit.clone(),
            signal.clone(),
        )
    };
    let description = ports.get_description();
    let server = match ports {
        PortConfig::InsecureOnly { host, port } => {
            MultiListener::new_insecure_only(base_listener_init(host, port).await?, protocol)
        }
        PortConfig::SecureOnly { host, ssl } => MultiListener::new_secure_only(
            base_listener_init(host, ssl.port).await?,
            ssl,
            protocol,
        )?,
        PortConfig::Multi { host, port, ssl } => {
            let secure_listener = base_listener_init(host, ssl.port).await?;
            let insecure_listener = base_listener_init(host, port).await?;
            MultiListener::new_multi(secure_listener, insecure_listener, ssl, protocol).await?
        }
    };
    log::info!("Server started on {description}");
    Ok(server)
}
