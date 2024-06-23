/*
 * Created on Fri Sep 15 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

pub mod protocol;

use {
    crate::engine::{
        config::ConfigEndpointTcp, error::RuntimeResult, fractal::error::ErrorContext,
        fractal::Global,
    },
    bytes::BytesMut,
    openssl::{
        pkey::PKey,
        ssl::Ssl,
        ssl::{SslAcceptor, SslMethod},
        x509::X509,
    },
    std::{cell::Cell, net::SocketAddr, pin::Pin, time::Duration},
    tokio::{
        io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter},
        net::{TcpListener, TcpStream},
        sync::{broadcast, mpsc, Semaphore},
    },
    tokio_openssl::SslStream,
};

pub trait Socket: AsyncWrite + AsyncRead + Unpin {}
pub type IoResult<T> = Result<T, std::io::Error>;

const BUF_WRITE_CAP: usize = 16384;
const BUF_READ_CAP: usize = 16384;
const CLIMIT: usize = 50000;

static CLIM: Semaphore = Semaphore::const_new(CLIMIT);

enum QueryLoopResult {
    Fin,
    Rst,
    HSFailed,
}

/*
    socket definitions
*/

impl Socket for TcpStream {}
impl Socket for SslStream<TcpStream> {}

struct NetBackoff {
    at: Cell<u8>,
}

impl NetBackoff {
    const BACKOFF_MAX: u8 = 64;
    fn new() -> Self {
        Self { at: Cell::new(1) }
    }
    async fn spin(&self) {
        let current = self.at.get();
        self.at.set(current << 1);
        tokio::time::sleep(Duration::from_secs(current as _)).await
    }
    fn should_disconnect(&self) -> bool {
        self.at.get() >= Self::BACKOFF_MAX
    }
}

unsafe impl Send for NetBackoff {}
unsafe impl Sync for NetBackoff {}

/*
    listener
*/

/// Connection handler for a remote connection
pub struct ConnectionHandler<S> {
    socket: BufWriter<S>,
    buffer: BytesMut,
    global: Global,
    sig_terminate: broadcast::Receiver<()>,
    _sig_inflight_complete: mpsc::Sender<()>,
}

impl<S: Socket> ConnectionHandler<S> {
    pub fn new(
        socket: S,
        global: Global,
        term_sig: broadcast::Receiver<()>,
        _inflight_complete: mpsc::Sender<()>,
    ) -> Self {
        Self {
            socket: BufWriter::with_capacity(BUF_WRITE_CAP, socket),
            buffer: BytesMut::with_capacity(BUF_READ_CAP),
            global,
            sig_terminate: term_sig,
            _sig_inflight_complete: _inflight_complete,
        }
    }
    pub async fn run(&mut self) -> IoResult<()> {
        let Self {
            socket,
            buffer,
            global,
            ..
        } = self;
        loop {
            tokio::select! {
                ret = protocol::query_loop(socket, buffer, global) => {
                    socket.flush().await?;
                    match ret {
                        Ok(QueryLoopResult::Fin) => return Ok(()),
                        Ok(QueryLoopResult::Rst) => error!("client io: connection reset"),
                        Ok(QueryLoopResult::HSFailed) => error!("client: client handshake failed"),
                        Err(e) => {
                            error!("client io: error while handling connection: {e}");
                            return Err(e);
                        }
                    }
                    return Ok(())
                },
                _ = self.sig_terminate.recv() => {
                    return Ok(());
                }
            }
        }
    }
}

/// A TCP listener bound to a socket
pub struct Listener {
    global: Global,
    listener: TcpListener,
    sig_shutdown: broadcast::Sender<()>,
    sig_inflight: mpsc::Sender<()>,
    sig_inflight_wait: mpsc::Receiver<()>,
}

impl Listener {
    pub async fn new_cfg(
        tcp: &ConfigEndpointTcp,
        global: Global,
        sig_shutdown: broadcast::Sender<()>,
    ) -> RuntimeResult<Self> {
        Self::new(tcp.host(), tcp.port(), global, sig_shutdown).await
    }
    pub async fn new(
        host: &str,
        port: u16,
        global: Global,
        sig_shutdown: broadcast::Sender<()>,
    ) -> RuntimeResult<Self> {
        let (sig_inflight, sig_inflight_wait) = mpsc::channel(1);
        let listener = TcpListener::bind((host, port))
            .await
            .set_dmsg(format!("failed to bind to port {host}:{port}"))?;
        Ok(Self {
            global,
            listener,
            sig_shutdown,
            sig_inflight,
            sig_inflight_wait,
        })
    }
    pub async fn terminate(self) {
        let Self {
            mut sig_inflight_wait,
            sig_inflight,
            sig_shutdown,
            ..
        } = self;
        drop(sig_shutdown);
        drop(sig_inflight); // could be that we are the only ones holding this lol
        let _ = sig_inflight_wait.recv().await; // wait
    }
    async fn accept(&mut self) -> IoResult<(TcpStream, SocketAddr)> {
        let backoff = NetBackoff::new();
        loop {
            match self.listener.accept().await {
                Ok(s) => return Ok(s),
                Err(e) => {
                    if backoff.should_disconnect() {
                        // that's enough of your crappy connection dear sir
                        return Err(e.into());
                    }
                }
            }
            backoff.spin().await;
        }
    }
    pub async fn listen_tcp(&mut self) {
        loop {
            // acquire a permit
            let permit = CLIM.acquire().await.unwrap();
            let (stream, _) = match self.accept().await {
                Ok(s) => s,
                Err(e) => {
                    /*
                        SECURITY: IGNORE THIS ERROR
                    */
                    warn!("tcp: failed to accept connection: {e}");
                    continue;
                }
            };
            let mut handler = ConnectionHandler::new(
                stream,
                self.global.clone(),
                self.sig_shutdown.subscribe(),
                self.sig_inflight.clone(),
            );
            tokio::spawn(async move {
                if let Err(e) = handler.run().await {
                    warn!("tcp: error handling client connection: {e}");
                }
            });
            // return the permit
            drop(permit);
        }
    }
    pub fn init_tls(
        tls_cert: &str,
        tls_priv_key: &str,
        tls_key_password: &str,
    ) -> RuntimeResult<SslAcceptor> {
        let build_acceptor = || {
            let cert = X509::from_pem(tls_cert.as_bytes())?;
            let priv_key = PKey::private_key_from_pem_passphrase(
                tls_priv_key.as_bytes(),
                tls_key_password.as_bytes(),
            )?;
            let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
            builder.set_certificate(&cert)?;
            builder.set_private_key(&priv_key)?;
            builder.check_private_key()?;
            Ok::<_, openssl::error::ErrorStack>(builder.build())
        };
        let acceptor = build_acceptor().set_dmsg("failed to initialize TLS socket")?;
        Ok(acceptor)
    }
    pub async fn listen_tls(&mut self, acceptor: &SslAcceptor) {
        loop {
            let stream = async {
                let (stream, _) = self.accept().await?;
                let ssl = Ssl::new(acceptor.context())?;
                let mut stream = SslStream::new(ssl, stream)?;
                Pin::new(&mut stream).accept().await?;
                RuntimeResult::Ok(stream)
            };
            let stream = match stream.await {
                Ok(s) => s,
                Err(e) => {
                    /*
                        SECURITY: Once again, ignore this error
                    */
                    warn!("tls: failed to accept connection: {e}");
                    continue;
                }
            };
            let mut handler = ConnectionHandler::new(
                stream,
                self.global.clone(),
                self.sig_shutdown.subscribe(),
                self.sig_inflight.clone(),
            );
            tokio::spawn(async move {
                if let Err(e) = handler.run().await {
                    warn!("tls: error handling client connection: {e}");
                }
            });
        }
    }
}
