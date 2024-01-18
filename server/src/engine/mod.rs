/*
 * Created on Mon Sep 12 2022
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

#[macro_use]
mod macros;
mod config;
mod core;
mod data;
mod error;
mod fractal;
mod idx;
mod mem;
mod net;
mod ql;
mod storage;
mod sync;
mod txn;
// test
#[cfg(test)]
mod tests;
// re-export
pub use error::RuntimeResult;

use {
    self::{
        config::{ConfigEndpoint, ConfigEndpointTls, ConfigMode, ConfigReturn, Configuration},
        fractal::{
            context::{self, Subsystem},
            sys_store::SystemStore,
        },
        storage::{
            safe_interfaces::LocalFS,
            v1::loader::{self, SEInitState},
        },
    },
    crate::util::os::TerminationSignal,
    std::process::exit,
    tokio::sync::broadcast,
};

pub(super) fn set_context_init(msg: &'static str) {
    context::set(Subsystem::Init, msg)
}

/// Initialize all drivers, load all data
///
/// WARN: Must be in [`tokio::runtime::Runtime`] context!
pub fn load_all() -> RuntimeResult<(Configuration, fractal::GlobalStateStart)> {
    // load configuration
    info!("checking configuration ...");
    context::set(Subsystem::Init, "loading configuration");
    let config = match config::check_configuration()? {
        ConfigReturn::Config(cfg) => cfg,
        ConfigReturn::HelpMessage(msg) => {
            eprintln!("{msg}");
            exit(0x00);
        }
    };
    if config.mode == ConfigMode::Dev {
        warn!("running in dev mode");
    }
    // restore system database
    info!("loading system database ...");
    context::set_dmsg("loading system database");
    let (store, state) = SystemStore::<LocalFS>::open_or_restore(config.auth.clone(), config.mode)?;
    let sysdb_is_new = state.is_created();
    if state.is_existing_updated_root() {
        warn!("the root account was updated");
    }
    // now load all data
    if sysdb_is_new {
        info!("initializing storage engine ...");
    } else {
        info!("reinitializing storage engine...");
    }
    context::set_dmsg("restoring data");
    let SEInitState {
        txn_driver,
        model_drivers,
        gns,
    } = loader::SEInitState::try_init(sysdb_is_new)?;
    let global = unsafe {
        // UNSAFE(@ohsayan): this is the only entrypoint
        fractal::load_and_enable_all(gns, store, txn_driver, model_drivers)
    };
    Ok((config, global))
}

enum EndpointListeners {
    Insecure(net::Listener),
    Secure {
        listener: net::Listener,
        ssl: openssl::ssl::SslAcceptor,
    },
    Multi {
        tcp: net::Listener,
        tls: net::Listener,
        ssl: openssl::ssl::SslAcceptor,
    },
}

impl EndpointListeners {
    async fn listen(&mut self) {
        match self {
            Self::Insecure(l) => l.listen_tcp().await,
            Self::Secure { listener, ssl } => listener.listen_tls(ssl).await,
            Self::Multi { tcp, tls, ssl } => {
                tokio::join!(tcp.listen_tcp(), tls.listen_tls(ssl));
            }
        }
    }
    async fn finish(self) {
        match self {
            Self::Insecure(l) | Self::Secure { listener: l, .. } => l.terminate().await,
            Self::Multi { tcp, tls, .. } => {
                tokio::join!(tcp.terminate(), tls.terminate());
            }
        }
    }
}

pub async fn start(
    termsig: TerminationSignal,
    Configuration {
        endpoints, system, ..
    }: Configuration,
    fractal::GlobalStateStart { global, boot }: fractal::GlobalStateStart,
) -> RuntimeResult<()> {
    // create our system-wide channel
    let (signal, _) = broadcast::channel::<()>(1);
    // start our services
    context::set_dmsg("starting fractal engine");
    let fractal_handle = boot.boot(&signal, system.reliability_system_window);
    // create our server
    context::set(Subsystem::Network, "initializing endpoints");
    let str;
    let mut endpoint_handles = match &endpoints {
        ConfigEndpoint::Secure(ConfigEndpointTls { tcp, .. }) | ConfigEndpoint::Insecure(tcp) => {
            let listener =
                net::Listener::new(tcp.host(), tcp.port(), global.clone(), signal.clone()).await?;
            if let ConfigEndpoint::Secure(s) = endpoints {
                context::set_dmsg("initializing TLS");
                let acceptor = net::Listener::init_tls(s.cert(), s.private_key(), s.pkey_pass())?;
                str = format!("listening on tls@{}:{}", s.tcp().host(), s.tcp().port());
                EndpointListeners::Secure {
                    listener,
                    ssl: acceptor,
                }
            } else {
                str = format!("listening on tcp@{}:{}", tcp.host(), tcp.port());
                EndpointListeners::Insecure(listener)
            }
        }
        ConfigEndpoint::Multi(insecure_ep, secure_ep) => {
            let tcp_listener =
                net::Listener::new_cfg(insecure_ep, global.clone(), signal.clone()).await?;
            let tls_listener =
                net::Listener::new_cfg(secure_ep.tcp(), global.clone(), signal.clone()).await?;
            context::set_dmsg("initializing TLS");
            let acceptor = net::Listener::init_tls(
                secure_ep.cert(),
                secure_ep.private_key(),
                secure_ep.pkey_pass(),
            )?;
            str = format!(
                "listening on tcp@{}:{} and tls@{}:{}",
                insecure_ep.host(),
                insecure_ep.port(),
                secure_ep.tcp().host(),
                secure_ep.tcp().port()
            );
            EndpointListeners::Multi {
                tcp: tcp_listener,
                tls: tls_listener,
                ssl: acceptor,
            }
        }
    };
    info!("{str}");
    tokio::select! {
        _ = endpoint_handles.listen() => {}
        _ = termsig => {
            info!("received terminate signal. waiting for inflight tasks to complete ...");
        }
    }
    drop(signal);
    endpoint_handles.finish().await;
    info!("waiting for fractal engine to exit ...");
    let (hp_handle, lp_handle) = tokio::join!(fractal_handle.hp_handle, fractal_handle.lp_handle);
    match (hp_handle, lp_handle) {
        (Err(e1), Err(e2)) => {
            error!("error while terminating fhp-executor and lhp-executor: {e1};{e2}")
        }
        (Err(e), _) => error!("error while terminating fhp-executor: {e}"),
        (_, Err(e)) => error!("error while terminating flp-executor: {e}"),
        _ => {}
    }
    Ok(())
}

pub fn finish(g: fractal::Global) {
    unsafe {
        // UNSAFE(@ohsayan): the only thing we do before exit
        g.unload_all();
    }
}
