/*
 * Created on Tue Aug 25 2020
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

//! This module contains automated tests for queries

#[macro_use]
mod macros;
#[cfg(not(feature = "persist-suite"))]
mod auth;
mod ddl_tests;
mod inspect_tests;
mod kvengine;
mod kvengine_encoding;
mod kvengine_list;
mod persist;
mod pipeline;
mod snapshot;

mod tls {
    use skytable::{query, Element};
    #[sky_macros::dbtest_func(tls_cert = "cert.pem", port = 2004)]
    async fn tls() {
        runeq!(
            con,
            query!("heya", "abcd"),
            Element::String("abcd".to_owned())
        );
    }
}

mod sys {
    use crate::protocol::{LATEST_PROTOCOL_VERSION, LATEST_PROTOCOL_VERSIONSTRING};
    use libsky::VERSION;
    use sky_macros::dbtest_func as dbtest;
    use skytable::{query, Element, RespCode};
    #[dbtest]
    async fn sys_info_aerr() {
        runeq!(
            con,
            query!("sys", "info"),
            Element::RespCode(RespCode::ActionError)
        );
        runeq!(
            con,
            query!(
                "sys",
                "info",
                "this is cool",
                "but why this extra argument?"
            ),
            Element::RespCode(RespCode::ActionError)
        )
    }
    #[dbtest]
    async fn sys_info_protocol() {
        runeq!(
            con,
            query!("sys", "info", "protocol"),
            Element::String(LATEST_PROTOCOL_VERSIONSTRING.to_owned())
        )
    }
    #[dbtest]
    async fn sys_info_protover() {
        runeq!(
            con,
            query!("sys", "info", "protover"),
            Element::Float(LATEST_PROTOCOL_VERSION)
        )
    }
    #[dbtest]
    async fn sys_info_version() {
        runeq!(
            con,
            query!("sys", "info", "version"),
            Element::String(VERSION.to_owned())
        )
    }
    #[dbtest]
    async fn sys_metric_aerr() {
        runeq!(
            con,
            query!("sys", "metric"),
            Element::RespCode(RespCode::ActionError)
        );
        runeq!(
            con,
            query!("sys", "metric", "health", "but why this extra argument?"),
            Element::RespCode(RespCode::ActionError)
        )
    }
    #[dbtest]
    async fn sys_metric_health() {
        runeq!(
            con,
            query!("sys", "metric", "health"),
            Element::String("good".to_owned())
        )
    }
    #[dbtest]
    async fn sys_storage_usage() {
        runmatch!(
            con,
            query!("sys", "metric", "storage"),
            Element::UnsignedInt
        )
    }
}
