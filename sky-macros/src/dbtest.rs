/*
 * Created on Wed Nov 29 2023
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

use {
    crate::util::{self, AttributeKind},
    proc_macro::TokenStream,
    quote::quote,
    std::collections::HashMap,
    syn::{parse_macro_input, AttributeArgs, ItemFn},
};

/*
    host setup
*/

#[derive(Debug)]
enum DbTestClient {
    Skyhash,
    Tcp,
}

struct DbConfig {
    client: DbTestClient,
    port: u16,
    host: String,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            client: DbTestClient::Skyhash,
            port: libsky::test_utils::DEFAULT_PORT,
            host: libsky::test_utils::DEFAULT_HOST.into(),
        }
    }
}

/*
    client setup
*/

#[derive(Debug)]
struct ClientConfig {
    username: String,
    password: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            username: libsky::test_utils::DEFAULT_USER_NAME.into(),
            password: libsky::test_utils::DEFAULT_USER_PASS.into(),
        }
    }
}

/*
    test setup
*/

#[derive(Debug)]
enum TestStrategy {
    Standard,
    Relogin { username: String, password: String },
}

impl TestStrategy {
    fn is_relogin(&self) -> bool {
        matches!(self, TestStrategy::Relogin { .. })
    }
}

struct TestSetup {
    client: ClientConfig,
    db: DbConfig,
    strategy: TestStrategy,
}

fn parse_attrs(attrs: AttributeArgs) -> TestSetup {
    let mut db_config = DbConfig::default();
    let mut client_config = ClientConfig::default();
    let mut collected_attrs = HashMap::new();
    let mut strategy = TestStrategy::Standard;
    for attr in attrs {
        match util::extract_attribute(&attr) {
            AttributeKind::Pair(k, v) => {
                assert!(
                    collected_attrs.insert(k.to_string(), v).is_none(),
                    "duplicate key: {}",
                    k.to_string()
                );
                continue;
            }
            AttributeKind::NestedAttrs { name, attrs } => match name.to_string().as_str() {
                "switch_user" => {
                    if strategy.is_relogin() {
                        panic!("already set `switch_user` strategy");
                    }
                    let mut username = None;
                    let mut password = None;
                    for (key, data) in attrs.into_iter().map(AttributeKind::into_pair) {
                        match key.to_string().as_str() {
                            "username" => {
                                assert!(username.is_none(), "username already set");
                                username = Some(util::extract_str_from_lit(&data).unwrap());
                            }
                            "password" => {
                                assert!(password.is_none(), "password already set");
                                password = Some(util::extract_str_from_lit(&data).unwrap());
                            }
                            unknown_subattr => panic!(
                                "unknown sub-attribute for `switch_user`: `{unknown_subattr}`"
                            ),
                        }
                    }
                    assert!(username.is_some(), "username must be set");
                    strategy = TestStrategy::Relogin {
                        username: username.unwrap(),
                        password: password.unwrap_or(libsky::test_utils::DEFAULT_USER_PASS.into()),
                    };
                }
                unknown => panic!("unknown nested attribute `{unknown}`"),
            },
            AttributeKind::Path(_) | AttributeKind::Lit(_) => {
                panic!("unexpected tokens")
            }
        }
    }
    for (attr_name, attr_val) in collected_attrs {
        match attr_name.as_str() {
            "client" => match util::extract_str_from_lit(&attr_val).unwrap().as_str() {
                "skyhash" => db_config.client = DbTestClient::Skyhash,
                "tcp" => db_config.client = DbTestClient::Tcp,
                unknown_client => panic!("unknown client mode {unknown_client}"),
            },
            "port" => db_config.port = util::extract_int_from_lit(&attr_val).unwrap(),
            "host" => db_config.host = util::extract_str_from_lit(&attr_val).unwrap(),
            "username" => {
                assert!(
                    !strategy.is_relogin(),
                    "`username` makes no sense when used with strategy `switch_user`. instead, set dbtest(switch_user(username = ...))"
                );
                client_config.username = util::extract_str_from_lit(&attr_val).unwrap()
            }
            "password" => {
                assert!(
                    !strategy.is_relogin(),
                    "`password` makes no sense when used with strategy `switch_user`. instead, set dbtest(switch_user(password = ...))"
                );
                client_config.password = util::extract_str_from_lit(&attr_val).unwrap();
            }
            unknown_attr => panic!("unknown dbtest attribute `{unknown_attr}`"),
        }
    }
    TestSetup {
        client: client_config,
        db: db_config,
        strategy,
    }
}

pub fn dbtest(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = parse_macro_input!(attrs as AttributeArgs);
    let input_fn = parse_macro_input!(item as ItemFn);
    let TestSetup {
        client:
            ClientConfig {
                username: login_username,
                password: login_password,
            },
        db: DbConfig { client, port, host },
        strategy,
    } = parse_attrs(attr_args);

    let function_attrs = &input_fn.attrs;
    let function_vis = &input_fn.vis;
    let function_sig = &input_fn.sig;
    let function_block = &input_fn.block;

    let retfn = quote!(
        #(#function_attrs)* #function_vis #function_sig
    );
    let mut block = quote! {
        const __DBTEST_HOST: &str = #host;
        const __DBTEST_PORT: u16 = #port;
    };
    match strategy {
        TestStrategy::Standard => {
            block = quote! {
                #block
                /// username set by [`sky_macros::dbtest`]
                const __DBTEST_USER: &str = #login_username;
                /// password set by [`sky_macros::dbtest`]
                const __DBTEST_PASS: &str = #login_password;
            };
        }
        TestStrategy::Relogin {
            username: ref new_username,
            password: ref new_password,
        } => {
            // we need to create an user, log in and log out
            block = quote! {
                #block
                /// username set by [`sky_macros::dbtest`] (relogin)
                const __DBTEST_USER: &str = #new_username;
                /// password set by [`sky_macros::dbtest`] (relogin)
                const __DBTEST_PASS: &str = #new_password;
                {
                    let query_assembled = format!("sysctl create user {} with {{ password: ? }}", #new_username);
                    let mut db = skytable::Config::new(#host, #port, #login_username, #login_password).connect().unwrap();
                    db.query_parse::<()>(&skytable::query!(query_assembled, #new_password)).unwrap();
                }
            };
        }
    }
    match client {
        DbTestClient::Skyhash => {
            block = quote! {
                #block
                /// Get a Skyhash connection the database (defined by [`sky_macros::dbtest`])
                macro_rules! db {
                    () => {{
                        skytable::Config::new(__DBTEST_HOST, __DBTEST_PORT, __DBTEST_USER, __DBTEST_PASS).connect().unwrap()
                    }}
                }
            };
        }
        DbTestClient::Tcp => {
            block = quote! {
                #block
                /// Get a TCP connection the database (defined by [`sky_macros::dbtest`])
                macro_rules! tcp {
                    () => {{
                        std::net::TcpStream::connect((__DBTEST_HOST, __DBTEST_PORT)).unwrap()
                    }}
                }
            };
        }
    }
    let mut ret_block = quote! {
        #block
        #function_block
    };
    match strategy {
        TestStrategy::Relogin { ref username, .. } => {
            let new_username = username;
            ret_block = quote! {
                #ret_block
                {
                    let query_assembled = format!("sysctl drop user {}", #new_username);
                    let mut db = skytable::Config::new(#host, #port, #login_username, #login_password).connect().unwrap();
                    db.query_parse::<()>(&skytable::query!(query_assembled)).unwrap();
                }
            };
        }
        TestStrategy::Standard => {}
    }
    let ret = quote! {
        #[core::prelude::v1::test]
        #retfn {
            #ret_block
        }
    };
    ret.into()
}
