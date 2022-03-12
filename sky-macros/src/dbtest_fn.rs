/*
 * Created on Wed Mar 09 2022
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

use crate::util;
use proc_macro::TokenStream;
use quote::quote;
use syn::AttributeArgs;

type OptString = Option<String>;

pub struct DBTestFunctionConfig {
    table_decl: String,
    port: u16,
    host: String,
    tls_cert: OptString,
    login: (OptString, OptString),
    testuser: bool,
    rootuser: bool,
    norun: bool,
}

impl DBTestFunctionConfig {
    pub fn default() -> Self {
        Self {
            table_decl: "keymap(str,str)".to_owned(),
            port: 2003,
            host: "127.0.0.1".to_owned(),
            tls_cert: None,
            login: (None, None),
            testuser: false,
            rootuser: false,
            norun: false,
        }
    }
    pub fn get_connection_tokens(&self) -> impl quote::ToTokens {
        let DBTestFunctionConfig {
            port,
            host,
            tls_cert,
            ..
        } = &self;
        match tls_cert {
            Some(cert) => {
                quote! {
                    let certpath = ::std::format!("{}/{}", crate::ROOT_DIR, #cert);
                    let mut con = skytable::aio::TlsConnection::new(
                        #host, #port, &certpath
                    ).await.unwrap();
                }
            }
            None => quote! {
                let mut con = skytable::AsyncConnection::new(#host, #port).await.unwrap();
            },
        }
    }
    pub fn get_create_table_tokens(&self, table_name: &str) -> impl quote::ToTokens {
        let Self { table_decl, .. } = self;
        quote! {
            con.run_simple_query(
                &skytable::query!(
                    "create",
                    "table",
                    #table_name,
                    #table_decl,
                    "volatile"
                )
            ).await.unwrap()
        }
    }
    pub fn get_login_tokens(&self) -> Option<impl quote::ToTokens> {
        let Self {
            login,
            testuser,
            rootuser,
            ..
        } = self;
        let conflict = (*rootuser && *testuser)
            || ((*rootuser || *testuser) && (login.0.is_some() || login.1.is_some()));
        if conflict {
            panic!("Expected either of `username` and `password`, or `auth_rootuser`, or `auth_testuser`");
        }
        let ret;
        if *testuser {
            ret = quote! {
                let __username__ = crate::auth::provider::testsuite_data::TESTSUITE_TEST_USER;
                let __password__ = crate::auth::provider::testsuite_data::TESTSUITE_TEST_TOKEN;
            };
        } else if *rootuser {
            ret = quote! {
                let __username__ = crate::auth::provider::testsuite_data::TESTSUITE_ROOT_USER;
                let __password__ = crate::auth::provider::testsuite_data::TESTSUITE_ROOT_TOKEN;
            };
        } else {
            let (username, password) = login;
            match (username, password) {
                (Some(username), Some(password)) => {
                    ret = quote! {
                        let __username__ = #username;
                        let __password__ = #password;
                    }
                }
                (None, None) => return None,
                _ => panic!("Expected both `username` and `password`"),
            }
        }
        Some(quote! {
            #ret
            let __loginquery__ = ::skytable::query!("auth", "login", __username__, __password__);
            assert_eq!(
                con.run_simple_query(&__loginquery__).await.unwrap(),
                ::skytable::Element::RespCode(::skytable::RespCode::Okay),
                "Failed to login"
            );
        })
    }
}

pub fn parse_dbtest_func_args(
    arg: &str,
    lit: &syn::Lit,
    span: proc_macro2::Span,
    fcfg: &mut DBTestFunctionConfig,
) {
    match arg {
        "table" => {
            // check if the user wants some special table declaration
            fcfg.table_decl =
                util::parse_string(lit, span, "table").expect("Expected a value for `table`");
        }
        "port" => {
            // check if we need a custom port
            fcfg.port = util::parse_number(lit, span, "port").expect("Expected a u16");
        }
        "host" => {
            fcfg.host = util::parse_string(lit, span, "host").expect("Expected a string");
        }
        "tls_cert" => {
            fcfg.tls_cert = Some(util::parse_string(lit, span, "host").expect("Expected a string"));
        }
        "username" => {
            fcfg.login.0 =
                Some(util::parse_string(lit, span, "username").expect("Expected a string"))
        }
        "password" => {
            fcfg.login.1 =
                Some(util::parse_string(lit, span, "password").expect("Expected a string"))
        }
        "auth_testuser" => {
            fcfg.testuser = util::parse_bool(lit, span, "auth_testuser").expect("Expected a bool")
        }
        "auth_rootuser" => {
            fcfg.rootuser = util::parse_bool(lit, span, "auth_testuser").expect("Expected a bool")
        }
        "norun" => fcfg.norun = util::parse_bool(lit, span, "norun").expect("Expected a bool"),
        x => panic!("unknown attribute {x} specified"),
    }
}

/// This parses a function within a `dbtest` module
///
/// This accepts an `async` function and returns a non-`async` version of it - by
/// making the body of the function use the `tokio` runtime
fn generate_dbtest(
    mut input: syn::ItemFn,
    rng: &mut impl rand::Rng,
    fcfg: &DBTestFunctionConfig,
) -> Result<TokenStream, syn::Error> {
    let sig = &mut input.sig;
    let fname = sig.ident.to_string();
    let testbody = &input.block;
    let attrs = &input.attrs;
    let vis = &input.vis;
    let header = quote! {
        #[::core::prelude::v1::test]
    };
    if sig.asyncness.is_none() {
        let msg = "`dbtest` functions need to be async";
        return Err(syn::Error::new_spanned(sig.fn_token, msg));
    }
    sig.asyncness = None;
    let rand_string = util::get_rand_string(rng);
    let mut body = quote! {};

    // first add connection tokens
    let connection_tokens = fcfg.get_connection_tokens();
    body = quote! {
        #body
        #connection_tokens
    };

    // check if we need to log in
    if let Some(login_tokens) = fcfg.get_login_tokens() {
        body = quote! {
            #body
            #login_tokens
        };
    }

    if !fcfg.norun {
        // now create keyspace
        body = quote! {
            #body
            let __create_ks =
                con.run_simple_query(
                    &skytable::query!("create", "keyspace", "testsuite")
                ).await.unwrap();
            if !(
                __create_ks == skytable::Element::RespCode(skytable::RespCode::Okay) ||
                __create_ks == skytable::Element::RespCode(
                    skytable::RespCode::ErrorString(
                        skytable::error::errorstring::ERR_ALREADY_EXISTS.to_owned()
                    )
                )
            ) {
                panic!("Failed to create keyspace: {:?}", __create_ks);
            }
        };
        // now switch keyspace
        body = quote! {
            #body
            let __switch_ks =
                con.run_simple_query(
                    &skytable::query!("use", "testsuite")
                ).await.unwrap();
            if (__switch_ks != skytable::Element::RespCode(skytable::RespCode::Okay)) {
                panic!("Failed to switch keyspace: {:?}", __switch_ks);
            }
        };
        // now create table
        let create_table_tokens = fcfg.get_create_table_tokens(&rand_string);
        body = quote! {
            #body
            assert_eq!(
                #create_table_tokens,
                skytable::Element::RespCode(skytable::RespCode::Okay),
                "Failed to create table"
            );
        };
        // now generate the __MYENTITY__ string
        body = quote! {
            #body
            let mut __concat_entity = std::string::String::new();
            __concat_entity.push_str("testsuite:");
            __concat_entity.push_str(&#rand_string);
            let __MYENTITY__: String = __concat_entity.clone();
        };
        // now switch to the temporary table we created
        body = quote! {
            #body
            let __switch_entity =
                con.run_simple_query(
                    &skytable::query!("use", __concat_entity)
                ).await.unwrap();
            assert_eq!(
                __switch_entity, skytable::Element::RespCode(skytable::RespCode::Okay), "Failed to switch"
            );
        };
    }
    // now give the query ghost variable
    body = quote! {
        #body
        let mut query = skytable::Query::new();
    };
    // IMPORTANT: now append the actual test body
    body = quote! {
        #body
        #testbody
    };
    if !fcfg.norun {
        // now we're done with the test so flush the table
        body = quote! {
            #body
            {
                let mut __flush__ = skytable::Query::from("flushdb");
                std::assert_eq!(
                    con.run_simple_query(&__flush__).await.unwrap(),
                    skytable::Element::RespCode(skytable::RespCode::Okay)
                );
            }
        };
    }
    let result = quote! {
        #header
        #(#attrs)*
        #vis #sig {
            tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name(#fname)
            .thread_stack_size(3 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { #body });
        }
    };
    Ok(result.into())
}

/// This function checks if the current function is eligible to be a test and if so, returns
/// the generated test
pub fn generate_test(
    input: syn::ItemFn,
    rng: &mut impl rand::Rng,
    fcfg: &DBTestFunctionConfig,
) -> TokenStream {
    for attr in &input.attrs {
        if attr.path.is_ident("test") {
            let msg = "second test attribute is supplied";
            return syn::Error::new_spanned(&attr, msg)
                .to_compile_error()
                .into();
        }
    }

    if !input.sig.inputs.is_empty() {
        let msg = "the test function cannot accept arguments";
        return syn::Error::new_spanned(&input.sig.inputs, msg)
            .to_compile_error()
            .into();
    }
    generate_dbtest(input, rng, fcfg).unwrap_or_else(|e| e.to_compile_error().into())
}

fn parse_dbtest_func_attrs(attrs: AttributeArgs) -> DBTestFunctionConfig {
    let mut fcfg = DBTestFunctionConfig::default();
    attrs.iter().for_each(|arg| {
        if let syn::NestedMeta::Meta(syn::Meta::NameValue(namevalue)) = arg {
            let (ident, lit, span) = util::get_metanamevalue_data(&namevalue);
            parse_dbtest_func_args(&ident, lit, span, &mut fcfg)
        }
    });
    fcfg
}

pub fn dbtest_func(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    let attrs = syn::parse_macro_input!(args as AttributeArgs);
    let mut rng = rand::thread_rng();
    let fcfg = parse_dbtest_func_attrs(attrs);
    let func = generate_dbtest(input, &mut rng, &fcfg).unwrap();
    func
}
