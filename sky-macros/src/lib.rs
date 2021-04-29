/*
 * Created on Sun Sep 13 2020
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

//! A library containing a collection of custom derives used by Skytable
//!
//! ## Ghost values
//! We extensively use jargon like 'Ghost values'...but what exactly are they?
//! Ghost values are variables which are provided by the compiler macros, i.e the
//! _proc macros_. These values are just like normal variables except for the fact
//! that they aren't explicitly declared in code, and should be used directly. Make
//! sure that you don't overwrite a macro provided variable!
//!
//! ### Macros and ghost values
//! - `#[dbtest]`:
//!     - `stream` - `tokio::net::TcpListener`
//!     - `asyncdb` - `sdb::coredb::CoreDB`

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use rand::*;
use std::collections::HashSet;
use syn::{self};

/// This parses a function within a `dbtest` module
///
/// This accepts an `async` function and returns a non-`async` version of it - by
/// making the body of the function use the `tokio` runtime
fn parse_dbtest(mut input: syn::ItemFn, rand: u16) -> Result<TokenStream, syn::Error> {
    let sig = &mut input.sig;
    let fname = sig.ident.to_string();
    let body = &input.block;
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
    let body = quote! {
        let asyncdb = crate::coredb::CoreDB::new_empty(0, std::sync::Arc::new(None));
        let addr = crate::tests::start_test_server(#rand, Some(asyncdb.clone())).await;
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        #body
        stream.shutdown().await.unwrap();
        asyncdb.finish_db();
        drop(asyncdb);
    };
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

/// This function checks if the current function is eligible to be a test
fn parse_test_sig(input: syn::ItemFn, rand: u16) -> TokenStream {
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
    parse_dbtest(input, rand).unwrap_or_else(|e| e.to_compile_error().into())
}

/// This function accepts an entire module which comprises of `dbtest` functions.
/// It takes each function in turn, and generates `#[test]`able functions for them
fn parse_test_module(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemMod);
    let content = match input.content {
        Some((_, c)) => c,
        None => {
            return syn::Error::new_spanned(&input, "Couldn't get the module content")
                .to_compile_error()
                .into()
        }
    };
    let args = syn::parse_macro_input!(args as syn::AttributeArgs);
    let mut skips = Vec::new();
    for arg in args {
        match arg {
            syn::NestedMeta::Meta(syn::Meta::NameValue(namevalue)) => {
                let ident = namevalue.path.get_ident();
                if ident.is_none() {
                    let msg = "Must have specified ident";
                    return syn::Error::new_spanned(namevalue, msg)
                        .to_compile_error()
                        .into();
                }
                match ident.unwrap().to_string().to_lowercase().as_str() {
                    "skip" => {
                        let skip_lit = namevalue.lit.clone();
                        let span = skip_lit.span();
                        skips = match parse_string(skip_lit, span, "skip") {
                            Ok(s) => s,
                            Err(_) => {
                                return syn::Error::new_spanned(
                                    namevalue,
                                    "Expected a value for argument `skip`",
                                )
                                .to_compile_error()
                                .into();
                            }
                        }
                        .split_whitespace()
                        .map(|val| val.to_string())
                        .collect();
                    }
                    x => {
                        let msg = format!("Unknown attribute {} is specified; expected `skip`", x);
                        return syn::Error::new_spanned(namevalue, msg)
                            .to_compile_error()
                            .into();
                    }
                }
            }
            _ => (),
        }
    }
    let modname = &input.ident;
    if modname.to_string() != "__private" {
        return syn::Error::new_spanned(
            modname,
            "By convention, all the modules using the `dbtest` macro have to be called `__private`",
        )
        .to_compile_error()
        .into();
    }
    let mut rng = thread_rng();
    let mut in_set = HashSet::<u16>::new();
    /*
     * As per [this comment](https://github.com/actions/virtual-environments/issues/3275#issuecomment-828214572)
     * from the GitHub Actions team, Windows reserves several ports. As our runners are currently hosted on GHA which use Hyper-V VMs
     * these ports will be blocked too and thse blocks are the reasons behind spurious test failures on Windows.
     * As a consequence to this, we will exclude these port ranges from the random port allocation set
     * (by setting them to 'already used' or 'already in in_set').
     */
    #[cfg(windows)]
    add_reserved_ports(&mut in_set);
    let mut result = quote! {};
    for item in content {
        // We set the port range to the 'dynamic port range' as per IANA's allocation guidelines
        let mut rand: u16 = rng.gen_range(49152..=65535);
        while in_set.contains(&rand) {
            rand = rng.gen_range(49152..=65535);
        }
        in_set.insert(rand);
        match item {
            // We just care about functions, so parse functions and ignore everything
            // else
            syn::Item::Fn(function) => {
                if skips.contains(&function.sig.ident.to_string()) {
                    result = quote! {
                        #result
                        #function
                    };
                    continue;
                }
                let inp = parse_test_sig(function, rand);
                let __tok: syn::ItemFn = syn::parse_macro_input!(inp as syn::ItemFn);
                let tok = quote! {
                    #__tok
                };
                result = quote! {
                    #result
                    #tok
                };
            }
            token => {
                result = quote! {
                    #result
                    #token
                };
            }
        }
    }
    result.into()
}

fn parse_string(int: syn::Lit, span: Span, field: &str) -> Result<String, syn::Error> {
    match int {
        syn::Lit::Str(s) => Ok(s.value()),
        syn::Lit::Verbatim(s) => Ok(s.to_string()),
        _ => Err(syn::Error::new(
            span,
            format!("Failed to parse {} into a string.", field),
        )),
    }
}

#[proc_macro_attribute]
/// The `dbtest` macro starts an async server in the background and is meant for
/// use within the `sdb` or `WORKSPACEROOT/server/` crate. If you use this compiler
/// macro in any other crate, you'll simply get compilation errors
///
/// ## _Ghost_ values
/// This macro gives a `tokio::net::TcpStream` accessible by the `stream` variable and a `sdb::coredb::CoreDB`
/// accessible by the `asyncdb` variable.
///
/// ## Requirements
///
/// The `#[dbtest]` macro expects several things. The calling crate:
/// - should have the `tokio` crate as a dependency and should have the
/// `features` set to full
/// - should have a function to start an async test server, available with the following path:
/// `crate::tests::start_test_server` which accepts an `u16` as the port number
///
/// ## Conventions
/// Since `proc_macro` cannot accept _file-linked_ modules and only accepts inline modules, we have made a workaround, which
/// has led to making this a _convention_.
/// So let's say we have a module `kvengine` in which we have our tests. So, we'll have to wrap around all these test functions
/// in a module `__private` within `kvengine`
///
pub fn dbtest(args: TokenStream, item: TokenStream) -> TokenStream {
    parse_test_module(args, item)
}

#[cfg(windows)]
/// We will parse the output from `netsh interface ipv4 show excludedportrange protocol=tcp` on Windows
/// We will then use this to add the port ranges to our `in_set` to not use them
///
/// This is what a typical output of the above command looks like:
/// ```text
///
/// Protocol tcp Port Exclusion Ranges
///
/// Start Port    End Port
/// ----------    --------
///       8501        8501
///      47001       47001
///
/// * - Administered port exclusions.
///
/// ```
/// So, we first ignore all empty lines and then validate the headers (i.e "start port", "end port", "protocol tcp", etc)
/// and then once that's all good -- we parse the start and end ports and then turn it into a range, and run an iterator
/// over every element in this range, pushing elements into our `set` (or `in_set`)
fn add_reserved_ports(set: &mut HashSet<u16>) {
    use std::process::Command;
    let mut netsh = Command::new("netsh");
    netsh
        .arg("interface")
        .arg("ipv4")
        .arg("show")
        .arg("excludedportrange")
        .arg("protocol=tcp");
    let output = netsh.output().unwrap();
    if output.stderr.len() != 0 {
        panic!("Errored while trying to get port exclusion ranges on Windows");
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout
        .lines()
        .filter(|line| line.len() != 0)
        .map(|line| line.trim())
        .collect();
    let mut line_iter = lines.into_iter();
    if let Some("Protocol tcp Port Exclusion Ranges") = line_iter.next() {
    } else {
        panic!("netsh returned bad output on Windows");
    }
    match (line_iter.next(), line_iter.next()) {
        (Some(line2), Some(line3))
            if (line2.contains("Start Port") && line2.contains("End Port"))
                && (line3.contains("---")) => {}
        _ => panic!("netsh returned bad stdout for parsing port exclusion ranges on Windows"),
    }
    // Great, so now we the stdout is as we expected it to be
    // Now we will trim each line, get the port range and parse it into u16s
    for line in line_iter {
        if line.starts_with("*") {
            // The last line should look like `* - Administered port exclusions.`
            break;
        }
        let port_high_low: Vec<u16> = line
            .split_whitespace()
            .map(|port_string| {
                port_string
                    .parse::<u16>()
                    .expect("Returned port by netsh was not a valid u16")
            })
            .collect();
        if port_high_low.len() != 2 {
            panic!("netsh returned more than three columns instead of the expected two for parsing port exclusion ranges");
        }
        let (range_low, range_high) = (port_high_low[0], port_high_low[1]);
        (range_low..=range_high).into_iter().for_each(|port| {
            set.insert(port);
        })
    }
}
