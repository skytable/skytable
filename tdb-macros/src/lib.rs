/*
 * Created on Sun Sep 13 2020
 *
 * This file is a part of TerrabaseDB
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
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

//! A library containing a collection of custom derives used by TerrabaseDB

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use rand::*;
use std::collections::HashSet;
use syn::{self};

// TODO(@ohsayan): Write docs and also make this use the tokio runtime

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
        use crate::tests::{fresp, terrapipe, TcpStream};
        use crate::__func__;
        use tokio::prelude::*;
        let addr = crate::tests::start_test_server(#rand).await;
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        #body
        stream.shutdown(::std::net::Shutdown::Write).unwrap();
    };
    let result = quote! {
        #header
        #(#attrs)*
        #vis #sig {
            tokio::runtime::Builder::new()
            .threaded_scheduler()
            .core_threads(4)
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
    let vis = &input.vis;
    let mod_token = &input.mod_token;
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

    let mut result = quote! {};
    for item in content {
        /*
        Since standard (non-root) users can only access ports greater than 1024
        we will set the limit to (1024, 65535)
        */
        let mut rand: u16 = rng.gen_range(1025, 65535);
        while in_set.contains(&rand) {
            rand = rng.gen_range(1025, 65535);
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
            _ => continue,
        }
    }
    let result = quote! {
        #result
    };
    let finalres = quote! {
        #mod_token #vis #modname {
            #result
        }
    };
    finalres.into()
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
pub fn dbtest(args: TokenStream, item: TokenStream) -> TokenStream {
    parse_test_module(args, item)
}
