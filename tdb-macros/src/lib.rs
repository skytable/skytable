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
use quote::quote;
use rand::*;
use std::collections::HashSet;
use syn;

// TODO(@ohsayan): Write docs and also make this use the tokio runtime

fn parse_dbtest(mut input: syn::ItemFn, rand: u16) -> Result<TokenStream, syn::Error> {
    let sig = &mut input.sig;
    let fname = sig.ident.to_string();
    let body = &input.block;
    let attrs = &input.attrs;
    let vis = input.vis;
    let header = quote! {
        #[::core::prelude::v1::test]
    };
    if sig.asyncness.is_none() {
        let msg = "`dbtest` functions need to be async";
        return Err(syn::Error::new_spanned(sig.fn_token, msg));
    }
    sig.asyncness = None;
    let body = quote! {
        let mut socket = tokio::net::TcpStream::connect(#rand).await.unwrap();
        let fut1 = tokio::spawn(crate::tests::start_server(db.clone(), socket));
        #body
        socket.shutdown(::std::net::Shutdown::Both).unwrap();
        ::std::mem::drop(fut1);
    };
    let result = quote! {
        #header
        #(#attrs)*
        #vis #sig {
            let db = ::std::sync::Arc::new(crate::coredb::CoreDB::new_empty(2));
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name(#fname)
                .thread_stack_size(3 * 1024 * 1024)
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

fn parse_test_module(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemMod);
    let content = match input.content {
        Some((_, c)) => c,
        None => {
            return syn::Error::new_spanned(&input, "Couldn't get the module content")
                .to_compile_error()
                .into()
        }
    };
    let attrs = input.attrs;
    let vis = input.vis;
    let mod_token = input.mod_token;
    let modname = input.ident;
    let mut rng = thread_rng();
    let mut in_set = HashSet::<u16>::new();
    in_set.insert(80);
    in_set.insert(443);
    let mut result = quote! {};
    for item in content {
        let mut rand: u16 = rng.gen_range(0, 65535);
        while in_set.contains(&rand) {
            rand = rng.gen_range(0, 65535);
        }
        match item {
            // We just care about functions, so parse functions and ignore everything
            // else
            syn::Item::Fn(function) => {
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
        #(#attrs)*
        #mod_token #vis #modname {
            #result
        }
    };
    finalres.into()
}

#[proc_macro_attribute]
pub fn dbtest(args: TokenStream, item: TokenStream) -> TokenStream {
    parse_test_module(args, item)
}
