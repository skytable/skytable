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

use proc_macro::TokenStream;
use quote::quote;
use syn;

fn parse_toks(mut input: syn::ItemFn) -> Result<TokenStream, syn::Error> {
    let sig = &mut input.sig;
    let body = &input.block;
    let attrs = &input.attrs;
    let vis = input.vis;
    let header = quote! {
        #[::core::prelude::v1::test]
    };
    let result = quote! {
        #header
        #(#attrs)*
        #vis #sig {
            let runtime = service::BackGroundTask::new();
            runtime.execute(|| {#body});
            drop(runtime);
        }
    };
    Ok(result.into())
}

fn test(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
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
    parse_toks(input).unwrap_or_else(|e| e.to_compile_error().into())
}

#[proc_macro_attribute]
/// Execute the function as a test
/// This function starts the server in the background and terminates it when
/// the test is over. Do note that, at the moment, this expects a `service` module
/// to have a `BackgroundTask` object which should start the background the server
pub fn dbtest(args: TokenStream, item: TokenStream) -> TokenStream {
    test(args, item)
}
