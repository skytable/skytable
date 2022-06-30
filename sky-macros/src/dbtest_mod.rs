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

use {
    crate::{
        dbtest_fn::{self, DBTestFunctionConfig},
        util,
    },
    proc_macro::TokenStream,
    quote::quote,
    std::collections::HashSet,
    syn::{self, AttributeArgs},
};

struct DBTestModuleConfig {
    fcfg: DBTestFunctionConfig,
    skips: HashSet<String>,
}

impl DBTestModuleConfig {
    fn default() -> Self {
        Self {
            skips: HashSet::new(),
            fcfg: DBTestFunctionConfig::default(),
        }
    }
}

fn parse_dbtest_module_args(args: AttributeArgs) -> DBTestModuleConfig {
    let mut modcfg = DBTestModuleConfig::default();
    for arg in args {
        if let syn::NestedMeta::Meta(syn::Meta::NameValue(namevalue)) = arg {
            let (ident, lit, span) = util::get_metanamevalue_data(&namevalue);
            match ident.as_str() {
                "skip" => {
                    modcfg.skips = util::parse_string(lit, span, "skip")
                        .expect("Expected a value for argument `skip`")
                        .split_whitespace()
                        .map(|val| val.to_string())
                        .collect();
                }
                possibly_func_arg => dbtest_fn::parse_dbtest_func_args(
                    possibly_func_arg,
                    lit,
                    span,
                    &mut modcfg.fcfg,
                ),
            }
        }
    }
    modcfg
}

/// This function accepts an entire **inline** module which comprises of `dbtest` functions.
/// It takes each function in turn, and generates `#[test]`able functions for them
pub fn parse_test_module(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemMod);
    let content = match input.content {
        Some((_, c)) => c,
        None => {
            return syn::Error::new_spanned(&input, "Couldn't get the module content")
                .to_compile_error()
                .into()
        }
    };
    let modcfg = parse_dbtest_module_args(syn::parse_macro_input!(args as AttributeArgs));
    let mut result = quote! {};
    let mut rng = rand::thread_rng();
    for item in content {
        match item {
            // We just care about functions, so parse functions and ignore everything
            // else
            syn::Item::Fn(function) if !modcfg.skips.contains(&function.sig.ident.to_string()) => {
                let generated_fn = dbtest_fn::generate_test(function, &mut rng, &modcfg.fcfg);
                let __tok: syn::ItemFn = syn::parse_macro_input!(generated_fn as syn::ItemFn);
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
