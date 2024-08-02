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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

#![deny(unused_crate_dependencies)]
#![deny(unused_imports)]

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
//! - `#[dbtest_func]` and `#[dbtest_module]`:
//!     - `con` - `skytable::AsyncConnection`
//!     - `query` - `skytable::Query`
//!     - `__MYENTITY__` - `String` with entity
//!

use {
    proc_macro::TokenStream,
    proc_macro2::{Ident, TokenStream as TokenStream2},
    quote::quote,
    syn::{
        parse_macro_input, AttributeArgs, Data, DataStruct, DeriveInput, Expr, Fields, ItemFn,
        Meta, NestedMeta,
    },
};

mod dbtest;
mod util;

#[proc_macro_attribute]
/// A `dbtest` is a test harness that provides an easy way to connect to a Skytable instance during a test with appropriate connection and graceful
/// disconnection.
///
/// **This test only runs in _non-miri_ configurations**
pub fn dbtest(attrs: TokenStream, item: TokenStream) -> TokenStream {
    dbtest::dbtest(attrs, item)
}

#[proc_macro_derive(Wrapper)]
/// Implements necessary traits for some type `T` to make it identify as a different type but mimic the functionality
/// as the inner type it wraps around
pub fn derive_wrapper(t: TokenStream) -> TokenStream {
    let item = syn::parse_macro_input!(t as DeriveInput);
    let r = wrapper(item);
    r.into()
}

fn wrapper(item: DeriveInput) -> TokenStream2 {
    let st_name = &item.ident;
    let fields = match item.data {
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(ref f),
            ..
        }) if f.unnamed.len() == 1 => f,
        _ => panic!("only works on tuple structs with one field"),
    };
    let field = &fields.unnamed[0];
    let ty = &field.ty;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics #st_name #ty_generics #where_clause { pub fn into_inner(self) -> #ty { self.0 } }
        #[automatically_derived]
        impl #impl_generics ::core::ops::Deref for #st_name #ty_generics #where_clause {
            type Target = #ty;
            fn deref(&self) -> &Self::Target { &self.0 }
        }
        #[automatically_derived]
        impl #impl_generics ::core::ops::DerefMut for #st_name #ty_generics #where_clause {
            fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
        }
        #[automatically_derived]
        impl #impl_generics ::core::cmp::PartialEq<#ty> for #st_name #ty_generics #where_clause {
            fn eq(&self, other: &#ty) -> bool { ::core::cmp::PartialEq::eq(&self.0, other) }
        }
        #[automatically_derived]
        impl #impl_generics ::core::cmp::PartialEq<#st_name #ty_generics> for #ty #where_clause {
            fn eq(&self, other: &#st_name #ty_generics) -> bool { ::core::cmp::PartialEq::eq(self, &other.0) }
        }
    }
}

#[proc_macro_derive(TaggedEnum)]
pub fn derive_tagged_enum(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let (enum_name, _, value_expressions, variant_len, repr_type_ident, _) =
        process_enum_tags(&ast);
    quote! {
        impl crate::util::compiler::TaggedEnum for #enum_name {
            type Dscr = #repr_type_ident;
            const MAX_DSCR: #repr_type_ident = {
                let values = #value_expressions;
                let mut i = 1;
                let mut max = values[0];
                while i < values.len() {
                    if values[i] > max {
                        max = values[i];
                    }
                    i = i + 1;
                }
                max
            };
            const VARIANT_COUNT: usize = #variant_len;
            fn dscr(&self) -> #repr_type_ident {
                unsafe {
                    core::mem::transmute(*self)
                }
            }
            fn dscr_u64(&self) -> u64 {
                self.dscr() as u64
            }
            unsafe fn from_raw(d: #repr_type_ident) -> Self {
                core::mem::transmute(d)
            }
        }
    }
    .into()
}

#[proc_macro_derive(EnumMethods)]
pub fn derive_value_methods(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let (enum_name, repr_type, _, variant_len, repr_type_ident, variants) = process_enum_tags(&ast);
    let repr_type_ident_func = syn::Ident::new(
        &format!("value_{repr_type}"),
        proc_macro2::Span::call_site(),
    );
    let mut decls = quote! {};
    for (variant, _) in variants {
        decls = quote! {
            #decls
            #enum_name::#variant,
        };
    }
    let variants_array = quote! { [#decls] };
    let gen = quote! {
        impl #enum_name {
            pub const VARIANTS: [Self; #variant_len] = #variants_array;
            pub const fn #repr_type_ident_func(&self) -> #repr_type_ident { unsafe { core::mem::transmute(*self) } }
            pub const fn value_word(&self) -> usize { self.#repr_type_ident_func() as usize }
            pub const fn value_qword(&self) -> u64 { self.#repr_type_ident_func() as u64 }
        }
    };
    gen.into()
}

fn process_enum_tags(
    ast: &DeriveInput,
) -> (
    &proc_macro2::Ident,
    String,
    TokenStream2,
    usize,
    proc_macro2::Ident,
    Vec<(Ident, Expr)>,
) {
    let enum_name = &ast.ident;
    let mut repr_type = None;
    // Get repr attribute
    for attr in &ast.attrs {
        if attr.path.is_ident("repr") {
            if let Meta::List(list) = attr.parse_meta().unwrap() {
                if let Some(NestedMeta::Meta(Meta::Path(path))) = list.nested.first() {
                    repr_type = Some(path.get_ident().unwrap().to_string());
                }
            }
        }
    }
    let repr_type = repr_type.expect("Must have repr(u8) or repr(u16) etc.");
    let mut variant_exprs = vec![];
    // Ensure all variants have explicit discriminants
    if let Data::Enum(data) = &ast.data {
        for variant in &data.variants {
            match &variant.fields {
                Fields::Unit => {
                    let (_, dscr_expr) = variant
                        .discriminant
                        .as_ref()
                        .expect("All enum variants must have explicit discriminants");
                    variant_exprs.push((variant.ident.clone(), dscr_expr.clone()));
                }
                _ => panic!("All enum variants must be unit variants"),
            }
        }
    } else {
        panic!("This derive macro only works on enums");
    }
    assert!(!variant_exprs.is_empty(), "must be a non-empty enumeration");
    let val_exprs = variant_exprs.iter().map(|(_, vexp)| vexp);
    let value_expressions = quote! {
        [#(#val_exprs),*]
    };
    let variant_len = variant_exprs.len();
    let repr_type_ident = syn::Ident::new(&repr_type, proc_macro2::Span::call_site());
    (
        enum_name,
        repr_type,
        value_expressions,
        variant_len,
        repr_type_ident,
        variant_exprs,
    )
}

#[proc_macro_attribute]
/// Run this test when either:
/// - Non-leaky-permissive miri harness is enabled
/// - Only test mode is enabled
pub fn miri_leaky_test(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = parse_macro_input!(attrs as AttributeArgs);
    assert!(attr_args.is_empty(), "no args allowed here");
    let input_fn = parse_macro_input!(item as ItemFn);
    quote! {
        #[cfg(
            any(
                all(feature = "miri-leaks", miri),
                all(not(miri), not(feature = "miri-leaks")),
            )
        )]
        #[::core::prelude::v1::test]
        #input_fn
    }
    .into()
}

#[proc_macro_attribute]
/// Run this test only when either:
/// - Miri and strict leak policy harness is enabled
/// - Only test mode is enabled
pub fn test(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = parse_macro_input!(attrs as AttributeArgs);
    assert!(attr_args.is_empty(), "no args allowed here");
    let input_fn = parse_macro_input!(item as ItemFn);
    quote! {
        #[cfg(not(feature = "miri-leaks"))]
        #[::core::prelude::v1::test]
        #input_fn
    }
    .into()
}

#[proc_macro_attribute]
/// Run this test in non-miri configs only
pub fn non_miri_test(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let attr_args = parse_macro_input!(attrs as AttributeArgs);
    assert!(attr_args.is_empty(), "no args allowed here");
    let input_fn = parse_macro_input!(item as ItemFn);
    quote! {
        #[cfg(not(miri))]
        #[::core::prelude::v1::test]
        #input_fn
    }
    .into()
}
