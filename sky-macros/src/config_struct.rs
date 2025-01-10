/*
 * This file is a part of Skytable
 *
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2025, Sayan Nandan <nandansayan@outlook.com>
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
    quote::{quote, ToTokens},
    syn::{
        braced,
        parse::{Parse, ParseBuffer, ParseStream},
        token, Attribute, Error, Ident, ItemEnum, Token, Visibility,
    },
};

pub struct NestedStructDefinition(pub proc_macro2::TokenStream);

impl NestedStructDefinition {
    /// for values that return from structs / custom impls, use this to gather information on whether the value was modified or not
    fn apply_return_validation(
        lhs: proc_macro2::TokenStream,
        rhs: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        quote! {
            #lhs = match #rhs {
                ConfigReturn::Modified(v) => { modified = true; v },
                ConfigReturn::Unmodified(v) => v,
            };
        }
    }
    /// see if there is a default declaration
    fn get_default_decl(stream: ParseStream) -> syn::Result<Option<proc_macro2::TokenStream>> {
        Ok(if stream.peek(Token![=]) {
            stream.parse::<Token![=]>()?;
            let default_decl: syn::Expr = stream.parse()?;
            Some(quote! { #default_decl })
        } else {
            None
        })
    }
    /// for a given non-nested field:
    /// - if there is a default impl:
    ///     - if there is an override: error
    ///     - if no override, fetch the value and if absent, use the default
    /// - if there is no defualt impl:
    ///     - if there is an override, then let the override impl do what needs to be done (apply return validation)
    ///     - if no override, fetch the value and parse; otherwise return missing err
    fn add_cli_impl_for_field(
        field_name: Ident,
        field_type: impl ToTokens,
        current_cli_path: &str,
        is_override: bool,
        default_decl: Option<&proc_macro2::TokenStream>,
        cli_impl_tree: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let __cli = format!(
            "{current_cli_path}-{}",
            field_name.to_string().replace('_', "-")
        );
        if let Some(default_decl) = default_decl {
            if is_override {
                panic!("can't have override and default. implement default manually")
            } else {
                // check if modified (i.e some(..))
                quote! {
                    #cli_impl_tree let #field_name: #field_type = match args.take_opt(#__cli)? {
                        Some(v) => {modified = true; v},
                        None => { #default_decl },
                    };
                }
            }
        } else {
            if is_override {
                // not necessarily modified
                Self::apply_return_validation(
                    quote! { #cli_impl_tree let #field_name: #field_type },
                    quote! {<#field_type as crate::engine::config::v2::ConfigGroupOverride>::from_cli(args, #current_cli_path)?},
                )
            } else {
                // definitely modified as reqd. key
                quote! {
                    #cli_impl_tree let #field_name: #field_type = args.take(#__cli)?;
                    modified = true;
                }
            }
        }
    }
    fn add_env_impl_for_field(
        field_name: Ident,
        field_type: impl ToTokens,
        current_env_path: &str,
        is_override: bool,
        default_decl: Option<proc_macro2::TokenStream>,
        env_impl_tree: proc_macro2::TokenStream,
        env_test_impl_tree: proc_macro2::TokenStream,
    ) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
        let __var = format!(
            "{current_env_path}_{}",
            field_name.to_string().to_uppercase()
        );
        let (env_tt, env_test_tt);
        if let Some(default_decl) = default_decl {
            if is_override {
                panic!("can't have override and default. implement default manually")
            } else {
                // check if modified (i.e some(..))
                env_tt = quote! {
                    #env_impl_tree let #field_name: #field_type = match crate::engine::config::v2::get_var(#__var)? {
                        Some(v) => match v.parse() {
                            Ok(v) => {modified = true; v},
                            Err(e) => return Err(crate::engine::config::v2::ConfigError::parse_error(#__var, e)),
                        },
                        None => {
                            #default_decl
                        },
                    };
                };
                env_test_tt = quote! {
                    #env_impl_tree let #field_name: #field_type = match args.take_opt(#__var)? {
                        Some(v) => {modified = true; v},
                        None => { #default_decl },
                    };
                };
            }
        } else {
            // not necessarily modified
            if is_override {
                env_tt = Self::apply_return_validation(
                    quote! { #env_impl_tree let #field_name: #field_type },
                    quote! { <#field_type as crate::engine::config::v2::ConfigGroupOverride>::from_env(#current_env_path)? },
                );
                env_test_tt = Self::apply_return_validation(
                    quote! { #env_test_impl_tree let #field_name: #field_type },
                    quote! { <#field_type as crate::engine::config::v2::ConfigGroupOverride>::from_env_test(args, #current_env_path)? },
                );
            } else {
                // TODO(@ohsayan): nullable or reqd.
                // definitely modified
                env_tt = quote! {
                    #env_impl_tree let #field_name: #field_type = { if let Some(v) = crate::engine::config::v2::get_var(#__var)? { v } else {
                        return Err(crate::engine::config::v2::ConfigError::Required(::std::string::String::from(#__var)))
                    }}.parse().map_err(|e| crate::engine::config::v2::ConfigError::parse_error(#__var, e))?;
                    modified = true;
                };
                env_test_tt = quote! { #env_test_impl_tree let #field_name: #field_type = args.take(#__var)?; modified = true; };
            }
        }
        (env_tt, env_test_tt)
    }
    fn add_impls_for_field(
        field_name: Ident,
        field_type: impl ToTokens,
        current_cli_path: &str,
        current_env_path: &str,
        is_override: bool,
        default_decl: Option<proc_macro2::TokenStream>,
        cli_impl_tree: proc_macro2::TokenStream,
        env_impl_tree: proc_macro2::TokenStream,
        env_test_impl_tree: proc_macro2::TokenStream,
    ) -> (
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
        proc_macro2::TokenStream,
    ) {
        let cli_impls = Self::add_cli_impl_for_field(
            field_name.clone(),
            &field_type,
            current_cli_path,
            is_override,
            default_decl.as_ref(),
            cli_impl_tree,
        );
        let (env_impl, env_test_impl) = Self::add_env_impl_for_field(
            field_name,
            field_type,
            current_env_path,
            is_override,
            default_decl,
            env_impl_tree,
            env_test_impl_tree,
        );
        (cli_impls, env_impl, env_test_impl)
    }
    fn skip_comma(pb: &ParseBuffer) -> syn::Result<()> {
        if pb.peek(Token![,]) {
            pb.parse::<Token![,]>()?;
        }
        Ok(())
    }
    fn expand_structs(
        mut main_tree: proc_macro2::TokenStream,
        stream: ParseStream,
        attributes: Option<Vec<Attribute>>,
        c_cli_path: String,
        c_env_path: String,
    ) -> syn::Result<proc_macro2::TokenStream> {
        /*
            prepare various token trees:
            (1) full struct decl tree
            (2) env var impl tree
            (3) cli impl tree
        */
        let mut decl_tree = quote! {};
        let mut cli_impl_tree = quote! {};
        let mut env_impl_tree = quote! {};
        let mut env_test_impl_tree = quote! {};
        let mut fields = vec![];
        // struct attributes
        let attrs = match attributes {
            Some(att) => att,
            None => stream.call(Attribute::parse_outer)?,
        };
        // visibility
        let struct_vis = if stream.peek(Token![pub]) {
            stream.parse()?
        } else {
            Visibility::Inherited
        };
        // ensure `struct` keyword
        let _: token::Struct = stream.parse()?;
        // get struct name
        let struct_name: Ident = stream.parse()?;
        // now get field block
        let block;
        let _ = braced!(block in stream);
        let stream = block;

        // process fields
        loop {
            if stream.is_empty() {
                break;
            }
            // field attrs
            let field_attrs = stream.call(Attribute::parse_outer)?;
            decl_tree = quote! { #decl_tree #(#field_attrs)* };
            // see if override
            let is_override = if stream.peek(Token![override]) && stream.peek2(Token![impl]) {
                stream.parse::<Token![override]>()?;
                stream.parse::<Token![impl]>()?;
                true
            } else {
                false
            };
            // see if field is pub
            let field_vis = if stream.peek(Token![pub]) {
                stream.parse()?
            } else {
                Visibility::Inherited
            };
            // field name
            let field_name: Ident = stream.parse()?;
            fields.push(field_name.clone());
            // :
            stream.parse::<Token![:]>()?;
            // parse item attributes
            let field_item_attrs: Vec<Attribute> = stream.call(Attribute::parse_outer)?;
            // enum / struct / actual ty
            if stream.peek(Token![enum]) || (stream.peek(Token![pub]) && stream.peek2(Token![enum]))
            {
                let enumeration: ItemEnum = stream.parse()?;
                let enumeration_id = enumeration.ident.clone();
                // add enum definition to main tree
                main_tree = quote! { #main_tree #(#field_item_attrs)* #enumeration };
                // add field definition to local struct tree
                decl_tree = quote! { #decl_tree #field_name: #enumeration_id, };
                // add impls to tree
                (cli_impl_tree, env_impl_tree, env_test_impl_tree) = Self::add_impls_for_field(
                    field_name,
                    enumeration_id,
                    &c_cli_path,
                    &c_env_path,
                    is_override,
                    Self::get_default_decl(&stream)?,
                    cli_impl_tree,
                    env_impl_tree,
                    env_test_impl_tree,
                );
            } else if stream.peek(Token![struct])
                || (stream.peek(Token![pub]) && stream.peek2(Token![struct]))
            {
                // an actual struct; fork the stream and get the struct name
                let struct_name: Ident = {
                    let tmp_stream = stream.fork();
                    if stream.peek(Token![pub]) {
                        let _: Visibility = tmp_stream.parse()?;
                    }
                    let _: Token![struct] = tmp_stream.parse()?;
                    tmp_stream.parse()?
                };
                main_tree = Self::expand_structs(
                    main_tree,
                    &stream,
                    Some(field_item_attrs),
                    format!("{c_cli_path}-{}", field_name.to_string().replace('_', "-")),
                    format!("{c_env_path}_{}", field_name.to_string().to_uppercase()),
                )?;
                // add field to local struct tree
                decl_tree = quote! { #decl_tree #field_name: #struct_name, };
                // add impls to tree
                cli_impl_tree = Self::apply_return_validation(
                    quote! { #cli_impl_tree let #field_name },
                    quote! { <#struct_name as crate::engine::config::v2::ConfigGroup>::from_cli(args)? },
                );
                env_impl_tree = Self::apply_return_validation(
                    quote! { #env_impl_tree let #field_name },
                    quote! { <#struct_name as crate::engine::config::v2::ConfigGroup>::from_env()? },
                );
                env_test_impl_tree = Self::apply_return_validation(
                    quote! { #env_test_impl_tree let #field_name },
                    quote! { <#struct_name as crate::engine::config::v2::ConfigGroup>::from_env_test(args)? },
                );
            } else {
                if stream.peek(Ident) {
                    if !field_item_attrs.is_empty() {
                        return Err(Error::new(stream.span(), "attributes can't be used here"));
                    }
                    // this is the field type
                    let field_type: syn::TypePath = stream.parse()?;
                    // add field definition to local struct tree
                    decl_tree = quote! { #decl_tree #field_vis #field_name: #field_type, };
                    // add impls to tree
                    (cli_impl_tree, env_impl_tree, env_test_impl_tree) = Self::add_impls_for_field(
                        field_name,
                        field_type,
                        &c_cli_path,
                        &c_env_path,
                        is_override,
                        Self::get_default_decl(&stream)?,
                        cli_impl_tree,
                        env_impl_tree,
                        env_test_impl_tree,
                    );
                }
            }
            Self::skip_comma(&stream)?;
        }

        // prepare final output token trees
        if stream.is_empty() {
            // prep impl code
            let impl_code = quote! {
                #[automatically_derived]
                impl crate::engine::config::v2::ConfigGroup for #struct_name {
                    fn from_cli(args: &mut crate::engine::config::v2::ConfigMap) -> crate::engine::config::v2::ConfigResult<crate::engine::config::v2::ConfigReturn<Self>> {
                        let mut modified = false;
                        #cli_impl_tree
                        Ok(if modified {
                            ConfigReturn::Modified(Self { #(#fields),* })
                        } else {
                            ConfigReturn::Unmodified(Self { #(#fields),* })
                        })
                    }
                    fn from_env() -> crate::engine::config::v2::ConfigResult<crate::engine::config::v2::ConfigReturn<Self>> {
                        let mut modified = false;
                        #env_impl_tree
                        Ok(if modified {
                            ConfigReturn::Modified(Self { #(#fields),* })
                        } else {
                            ConfigReturn::Unmodified(Self { #(#fields),* })
                        })
                    }
                    fn from_env_test(args: &mut crate::engine::config::v2::ConfigMap) -> crate::engine::config::v2::ConfigResult<crate::engine::config::v2::ConfigReturn<Self>> {
                        let mut modified = false;
                        #env_test_impl_tree
                        Ok(if modified {
                            ConfigReturn::Modified(Self { #(#fields),* })
                        } else {
                            ConfigReturn::Unmodified(Self { #(#fields),* })
                        })
                    }
                }
            };
            // add struct code and impl code
            let this_struct = quote! {
                #(#attrs)* #struct_vis struct #struct_name { #decl_tree } #impl_code
            };
            // merge with full tree
            let final_tree = quote! {
                #this_struct #main_tree
            };
            Ok(final_tree.into())
        } else {
            Err(Error::new(stream.span(), "unexpected trailing syntax"))
        }
    }
}

impl Parse for NestedStructDefinition {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let tt = Self::expand_structs(quote! {}, input, None, "-".to_owned(), "SKYD".to_owned())?;
        Ok(Self(tt))
    }
}
