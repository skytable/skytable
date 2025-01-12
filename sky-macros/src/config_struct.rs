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
        braced, bracketed, parenthesized,
        parse::{Parse, ParseBuffer, ParseStream},
        spanned::Spanned,
        token, Attribute, Error, Ident, ItemEnum, Token, Visibility,
    },
};

pub struct NestedStructDefinition<const DERIVE: bool>(pub proc_macro2::TokenStream);

struct StructTokenTree {
    struct_tree: proc_macro2::TokenStream,
    impl_tree_cli: proc_macro2::TokenStream,
    impl_tree_env: proc_macro2::TokenStream,
    impl_tree_env_test: proc_macro2::TokenStream,
}

impl StructTokenTree {
    fn new() -> Self {
        Self {
            struct_tree: quote! {},
            impl_tree_cli: quote! {},
            impl_tree_env: quote! {},
            impl_tree_env_test: quote! {},
        }
    }
    fn base(&mut self, f: impl Fn(&proc_macro2::TokenStream) -> proc_macro2::TokenStream) {
        self.struct_tree = f(&self.struct_tree)
    }
    fn impl_cli(&mut self, f: impl Fn(&proc_macro2::TokenStream) -> proc_macro2::TokenStream) {
        self.impl_tree_cli = f(&self.impl_tree_cli)
    }
    fn impl_env(&mut self, f: impl Fn(&proc_macro2::TokenStream) -> proc_macro2::TokenStream) {
        self.impl_tree_env = f(&self.impl_tree_env)
    }
    fn impl_env_test(&mut self, f: impl Fn(&proc_macro2::TokenStream) -> proc_macro2::TokenStream) {
        self.impl_tree_env_test = f(&self.impl_tree_env_test)
    }
}

struct DeclarationPaths {
    env_path: String,
    cli_path: String,
}

impl DeclarationPaths {
    fn init() -> Self {
        Self {
            env_path: "SKYD".to_owned(),
            cli_path: "-".to_owned(),
        }
    }
    fn step(&self, name: &Ident) -> Self {
        let name = name.to_string();
        Self {
            env_path: format!("{}_{}", self.env_path, name.to_uppercase()),
            cli_path: format!("{}-{}", self.cli_path, name.replace('_', "-")),
        }
    }
}

impl<const DERIVE: bool> NestedStructDefinition<DERIVE> {
    fn expand_structs(
        mut main_tree: proc_macro2::TokenStream,
        stream: ParseStream,
        attributes: Option<Vec<Attribute>>,
        paths: DeclarationPaths,
    ) -> syn::Result<proc_macro2::TokenStream> {
        /*
            prepare various token trees:
            (1) full struct decl tree
            (2) env var impl tree
            (3) cli impl tree
        */
        let mut token_tree = StructTokenTree::new();
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
            let mut field_attrs = stream.call(Attribute::parse_outer)?;
            // see if this specific field requires any custom wizardry
            let field_custom_options = if let Some(attr_idx) = field_attrs
                .iter()
                .position(|attr| attr.path.segments[0].ident == "config_group")
            {
                if DERIVE {
                    let attr = field_attrs.remove(attr_idx);
                    syn::parse(attr.tokens.into())?
                } else {
                    return Err(Error::new(field_attrs[attr_idx].span(), "invalid argument"));
                }
            } else {
                ConfigGroupAdvancedOptions::None
            };
            token_tree.base(|decl_tree| quote! { #decl_tree #(#field_attrs)* });
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
                token_tree.base(|decl_tree| quote! { #decl_tree #field_name: #enumeration_id, });
                if DERIVE {
                    // add impls to tree
                    Self::add_impls_for_field(
                        field_name,
                        enumeration_id,
                        &paths,
                        Self::get_default_decl(&stream)?,
                        &mut token_tree,
                        &field_custom_options,
                    );
                }
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
                    paths.step(&field_name),
                )?;
                // add field to local struct tree
                token_tree.base(|decl_tree| quote! { #decl_tree #field_name: #struct_name, });
                if DERIVE {
                    // add impls to tree
                    token_tree.impl_cli(|cli_impl_tree| {
                        Self::apply_return_validation(
                            quote! { #cli_impl_tree let #field_name },
                            quote! { <#struct_name as crate::engine::config::v2::ConfigGroup>::from_cli(args)? },
                        )
                    });
                    token_tree.impl_env(|env_impl_tree| {
                        Self::apply_return_validation(
                            quote! { #env_impl_tree let #field_name },
                            quote! { <#struct_name as crate::engine::config::v2::ConfigGroup>::from_env()? },
                        )
                    });
                    token_tree.impl_env_test(|env_test_impl_tree| {
                        Self::apply_return_validation(
                            quote! { #env_test_impl_tree let #field_name },
                            quote! { <#struct_name as crate::engine::config::v2::ConfigGroup>::from_env_test(args)? },
                        )
                    });
                }
            } else {
                if stream.peek(Ident) {
                    if !field_item_attrs.is_empty() {
                        return Err(Error::new(stream.span(), "attributes can't be used here"));
                    }
                    // this is the field type
                    let field_type: syn::TypePath = stream.parse()?;
                    // add field definition to local struct tree
                    token_tree.base(
                        |decl_tree| quote! { #decl_tree #field_vis #field_name: #field_type, },
                    );
                    if DERIVE {
                        // add impls to tree
                        Self::add_impls_for_field(
                            field_name,
                            field_type,
                            &paths,
                            Self::get_default_decl(&stream)?,
                            &mut token_tree,
                            &field_custom_options,
                        );
                    }
                }
            }
            Self::skip_comma(&stream)?;
        }

        // prepare final output token trees
        let StructTokenTree {
            struct_tree,
            impl_tree_cli,
            impl_tree_env,
            impl_tree_env_test,
        } = token_tree;
        if stream.is_empty() {
            // prep impl code
            let impl_code = if DERIVE {
                quote! {
                    #[automatically_derived]
                    impl crate::engine::config::v2::ConfigGroup for #struct_name {
                        fn from_cli(args: &mut crate::engine::config::v2::ConfigMap) -> crate::engine::config::v2::ConfigResult<crate::engine::config::v2::ConfigReturn<Self>> {
                            let mut modified = false;
                            #impl_tree_cli
                            Ok(if modified {
                                ConfigReturn::Modified(Self { #(#fields),* })
                            } else {
                                ConfigReturn::Unmodified(Self { #(#fields),* })
                            })
                        }
                        fn from_env() -> crate::engine::config::v2::ConfigResult<crate::engine::config::v2::ConfigReturn<Self>> {
                            let mut modified = false;
                            #impl_tree_env
                            Ok(if modified {
                                ConfigReturn::Modified(Self { #(#fields),* })
                            } else {
                                ConfigReturn::Unmodified(Self { #(#fields),* })
                            })
                        }
                        fn from_env_test(args: &mut crate::engine::config::v2::ConfigMap) -> crate::engine::config::v2::ConfigResult<crate::engine::config::v2::ConfigReturn<Self>> {
                            let mut modified = false;
                            #impl_tree_env_test
                            Ok(if modified {
                                ConfigReturn::Modified(Self { #(#fields),* })
                            } else {
                                ConfigReturn::Unmodified(Self { #(#fields),* })
                            })
                        }
                    }
                }
            } else {
                quote!()
            };
            // add struct code and impl code
            let this_struct = quote! {
                #(#attrs)* #struct_vis struct #struct_name { #struct_tree } #impl_code
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

impl<const DERIVE: bool> NestedStructDefinition<DERIVE> {
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
    fn add_impls_for_field(
        field_name: Ident,
        field_type: impl ToTokens,
        paths: &DeclarationPaths,
        default_decl: Option<proc_macro2::TokenStream>,
        token_tree: &mut StructTokenTree,
        field_custom_options: &ConfigGroupAdvancedOptions,
    ) {
        let DeclarationPaths {
            env_path: __var,
            cli_path: __cli,
        } = paths.step(&field_name);
        match field_custom_options {
            ConfigGroupAdvancedOptions::None => {
                if let Some(default_decl) = default_decl {
                    // env
                    token_tree.impl_env(|env_impl_tree| {
                        quote! {
                            #env_impl_tree let #field_name: #field_type = match crate::engine::config::v2::get_var(#__var)? {
                                Some(v) => {modified = true; v},
                                None => #default_decl,
                            };
                        }
                    });
                    // env test
                    token_tree.impl_env_test(|env_test_impl_tree| {
                        quote! {
                            #env_test_impl_tree let #field_name: #field_type = match args.take_opt(#__var)? {
                                Some(v) => {modified = true; v},
                                None => { #default_decl },
                            };
                        }
                    });
                    // cli
                    token_tree.impl_cli(|cli_impl_tree| {
                        quote! {
                            #cli_impl_tree let #field_name: #field_type = match args.take_opt(#__cli)? {
                                Some(v) => {modified = true; v},
                                None => { #default_decl },
                            };
                        }
                    });
                } else {
                    // env
                    token_tree.impl_env(|env_impl_tree| {
                        quote! {
                            #env_impl_tree let #field_name: #field_type = crate::engine::config::v2::get_var(#__var)?
                                .ok_or_else(|| crate::engine::config::v2::ConfigError::Required(#__var.to_owned()))?;
                            modified = true;
                        }
                    });
                    // env test
                    token_tree.impl_env_test(|env_test_impl_tree| {
                        quote! { #env_test_impl_tree let #field_name: #field_type = args.take(#__var)?; modified = true; }
                    });
                    // cli
                    token_tree.impl_cli(|cli_impl_tree| {
                        quote! {
                            #cli_impl_tree let #field_name: #field_type = args.take(#__cli)?;
                            modified = true;
                        }
                    });
                }
            }
            ConfigGroupAdvancedOptions::OverrideInputKeys(override_keys) => {
                assert!(
                    default_decl.is_none(),
                    "can't use both override and default decl"
                );
                let env_args: Vec<String> = override_keys
                    .iter()
                    .map(|key| format!("{}_{}", paths.env_path, key.to_string().to_uppercase()))
                    .collect();
                let cli_args: Vec<String> = override_keys
                    .iter()
                    .map(|key| format!("{}-{}", paths.cli_path, key.to_string().replace('_', "-")))
                    .collect();
                // env
                token_tree.impl_env(|env_impl_tree| Self::apply_return_validation(
                    quote! { #env_impl_tree let #field_name },
                    quote! {
                        {
                            #(
                                let #override_keys = crate::engine::config::v2::get_var(#env_args)?;
                                modified |= ::core::option::Option::is_some(&#override_keys);
                            )*
                            #field_type::__override_config_load(#(#override_keys),*)?
                        }
                    },
                ));
                // env test
                token_tree.impl_env_test(|env_test_impl_tree| {
                    Self::apply_return_validation(
                        quote! { #env_test_impl_tree let #field_name },
                        quote! {
                            {
                                #(
                                    let #override_keys = args.take_opt(#env_args)?;
                                    modified |= ::core::option::Option::is_some(&#override_keys);
                                )*
                                #field_type::__override_config_load(#(#override_keys),*)?
                            }
                        },
                    )
                });
                // cli
                token_tree.impl_cli(|cli_impl_tree| {
                    Self::apply_return_validation(
                        quote! { #cli_impl_tree let #field_name },
                        quote! {
                            {
                                #(
                                    let #override_keys = args.take_opt(#cli_args)?;
                                    modified |= ::core::option::Option::is_some(&#override_keys);
                                )*
                                #field_type::__override_config_load(#(#override_keys),*)?
                            }
                        },
                    )
                });
            }
        }
    }
    fn skip_comma(pb: &ParseBuffer) -> syn::Result<()> {
        if pb.peek(Token![,]) {
            pb.parse::<Token![,]>()?;
        }
        Ok(())
    }
}

impl<const DERIVE: bool> Parse for NestedStructDefinition<DERIVE> {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let tt = Self::expand_structs(quote! {}, input, None, DeclarationPaths::init())?;
        Ok(Self(tt))
    }
}

#[derive(Debug)]
enum ConfigGroupAdvancedOptions {
    None,
    OverrideInputKeys(Vec<Ident>),
}

impl Parse for ConfigGroupAdvancedOptions {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // get parenthesized
        let input_;
        let _ = parenthesized!(input_ in input);
        let input = input_;
        // get attribute type
        let attr_type: Ident = input.parse()?;
        if attr_type != "override_input_fields" {
            // we don't know this
            return Err(Error::new(
                input.span(),
                "unrecognized config_group attribute",
            ));
        }
        // =
        let _: Token![=] = input.parse()?;
        // get square bracketed list of idents
        let items;
        let _ = bracketed!(items in input);
        Ok(Self::OverrideInputKeys(
            items
                .parse_terminated::<_, Token![,]>(Ident::parse)?
                .into_iter()
                .collect(),
        ))
    }
}
