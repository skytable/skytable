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

use proc_macro::TokenStream;

mod dbtest_fn;
mod dbtest_mod;
mod util;

#[proc_macro_attribute]
/// The `dbtest_module` function accepts an inline module of `dbtest_func` compatible functions,
/// unpacking each function into a dbtest
pub fn dbtest_module(args: TokenStream, item: TokenStream) -> TokenStream {
    dbtest_mod::parse_test_module(args, item)
}

/// The `dbtest_func` macro starts an async server in the background and is meant for
/// use within the `skyd` or `WORKSPACEROOT/server/` crate. If you use this compiler
/// macro in any other crate, you'll simply get compilation errors
///
/// All tests will clean up all values once a single test is over
///
/// ## Arguments
/// - `table -> str`: Custom table declaration
/// - `port -> u16`: Custom port
/// - `host -> str`: Custom host
/// - `tls_cert -> str`: TLS cert (makes the connection a TLS one)
/// - `username -> str`: Username for authn
/// - `password -> str`: Password for authn
/// - `auth_testuser -> bool`: Login as the test user
/// - `auth_rootuser -> bool`: Login as the root user
/// - `norun -> bool`: Don't execute anything on the connection
///
/// ## _Ghost_ values
/// This macro gives:
/// - `con`: a `skytable::AsyncConnection`
/// - `query`: a mutable `skytable::Query`
/// - `__MYENTITY__`: the entity set on launch
/// - `__MYTABLE__`: the table set on launch
/// - `__MYKS__`: the keyspace set on launch
///
/// ## Requirements
///
/// The `#[dbtest]` macro expects several things. The calling crate:
/// - should have the `tokio` crate as a dependency and should have the
/// `features` set to full
/// - should have the `skytable` crate as a dependency and should have the `features` set to `async` and version
/// upstreamed to `next` on skytable/client-rust
///
/// ## Collisions
///
/// The sample space for table name generation is so large (in the order of 4.3 to the 50) that collisions
/// are practially impossible. Hence we do not bother with a global random string table and instead proceed
/// to generate tables randomly at the point of invocation
///
#[proc_macro_attribute]
pub fn dbtest_func(args: TokenStream, item: TokenStream) -> TokenStream {
    dbtest_fn::dbtest_func(args, item)
}
