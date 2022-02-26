/*
 * Created on Thu Nov 11 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

/*
 Don't modulo because it's an L1 miss and an L2 hit. Use lowbit checks to check for parity
*/

#[macro_export]
/// endian independent check to see if the lowbit is set or not. Returns true if the lowbit
/// is set. this is undefined to be applied on signed values on one's complement targets
macro_rules! is_lowbit_set {
    ($v:expr) => {
        $v & 1 == 1
    };
}

#[macro_export]
/// endian independent check to see if the lowbit is unset or not. Returns true if the lowbit
/// is unset. this is undefined to be applied on signed values on one's complement targets
macro_rules! is_lowbit_unset {
    ($v:expr) => {
        $v & 1 == 0
    };
}

#[macro_export]
macro_rules! conwrite {
    ($con:expr, $what:expr) => {
        $con.write_response($what)
            .await
            .map_err(|e| crate::actions::ActionError::IoError(e))
    };
}

#[macro_export]
macro_rules! aerr {
    ($con:expr) => {
        return conwrite!($con, crate::protocol::responses::groups::ACTION_ERR)
    };
}

#[macro_export]
macro_rules! get_tbl {
    ($entity:expr, $store:expr, $con:expr) => {{
        use crate::corestore::memstore::DdlError;
        match $store.get_table($entity) {
            Ok(tbl) => tbl,
            Err(DdlError::DefaultNotFound) => {
                return conwrite!($con, crate::protocol::responses::groups::DEFAULT_UNSET);
            }
            Err(DdlError::ObjectNotFound) => {
                return conwrite!(
                    $con,
                    crate::protocol::responses::groups::CONTAINER_NOT_FOUND
                );
            }
            Err(_) => unsafe { impossible!() },
        }
    }};
    ($store:expr, $con:expr) => {{
        match $store.get_ctable() {
            Some(tbl) => tbl,
            None => return conwrite!($con, crate::protocol::responses::groups::DEFAULT_UNSET),
        }
    }};
}

#[macro_export]
macro_rules! handle_entity {
    ($con:expr, $ident:expr) => {{
        match crate::queryengine::parser::get_query_entity(&$ident) {
            Ok(e) => e,
            Err(e) => return conwrite!($con, e),
        }
    }};
}
