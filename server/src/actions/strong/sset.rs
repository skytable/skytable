/*
 * Created on Fri Jul 30 2021
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

use crate::actions::strong::StrongActionResult;
use crate::corestore::Data;
use crate::dbnet::connection::prelude::*;
use crate::kvengine::DoubleEncoder;
use crate::kvengine::KVEngine;
use crate::util::compiler;

action! {
    /// Run an `SSET` query
    ///
    /// This either returns `Okay` if all the keys were set, or it returns an
    /// `Overwrite Error` or code `2`
    fn sset(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter<'a>) {
        let howmany = act.len();
        ensure_length(howmany, |size| size & 1 == 0 && size != 0)?;
        let kve = handle.get_table_with::<KVE>()?;
        if registry::state_okay() {
            let encoder = kve.get_encoder();
            let outcome = {
                self::snapshot_and_insert(kve, encoder, act)
            };
            match outcome {
                StrongActionResult::Okay => conwrite!(con, groups::OKAY)?,
                StrongActionResult::OverwriteError => conwrite!(con, groups::OVERWRITE_ERR)?,
                StrongActionResult::ServerError => conwrite!(con, groups::SERVER_ERR)?,
                StrongActionResult::EncodingError => {
                    // error we love to hate: encoding error, ugh
                    compiler::cold_err(conwrite!(con, groups::ENCODING_ERROR))?
                },
                StrongActionResult::Nil => unsafe {
                    // SAFETY check: never the case
                    impossible!()
                }
            }
        } else {
            conwrite!(con, groups::SERVER_ERR)?;
        }
        Ok(())
    }
}

/// Take a consistent snapshot of the database at this current point in time
/// and then mutate the entries, respecting concurrency guarantees
pub(super) fn snapshot_and_insert(
    kve: &KVEngine,
    encoder: DoubleEncoder,
    mut act: ActionIter,
) -> StrongActionResult {
    let mut enc_err = false;
    let lowtable = kve.__get_inner_ref();
    let key_iter_stat_ok;
    {
        key_iter_stat_ok = act.chunks_exact(2).all(|kv| unsafe {
            let key = ucidx!(kv, 0).as_slice();
            let value = ucidx!(kv, 1).as_slice();
            if compiler::likely(encoder.is_ok(key, value)) {
                lowtable.get(key).is_none()
            } else {
                enc_err = true;
                false
            }
        });
    }
    cfg_test!({
        // give the caller 10 seconds to do some crap
        do_sleep!(10 s);
    });
    if compiler::unlikely(enc_err) {
        return compiler::cold_err(StrongActionResult::EncodingError);
    }
    if registry::state_okay() {
        if key_iter_stat_ok {
            let _kve = kve;
            let lowtable = lowtable;
            // fine, the keys were non-existent when we looked at them
            while let (Some(key), Some(value)) = (act.next(), act.next()) {
                if let Some(fresh) = lowtable.fresh_entry(Data::copy_from_slice(key)) {
                    fresh.insert(Data::copy_from_slice(value));
                }
                // we don't care if some other thread initialized the value we checked
                // it. We expected a fresh entry, so that's what we'll check and use
            }
            StrongActionResult::Okay
        } else {
            StrongActionResult::OverwriteError
        }
    } else {
        StrongActionResult::ServerError
    }
}

/// Take a consistent snapshot of the database at this current point in time
/// and then mutate the entries, respecting concurrency guarantees
#[cfg(test)]
pub(super) fn snapshot_and_insert_test(
    kve: &KVEngine,
    encoder: DoubleEncoder,
    mut act: std::vec::IntoIter<bytes::Bytes>,
) -> StrongActionResult {
    let mut enc_err = false;
    let lowtable = kve.__get_inner_ref();
    let key_iter_stat_ok;
    {
        key_iter_stat_ok = act.as_ref().chunks_exact(2).all(|kv| unsafe {
            let key = &ucidx!(kv, 0);
            let value = &ucidx!(kv, 1);
            if compiler::likely(encoder.is_ok(key, value)) {
                lowtable.get(key).is_none()
            } else {
                enc_err = true;
                false
            }
        });
    }
    cfg_test!({
        // give the caller 10 seconds to do some crap
        do_sleep!(10 s);
    });
    if compiler::unlikely(enc_err) {
        return compiler::cold_err(StrongActionResult::EncodingError);
    }
    if registry::state_okay() {
        if key_iter_stat_ok {
            let _kve = kve;
            let lowtable = lowtable;
            // fine, the keys were non-existent when we looked at them
            while let (Some(key), Some(value)) = (act.next(), act.next()) {
                if let Some(fresh) = lowtable.fresh_entry(Data::from(key)) {
                    fresh.insert(Data::from(value));
                }
                // we don't care if some other thread initialized the value we checked
                // it. We expected a fresh entry, so that's what we'll check and use
            }
            StrongActionResult::Okay
        } else {
            StrongActionResult::OverwriteError
        }
    } else {
        StrongActionResult::ServerError
    }
}
