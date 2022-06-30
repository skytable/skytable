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

use {
    crate::{
        actions::strong::StrongActionResult,
        corestore::Data,
        dbnet::connection::prelude::*,
        kvengine::{DoubleEncoder, KVEStandard},
        protocol::iter::DerefUnsafeSlice,
        util::compiler,
    },
    core::slice::Iter,
};

action! {
    /// Run an `SSET` query
    ///
    /// This either returns `Okay` if all the keys were set, or it returns an
    /// `Overwrite Error` or code `2`
    fn sset(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter<'a>) {
        let howmany = act.len();
        ensure_length::<P>(howmany, |size| size & 1 == 0 && size != 0)?;
        let kve = handle.get_table_with::<P, KVEBlob>()?;
        if registry::state_okay() {
            let encoder = kve.get_double_encoder();
            let outcome = unsafe {
                // UNSAFE(@ohsayan): The lifetime of `act` guarantees that the
                // pointers remain valid
                self::snapshot_and_insert(kve, encoder, act.into_inner())
            };
            match outcome {
                StrongActionResult::Okay => con._write_raw(P::RCODE_OKAY).await?,
                StrongActionResult::OverwriteError => return util::err(P::RCODE_OVERWRITE_ERR),
                StrongActionResult::ServerError => return util::err(P::RCODE_SERVER_ERR),
                StrongActionResult::EncodingError => {
                    // error we love to hate: encoding error, ugh
                    return util::err(P::RCODE_ENCODING_ERROR);
                },
                StrongActionResult::Nil => unsafe {
                    // SAFETY check: never the case
                    impossible!()
                }
            }
        } else {
            return util::err(P::RCODE_SERVER_ERR);
        }
        Ok(())
    }
}

/// Take a consistent snapshot of the database at this current point in time
/// and then mutate the entries, respecting concurrency guarantees
pub(super) fn snapshot_and_insert<'a, T: 'a + DerefUnsafeSlice>(
    kve: &'a KVEStandard,
    encoder: DoubleEncoder,
    mut act: Iter<'a, T>,
) -> StrongActionResult {
    let mut enc_err = false;
    let lowtable = kve.get_inner_ref();
    let key_iter_stat_ok;
    {
        key_iter_stat_ok = act.as_ref().chunks_exact(2).all(|kv| unsafe {
            let key = ucidx!(kv, 0).deref_slice();
            let value = ucidx!(kv, 1).deref_slice();
            if compiler::likely(encoder(key, value)) {
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
                unsafe {
                    if let Some(fresh) =
                        lowtable.fresh_entry(Data::copy_from_slice(key.deref_slice()))
                    {
                        fresh.insert(Data::copy_from_slice(value.deref_slice()));
                    }
                    // we don't care if some other thread initialized the value we checked
                    // it. We expected a fresh entry, so that's what we'll check and use
                }
            }
            StrongActionResult::Okay
        } else {
            StrongActionResult::OverwriteError
        }
    } else {
        StrongActionResult::ServerError
    }
}
