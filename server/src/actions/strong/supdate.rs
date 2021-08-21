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
    /// Run an `SUPDATE` query
    ///
    /// This either returns `Okay` if all the keys were updated, or it returns `Nil`
    /// or code `1`
    fn supdate(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter<'a>) {
        let howmany = act.len();
        if is_lowbit_set!(howmany) || howmany == 0 {
            return con.write_response(responses::groups::ACTION_ERR).await;
        }
        let kve = kve!(con, handle);
        if registry::state_okay() {
            let encoder = kve.get_encoder();
            let outcome = {
                self::snapshot_and_update(kve, encoder, act)
            };
            match outcome {
                StrongActionResult::Okay => conwrite!(con, groups::OKAY)?,
                StrongActionResult::Nil => {
                    // good, it failed because some key didn't exist
                    conwrite!(con, groups::NIL)?;
                },
                StrongActionResult::ServerError => conwrite!(con, groups::SERVER_ERR)?,
                StrongActionResult::EncodingError => {
                    // error we love to hate: encoding error, ugh
                    compiler::cold_err(conwrite!(con, groups::ENCODING_ERROR))?
                },
                StrongActionResult::OverwriteError => unsafe {
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

/// Take a consistent snapshot of the database at this point in time. Once snapshotting
/// completes, mutate the entries in place while keeping up with isolation guarantees
/// `(all_okay, enc_err)`
pub(super) fn snapshot_and_update(
    kve: &KVEngine,
    encoder: DoubleEncoder,
    mut act: ActionIter,
) -> StrongActionResult {
    let mut enc_err = false;
    let mut snapshots = Vec::with_capacity(act.len());
    let iter_stat_ok;
    {
        // snapshot the values at this point in time
        iter_stat_ok = act.chunks_exact(2).all(|kv| unsafe {
            let key = kv.get_unchecked(0).as_slice();
            let value = kv.get_unchecked(1).as_slice();
            if compiler::likely(encoder.is_ok(key, value)) {
                if let Some(snapshot) = kve.take_snapshot(key) {
                    snapshots.push(snapshot);
                    true
                } else {
                    false
                }
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
        // uphold consistency
        if iter_stat_ok {
            let kve = kve;
            // good, so all the values existed when we snapshotted them; let's update 'em
            let mut snap_cc = snapshots.into_iter();
            let lowtable = kve.__get_inner_ref();
            while let (Some(key), Some(value), Some(snapshot)) =
                (act.next(), act.next(), snap_cc.next())
            {
                // When we snapshotted, we looked at `snapshot`. If the value is still the
                // same, then we'll update it. Otherwise, let it be
                if let Some(mut mutable) = lowtable.mut_entry(Data::copy_from_slice(key)) {
                    if mutable.value().eq(&snapshot) {
                        mutable.insert(Data::copy_from_slice(value));
                    } else {
                        drop(mutable);
                    }
                }
            }
            StrongActionResult::Okay
        } else {
            StrongActionResult::Nil
        }
    } else {
        StrongActionResult::ServerError
    }
}

/// Take a consistent snapshot of the database at this point in time. Once snapshotting
/// completes, mutate the entries in place while keeping up with isolation guarantees
/// `(all_okay, enc_err)`
#[cfg(test)]
pub(super) fn snapshot_and_update_test(
    kve: &KVEngine,
    encoder: DoubleEncoder,
    mut act: std::vec::IntoIter<bytes::Bytes>,
) -> StrongActionResult {
    let mut enc_err = false;
    let mut snapshots = Vec::with_capacity(act.len());
    let iter_stat_ok;
    {
        // snapshot the values at this point in time
        iter_stat_ok = act.as_ref().chunks_exact(2).all(|kv| unsafe {
            let key = kv.get_unchecked(0);
            let value = kv.get_unchecked(1);
            if compiler::likely(encoder.is_ok(key, value)) {
                if let Some(snapshot) = kve.take_snapshot(key) {
                    snapshots.push(snapshot);
                    true
                } else {
                    false
                }
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
        // uphold consistency
        if iter_stat_ok {
            let kve = kve;
            // good, so all the values existed when we snapshotted them; let's update 'em
            let mut snap_cc = snapshots.into_iter();
            let lowtable = kve.__get_inner_ref();
            while let (Some(key), Some(value), Some(snapshot)) =
                (act.next(), act.next(), snap_cc.next())
            {
                // When we snapshotted, we looked at `snapshot`. If the value is still the
                // same, then we'll update it. Otherwise, let it be
                if let Some(mut mutable) = lowtable.mut_entry(Data::from(key)) {
                    if mutable.value().eq(&snapshot) {
                        mutable.insert(Data::from(value));
                    } else {
                        drop(mutable);
                    }
                }
            }
            StrongActionResult::Okay
        } else {
            StrongActionResult::Nil
        }
    } else {
        StrongActionResult::ServerError
    }
}
