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
use crate::dbnet::connection::prelude::*;
use crate::kvengine::{KVEStandard, SingleEncoder};
use crate::protocol::iter::DerefUnsafeSlice;
use crate::util::compiler;
use core::slice::Iter;

action! {
    /// Run an `SDEL` query
    ///
    /// This either returns `Okay` if all the keys were `del`eted, or it returns a
    /// `Nil`, which is code `1`
    fn sdel(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter<'a>) {
        ensure_length(act.len(), |len| len != 0)?;
        let kve = handle.get_table_with::<KVE>()?;
        if registry::state_okay() {
            // guarantee one check: consistency
            let key_encoder = kve.get_key_encoder();
            let outcome = unsafe {
                // UNSAFE(@ohsayan): The lifetime of `act` ensures that the
                // pointers are still valid
                self::snapshot_and_del(kve, key_encoder, act.into_inner())
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

/// Snapshot the current status and then delete maintaining concurrency
/// guarantees
pub(super) fn snapshot_and_del<'a, T: 'a + DerefUnsafeSlice>(
    kve: &'a KVEStandard,
    key_encoder: SingleEncoder,
    act: Iter<'a, T>,
) -> StrongActionResult {
    let mut snapshots = Vec::with_capacity(act.len());
    let mut err_enc = false;
    let iter_stat_ok;
    {
        iter_stat_ok = act.as_ref().iter().all(|key| {
            let key = unsafe {
                // UNSAFE(@ohsayan): The caller has passed a slice and they should
                // ensure that it is valid
                key.deref_slice()
            };
            if compiler::likely(key_encoder(key)) {
                if let Some(snap) = kve.take_snapshot_unchecked(key) {
                    snapshots.push(snap);
                    true
                } else {
                    false
                }
            } else {
                err_enc = true;
                false
            }
        });
    }
    cfg_test!({
        // give the caller 10 seconds to do some crap
        do_sleep!(10 s);
    });
    if compiler::unlikely(err_enc) {
        return compiler::cold_err(StrongActionResult::EncodingError);
    }
    if registry::state_okay() {
        // guarantee upholded: consistency
        if iter_stat_ok {
            // nice, all keys exist; let's plonk 'em
            let kve = kve;
            let lowtable = kve.get_inner_ref();
            act.zip(snapshots).for_each(|(key, snapshot)| {
                let key = unsafe {
                    // UNSAFE(@ohsayan): The caller has passed a slice and they should
                    // ensure that it is valid
                    key.deref_slice()
                };
                // the check is very important: some thread may have updated the
                // value after we snapshotted it. In that case, let this key
                // be whatever the "newer" value is. Since our snapshot is a "happens-before"
                // thing, this is absolutely fine
                let _ = lowtable.remove_if(key, |_, val| val.eq(&snapshot));
            });
            StrongActionResult::Okay
        } else {
            StrongActionResult::Nil
        }
    } else {
        StrongActionResult::ServerError
    }
}
