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

use crate::dbnet::connection::prelude::*;
use crate::kvengine::KVEngine;
use crate::kvengine::SingleEncoder;
use crate::util::compiler;

action! {
    /// Run an `SDEL` query
    ///
    /// This either returns `Okay` if all the keys were `del`eted, or it returns a
    /// `Nil`, which is code `1`
    fn sdel(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter) {
        err_if_len_is!(act, con, eq 0);
        let kve = kve!(con, handle);
        let key_encoder = kve.get_key_encoder();
        let (all_okay, enc_err) = {
            self::snapshot_and_del(kve, key_encoder, act)
        };
        if all_okay {
            conwrite!(con, groups::OKAY)?;
        } else if compiler::unlikely(enc_err) {
            // the errors we love to hate: encoding error
            conwrite!(con, groups::ENCODING_ERROR)?;
        } else {
            // good, it failed because some key didn't exist
            conwrite!(con, groups::NIL)?;
        }
        Ok(())
    }
}

/// Snapshot the current status and then delete maintaining concurrency
/// guarantees. `(all_okay, enc_err)`
fn snapshot_and_del(kve: &KVEngine, key_encoder: SingleEncoder, act: ActionIter) -> (bool, bool) {
    let mut snapshots = Vec::with_capacity(act.len());
    let mut err_enc = false;
    let iter_stat_ok;
    {
        iter_stat_ok = act.as_ref().iter().all(|key| {
            if compiler::likely(key_encoder.is_ok(key)) {
                if let Some(snap) = kve.take_snapshot(key) {
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
    if iter_stat_ok {
        // nice, all keys exist; let's plonk 'em
        let kve = kve;
        let lowtable = kve.__get_inner_ref();
        act.zip(snapshots).for_each(|(key, snapshot)| {
            // the check is very important: some thread may have updated the
            // value after we snapshotted it. In that case, let this key
            // be whatever the "newer" value is. Since our snapshot is a "happens-before"
            // thing, this is absolutely fine
            let _ = lowtable.remove_if(&key, |_, val| val.eq(&snapshot));
        });
        (true, false)
    } else {
        (iter_stat_ok, err_enc)
    }
}
