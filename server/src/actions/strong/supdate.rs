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
    fn supdate(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter) {
        let howmany = act.len();
        if is_lowbit_set!(howmany) || howmany == 0 {
            return con.write_response(responses::groups::ACTION_ERR).await;
        }
        let kve = kve!(con, handle);
        let encoder = kve.get_encoder();
        let (all_okay, enc_err) = {
            self::snapshot_and_update(kve, encoder, act)
        };
        if all_okay {
            conwrite!(con, groups::OKAY)?;
        } else if compiler::unlikely(enc_err) {
            conwrite!(con, groups::ENCODING_ERROR)?;
        } else {
            conwrite!(con, groups::NIL)?;
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
) -> (bool, bool) {
    let mut err_enc = false;
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
                err_enc = true;
                false
            }
        });
    }
    cfg_test!({
        // give the caller 10 seconds to do some crap
        do_sleep!(10 s);
    });
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
                if mutable.get().eq(&snapshot) {
                    mutable.insert(Data::from(value));
                } else {
                    drop(mutable);
                }
            }
        }
        (true, false)
    } else {
        (iter_stat_ok, err_enc)
    }
}
