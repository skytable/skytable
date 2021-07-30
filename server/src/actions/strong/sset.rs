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
    /// Run an `SSET` query
    ///
    /// This either returns `Okay` if all the keys were set, or it returns an
    /// `Overwrite Error` or code `2`
    fn sset(handle: &crate::corestore::Corestore, con: &mut T, act: ActionIter) {
        let howmany = act.len();
        if is_lowbit_set!(howmany) || howmany == 0 {
            return con.write_response(responses::groups::ACTION_ERR).await;
        }
        let kve = kve!(con, handle);
        let encoder = kve.get_encoder();
        let (all_okay, enc_err) = {
            self::snapshot_and_insert(kve, encoder, act)
        };
        if all_okay {
            conwrite!(con, groups::OKAY)?;
        } else if compiler::unlikely(enc_err) {
            conwrite!(con, groups::ENCODING_ERROR)?;
        } else {
            conwrite!(con, groups::OVERWRITE_ERR)?;
        }
        Ok(())
    }
}

/// Take a consistent snapshot of the database at this current point in time
/// and then mutate the entries, respecting concurrency guarantees
/// `(all_okay, enc_err)`
pub(super) fn snapshot_and_insert(
    kve: &KVEngine,
    encoder: DoubleEncoder,
    mut act: ActionIter,
) -> (bool, bool) {
    let mut enc_err = false;
    let lowtable = kve.__get_inner_ref();
    let key_iter_stat_ok;
    {
        key_iter_stat_ok = act.as_ref().chunks_exact(2).all(|kv| unsafe {
            let key = kv.get_unchecked(0);
            let value = kv.get_unchecked(1);
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
        (true, false)
    } else {
        (key_iter_stat_ok, enc_err)
    }
}
