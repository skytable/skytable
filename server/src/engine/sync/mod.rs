/*
 * Created on Thu Jan 19 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

pub(super) mod atm;
pub(super) mod cell;
pub(super) mod queue;
pub(super) mod smart;

use std::{cell::Cell, hint::spin_loop, thread};

/// Type to perform exponential backoff
pub struct Backoff {
    cur: Cell<u8>,
}

impl Backoff {
    const MAX_SPIN: u8 = 6;
    const MAX_YIELD: u8 = 8;
    pub fn new() -> Self {
        Self { cur: Cell::new(0) }
    }
    /// Spin a few times, giving way to the CPU but if we have spun too many times,
    /// then block by yielding to the OS scheduler. This will **eventually block**
    /// if we spin more than the set `MAX_SPIN`
    pub fn snooze(&self) {
        if self.cur.get() <= Self::MAX_SPIN {
            // we can still spin (exp)
            for _ in 0..1 << self.cur.get() {
                spin_loop();
            }
        } else {
            // nope, yield to scheduler
            thread::yield_now();
        }
        if self.cur.get() <= Self::MAX_YIELD {
            // bump current step
            self.cur.set(self.cur.get() + 1)
        }
    }
}
