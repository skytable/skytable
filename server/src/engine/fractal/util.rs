/*
 * Created on Sat Sep 09 2023
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

use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct Status {
    okay: AtomicBool,
}

impl Status {
    pub const fn new_okay() -> Self {
        Self::new(true)
    }
    const fn new(v: bool) -> Self {
        Self {
            okay: AtomicBool::new(v),
        }
    }
}

impl Status {
    pub fn is_iffy(&self) -> bool {
        !self._get()
    }
    pub fn is_healthy(&self) -> bool {
        self._get()
    }
    fn _get(&self) -> bool {
        self.okay.load(Ordering::Acquire)
    }
}

impl Status {
    pub(super) fn set_okay(&self) {
        self._set(true)
    }
    pub(super) fn set_iffy(&self) {
        self._set(false)
    }
    fn _set(&self, v: bool) {
        self.okay.store(v, Ordering::Release)
    }
}

/// A special token for fractal calls
pub struct FractalToken(());
impl FractalToken {
    pub(super) fn new() -> Self {
        Self(())
    }
}
