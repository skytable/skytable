/*
 * Created on Mon Jul 26 2021
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

//! # System-wide registry
//!
//! The registry module provides interfaces for system-wide, global state management
//!

use core::sync::atomic::AtomicBool;
use core::sync::atomic::Ordering;

const ORD_ACQ: Ordering = Ordering::Acquire;
const ORD_REL: Ordering = Ordering::Release;

static GLOBAL_STATE: AtomicBool = AtomicBool::new(true);

pub fn state_okay() -> bool {
    GLOBAL_STATE.load(ORD_ACQ)
}

pub fn poison() {
    GLOBAL_STATE.store(false, ORD_REL)
}

pub fn unpoison() {
    GLOBAL_STATE.store(true, ORD_REL)
}
