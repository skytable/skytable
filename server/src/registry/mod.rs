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

use {
    crate::corestore::lock::{QLGuard, QuickLock},
    core::sync::atomic::{AtomicBool, Ordering},
};

const ORD_ACQ: Ordering = Ordering::Acquire;
const ORD_REL: Ordering = Ordering::Release;
const ORD_SEQ: Ordering = Ordering::SeqCst;

/// A digital _trip switch_ that can be tripped and untripped in a thread
/// friendly, consistent manner. It is slightly expensive on processors
/// with weaker memory ordering (like ARM) when compared to the native
/// strong ordering provided by some platforms (like x86).
pub struct Trip {
    /// the switch
    inner: AtomicBool,
}

impl Trip {
    /// Get an untripped switch
    pub const fn new_untripped() -> Self {
        Self {
            inner: AtomicBool::new(false),
        }
    }
    /// trip the switch
    pub fn trip(&self) {
        // we need the strongest consistency here
        self.inner.store(true, ORD_SEQ)
    }
    /// reset the switch
    pub fn untrip(&self) {
        // we need the strongest consistency here
        self.inner.store(false, ORD_SEQ)
    }
    /// check if the switch has tripped
    pub fn is_tripped(&self) -> bool {
        self.inner.load(ORD_SEQ)
    }
    /// Returns the previous state and untrips the switch. **Single op**
    pub fn check_and_untrip(&self) -> bool {
        self.inner.swap(false, ORD_SEQ)
    }
}

/// The global system health
static GLOBAL_STATE: AtomicBool = AtomicBool::new(true);
/// The global flush state
static FLUSH_STATE: QuickLock<()> = QuickLock::new(());
/// The preload trip switch
static PRELOAD_TRIPSWITCH: Trip = Trip::new_untripped();
static CLEANUP_TRIPSWITCH: Trip = Trip::new_untripped();

/// Check the global system state
pub fn state_okay() -> bool {
    GLOBAL_STATE.load(ORD_ACQ)
}

/// Lock the global flush state. **Remember to drop the lock guard**; else you'll
/// end up pausing all sorts of global flushing/transactional systems
pub fn lock_flush_state() -> QLGuard<'static, ()> {
    FLUSH_STATE.lock()
}

/// Poison the global system state
pub fn poison() {
    GLOBAL_STATE.store(false, ORD_REL)
}

/// Unpoison the global system state
pub fn unpoison() {
    GLOBAL_STATE.store(true, ORD_REL)
}

/// Get a static reference to the global preload trip switch
pub fn get_preload_tripswitch() -> &'static Trip {
    &PRELOAD_TRIPSWITCH
}

/// Get a static reference to the global cleanup trip switch
pub fn get_cleanup_tripswitch() -> &'static Trip {
    &CLEANUP_TRIPSWITCH
}
