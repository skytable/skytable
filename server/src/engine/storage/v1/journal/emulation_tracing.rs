/*
 * Created on Thu Jan 04 2024
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

#[cfg(test)]
local! {
    /// event trace
    pub(super) static _EVTRACE: Vec<_JournalEventTrace> = Vec::new();
    /// fault injection
    pub(super) static _FAULT_INJECTION: Vec<_EmulateInjection> = Vec::new();
    /// set to true if we inject a fault and are then required to add a recovery block
    pub(super) static _EMULATE_ALLOW_RECOVER: bool = true;
    pub(super) static __PENDING_APPEND_RECOVERY_BLOCK: bool = false;
}

#[derive(Debug, PartialEq)]
#[cfg_attr(not(test), allow(unused))]
/// Emulate a fault
pub enum _EmulateInjection {
    /// do not change the event ID
    EventIDCorrupted,
    /// corrupt the metadata
    EventSourceCorrupted,
    /// add an illegal checksum
    EventChecksumCorrupted,
    /// mess up the payload length
    EventPayloadLenIsGreaterBy(u64),
    /// don't append the payload
    PayloadMissing,
}

#[cfg(test)]
/// Emulate a fault
///
/// NB: This will be repeated for every event!
pub(in crate::engine::storage::v1) fn __emulate_injection(emulate: _EmulateInjection) {
    local_mut!(_FAULT_INJECTION, |em| em.push(emulate))
}

#[cfg(test)]
/// Don't trigger a recovery event, even if a fault was injected
pub(in crate::engine::storage::v1) fn __emulate_disallow_recovery() {
    local_mut!(_EMULATE_ALLOW_RECOVER, |em| *em = false);
}

#[inline(always)]
pub(in crate::engine::storage::v1) fn __is_emulated_fault() -> bool {
    let ret;
    #[cfg(test)]
    {
        ret = local_mut!(__PENDING_APPEND_RECOVERY_BLOCK, |need_recovery| {
            let r = *need_recovery;
            *need_recovery ^= *need_recovery;
            r
        });
    }
    #[cfg(not(test))]
    {
        ret = false;
    }
    ret
}

/// This macro either injects a fault in test mode, or it will simply do the "expected" thing in a
/// non-test configuration
macro_rules! __inject_during_test_or {
    (if let @$event:ident $if:block else $else:block) => {{
        let ret;
        #[cfg(test)]
        {
            ret = local_ref!(
                crate::engine::storage::v1::journal::emulation_tracing::_FAULT_INJECTION,
                |em| {
                    for em in em.iter() {
                        if let crate::engine::storage::v1::journal::emulation_tracing::_EmulateInjection::$event = em {
                            local_mut!(crate::engine::storage::v1::journal::emulation_tracing::__PENDING_APPEND_RECOVERY_BLOCK, |v| *v = true);
                            return $if;
                        }
                    }
                    $else
                }
            )
        }
        #[cfg(not(test))]
        {
            ret = $else;
        }
        ret
    }};
    (if let @$event:ident($val:ident) $if:block else $else:block) => {{
        let ret;
        #[cfg(test)]
        {
            ret = local_ref!(crate::engine::storage::v1::journal::emulation_tracing::_FAULT_INJECTION, |em| {
                for em in em.iter() {
                    if let crate::engine::storage::v1::journal::emulation_tracing::_EmulateInjection::$event($val) = em {
                        return $if;
                    }
                }
                return $else;
            })
        }
        #[cfg(not(test))]
        {
            ret = $else;
        }
        ret
    }};
}

#[derive(Debug, PartialEq)]
#[cfg_attr(not(test), allow(unused))]
pub enum _JournalReaderTraceEvent {
    Initialized,
    BeginEventsScan,
    ErrorUnclosed,
    EOF,
    EntryReadRawMetadata,
    IffyEventIDMismatch(u128),
    EventKindStandard(u128),
    IffyReopen,
    ErrorUnexpectedEvent,
    Success,
    ErrorExpectedPayloadButEOF,
    ErrorChecksumMismatch,
    ErrorFailedToApplyEvent,
    CompletedEvent,
    ReopenCheck,
    ReopenSuccess(u64),
    ErrorReopenFailedBadBlock,
    ErrorExpectedReopenGotEOF,
    HitClose(u128),
}

#[derive(Debug, PartialEq)]
#[cfg_attr(not(test), allow(unused))]
pub enum _JournalReaderTraceRecovery {
    InitialCursorRestoredForRecoveryBlockCheck,
    ExitWithFailedToReadBlock,
    Success(u64),
    ExitWithInvalidBlock,
}

#[derive(Debug, PartialEq)]
#[cfg_attr(not(test), allow(unused))]
pub enum _JournalWriterTraceEvent {
    Initialized,
    Reopened(u64),
    Reinitializing,
    CompletedEventAppend(u64),
    ErrorAddingNewEvent,
    RecoveryEventAdded(u64),
    ErrorRecoveryFailed,
    Closed(u64),
}

direct_from! {
    _JournalEventTrace => {
        _JournalReaderTraceEvent as Reader,
        _JournalReaderTraceRecovery as ReaderRecovery,
        _JournalWriterTraceEvent as Writer,
        _JournalWriterInjectedWith as WriterInjection,
    }
}

#[derive(Debug, PartialEq)]
#[cfg_attr(not(test), allow(unused))]
pub enum _JournalWriterInjectedWith {
    BadEventID,
    BadSource,
    BadChecksum,
    BadPayloadLen(u64),
    MissingPayload,
}

#[derive(Debug, PartialEq)]
#[cfg_attr(not(test), allow(unused))]
pub enum _JournalEventTrace {
    InitCreated,
    InitRestored,
    Reader(_JournalReaderTraceEvent),
    ReaderRecovery(_JournalReaderTraceRecovery),
    Writer(_JournalWriterTraceEvent),
    WriterInjection(_JournalWriterInjectedWith),
}

#[inline(always)]
/// Push a trace event to the trace
pub(super) fn __journal_evtrace(_trace: impl Into<_JournalEventTrace>) {
    #[cfg(test)]
    {
        local_mut!(_EVTRACE, |events| events.push(_trace.into()))
    }
}

#[cfg(test)]
/// Unwind the event trace
pub(in crate::engine::storage::v1) fn __unwind_evtrace() -> Vec<_JournalEventTrace> {
    local_mut!(_EVTRACE, core::mem::take)
}
