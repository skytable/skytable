/*
 * Created on Tue Feb 13 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
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

use {
    self::raw::{JournalAdapter, JournalWriter},
    crate::engine::{
        core::GlobalNS,
        error::TransactionError,
        mem::BufferedScanner,
        storage::{
            common_encoding::r1::impls::gns::GNSEvent,
            safe_interfaces::{FSInterface, LocalFS},
        },
        txn::gns,
        RuntimeResult,
    },
};

pub mod raw;

/// The GNS transaction driver is used to handle DDL transactions
pub struct GNSTransactionDriverAnyFS<Fs: FSInterface = LocalFS> {
    journal: JournalWriter<Fs, GNSAdapter>,
}

impl<Fs: FSInterface> GNSTransactionDriverAnyFS<Fs> {
    pub fn new(journal: JournalWriter<Fs, GNSAdapter>) -> Self {
        Self { journal }
    }
    pub fn into_inner(self) -> JournalWriter<Fs, GNSAdapter> {
        self.journal
    }
    pub fn __journal_mut(&mut self) -> &mut JournalWriter<Fs, GNSAdapter> {
        &mut self.journal
    }
    /// Attempts to commit the given event into the journal, handling any possible recovery triggers and returning
    /// errors (if any)
    pub fn try_commit<GE: GNSEvent>(&mut self, gns_event: GE) -> RuntimeResult<()> {
        let mut buf = vec![];
        buf.extend((GE::CODE as u16).to_le_bytes());
        GE::encode_event(gns_event, &mut buf);
        self.journal
            .append_event_with_recovery_plugin(GNSSuperEvent(buf.into_boxed_slice()))?;
        Ok(())
    }
}

/*
    journal implementor
*/

pub struct GNSSuperEvent(Box<[u8]>);

/// the journal adapter for DDL queries on the GNS
#[derive(Debug)]
pub struct GNSAdapter;

impl JournalAdapter for GNSAdapter {
    const RECOVERY_PLUGIN: bool = true;
    type JournalEvent = GNSSuperEvent;
    type GlobalState = GlobalNS;
    type Error = crate::engine::fractal::error::Error;
    fn encode(GNSSuperEvent(b): Self::JournalEvent) -> Box<[u8]> {
        b
    }
    fn decode_and_update_state(payload: &[u8], gs: &Self::GlobalState) -> RuntimeResult<()> {
        macro_rules! dispatch {
            ($($item:ty),* $(,)?) => {
                [$(<$item as GNSEvent>::decode_and_update_global_state),*, |_, _| Err(TransactionError::DecodeUnknownTxnOp.into())]
            };
        }
        static DISPATCH: [fn(&mut BufferedScanner, &GlobalNS) -> RuntimeResult<()>; 9] = dispatch!(
            gns::space::CreateSpaceTxn,
            gns::space::AlterSpaceTxn,
            gns::space::DropSpaceTxn,
            gns::model::CreateModelTxn,
            gns::model::AlterModelAddTxn,
            gns::model::AlterModelRemoveTxn,
            gns::model::AlterModelUpdateTxn,
            gns::model::DropModelTxn
        );
        if payload.len() < 2 {
            return Err(TransactionError::DecodedUnexpectedEof.into());
        }
        let mut scanner = BufferedScanner::new(&payload);
        let opc = unsafe {
            // UNSAFE(@ohsayan): first branch ensures atleast two bytes
            u16::from_le_bytes(scanner.next_chunk())
        };
        match DISPATCH[(opc as usize).min(DISPATCH.len())](&mut scanner, gs) {
            Ok(()) if scanner.eof() => return Ok(()),
            Ok(_) => Err(TransactionError::DecodeCorruptedPayloadMoreBytes.into()),
            Err(e) => Err(e),
        }
    }
}
