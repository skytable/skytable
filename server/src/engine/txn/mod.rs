/*
 * Created on Sun Aug 20 2023
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

pub mod gns;

use super::storage::v1::SDSSError;
pub type TransactionResult<T> = Result<T, TransactionError>;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum TransactionError {
    /// corrupted txn payload. has more bytes than expected
    DecodeCorruptedPayloadMoreBytes,
    /// transaction payload is corrupted. has lesser bytes than expected
    DecodedUnexpectedEof,
    /// unknown transaction operation. usually indicates a corrupted payload
    DecodeUnknownTxnOp,
    /// While restoring a certain item, a non-resolvable conflict was encountered in the global state, because the item was
    /// already present (when it was expected to not be present)
    OnRestoreDataConflictAlreadyExists,
    /// On restore, a certain item that was expected to be present was missing in the global state
    OnRestoreDataMissing,
    /// On restore, a certain item that was expected to match a certain value, has a different value
    OnRestoreDataConflictMismatch,
    SDSSError(SDSSError),
}

direct_from! {
    TransactionError => {
        SDSSError as SDSSError
    }
}
