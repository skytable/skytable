/*
 * Created on Mon Feb 21 2022
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2022, Sayan Nandan <ohsayan@outlook.com>
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
    super::provider::{Authkey, AUTHKEY_SIZE},
    crate::corestore::array::Array,
    base64::{
        alphabet::BCRYPT,
        engine::{GeneralPurpose, GeneralPurposeConfig},
        Engine,
    },
};

type AuthkeyArray = Array<u8, { AUTHKEY_SIZE }>;
const RAN_BYTES_SIZE: usize = 40;
const BASE64: GeneralPurpose = GeneralPurpose::new(&BCRYPT, GeneralPurposeConfig::new());

/// Return a "human readable key" and the "authbytes" that can be stored
/// safely. To do this:
/// - Generate 64 random bytes
/// - Encode that into base64. This is the client key
/// - Hash the key using rcrypt. This is the server key that
/// will be stored
pub fn generate_full() -> (String, Authkey) {
    let mut bytes: [u8; RAN_BYTES_SIZE] = [0u8; RAN_BYTES_SIZE];
    openssl::rand::rand_bytes(&mut bytes).unwrap();
    let ret = BASE64.encode(&bytes);
    let hash = rcrypt::hash(&ret, rcrypt::DEFAULT_COST).unwrap();
    let store_in_db = unsafe {
        let mut array = AuthkeyArray::new();
        // we guarantee that the size is equal to 40
        array.extend_from_slice_unchecked(&hash);
        array.into_array_unchecked()
    };
    (ret, store_in_db)
}

/// Verify a "human readable key" against the provided "authbytes"
pub fn verify_key(input: &[u8], hash: &[u8]) -> Option<bool> {
    rcrypt::verify(input, hash).ok()
}
