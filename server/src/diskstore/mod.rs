/*
 * Created on Wed Aug 05 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020, Sayan Nandan <ohsayan at outlook dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

use crate::coredb::Data;
use bincode;
use bytes::Bytes;
use corelib::TResult;
use std::collections::HashMap;
use std::fs;
use std::io::{ErrorKind, Write};
use std::iter::FromIterator;

type DiskStore = (Vec<String>, Vec<Vec<u8>>);

/// Try to get the saved data from disk
pub fn get_saved() -> TResult<Option<HashMap<String, Data>>> {
    let file = match fs::read("./data.bin") {
        Ok(f) => f,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => return Ok(None),
            _ => return Err("Couldn't read flushed data from disk".into()),
        },
    };
    let parsed: DiskStore = bincode::deserialize(&file)?;
    let parsed: HashMap<String, Data> = HashMap::from_iter(
        parsed
            .0
            .into_iter()
            .zip(parsed.1.into_iter())
            .map(|(key, value)| {
                let data = Data::from_blob(Bytes::from(value));
                (key, data)
            }),
    );
    Ok(Some(parsed))
}

/// Flush the in-memory table onto disk
pub fn flush_data(data: &HashMap<String, Data>) -> TResult<()> {
    let ds: DiskStore = (
        data.keys().into_iter().map(|val| val.to_string()).collect(),
        data.values().map(|val| val.get_blob().to_vec()).collect(),
    );
    let encoded = bincode::serialize(&ds)?;
    let mut file = fs::File::create("./data.bin")?;
    file.write_all(&encoded)?;
    Ok(())
}
