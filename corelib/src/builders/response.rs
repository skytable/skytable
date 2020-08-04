/*
 * Created on Tue Aug 04 2020
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

//! # The `Response` module
//! This module can be used to build responses that can be sent to clients.
//! It prepares packets following the Terrapipe protocol.

pub struct SResp {
    metaline: Vec<u8>,
    metalayout: Vec<u8>,
    dataframe: Vec<u8>,
}

impl SResp {
    pub fn new(respcode: char) -> Self {
        let mut metaline = Vec::with_capacity(46);
        metaline.push(b'*');
        metaline.push(b'!');
        metaline.push(respcode as u8);
        metaline.push(b'!');
        SResp {
            metaline,
            metalayout: Vec::with_capacity(128),
            dataframe: Vec::with_capacity(1024),
        }
    }
    pub fn add(&mut self, cmd: impl Into<Vec<u8>>) {
        let cmd = cmd.into();
        let l = cmd.len().to_string();
        self.metalayout.push(b'#');
        self.metalayout.extend(l.as_bytes());
        self.dataframe.extend(&cmd);
        self.dataframe.push(b'\n');
    }
    pub fn prepare_query(mut self) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        self.metaline
            .extend(self.dataframe.len().to_string().as_bytes());
        self.metaline.push(b'!');
        self.metaline
            .extend((self.metalayout.len() + 1).to_string().as_bytes());
        self.metaline.push(b'\n');
        self.metalayout.push(b'\n');
        (self.metaline, self.metalayout, self.dataframe)
    }
}
