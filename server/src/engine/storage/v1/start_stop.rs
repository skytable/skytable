/*
 * Created on Mon May 29 2023
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

use {
    super::{SDSSError, SDSSErrorContext, SDSSResult},
    crate::util::os,
    std::{
        fs::File,
        io::{ErrorKind, Read, Write},
    },
};

#[cfg(not(test))]
const START_FILE: &'static str = ".start";
#[cfg(test)]
const START_FILE: &'static str = ".start_testmode";
#[cfg(not(test))]
const STOP_FILE: &'static str = ".stop";
#[cfg(test)]
const STOP_FILE: &'static str = ".stop_testmode";

const EMSG_FAILED_WRITE_START_FILE: &str =
    concat_str_to_str!("failed to write to `", START_FILE, "` file");
const EMSG_FAILED_WRITE_STOP_FILE: &str =
    concat_str_to_str!("failed to write to `", STOP_FILE, "` file");
const EMSG_FAILED_OPEN_START_FILE: &str =
    concat_str_to_str!("failed to open `", START_FILE, "` file");
const EMSG_FAILED_OPEN_STOP_FILE: &str =
    concat_str_to_str!("failed to open `", STOP_FILE, "` file");
const EMSG_FAILED_VERIFY: &str = concat_str_to_str!(
    "failed to verify `",
    START_FILE,
    concat_str_to_str!("` and `", STOP_FILE, "` timestamps")
);

#[derive(Debug)]
pub struct StartStop {
    begin: u128,
    stop_file: File,
}

#[derive(Debug)]
enum ReadNX {
    Created(File),
    Read(File, u128),
}

impl ReadNX {
    const fn created(&self) -> bool {
        matches!(self, Self::Created(_))
    }
    fn file_mut(&mut self) -> &mut File {
        match self {
            Self::Created(ref mut f) => f,
            Self::Read(ref mut f, _) => f,
        }
    }
    fn into_file(self) -> File {
        match self {
            Self::Created(f) => f,
            Self::Read(f, _) => f,
        }
    }
}

impl StartStop {
    fn read_time_file(f: &str, create_new_if_nx: bool) -> SDSSResult<ReadNX> {
        let mut f = match File::options().write(true).read(true).open(f) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound && create_new_if_nx => {
                let f = File::create(f)?;
                return Ok(ReadNX::Created(f));
            }
            Err(e) => return Err(e.into()),
        };
        let len = f.metadata().map(|m| m.len())?;
        if len != sizeof!(u128) as u64 {
            return Err(SDSSError::corrupted_file(START_FILE));
        }
        let mut buf = [0u8; sizeof!(u128)];
        f.read_exact(&mut buf)?;
        Ok(ReadNX::Read(f, u128::from_le_bytes(buf)))
    }
    pub fn terminate(mut self) -> SDSSResult<()> {
        self.stop_file
            .write_all(self.begin.to_le_bytes().as_ref())
            .map_err(|e| e.with_extra(EMSG_FAILED_WRITE_STOP_FILE))
    }
    pub fn verify_and_start() -> SDSSResult<Self> {
        // read start file
        let mut start_file = Self::read_time_file(START_FILE, true)
            .map_err(|e| e.with_ioerror_extra(EMSG_FAILED_OPEN_START_FILE))?;
        // read stop file
        let stop_file = Self::read_time_file(STOP_FILE, start_file.created())
            .map_err(|e| e.with_ioerror_extra(EMSG_FAILED_OPEN_STOP_FILE))?;
        // read current time
        let ctime = os::get_epoch_time();
        match (&start_file, &stop_file) {
            (ReadNX::Read(_, time_start), ReadNX::Read(_, time_stop))
                if time_start == time_stop => {}
            (ReadNX::Created(_), ReadNX::Created(_)) => {}
            _ => return Err(SDSSError::OtherError(EMSG_FAILED_VERIFY)),
        }
        start_file
            .file_mut()
            .write_all(&ctime.to_le_bytes())
            .map_err(|e| e.with_extra(EMSG_FAILED_WRITE_START_FILE))?;
        Ok(Self {
            stop_file: stop_file.into_file(),
            begin: ctime,
        })
    }
}

#[test]
fn verify_test() {
    let x = || -> SDSSResult<()> {
        let ss = StartStop::verify_and_start()?;
        ss.terminate()?;
        let ss = StartStop::verify_and_start()?;
        ss.terminate()?;
        std::fs::remove_file(START_FILE)?;
        std::fs::remove_file(STOP_FILE)?;
        Ok(())
    };
    x().unwrap();
}
