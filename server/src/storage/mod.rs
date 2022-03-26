/*
 * Created on Sat Mar 05 2022
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

/*!
# Storage Engine

The main code in here lies inside `v1`. The reason we've chose to do so is for backwards compatibility.
Unlike other projects that can _just break_, well, we can't. A database has no right to break data no
matter what the reason. You can't just mess up someone's data because you found a more efficient
way to store things. That's why we'll version modules that correspond to version of Cyanstore. It is
totally legal for one version to call data that correspond to other versions.

## How to break

Whenever we're making changes, here's what we need to keep in mind:
1. If the format has only changed, but not the corestore structures, then simply gate a v2 and change
the functions here
2. If the format has changed and so have the corestore structures, then:
    1. Move out all the _old_ corestore structures into that version gate
    2. Then create the new structures in corestore, as appropriate
    3. The methods here should "identify" a version (usually by bytemarks on the `PRELOAD` which
    is here to stay)
    4. Now, the appropriate (if any) version's decoder is called, then the old structures are restored.
    Now, create the new structures using the old ones and then finally return them

Here's some rust-flavored pseudocode:
```
let version = find_version(preload_file_contents)?;
match version {
    V1 => {
        migration::migrate(v1::read_full()?)
    }
    V2 => {
        v2::read_full()
    }
    _ => error!("Unknown version"),
}
```

The migration module, which doesn't exist, yet will always have a way to transform older structures into
the current one. This can be achieved with some trait/generic hackery (although it might be pretty simple
in practice).
*/

pub mod v1;

pub mod unflush {
    use crate::{corestore::memstore::Memstore, storage::v1::error::StorageEngineResult};
    pub fn read_full() -> StorageEngineResult<Memstore> {
        super::v1::unflush::read_full()
    }
}
