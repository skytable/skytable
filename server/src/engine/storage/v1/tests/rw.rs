/*
 * Created on Tue Sep 05 2023
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

use crate::engine::storage::v1::{
    header_impl::{FileScope, FileSpecifier, FileSpecifierVersion, HostRunMode},
    rw::{FileOpen, SDSSFileIO},
};

#[test]
fn create_delete() {
    {
        let f = SDSSFileIO::<super::VirtualFS>::open_or_create_perm_rw::<false>(
            "hello_world.db-tlog",
            FileScope::Journal,
            FileSpecifier::GNSTxnLog,
            FileSpecifierVersion::__new(0),
            0,
            HostRunMode::Prod,
            0,
        )
        .unwrap();
        match f {
            FileOpen::Existing(_) => panic!(),
            FileOpen::Created(_) => {}
        };
    }
    let open = SDSSFileIO::<super::VirtualFS>::open_or_create_perm_rw::<false>(
        "hello_world.db-tlog",
        FileScope::Journal,
        FileSpecifier::GNSTxnLog,
        FileSpecifierVersion::__new(0),
        0,
        HostRunMode::Prod,
        0,
    )
    .unwrap();
    let h = match open {
        FileOpen::Existing((_, header)) => header,
        _ => panic!(),
    };
    assert_eq!(h.gr_mdr().file_scope(), FileScope::Journal);
    assert_eq!(h.gr_mdr().file_spec(), FileSpecifier::GNSTxnLog);
    assert_eq!(h.gr_mdr().file_spec_id(), FileSpecifierVersion::__new(0));
    assert_eq!(h.gr_hr().run_mode(), HostRunMode::Prod);
    assert_eq!(h.gr_hr().setting_version(), 0);
    assert_eq!(h.gr_hr().startup_counter(), 0);
}
