/*
 * Created on Wed May 17 2023
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

const VERSION_TAGS: [&str; 52] = [
    "v0.1.0",
    "v0.2.0",
    "v0.3.0",
    "v0.3.1",
    "v0.3.2",
    "v0.4.0-alpha.1",
    "v0.4.0-alpha.2",
    "v0.4.0",
    "v0.4.1-alpha.1",
    "v0.4.1",
    "v0.4.2-alpha.1",
    "v0.4.2",
    "v0.4.3-alpha.1",
    "v0.4.3",
    "v0.4.4",
    "v0.4.5-alpha.1",
    "v0.4.5-alpha.2",
    "v0.4.5",
    "v0.5.0-alpha.1",
    "v0.5.0-alpha.2",
    "v0.5.0",
    "v0.5.1-alpha.1",
    "v0.5.1",
    "v0.5.2",
    "v0.5.3",
    "v0.6.0",
    "v0.6.1",
    "v0.6.2-testrelease.1",
    "v0.6.2",
    "v0.6.3-alpha.1",
    "v0.6.3",
    "v0.6.4-alpha.1",
    "v0.6.4",
    "v0.7.0-RC.1",
    "v0.7.0-alpha.1",
    "v0.7.0-alpha.2",
    "v0.7.0-beta.1",
    "v0.7.0",
    "v0.7.1-alpha.1",
    "v0.7.1",
    "v0.7.2-alpha.1",
    "v0.7.2",
    "v0.7.3-alpha.1",
    "v0.7.3-alpha.2",
    "v0.7.3-alpha.3",
    "v0.7.3",
    "v0.7.4",
    "v0.7.5",
    "v0.7.6",
    "v0.7.7",
    "v0.8.0-alpha.1",
    "v0.8.0",
];
const VERSION_TAGS_LEN: usize = VERSION_TAGS.len();
pub const fn fetch_id(id: &str) -> usize {
    // this is ct, so a O(n) doesn't matter
    let mut i = 0;
    while i < VERSION_TAGS_LEN {
        let bytes = VERSION_TAGS[i].as_bytes();
        let given = id.as_bytes();
        let mut j = 0;
        let mut eq = true;
        while (j < bytes.len()) & (bytes.len() == given.len()) {
            if bytes[i] != given[i] {
                eq = false;
                break;
            }
            j += 1;
        }
        if eq {
            return i;
        }
        i += 1;
    }
    panic!("version not found")
}
