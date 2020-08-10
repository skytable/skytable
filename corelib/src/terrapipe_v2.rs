/*
 * Created on Sun Aug 09 2020
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

//! This v2 of the Terrapipe protocol, or more like an attempt to build the
//! first stable version of Terrapipe.  
//! **WARNING ⚠**:  This is **purely experimental**  
//! **Note:** Checking with older benchmarks, the new evaluation would add to about
//! 1 microsec of processing time, as opposed to the 0.1 microsec processing time
//! of the previous layout. Nothing is finalized yet, but this is what it looks like
//! currently

#[derive(Debug, PartialEq)]
pub struct Action(Vec<String>);

type Dataset = Vec<String>;

pub enum Actions {
    DEL(Dataset),
    EXISTS(Dataset),
    GET(Dataset),
    HEYA,
    SET(Dataset),
    UPDATE(Dataset),
}

pub fn parse_df(buf: Vec<u8>, sizes: Vec<usize>, nc: usize) -> Option<Vec<Action>> {
    let (mut i, mut pos) = (0, 0);
    if buf.len() < 1 || sizes.len() < 1 {
        // Having fun, eh? Why're you giving empty dataframes?
        return None;
    }
    let mut commands = Vec::with_capacity(nc);
    while i < sizes.len() && pos < buf.len() {
        // Allocate everything first
        unsafe {
            let cursize = sizes.get_unchecked(0);
            i += 1; // We've just read a line push it ahead
                    // Get the current line-> pos..pos+cursize+1
            let curline = match buf.get(pos..pos + cursize + 1) {
                Some(line) => line,
                None => return None,
            };
            // We've read `cursize` number of elements, so skip them
            // Also skip the newline
            pos += cursize + 1;
            if *curline.get_unchecked(0) == b'&' {
                // A valid action array
                let mut cursize = 0usize; // The number of elements in this action array
                let mut k = 1; // Skip the '&' character in `curline`
                while k < (curline.len() - 1) {
                    let cur_dig: usize = match curline.get_unchecked(k).checked_sub(48) {
                        Some(dig) => {
                            if dig > 9 {
                                // For the UTF8 character to be a number (0-9)
                                // `dig` must be lesser than 9, since `48` is the UTF8
                                // code for 0
                                return None;
                            } else {
                                dig.into()
                            }
                        }
                        None => return None,
                    };
                    cursize = (cursize * 10) + cur_dig;
                    k += 1;
                }
                let mut toks: Vec<String> = sizes
                    .iter()
                    .take(cursize)
                    .map(|sz| String::with_capacity(*sz))
                    .collect();
                let mut l = 0;
                // We now know the array size, so let's parse it!
                // Get all the sizes of the array elements
                let arr_elem_sizes = match sizes.get(i..(i + cursize)) {
                    Some(sizes) => sizes,
                    None => return None,
                };
                i += cursize; // We've already read `cursize` items from the `sizes` array
                arr_elem_sizes
                    .into_iter()
                    .zip(toks.iter_mut())
                    .for_each(|(size, empty_buf)| {
                        let extracted = match buf.get(pos..pos + size) {
                            Some(ex) => ex,
                            None => return (),
                        };
                        pos += size + 1; // Advance `pos` by `sz` and `1` for the newline
                        l += 1; // Move ahead
                        *empty_buf = String::from_utf8_lossy(extracted).to_string();
                    });
                if toks.len() != cursize {
                    return None;
                }
                // We're done with parsing the entire array, return it
                commands.push(Action(toks));
            } else {
                i += 1;
                continue;
            }
        }
    }
    Some(commands)
}

#[cfg(test)]
#[test]
fn test_df() {
    let ss: Vec<usize> = vec![2, 3, 5, 6, 6];
    let df = "&4\nGET\nsayan\nfoobar\nopnsrc\n".as_bytes().to_owned();
    let parsed = parse_df(df, ss, 1).unwrap();
    assert_eq!(
        parsed,
        vec![Action(vec![
            "GET".to_owned(),
            "sayan".to_owned(),
            "foobar".to_owned(),
            "opnsrc".to_owned()
        ])]
    );
}
