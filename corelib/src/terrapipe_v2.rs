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
enum DataType {
    St(String),
    Arr(Vec<String>),
}

// TODO(@ohsayan): Optmimize this function
fn parse_dataframe(df: Vec<u8>, sizes: Vec<usize>) -> Vec<DataType> {
    unsafe {
        (0..sizes.len())
            .into_iter()
            .scan((df.into_iter(), sizes.into_iter()), |(df, sizes), _| {
                let cursize = match sizes.next() {
                    Some(s) => s,
                    _ => return None,
                };
                let curline: Vec<u8> = df.take(cursize).collect();
                if curline.len() == 0 {
                    return None;
                }
                let _ = df.next();
                match curline.get_unchecked(0) {
                    b'+' => {
                        // This is a string
                        return Some(DataType::St(
                            String::from_utf8_lossy(&curline.get_unchecked(1..)).to_string(),
                        ));
                    }
                    b'&' => {
                        // This is an array
                        let mut remsize = 0;
                        let mut it = curline.into_iter().skip(1).peekable();
                        while let Some(tok) = it.next() {
                            if it.next().is_some() {
                                let s: usize = match tok.checked_sub(48) {
                                    Some(x) => x.into(),
                                    _ => return None,
                                };
                                remsize += s;
                            }
                        }
                        let array_elems: Vec<String> = sizes
                            .take(remsize)
                            .into_iter()
                            .scan(0, |_, size| {
                                let tok: Vec<u8> = df.take(size).collect();
                                let _ = df.next();
                                Some(
                                    String::from_utf8_lossy(&tok.get_unchecked(..size - 1))
                                        .to_string(),
                                )
                            })
                            .collect();
                        // .into_iter()
                        // .map(|elemsize| {
                        //     let v: Vec<u8> = df.take(elemsize).collect();
                        //     let _ = df.next();
                        //     String::from_utf8_lossy(&v.get_unchecked(..v.len() - 1)).to_string()
                        // })
                        // .collect();
                        return Some(DataType::Arr(array_elems));
                    }
                    _ => return None,
                }
            })
            .collect()
    }
}

#[cfg(test)]
#[test]
fn test_df() {
    let ss: Vec<usize> = vec![4, 3, 6, 7, 7];
    let df = "+GET\n&3\n+sayan\n+foobar\n+opnsrc\n".as_bytes().to_owned();
    let parsed = parse_dataframe(df, ss);
    assert_eq!(
        parsed,
        vec![
            DataType::St("GET".to_owned()),
            DataType::Arr(vec![
                "sayan".to_owned(),
                "foobar".to_owned(),
                "opnsrc".to_owned(),
            ])
        ]
    );
}
