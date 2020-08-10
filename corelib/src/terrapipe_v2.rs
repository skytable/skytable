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
    let mut tokens = Vec::with_capacity(nc);
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
                tokens.push(Action(toks));
            } else {
                i += 1;
                continue;
            }
        }
    }
    Some(tokens)
}

mod builders {
    use crate::terrapipe::*;
    pub trait IntoTpArgs {
        fn into_tp_args(self) -> (Vec<usize>, Vec<u8>);
    }
    // A simple action like `HEYA`
    impl IntoTpArgs for String {
        fn into_tp_args(self) -> (Vec<usize>, Vec<u8>) {
            let mut sizeline = vec![b'&'];
            let mut sizes = Vec::with_capacity(2);
            sizeline.push(b'1');
            sizes.push(sizeline.len());
            sizeline.push(b'\n');
            let mut bts = self.as_bytes().to_owned();
            sizes.push(bts.len());
            bts.push(b'\n');
            let qbytes = [sizeline, bts].concat();
            (sizes, qbytes)
        }
    }

    impl IntoTpArgs for &str {
        fn into_tp_args(self) -> (Vec<usize>, Vec<u8>) {
            let mut sizeline = vec![b'&'];
            let mut sizes = Vec::with_capacity(2);
            sizeline.push(b'1');
            sizes.push(sizeline.len());
            sizeline.push(b'\n');
            let mut bts = self.as_bytes().to_owned();
            sizes.push(bts.len());
            bts.push(b'\n');
            let qbytes = [sizeline, bts].concat();
            (sizes, qbytes)
        }
    }

    // Actions like ["GET", "foo", "bar", "sayan"]
    impl IntoTpArgs for &[&str] {
        fn into_tp_args(self) -> (Vec<usize>, Vec<u8>) {
            let mut sizes = Vec::with_capacity(self.len());
            let mut sizeline = vec![b'&'];
            sizeline.extend(self.len().to_string().as_bytes());
            sizes.push(sizeline.len());
            sizeline.push(b'\n');
            let arg_bytes = [
                sizeline,
                self.into_iter()
                    .map(|arg| {
                        let mut x = arg.as_bytes().to_owned();
                        sizes.push(x.len());
                        x.push(b'\n');
                        x
                    })
                    .flatten()
                    .collect::<Vec<u8>>(),
            ]
            .concat();
            (sizes, arg_bytes)
        }
    }

    pub enum QueryBuilder {
        Simple(SQuery),
        // TODO(@ohsayan): Implement pipelined queries
    }

    impl QueryBuilder {
        pub fn new_simple() -> SQuery {
            SQuery::new()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct SQuery {
        metaline: Vec<u8>,
        metalayout: Vec<u8>,
        dataframe: Vec<u8>,
    }

    impl SQuery {
        pub fn new() -> Self {
            let mut metaline = Vec::with_capacity(DEF_QMETALINE_BUFSIZE);
            metaline.push(b'*');
            metaline.push(b'!');
            SQuery {
                metaline,
                metalayout: Vec::with_capacity(128),
                dataframe: Vec::with_capacity(512),
            }
        }
        pub fn add_action(&mut self, args: impl IntoTpArgs) {
            let (skips, action_bytes) = args.into_tp_args();
            self.dataframe.extend(action_bytes);
            skips.into_iter().for_each(|skip| {
                self.metalayout.push(b'#');
                self.metalayout.extend(skip.to_string().as_bytes());
            });
        }
        pub fn into_query(mut self) -> Vec<u8> {
            self.metaline
                .extend(self.dataframe.len().to_string().as_bytes());
            self.metaline.push(b'!');
            self.metaline
                .extend((self.metalayout.len() + 1).to_string().as_bytes());
            self.metaline.push(b'\n');
            self.metalayout.push(b'\n');
            [self.metaline, self.metalayout, self.dataframe].concat()
        }
    }

    #[cfg(test)]
    #[test]
    fn test_traits() {
        let arg = ["get", "sayan", "foo", "bar"];
        let (sizes, resp) = arg.into_tp_args();
        let resp_should_be = "&4\nget\nsayan\nfoo\nbar\n".as_bytes();
        let sizes_should_be: Vec<usize> = vec![2, 3, 5, 3, 3];
        assert_eq!(resp, resp_should_be);
        assert_eq!(sizes, sizes_should_be);
    }

    #[cfg(test)]
    #[test]
    fn test_querybuilder() {
        let mut query = QueryBuilder::new_simple();
        query.add_action("heya");
        assert_eq!(
            "*!8!5\n#2#4\n&1\nheya\n".as_bytes().to_owned(),
            query.into_query()
        );
        let mut query = QueryBuilder::new_simple();
        let action = ["get", "sayan", "foo", "bar"];
        query.add_action(&action[..]);
        assert_eq!(
            "*!21!11\n#2#3#5#3#3\n&4\nget\nsayan\nfoo\nbar\n"
                .as_bytes()
                .to_owned(),
            query.into_query()
        )
    }

    pub trait IntoTpResponse {
        fn into_tp_response(&self) -> (Vec<usize>, Vec<u8>);
    }

    impl IntoTpResponse for String {
        fn into_tp_response(&self) -> (Vec<usize>, Vec<u8>) {
            let mut sizes = Vec::with_capacity(2);
            let mut bts = Vec::with_capacity(self.len() + 1);
            bts.push(b'+');
            bts.extend(self.as_bytes().to_owned());
            sizes.push(bts.len());
            bts.push(b'\n');
            (sizes, bts)
        }
    }

    impl IntoTpResponse for &str {
        fn into_tp_response(&self) -> (Vec<usize>, Vec<u8>) {
            let mut sizes = Vec::with_capacity(2);
            let mut bts = Vec::with_capacity(self.len() + 1);
            bts.push(b'+');
            bts.extend(self.as_bytes().to_owned());
            sizes.push(bts.len());
            bts.push(b'\n');
            (sizes, bts)
        }
    }

    impl IntoTpResponse for RespCodes {
        fn into_tp_response(&self) -> (Vec<usize>, Vec<u8>) {
            use RespCodes::*;
            match self {
                Okay => (vec![3], vec![b'!', b'0']),
                NotFound => (vec![3], vec![b'!', b'1']),
                OverwriteError => (vec![3], vec![b'!', b'2']),
                InvalidMetaframe => (vec![3], vec![b'!', b'3']),
                ArgumentError => (vec![3], vec![b'!', b'4']),
                ServerError => (vec![3], vec![b'!', b'5']),
                OtherError(e) => {
                    if let Some(e) = e {
                        let mut respline = e.as_bytes().to_owned();
                        respline.push(b'\n');
                        // One for the ! character and one for the LF
                        let resplen = respline.len() + 2;
                        (vec![resplen], [vec![b'!'], respline].concat())
                    } else {
                        (vec![3], vec![b'!', b'6'])
                    }
                }
            }
        }
    }

    pub enum ResponseBuilder {
        Simple(SResp),
        // TODO(@ohsayan): Add pipelined responses here
    }

    impl ResponseBuilder {
        pub fn new_simple() -> SResp {
            SResp::new()
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct SResp {
        metaline: Vec<u8>,
        metalayout: Vec<u8>,
        dataframe: Vec<u8>,
    }

    impl SResp {
        pub fn new() -> Self {
            let mut metaline = Vec::with_capacity(DEF_QMETALINE_BUFSIZE);
            metaline.push(b'*');
            metaline.push(b'!');
            SResp {
                metaline,
                metalayout: Vec::with_capacity(128),
                dataframe: Vec::with_capacity(1024),
            }
        }
        pub fn add_group(&mut self, args: impl IntoTpResponse) {
            let (skips, action_bytes) = args.into_tp_response();
            self.dataframe.extend(action_bytes);
            skips.into_iter().for_each(|skip| {
                self.metalayout.push(b'#');
                self.metalayout.extend(skip.to_string().as_bytes());
            });
        }
        pub fn into_response(mut self) -> Vec<u8> {
            self.metaline
                .extend(self.dataframe.len().to_string().as_bytes());
            self.metaline.push(b'!');
            self.metaline
                .extend((self.metalayout.len() + 1).to_string().as_bytes());
            self.metaline.push(b'\n');
            self.metalayout.push(b'\n');
            [self.metaline, self.metalayout, self.dataframe].concat()
        }
    }

    #[cfg(test)]
    #[test]
    fn test_sresp() {
        let mut builder = ResponseBuilder::new_simple();
        builder.add_group("HEY!".to_owned());
        println!("{}", String::from_utf8_lossy(&builder.into_response()));
        let mut builder = ResponseBuilder::new_simple();
        builder.add_group(RespCodes::Okay);
        println!("{}", String::from_utf8_lossy(&builder.into_response()));
    }
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
