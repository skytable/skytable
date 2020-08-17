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

//! # The `Query` module
//! This module can be used to build queries that can be sent to the database
//! server. It prepares packets following the Terrapipe protocol.

use super::{DF_BUF, MLINE_BUF, ML_BUF};

/// The size to skip through, dataframe bytes
type PQTuple = (usize, Vec<u8>);

/// The bytes for the metalayout, the bytes for the dataframe
type QueryTuple = (Vec<u8>, Vec<u8>);

/// This trait will return: ([val.as_bytes(), '\n'], len(val.len()))
pub trait PreQuery {
    fn into_pre_query(self) -> PQTuple;
}

/// Trait implementors should return a data group in the query packet.
/// It should have a structure, which has the following general format:
/// ```text
/// &<n>\n
/// item[0]\n
/// item[1]\n
/// ...
/// item[n-1]\n
/// item[n]\n
/// ```
pub trait IntoQueryGroup {
    fn into_query_group(self) -> QueryTuple;
}

/// Trait implementors must return a **complete** query packet  
/// A complete response packet looks like:
/// ```text
/// <* or $>!<CONTENT_LENGTH>!<METALAYOUT_LENGTH>\n
/// #a#b#c#d ...\n
/// < --- data --- >
/// ```
pub trait IntoQuery {
    fn into_query(self) -> Vec<u8>;
}

impl<T> PreQuery for T
where
    T: ToString,
{
    fn into_pre_query(self) -> PQTuple {
        let df_bytes = [self.to_string().as_bytes(), &[b'\n']].concat();
        (df_bytes.len() - 1, df_bytes)
    }
}

/// A query group which is a data group in a dataframe
pub struct QueryGroup {
    /// The skips sequence as `usize`s (i.e `#1#2#3...`)
    sizes: Vec<usize>,
    /// The bytes which can be appended to an existing dataframe
    df_bytes: Vec<u8>,
}

impl QueryGroup {
    /// Create a new `QueryGroup`
    pub fn new() -> Self {
        QueryGroup {
            sizes: Vec::with_capacity(ML_BUF),
            df_bytes: Vec::with_capacity(DF_BUF),
        }
    }
    /// Add an item to the `QueryGroup`
    pub fn add_item<T: PreQuery>(&mut self, item: T) {
        let (size, append_bytes) = item.into_pre_query();
        self.df_bytes.extend(append_bytes);
        self.sizes.push(size);
    }
}

// For queries which just have one value
impl<T> IntoQueryGroup for T
where
    T: ToString,
{
    fn into_query_group(self) -> QueryTuple {
        let st = self.to_string();
        let metalayout = [&[b'#', b'2', b'#'], (st.len()).to_string().as_bytes()].concat();
        let dataframe = [&[b'&', b'1', b'\n'], &[b'+'][..], st.as_bytes(), &[b'\n']].concat();
        (metalayout, dataframe)
    }
}

/// A `QueryBuilder` can be used to build queries
pub enum QueryBuilder {
    Simple(SQuery),
    // TODO(@ohsayan): Add pipelined response builder
}

impl QueryBuilder {
    pub fn new_simple() -> SQuery {
        SQuery::new()
    }
}

pub struct SQuery {
    metaline: Vec<u8>,
    metalayout: Vec<u8>,
    dataframe: Vec<u8>,
}

impl SQuery {
    pub fn new() -> Self {
        let mut metaline = Vec::with_capacity(MLINE_BUF);
        metaline.push(b'*');
        metaline.push(b'!');
        SQuery {
            metaline,
            metalayout: Vec::with_capacity(ML_BUF),
            dataframe: Vec::with_capacity(DF_BUF),
        }
    }
    pub fn add_group<T: IntoQueryGroup>(&mut self, group: T) {
        let (metalayout_ext, dataframe_ext) = group.into_query_group();
        self.dataframe.extend(dataframe_ext);
        self.metalayout.extend(metalayout_ext);
    }
    pub fn from_cmd(&mut self, cmds: String) {
        let mut group = QueryGroup::new();
        let cmds: Vec<&str> = cmds.split_whitespace().collect();
        cmds.into_iter().for_each(|cmd| group.add_item(cmd));
        self.add_group(group);
    }
}

impl IntoQuery for SQuery {
    fn into_query(self) -> Vec<u8> {
        /* UNSAFE(@ohsayan): We know what we're doing here: We convert an immutable reference
        to a mutable `SQuery` to avoid `concat`ing over and over again
         */
        unsafe {
            // Convert the immutable references to mutable references
            // because we don't want to use concat()
            let self_ptr = &self as *const _;
            let self_mut = self_ptr as *mut SQuery;
            // We need to add a newline to the metalayout
            (*self_mut).metalayout.push(b'\n');
            // Now add the content length + ! + metalayout length + '\n'
            (*self_mut)
                .metaline
                .extend(self.dataframe.len().to_string().into_bytes());
            (*self_mut).metaline.push(b'!');
            (*self_mut)
                .metaline
                .extend((*self_mut).metalayout.len().to_string().into_bytes());
            (*self_mut).metaline.push(b'\n');
        } // The raw pointers are dropped here
        [self.metaline, self.metalayout, self.dataframe].concat()
    }
}

// For an entire query which only comprises of a single value
impl<T> IntoQuery for T
where
    T: ToString,
{
    fn into_query(self) -> Vec<u8> {
        let (metalayout, dataframe) = self.to_string().into_query_group();
        let metaline = [
            &[b'*', b'!'],
            dataframe.len().to_string().as_bytes(),
            &[b'!'],
            metalayout.len().to_string().as_bytes(),
            &[b'\n'],
        ]
        .concat();
        [metaline, metalayout, dataframe].concat()
    }
}

impl IntoQueryGroup for QueryGroup {
    fn into_query_group(self) -> QueryTuple {
        // Get the number of items in the data group, convert it into it's UTF-8
        // equivalent.
        let sizeline = [&[b'&'], self.sizes.len().to_string().as_bytes(), &[b'\n']].concat();
        // Now we have the &<n>\n line
        // All we need to know is: add this line to the data bytes
        // also we need to add the len of the sizeline - 1 to the sizes
        let sizes: Vec<u8> = self
            .sizes
            .into_iter()
            .map(|size| [&[b'#'], size.to_string().as_bytes()].concat())
            .flatten()
            .collect();
        let metalayout = [
            vec![b'#'],
            (sizeline.len() - 1).to_string().as_bytes().to_vec(),
            sizes,
        ]
        .concat();
        let dataframe = [sizeline, self.df_bytes].concat();
        (metalayout, dataframe)
    }
}

#[cfg(test)]
#[test]
fn test_queries() {
    let mut query = QueryBuilder::new_simple();
    let mut group = QueryGroup::new();
    group.add_item("SET");
    group.add_item("foo");
    group.add_item("bar");
    query.add_group(group);
    assert_eq!(
        "*!15!9\n#2#3#3#3\n&3\nSET\nfoo\nbar\n"
            .as_bytes()
            .to_owned(),
        query.into_query()
    )
}
