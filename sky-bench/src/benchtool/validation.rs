/*
 * Created on Fri Nov 26 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2021, Sayan Nandan <ohsayan@outlook.com>
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

/// Just a sweet `*1\n`
pub(super) const SIMPLE_QUERY_SIZE: usize = 3;

/// For a dataframe, this returns the dataframe size for array responses.
///
/// For example,
/// ```text
/// &<n>\n
/// (<tsymbol><size>\n<element>)*
/// ```
#[allow(dead_code)] // TODO(@ohsayan): Remove this lint
pub fn calculate_array_dataframe_size(element_count: usize, per_element_size: usize) -> usize {
    let mut s = 0;
    s += 1; // `&`
    s += element_count.to_string().len(); // `<n>`
    s += 1; // `\n`
    let mut subsize = 0;
    subsize += 1; // `+`
    subsize += per_element_size.to_string().len(); // `<n>`
    subsize += 1; // `\n`
    subsize += per_element_size; // the element size itself
    subsize += 1; // `\n`
    s += subsize * element_count;
    s
}

/// For a dataframe with a typed array, calculate its size
///
/// **Warning:** Null entries are not yet supported (for a full null array, just pass `1` for the `per_element_size`)
#[allow(dead_code)]
pub fn calculate_typed_array_dataframe_size(
    element_count: usize,
    per_element_size: usize,
) -> usize {
    let mut s = 0usize;
    s += 2; // `@<tsymbol>`
    s += element_count.to_string().len(); // `<n>`
    s += 1; // `\n`

    // now for the payload
    let mut subsize = 0usize;
    subsize += per_element_size.to_string().len(); // `<n>`
    subsize += 1; // `\n`
    subsize += per_element_size; // the payload itself
    subsize += 1; // `\n`

    s += subsize * element_count;
    s
}

/// For a monoelement dataframe, this returns the size:
/// ```text
/// <tsymbol><size>\n
/// <element>\n
/// ```
///
/// For an `okay` respcode, it will look like this:
/// ```text
/// !1\n
/// 0\n
/// ```
pub fn calculate_monoelement_dataframe_size(per_element_size: usize) -> usize {
    let mut s = 0;
    s += 1; // the tsymbol (always one byte)
    s += per_element_size.to_string().len(); // the bytes in size string
    s += 1; // the LF
    s += per_element_size; // the element itself
    s += 1; // the final LF
    s
}

/// Returns the metaframe size
/// ```text
/// *<n>\n
/// ```
#[allow(dead_code)] // TODO(@ohsayan): Remove this lint
pub fn calculate_metaframe_size(queries: usize) -> usize {
    if queries == 1 {
        SIMPLE_QUERY_SIZE
    } else {
        let mut s = 0;
        s += 1; // `*`
        s += queries.to_string().len(); // the bytes in size string
        s += 1; // `\n`
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monoelement_calculation() {
        assert_eq!(calculate_monoelement_dataframe_size(1), 5);
    }
    #[test]
    fn test_simple_query_metaframe_size() {
        assert_eq!(calculate_metaframe_size(1), SIMPLE_QUERY_SIZE);
    }
    #[test]
    fn test_typed_array_dataframe_size() {
        let packet = b"@+3\n3\nhow\n3\nyou\n3\ndng\n";
        assert_eq!(calculate_typed_array_dataframe_size(3, 3), packet.len());
    }
}
