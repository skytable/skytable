/*
 * Created on Thu Jul 02 2020
 *
 * This file is a part of the source code for the Terrabase database
 * Copyright (c) 2020 Sayan Nandan
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

const METAFRAME_PROTOCOL_TAG: &'static str = "TP";
const METAFRAME_QUERY_TAG: &'static str = "Q";
const METAFRAME_QUERY_SET_TAG: &'static str = "SET";
const METAFRAME_QUERY_GET_TAG: &'static str = "GET";
const METAFRAME_QUERY_UPDATE_TAG: &'static str = "UPDATE";
const METAFRAME_QUERY_DEL_TAG: &'static str = "DEL";

pub struct Version(u8, u8, u8);

impl Version {
    pub fn new_from_str<'a>(val: &'a str) -> Option<Self> {
        let vals: Vec<&str> = val.split(".").collect();
        if vals.len() != 3 {
            return None;
        }
        let semver = (
            vals[0].parse::<u8>(),
            vals[1].parse::<u8>(),
            vals[2].parse::<u8>(),
        );
        if let (Ok(major), Ok(minor), Ok(patch)) = semver {
            return Some(Version(major, minor, patch));
        } else {
            return None;
        }
    }
    pub fn is_compatible_with(&self, other: &Version) -> bool {
        if self.0 == other.0 {
            true
        } else {
            false
        }
    }
}

type Key = String;

pub enum TPQueryType {
    GET(Key),
    SET(Key, Key),
    UPDATE(Key, Key),
    DEL(Key, Key),
}

pub fn parse_query_packet(own_version: Version, packet: &String) {
    let rlines: Vec<&str> = packet.lines().collect();
    // This should give us two or more lines
    if rlines.len() < 2 {
        eprintln!("error: 5 CorruptDataframe");
        return;
    }
    // This is the meta frame
    let meta_frame: Vec<&str> = rlines[0].split("/").collect();
    if meta_frame.len() != 3 {
        eprintln!("error: 4 InvalidMetaframe");
        return;
    }
    // This is the data frame
    let data_frame: Vec<&str> = rlines[1].split_whitespace().collect();
    // Check if version is valid
    let version_header: Vec<&str> = meta_frame[0].split(" ").collect();
    if let Some(version_tag) = version_header.get(0) {
        if version_tag == &METAFRAME_PROTOCOL_TAG {
            if let Some(version) = version_header.get(1) {
                if let Some(v) = Version::new_from_str(&version) {
                    if !v.is_compatible_with(&own_version) {
                        eprintln!("error: 6 ProtocolVersionMismatch");
                    } else {
                        ()
                    }
                } else {
                    eprintln!("error: 4 InvalidMetaframe");
                }
            } else {
                eprintln!("error: 4 InvalidMetaframe");
            }
        } else {
            eprintln!("error: 4 InvalidMetaframe");
        }
    } else {
        eprintln!("error: 4 InvalidMetaframe");
    }

    // Now get request type
    let request_partition: Vec<&str> = meta_frame[1].split(" ").collect();
    if let Some(request_tag) = request_partition.get(0) {
        if request_tag == &METAFRAME_QUERY_TAG {
            if let Some(qtype) = request_partition.get(1) {
                match qtype {
                    &METAFRAME_QUERY_SET_TAG => {
                        // This is a set request
                        if let Some(key) = data_frame.get(0) {
                            if let Some(value) = data_frame.get(1) {
                                if data_frame.get(2).is_none() {
                                    println!("SET {} {}", key, value);
                                } else {
                                    eprintln!("error: 5 Corrupt Dataframe");
                                    return;
                                }
                            } else {
                                eprintln!("error: 5 Corrupt Dataframe");
                                return;
                            }
                        } else {
                            eprintln!("error: 5 Corrupt Dataframe");
                            return;
                        }
                    }
                    &METAFRAME_QUERY_UPDATE_TAG => {
                        // This is an update request
                        if let Some(key) = data_frame.get(0) {
                            if let Some(value) = data_frame.get(1) {
                                if data_frame.get(2).is_none() {
                                    println!("UPDATE {} {}", key, value);
                                } else {
                                    eprintln!("error: 5 Corrupt Dataframe");
                                    return;
                                }
                            } else {
                                eprintln!("error: 5 Corrupt Dataframe");
                                return;
                            }
                        } else {
                            eprintln!("error: 5 Corrupt Dataframe");
                            return;
                        }
                    }
                    &METAFRAME_QUERY_GET_TAG => {
                        // This is a get request
                        if let Some(key) = data_frame.get(0) {
                            if data_frame.get(1).is_none() {
                                println!("GET {}", key);
                            } else {
                                eprintln!("error: 5 Corrupt Dataframe");
                                return;
                            }
                        } else {
                            eprintln!("error: 5 Corrupt Dataframe");
                            return;
                        }
                    }
                    _ => {
                        eprintln!("error: 4 Invalid Metaframe");
                        return;
                    }
                }
            } else {
                eprintln!("error: 5 Corrupt dataframe");
                return;
            }
        } else {
            eprintln!("error: 5 Corrupt dataframe");
            return;
        }
    } else {
        eprintln!("error: 5 Corrupt dataframe");
        return;
    }
}

#[cfg(test)]
#[test]
fn test_parse_header_query() {
    let query_packet_get = "TP 0.1.1/Q GET/5\nsayan".to_owned();
    parse_query_packet(Version(0, 1, 1), &query_packet_get);
    let query_packet_set = "TP 0.1.1/Q SET/8\nsayan 18".to_owned();
    parse_query_packet(Version(0, 1, 1), &query_packet_set);
    let erroring_packet_set = "TP 0.1.1/Q SET/13\nhi sayan".to_owned();
    parse_query_packet(Version(0, 1, 1), &erroring_packet_set);
}
