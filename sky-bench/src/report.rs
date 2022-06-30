/*
 * Created on Tue Aug 10 2021
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

use {
    crate::util,
    core::cmp::Ordering,
    std::collections::{hash_map::Entry, HashMap},
};

/// A map of reports
pub struct AggregatedReport {
    map: HashMap<&'static str, Report>,
    queries: usize,
    cap: usize,
}

impl AggregatedReport {
    /// Create a new aggregated report instance. Here:
    /// - `report_count`: Is the count of benches you will be running. For example, if you
    /// are testing GET and SET, this will be 2
    /// - `cap`: Is the number of repeats you will be running
    /// - `queries`: Is the number of queries you will run
    pub fn new(report_count: usize, cap: usize, queries: usize) -> Self {
        Self {
            map: HashMap::with_capacity(report_count),
            cap,
            queries,
        }
    }
    /// Insert a new statistic. The `name` should correspond to the bench name (for example GET)
    /// while the `time` should be the time taken for that bench to complete
    pub fn insert(&mut self, name: &'static str, time: u128) {
        match self.map.entry(name) {
            Entry::Occupied(mut oe) => oe.get_mut().times.push(time),
            Entry::Vacant(ve) => {
                let mut rep = Report::with_capacity(self.cap);
                rep.times.push(time);
                let _ = ve.insert(rep);
            }
        }
    }
    /// Returns a vector of sorted statistics (lexicographical) and the length of the longest
    /// bench name. `(Vec<Stat>, longest_bench_name)`
    pub fn into_sorted_stat(self) -> (Vec<Stat>, usize) {
        let Self { map, queries, .. } = self;
        let mut maxpad = 0usize;
        let mut repvec: Vec<Stat> = map
            .into_iter()
            .map(|(name, report)| {
                if name.len() > maxpad {
                    maxpad = name.len();
                }
                report.into_stat(queries, name)
            })
            .collect();
        repvec.sort();
        (repvec, maxpad)
    }
    /// Returns a minified JSON string
    pub fn into_json(self) -> String {
        serde_json::to_string(&self.into_sorted_stat().0).unwrap()
    }
}

#[derive(Debug)]
/// A report with a collection of times
pub struct Report {
    times: Vec<u128>,
}

impl Report {
    /// Returns a new report with space for atleast `cap` number of times
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            times: Vec::with_capacity(cap),
        }
    }
    /// Returns a [`Stat`] with the average time
    pub fn into_stat(self, reqs: usize, name: &'static str) -> Stat {
        let count = self.times.len();
        let avg: u128 = self.times.into_iter().sum();
        let avg = avg / count as u128;
        Stat {
            name,
            stat: util::calc(reqs, avg),
        }
    }
}

#[derive(serde::Serialize, Debug)]
/// A statistic: name of the bench and the result
pub struct Stat {
    name: &'static str,
    stat: f64,
}

impl Stat {
    /// Get a reference to the report name
    pub fn get_report(&self) -> &str {
        self.name
    }
    /// Get the statistic
    pub fn get_stat(&self) -> f64 {
        self.stat
    }
}

impl PartialEq for Stat {
    fn eq(&self, oth: &Self) -> bool {
        self.name == oth.name
    }
}

impl Eq for Stat {}
impl PartialOrd for Stat {
    fn partial_cmp(&self, oth: &Self) -> Option<Ordering> {
        self.name.partial_cmp(oth.name)
    }
}

impl Ord for Stat {
    fn cmp(&self, oth: &Self) -> std::cmp::Ordering {
        self.name.cmp(oth.name)
    }
}
