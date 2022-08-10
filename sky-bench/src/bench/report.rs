/*
 * Created on Wed Aug 10 2022
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

use serde::Serialize;

#[derive(Serialize)]
pub struct SingleReport {
    name: &'static str,
    stat: f64,
}

impl SingleReport {
    pub fn new(name: &'static str, stat: f64) -> Self {
        Self { name, stat }
    }

    pub fn stat(&self) -> f64 {
        self.stat
    }

    pub fn name(&self) -> &str {
        self.name
    }
}

pub struct AggregateReport {
    names: Vec<SingleReport>,
    query_count: usize,
}

impl AggregateReport {
    pub fn new(query_count: usize) -> Self {
        Self {
            names: Vec::new(),
            query_count,
        }
    }
    pub fn push(&mut self, report: SingleReport) {
        self.names.push(report)
    }
    pub(crate) fn into_json(self) -> String {
        let (_, report) = self.finish();
        serde_json::to_string(&report).unwrap()
    }

    pub(crate) fn finish(self) -> (usize, Vec<SingleReport>) {
        let mut maxpad = self.names[0].name.len();
        let mut reps = self.names;
        reps.iter_mut().for_each(|rep| {
            let total_time = rep.stat;
            let qps = (self.query_count as f64 / total_time) * 1_000_000_000_f64;
            rep.stat = qps;
            if rep.name.len() > maxpad {
                maxpad = rep.name.len();
            }
        });
        (maxpad, reps)
    }
}
