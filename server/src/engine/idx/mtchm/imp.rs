/*
 * Created on Sat Jan 28 2023
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

use super::{
    super::{DummyMetrics, IndexBaseSpec},
    meta::{AsHasher, Config},
    Tree,
};
use std::sync::Arc;

pub type MTArc<K, V, S, C> = Tree<Arc<(K, V)>, S, C>;

impl<K, V, S, C> IndexBaseSpec<K, V> for MTArc<K, V, S, C>
where
    C: Config,
    S: AsHasher,
{
    const PREALLOC: bool = false;

    type Metrics = DummyMetrics;

    fn idx_init() -> Self {
        MTArc::new()
    }

    fn idx_init_with(s: Self) -> Self {
        s
    }

    fn idx_metrics(&self) -> &Self::Metrics {
        &DummyMetrics
    }
}
