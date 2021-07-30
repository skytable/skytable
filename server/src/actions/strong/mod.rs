/*
 * Created on Fri Jul 30 2021
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

//! # Strong Actions
//! Strong actions are like "do all" or "fail all" actions, built specifically for
//! multiple keys. So let's say you used `SSET` instead of `MSET` for setting keys:
//! what'd be the difference?
//! In this case, if all the keys are non-existing, which is a requirement for `MSET`,
//! only then would the keys be set. That is, only if all the keys can be set, will the action
//! run and return code `0` - otherwise the action won't do anything and return an overwrite error.
//! There is no point of using _strong actions_ for a single key/value pair, since it will only
//! slow things down due to the checks performed.
//! Do note that this isn't the same as the gurantees provided by ACID transactions

pub use self::{sdel::sdel, sset::sset, supdate::supdate};
mod sdel;
mod sset;
mod supdate;

#[cfg(test)]
mod tests;
