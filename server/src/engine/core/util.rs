/*
 * Created on Thu Apr 06 2023
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

use crate::engine::{
    error::{QueryError, QueryResult},
    ql::ast::Entity,
};

pub trait EntityLocator<'a> {
    fn parse_entity(self) -> QueryResult<(&'a str, &'a str)>
    where
        Self: 'a;
}

impl<'a> EntityLocator<'a> for (&'a str, &'a str) {
    fn parse_entity(self) -> QueryResult<(&'a str, &'a str)> {
        Ok(self)
    }
}

impl<'a> EntityLocator<'a> for Entity<'a> {
    fn parse_entity(self) -> QueryResult<(&'a str, &'a str)>
    where
        Self: 'a,
    {
        self.into_full_str().ok_or(QueryError::QPExpectedEntity)
    }
}
