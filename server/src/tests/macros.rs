/*
 * Created on Sun Sep 05 2021
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

macro_rules! setkeys {
    ($con:ident, $($key:literal:$value:literal),*) => {
        let mut q = skytable::Query::new();
        q.push("MSET");
        let mut count = 0;
        $(
            q.push($key);
            q.push($value);
            count += 1;
        )*
        assert_eq!(
            $con.run_query_raw(&q).await.unwrap(),
            Element::UnsignedInt(count)
        );
    };
}

macro_rules! push {
    ($query:expr, $($val:expr),*) => {{
        $(
            $query.push($val);
        )*
    }};
}

macro_rules! runeq {
    ($con:expr, $query:expr, $eq:expr) => {
        assert_eq!($con.run_query_raw(&$query).await.unwrap(), $eq)
    };
}

macro_rules! runmatch {
    ($con:expr, $query:expr, $match:path) => {{
        let ret = $con.run_query_raw(&$query).await.unwrap();
        assert!(matches!(ret, $match(_)))
    }};
}

macro_rules! assert_okay {
    ($con:expr, $query:expr) => {
        assert_respcode!($con, $query, ::skytable::RespCode::Okay)
    };
}

macro_rules! assert_aerr {
    ($con:expr, $query:expr) => {
        assert_respcode!($con, $query, ::skytable::RespCode::ActionError)
    };
}

macro_rules! assert_skyhash_arrayeq {
    (str, $con:expr, $query:expr, $($val:expr),*) => {
        runeq!(
            $con,
            $query,
            skytable::Element::Array(skytable::types::Array::Str(
                vec![
                    $(Some($val.into()),)*
                ]
            ))
        )
    };
    (bin, $con:expr, $query:expr, $($val:expr),*) => {
        runeq!(
            $con,
            $query,
            skytable::Element::Array(skytable::types::Array::Bin(
                vec![
                    $(Some($val.into()),)*
                ]
            ))
        )
    };
}

macro_rules! assert_respcode {
    ($con:expr, $query:expr, $code:expr) => {
        runeq!($con, $query, ::skytable::Element::RespCode($code))
    };
}
