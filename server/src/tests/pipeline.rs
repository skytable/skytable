/*
 * Created on Sat Oct 30 2021
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2020, Sayan Nandan <ohsayan@outlook.com>
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

#[sky_macros::dbtest]
mod tests {
    use skytable::{query, Element, Pipeline, RespCode};
    async fn test_pipeline_heya_echo() {
        let pipe = Pipeline::new()
            .add(query!("heya", "first"))
            .add(query!("heya", "second"));
        let ret = con.run_pipeline(pipe).await.unwrap();
        assert_eq!(
            ret,
            vec![
                Element::String("first".to_owned()),
                Element::String("second".to_owned())
            ]
        )
    }
    async fn test_pipeline_basic() {
        let pipe = Pipeline::new().add(query!("heya")).add(query!("get", "x"));
        let ret = con.run_pipeline(pipe).await.unwrap();
        assert_eq!(
            ret,
            vec![
                Element::String("HEY!".to_owned()),
                Element::RespCode(RespCode::NotFound)
            ]
        );
    }
    // although an error is simply just a response, but we'll still add a test for sanity
    async fn test_pipeline_with_error() {
        let pipe = Pipeline::new()
            .add(query!("heya"))
            .add(query!("get", "x", "y"));
        let ret = con.run_pipeline(pipe).await.unwrap();
        assert_eq!(
            ret,
            vec![
                Element::String("HEY!".to_owned()),
                Element::RespCode(RespCode::ActionError)
            ]
        );
    }
}
