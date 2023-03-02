/*
 * Created on Thu Mar 02 2023
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

mod layer_validation {
    use crate::engine::{
        core::model::{Layer, LayerView},
        error::DatabaseError,
        ql::{ast::parse_ast_node_multiple_full, tests::lex_insecure as lex},
    };

    #[test]
    fn string() {
        let tok = lex(b"string").unwrap();
        let spec = parse_ast_node_multiple_full(&tok).unwrap();
        let view = LayerView::parse_layers(spec).unwrap();
        assert_eq!(view.layers(), [Layer::str()]);
    }

    #[test]
    fn nested_list() {
        let tok = lex(b"list { type: list { type: string } }").unwrap();
        let spec = parse_ast_node_multiple_full(&tok).unwrap();
        let view = LayerView::parse_layers(spec).unwrap();
        assert_eq!(view.layers(), [Layer::list(), Layer::list(), Layer::str()]);
    }

    #[test]
    fn invalid_list() {
        let tok = lex(b"list").unwrap();
        let spec = parse_ast_node_multiple_full(&tok).unwrap();
        assert_eq!(
            LayerView::parse_layers(spec).unwrap_err(),
            DatabaseError::DdlModelInvalidTypeDefinition
        );
    }

    #[test]
    fn invalid_flat() {
        let tok = lex(b"string { type: string }").unwrap();
        let spec = parse_ast_node_multiple_full(&tok).unwrap();
        assert_eq!(
            LayerView::parse_layers(spec).unwrap_err(),
            DatabaseError::DdlModelInvalidTypeDefinition
        );
    }
}
