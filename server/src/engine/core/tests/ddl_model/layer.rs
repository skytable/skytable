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

use crate::engine::{
    core::model::Field,
    error::QueryResult,
    ql::{ast::parse_ast_node_multiple_full, tests::lex_insecure},
};

fn layerview_nullable(layer_def: &str, nullable: bool) -> QueryResult<Field> {
    let tok = lex_insecure(layer_def.as_bytes()).unwrap();
    let spec = parse_ast_node_multiple_full(&tok).unwrap();
    Field::parse_layers(spec, nullable)
}
fn layerview(layer_def: &str) -> QueryResult<Field> {
    layerview_nullable(layer_def, false)
}

mod layer_spec_validation {
    use {
        super::layerview,
        crate::engine::{core::model::Layer, error::Error},
    };

    #[test]
    fn string() {
        assert_eq!(layerview("string").unwrap().layers(), [Layer::str()]);
    }

    #[test]
    fn nested_list() {
        assert_eq!(
            layerview("list { type: list { type: string } }")
                .unwrap()
                .layers(),
            [Layer::list(), Layer::list(), Layer::str()]
        );
    }

    #[test]
    fn invalid_list() {
        assert_eq!(
            layerview("list").unwrap_err(),
            Error::QPDdlInvalidTypeDefinition
        );
    }

    #[test]
    fn invalid_flat() {
        assert_eq!(
            layerview("string { type: string }").unwrap_err(),
            Error::QPDdlInvalidTypeDefinition
        );
    }
}

mod layer_data_validation {
    use {
        super::{layerview, layerview_nullable},
        crate::engine::{core::model, data::cell::Datacell},
    };
    #[test]
    fn bool() {
        let dc = Datacell::new_bool(true);
        let layer = layerview("bool").unwrap();
        assert!(layer.validate_data_fpath(&dc));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "bool"]);
    }
    #[test]
    fn uint() {
        let targets = [
            ("uint8", u8::MAX as u64),
            ("uint16", u16::MAX as _),
            ("uint32", u32::MAX as _),
            ("uint64", u64::MAX),
        ];
        targets
            .into_iter()
            .enumerate()
            .for_each(|(i, (layer, max))| {
                let this_layer = layerview(layer).unwrap();
                let dc = Datacell::new_uint(max);
                assert!(this_layer.validate_data_fpath(&dc), "{:?}", this_layer);
                assert_vecstreq_exact!(model::layer_traces(), ["fpath", "uint"]);
                for (lower, _) in targets[..i].iter() {
                    let layer = layerview(lower).unwrap();
                    assert!(!layer.validate_data_fpath(&dc), "{:?}", layer);
                    assert_vecstreq_exact!(model::layer_traces(), ["fpath", "uint"]);
                }
                for (higher, _) in targets[i + 1..].iter() {
                    let layer = layerview(higher).unwrap();
                    assert!(layer.validate_data_fpath(&dc), "{:?}", layer);
                    assert_vecstreq_exact!(model::layer_traces(), ["fpath", "uint"]);
                }
            });
    }
    #[test]
    fn sint() {
        let targets = [
            ("sint8", (i8::MIN as i64, i8::MAX as i64)),
            ("sint16", (i16::MIN as _, i16::MAX as _)),
            ("sint32", (i32::MIN as _, i32::MAX as _)),
            ("sint64", (i64::MIN, i64::MAX)),
        ];
        targets
            .into_iter()
            .enumerate()
            .for_each(|(i, (layer, (min, max)))| {
                let this_layer = layerview(layer).unwrap();
                let dc_min = Datacell::new_sint(min);
                let dc_max = Datacell::new_sint(max);
                assert!(this_layer.validate_data_fpath(&dc_min), "{:?}", this_layer);
                assert!(this_layer.validate_data_fpath(&dc_max), "{:?}", this_layer);
                assert_vecstreq_exact!(model::layer_traces(), ["fpath", "sint", "fpath", "sint"]);
                for (lower, _) in targets[..i].iter() {
                    let layer = layerview(lower).unwrap();
                    assert!(!layer.validate_data_fpath(&dc_min), "{:?}", layer);
                    assert!(!layer.validate_data_fpath(&dc_max), "{:?}", layer);
                    assert_vecstreq_exact!(
                        model::layer_traces(),
                        ["fpath", "sint", "fpath", "sint"]
                    );
                }
                for (higher, _) in targets[i + 1..].iter() {
                    let layer = layerview(higher).unwrap();
                    assert!(layer.validate_data_fpath(&dc_min), "{:?}", layer);
                    assert!(layer.validate_data_fpath(&dc_max), "{:?}", layer);
                    assert_vecstreq_exact!(
                        model::layer_traces(),
                        ["fpath", "sint", "fpath", "sint"]
                    );
                }
            });
    }
    #[test]
    fn float() {
        // l
        let f32_l = layerview("float32").unwrap();
        let f64_l = layerview("float64").unwrap();
        // dc
        let f32_dc_min = Datacell::new_float(f32::MIN as _);
        let f32_dc_max = Datacell::new_float(f32::MAX as _);
        let f64_dc_min = Datacell::new_float(f64::MIN as _);
        let f64_dc_max = Datacell::new_float(f64::MAX as _);
        // check (32)
        assert!(f32_l.validate_data_fpath(&f32_dc_min));
        assert!(f32_l.validate_data_fpath(&f32_dc_max));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "float", "fpath", "float"]);
        assert!(f64_l.validate_data_fpath(&f32_dc_min));
        assert!(f64_l.validate_data_fpath(&f32_dc_max));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "float", "fpath", "float"]);
        // check (64)
        assert!(!f32_l.validate_data_fpath(&f64_dc_min));
        assert!(!f32_l.validate_data_fpath(&f64_dc_max));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "float", "fpath", "float"]);
        assert!(f64_l.validate_data_fpath(&f64_dc_min));
        assert!(f64_l.validate_data_fpath(&f64_dc_max));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "float", "fpath", "float"]);
    }
    #[test]
    fn bin() {
        let layer = layerview("binary").unwrap();
        assert!(layer.validate_data_fpath(&Datacell::from("hello".as_bytes())));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "binary"]);
    }
    #[test]
    fn str() {
        let layer = layerview("string").unwrap();
        assert!(layer.validate_data_fpath(&Datacell::from("hello")));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "string"]);
    }
    #[test]
    fn list_simple() {
        let layer = layerview("list { type: string }").unwrap();
        let dc = Datacell::new_list(vec![
            Datacell::from("I"),
            Datacell::from("love"),
            Datacell::from("cats"),
        ]);
        assert!(layer.validate_data_fpath(&dc));
        assert_vecstreq_exact!(
            model::layer_traces(),
            ["list", "string", "string", "string"]
        );
    }
    #[test]
    fn list_nested_l1() {
        let layer = layerview("list { type: list { type: string } }").unwrap();
        let dc = Datacell::new_list(vec![
            Datacell::new_list(vec![Datacell::from("hello_11"), Datacell::from("hello_12")]),
            Datacell::new_list(vec![Datacell::from("hello_21"), Datacell::from("hello_22")]),
            Datacell::new_list(vec![Datacell::from("hello_31"), Datacell::from("hello_32")]),
        ]);
        assert!(layer.validate_data_fpath(&dc));
        assert_vecstreq_exact!(
            model::layer_traces(),
            [
                "list", // low
                "list", "string", "string", // cs: 1
                "list", "string", "string", // cs: 2
                "list", "string", "string", // cs: 3
            ]
        );
    }
    #[test]
    fn nullval_fpath() {
        let layer = layerview_nullable("string", true).unwrap();
        assert!(layer.validate_data_fpath(&Datacell::null()));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "bool"]);
    }
    #[test]
    fn nullval_nested_but_fpath() {
        let layer = layerview_nullable("list { type: string }", true).unwrap();
        assert!(layer.validate_data_fpath(&Datacell::null()));
        assert_vecstreq_exact!(model::layer_traces(), ["fpath", "bool"]);
    }
}
