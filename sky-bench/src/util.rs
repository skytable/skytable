/*
 * Created on Tue Aug 09 2022
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

use {
    crate::{
        config::ServerConfig,
        error::{BResult, Error},
    },
    skytable::{Connection, Element, Query, RespCode},
    std::thread,
};

/// Check if the provided keysize has enough combinations to support the given `queries` count
///
/// This function is heavily optimized and should take Θ(1) time. The `ALWAYS_TRUE_FACTOR` is
/// dependent on pointer width (more specifically the virtual address space size).
/// - For 64-bit address spaces: `(256!)/r!(256-r!)`; for a value of r >= 12, we'll hit the maximum
/// of the address space and hence this will always return true (because of the size of `usize`)
///     > The value for r = 12 is `1.27309515e+20` which largely exceeds `1.8446744e+19`
/// - For 32-bit address spaces: `(256!)/r!(256-r!)`; for a value of r >= 5, we'll hit the maximum
/// of the address space and hence this will always return true (because of the size of `usize`)
///     > The value for r = 5 is `8.81e+9` which largely exceeds `4.3e+9`
pub const fn has_enough_ncr(keysize: usize, queries: usize) -> bool {
    const LUT: [u64; 11] = [
        // 1B
        256,
        // 2B
        32640,
        // 3B
        2763520,
        // 4B
        174792640,
        // 5B
        8809549056,
        // 6B
        368532802176,
        // 7B
        13161885792000,
        // 8B
        409663695276000,
        // 9B
        11288510714272000,
        // 10B
        278826214642518400,
        // 11B
        6235568072914502400,
    ];
    #[cfg(target_pointer_width = "64")]
    const ALWAYS_TRUE_FACTOR: usize = 12;
    #[cfg(target_pointer_width = "32")]
    const ALWAYS_TRUE_FACTOR: usize = 5;
    keysize >= ALWAYS_TRUE_FACTOR || (LUT[keysize - 1] >= queries as _)
}

/// Run a sanity test, making sure that the server is ready for benchmarking. This function will do the
/// following tests:
/// - Connect to the instance
/// - Run a `heya` as a preliminary test
/// - Create a new table `tmpbench`. This is where we're supposed to run all the benchmarks.
/// - Switch to the new table
/// - Set a key, and get it checking the equality of the returned value
pub fn run_sanity_test(server_config: &ServerConfig) -> BResult<()> {
    let mut con = Connection::new(server_config.host(), server_config.port())?;
    let tests: [(Query, Element, &str); 5] = [
        (
            Query::from("HEYA"),
            Element::String("HEY!".to_owned()),
            "heya",
        ),
        (
            Query::from("CREATE MODEL default.tmpbench(binary, binary)"),
            Element::RespCode(RespCode::Okay),
            "create model",
        ),
        (
            Query::from("use default.tmpbench"),
            Element::RespCode(RespCode::Okay),
            "use",
        ),
        (
            Query::from("set").arg("x").arg("100"),
            Element::RespCode(RespCode::Okay),
            "set",
        ),
        (
            Query::from("get").arg("x"),
            Element::Binstr("100".as_bytes().to_owned()),
            "get",
        ),
    ];
    for (query, expected, test_kind) in tests {
        let r: Element = con.run_query(query)?;
        if r != expected {
            return Err(Error::Runtime(format!(
                "sanity test for `{test_kind}` failed"
            )));
        }
    }
    Ok(())
}

/// Ensures that the current thread is the main thread. If not, this function will panic
pub fn ensure_main_thread() {
    assert_eq!(
        thread::current().name().unwrap(),
        "main",
        "unsafe function called from non-main thread"
    )
}

/// Run a cleanup. This function attempts to remove the `default.tmpbench` entity
pub fn cleanup(server_config: &ServerConfig) -> BResult<()> {
    let mut c = Connection::new(server_config.host(), server_config.port())?;
    let r: Element = c.run_query(Query::from("drop model default.tmpbench force"))?;
    if r == Element::RespCode(RespCode::Okay) {
        Err(Error::Runtime("failed to run cleanup".into()))
    } else {
        Ok(())
    }
}
