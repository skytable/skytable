/*
 * Created on Tue Aug 18 2020
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

pub mod terminal {
    //! Utilities for Terminal I/O
    use std::fmt;
    use std::io::Write;
    use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
    /// Write to stdout with
    pub fn write_with_col<T: fmt::Display>(item: T, color: Option<Color>) -> fmt::Result {
        let mut stdout = StandardStream::stdout(ColorChoice::Always);
        if stdout.set_color(ColorSpec::new().set_fg(color)).is_err() {
            return Err(fmt::Error);
        }
        if write!(&mut stdout, "{}", item).is_err() {
            return Err(fmt::Error);
        }
        if stdout.reset().is_err() {
            return Err(fmt::Error);
        }
        Ok(())
    }
    pub fn write_info<T: fmt::Display>(item: T) -> fmt::Result {
        write_with_col(item, Some(Color::Cyan))
    }
    pub fn write_warning<T: fmt::Display>(item: T) -> fmt::Result {
        write_with_col(item, Some(Color::Yellow))
    }
    pub fn write_error<T: fmt::Display>(item: T) -> fmt::Result {
        write_with_col(item, Some(Color::Red))
    }
    pub fn write_success<T: fmt::Display>(item: T) -> fmt::Result {
        write_with_col(item, Some(Color::Green))
    }
}
