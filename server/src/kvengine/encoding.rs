/*
 * Created on Thu Jul 01 2021
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

/*
 This cannot be the work of a single person! A big thanks to:
 - Björn Höhrmann: https://bjoern.hoehrmann.de/
 - Professor Lemire: https://scholar.google.com/citations?user=q1ja-G8AAAAJ
 - Travis Downs: https://github.com/travisdowns
*/

/*
 * The UTF-8 validation that we use here uses an encoded finite state machine defined in this file.
 * A branchless (no cond) finite state machine is used which greatly simplifies how things work
 * than some amazing libraries out there while also providing huge performance and computationally
 * economical benefits. The dual stream that I have used here offers the best performance than a
 * triple or quad channel SM evaluation. I attempted a SM evaluation with four streams and sure
 * it was about 75% faster than the standard library's evaluation, but however fell short to the
 * 300% improvement that I got over a dual stream. Also, don't get too excited because this looks
 * like something recursive or _threadable_. DON'T. Call stacks have their own costs and why do
 * it at all when we can use a simple loop?
 * Now for the threading bit: remember, this is not a single game that you have to win.
 * There will potentially be millions if not thousands of callers requesting validation and
 * imagine spawning two threads for every validation. Firstly, you'll have to wait on the OS/Kernel
 * to hand over the response to a fork. Secondly, so many threads are useless because it'll just
 * burden the scheduler and hurt performance taking away any possible performance benefits that
 * you could've had. In a single benchmark, the threaded implementation might make you happy
 * and turn out to be 600% faster. But try doing multiple evaluations in parallel: you'll know
 * what we're talking about. Eliding bound checks gives us a ~2-5% edge over the one that
 * is checked. Why elide them? Just because we reduce our assembly size by ~15%!
 *
 * - Sayan N. <ohsayan@outlook.com> (July, 2021)
*/

/// This table maps bytes to character classes that helps us reduce the size of the
/// transition table and generate bitmasks
const UTF8_MAP_BYTE_TO_CHAR_CLASS: [u8; 256] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    8, 8, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    10, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 3, 3, 11, 6, 6, 6, 5, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8,
];

/// This table is a transition table that maps the combination of a state of the
/// automaton and a char class to a state
const UTF8_TRANSITION_MAP: [u8; 108] = [
    0, 12, 24, 36, 60, 96, 84, 12, 12, 12, 48, 72, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 0, 12, 12, 12, 12, 12, 0, 12, 0, 12, 12, 12, 24, 12, 12, 12, 12, 12, 24, 12, 24, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 24, 12, 12, 12, 12, 12, 24, 12, 12, 12, 12, 12, 12, 12, 24, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 36, 12, 36, 12, 12, 12, 36, 12, 12, 12, 12, 12, 36, 12, 36, 12, 12,
    12, 36, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
];

/// This method uses a dual-stream deterministic finite automaton
/// [(DFA)](https://en.wikipedia.org/wiki/Deterministic_finite_automaton) that is used to validate
/// UTF-8 bytes that use the encoded finite state machines defined in this module.
///
/// ## Tradeoffs
/// Read my comment in the source code (or above if you are not browsing rustdoc)
///
/// ## Why
/// This function gives us as much as a ~300% improvement over std's validation algorithm
pub fn is_utf8(bytes: impl AsRef<[u8]>) -> bool {
    let bytes = bytes.as_ref();
    let mut half = bytes.len() / 2;
    unsafe {
        while *bytes.get_unchecked(half) <= 0xBF && *bytes.get_unchecked(half) >= 0x80 && half > 0 {
            half -= 1;
        }
    }
    let (mut fsm_state_1, mut fsm_state_2) = (0u8, 0u8);
    let mut i = 0usize;
    let mut j = half;
    while i < half {
        unsafe {
            fsm_state_1 = *UTF8_TRANSITION_MAP.get_unchecked(
                (fsm_state_1
                    + (UTF8_MAP_BYTE_TO_CHAR_CLASS
                        .get_unchecked((*bytes.get_unchecked(i)) as usize)))
                    as usize,
            );
            fsm_state_2 = *UTF8_TRANSITION_MAP.get_unchecked(
                (fsm_state_2
                    + (UTF8_MAP_BYTE_TO_CHAR_CLASS.get_unchecked(*bytes.get_unchecked(j) as usize)))
                    as usize,
            );
        }
        i += 1;
        j += 1;
    }
    let mut j = half * 2;
    while j < bytes.len() {
        unsafe {
            fsm_state_2 = *UTF8_TRANSITION_MAP.get_unchecked(
                (fsm_state_2
                    + (UTF8_MAP_BYTE_TO_CHAR_CLASS
                        .get_unchecked((*bytes.get_unchecked(j)) as usize)))
                    as usize,
            );
        }
        j += 1;
    }
    fsm_state_1 == 0 && fsm_state_2 == 0
}

#[test]
fn test_utf8_verity() {
    let unicode = gen_unicode();
    assert!(unicode.into_iter().all(self::is_utf8));
}

#[cfg(test)]
fn gen_unicode() -> Vec<String> {
    use std::env;
    use std::fs;
    use std::process::Command;
    let mut path = env::var("ROOT_DIR").expect("ROOT_DIR unset");
    path.push_str("/scripts/unicode.pl");
    fs::create_dir_all("./utf8/separated").unwrap();
    fs::create_dir_all("./utf8/unseparated").unwrap();
    let cmd = Command::new("perl").arg("-w").arg(path).output().unwrap();
    assert!(cmd.stderr.is_empty());
    let mut strings = vec![];
    for file in fs::read_dir("utf8/separated").unwrap() {
        strings.push(fs::read_to_string(file.unwrap().path()).unwrap());
    }
    for file in fs::read_dir("utf8/unseparated").unwrap() {
        strings.push(fs::read_to_string(file.unwrap().path()).unwrap());
    }
    fs::remove_dir_all("utf8").unwrap();
    strings
}

#[test]
fn test_invalid_simple() {
    assert!(!is_utf8(b"\xF3"));
    assert!(!is_utf8(b"\xC2"));
    assert!(!is_utf8(b"\xF1"));
    assert!(!is_utf8(b"\xF0\x99"));
    assert!(!is_utf8(b"\xF0\x9F\x94"));
}

#[test]
fn test_invalid_b32() {
    let mut invalid = b"s".repeat(31);
    invalid.push(b'\xF0');
    assert!(!is_utf8(invalid));
}

#[test]
fn test_invalid_b64() {
    let mut invalid = b"s".repeat(63);
    invalid.push(b'\xF2');
    assert!(!is_utf8(invalid));
}

#[test]
fn test_invalid_b64_len65() {
    let mut invalid = b"s".repeat(63);
    invalid.push(b'\xF3');
    invalid.push(b'a');
    assert!(!is_utf8(invalid));
}
