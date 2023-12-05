/*
 * Created on Tue May 23 2023
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

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct ByteStack<const N: usize> {
    array: [u8; N],
}

#[allow(dead_code)]
impl<const N: usize> ByteStack<N> {
    #[inline(always)]
    pub const fn data_copy(&self) -> [u8; N] {
        self.array
    }
    #[inline(always)]
    pub const fn new(array: [u8; N]) -> Self {
        Self { array }
    }
    #[inline(always)]
    pub const fn zeroed() -> Self {
        Self::new([0u8; N])
    }
    #[inline(always)]
    pub const fn slice(&self) -> &[u8] {
        &self.array
    }
    #[inline(always)]
    pub const fn read_byte(&self, position: usize) -> u8 {
        self.array[position]
    }
    #[inline(always)]
    pub const fn read_word(&self, position: usize) -> u16 {
        unsafe { core::mem::transmute([self.read_byte(position), self.read_byte(position + 1)]) }
    }
    #[inline(always)]
    pub const fn read_dword(&self, position: usize) -> u32 {
        unsafe {
            core::mem::transmute([
                self.read_word(position),
                self.read_word(position + sizeof!(u16)),
            ])
        }
    }
    #[inline(always)]
    pub const fn read_qword(&self, position: usize) -> u64 {
        unsafe {
            core::mem::transmute([
                self.read_dword(position),
                self.read_dword(position + sizeof!(u32)),
            ])
        }
    }
    #[inline(always)]
    pub const fn read_xmmword(&self, position: usize) -> u128 {
        unsafe {
            core::mem::transmute([
                self.read_qword(position),
                self.read_qword(position + sizeof!(u64)),
            ])
        }
    }
}
