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

//! Functions for validating the encoding of objects

use std::ptr;

pub struct BufferBlockReader<const N: usize> {
    buffer_ptr: *const u8,
    len: usize,
    len_after_step: usize,
    idx: usize,
}

impl<const N: usize> BufferBlockReader<N> {
    pub const fn new(buffer: &[u8]) -> Self {
        Self {
            buffer_ptr: buffer.as_ptr(),
            len: buffer.len(),
            len_after_step: if buffer.len() < N {
                0
            } else {
                buffer.len() - N
            },
            idx: 0,
        }
    }
    pub const fn block_index(&self) -> usize {
        self.idx
    }
    pub const fn has_full_block(&self) -> bool {
        self.idx < self.len_after_step
    }
    pub fn full_block(&self) -> &u8 {
        unsafe {
            // UNSAFE(@ohsayan): We're dereferencing a ptr that we know will not be null
            // because the len of the buffer gurantees that
            &*self.buffer_ptr.add(self.idx)
        }
    }
    pub unsafe fn get_remaining(&self, dst: *mut u8) -> usize {
        if self.len == self.idx {
            0
        } else {
            {
                ptr::write_bytes(dst, 0x20, N);
                ptr::copy_nonoverlapping(self.buffer_ptr.add(self.idx), dst, self.len - self.idx);
            }
            self.len - self.idx
        }
    }
    pub fn advance(&mut self) {
        self.idx += N
    }
}
