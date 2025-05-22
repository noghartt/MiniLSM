// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::{BufMut, Bytes, BytesMut};

mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

pub(crate) const MEMORY_SIZE: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_of_elements = self.offsets.len();

        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&self.data);

        for offset in &self.offsets {
            bytes.put_u16_le(*offset);
        }

        bytes.put_u16_le(num_of_elements as u16);

        bytes.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements =
            u16::from_le_bytes([data[data.len() - 2], data[data.len() - 1]]) as usize;

        let mut offsets = Vec::with_capacity(num_of_elements);
        let mut d = Vec::with_capacity(data.len() - num_of_elements * MEMORY_SIZE);

        let offset_end = data.len() - 3;
        let offset_start = offset_end - num_of_elements * MEMORY_SIZE;

        for i in 0..num_of_elements {
            let offset = u16::from_le_bytes([
                data[offset_start + i * MEMORY_SIZE + 1],
                data[offset_start + i * MEMORY_SIZE + 2],
            ]);
            offsets.push(offset);
        }

        for offset in offsets.iter().take(num_of_elements) {
            let key_len = data[*offset as usize] as usize;
            let value_len = data[*offset as usize + key_len + 1] as usize;
            d.extend_from_slice(
                &data[*offset as usize..*offset as usize + key_len + value_len + 2],
            );
        }

        Self { data: d, offsets }
    }
}
