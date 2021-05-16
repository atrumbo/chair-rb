/*
 * Copyright 2021 Andrew Trumbo
 * This work is a derivative of:
 * https://github.com/real-logic/aeron/
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::mem::size_of;

use crate::concurrent::AtomicBuffer;
use crate::util::bit_util;
use crate::util::bit_util::CACHE_LINE_LENGTH;
use crate::util::Index;

pub use self::many_to_one_ring_buffer::ManyToOneRingBuffer;
pub use self::one_to_one_ring_buffer::OneToOneRingBuffer;

pub mod many_to_one_ring_buffer;
pub mod one_to_one_ring_buffer;

pub struct RingBufferDescriptor;

impl RingBufferDescriptor {
    pub const TAIL_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 2;
    pub const HEAD_CACHE_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 4;
    pub const HEAD_POSITION_OFFSET: Index = CACHE_LINE_LENGTH * 6;
    pub const CORRELATION_COUNTER_OFFSET: Index = CACHE_LINE_LENGTH * 8;
    pub const CONSUMER_HEARTBEAT_OFFSET: Index = CACHE_LINE_LENGTH * 10;

    /* Total length of the trailer in bytes. */
    pub const TRAILER_LENGTH: Index = CACHE_LINE_LENGTH * 12;

    #[inline]
    fn check_capacity(capacity: Index) {
        if !bit_util::is_power_of_two(capacity) {
            panic!(
                "Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity={}",
                capacity
            )
        }
    }
}

/**
* Header length made up of fields for message length, message type, and then the encoded message.
* <p>
* Writing of the record length signals the message recording is complete.
* <pre>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |R|                       Record Length                         |
*  +-+-------------------------------------------------------------+
*  |                              Type                             |
*  +---------------------------------------------------------------+
*  |                       Encoded Message                        ...
* ...                                                              |
*  +---------------------------------------------------------------+
* </pre>
*/
pub struct RecordDescriptor;

impl RecordDescriptor {
    pub const HEADER_LENGTH: Index = size_of::<Index>() as Index * 2;
    pub const ALIGNMENT: Index = RecordDescriptor::HEADER_LENGTH;
    pub const PADDING_MSG_TYPE_ID: Index = -1;

    #[inline]
    pub fn length_offset(record_offset: Index) -> Index {
        record_offset
    }

    #[inline]
    pub fn type_offset(record_offset: Index) -> Index {
        record_offset + size_of::<Index>() as Index
    }

    #[inline]
    pub fn encoded_msg_offset(record_offset: Index) -> Index {
        record_offset + RecordDescriptor::HEADER_LENGTH
    }

    #[inline]
    pub fn make_header(length: i32, msg_type_id: i32) -> i64 {
        (((msg_type_id as i64) & 0xFFFFFFFF) << 32) | (length as i64 & 0xFFFFFFFF)
    }

    #[inline]
    pub fn record_length(header: i64) -> i32 {
        header as i32
    }

    #[inline]
    pub fn message_type_id(header: i64) -> i32 {
        (header >> 32) as i32
    }

    #[inline]
    pub fn check_msg_type_id(msg_type_id: i32) {
        if msg_type_id < 1 {
            panic!(
                "Message type id must be greater than zero, msgTypeId={}",
                msg_type_id
            );
        }
    }
}

pub trait RingBuffer {
    fn capacity(&self) -> Index;

    fn write(
        &self,
        msg_type_id: i32,
        src_buffer: &AtomicBuffer,
        src_index: Index,
        length: Index,
    ) -> bool;

    fn read<'a, F>(&'a self, handler: F, message_count_limit: u32) -> u32
    where
        F: FnMut(i32, &'a AtomicBuffer, Index, Index);

    fn max_msg_length(&self) -> Index;

    fn next_correlation_id(&self) -> i64;

    fn unblock(&self) -> bool;
}

pub trait MessageHandler {
    fn on_message(&self, msg_type_id: i32, buffer: &AtomicBuffer, index: Index, length: Index);
}
