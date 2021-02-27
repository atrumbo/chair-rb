use std::mem::size_of;

use crate::concurrent::AtomicBuffer;
use crate::util::bit_util;
use crate::util::bit_util::CACHE_LINE_LENGTH;
use crate::util::Index;

pub use self::one_to_one_ring_buffer::OneToOneRingBuffer;
pub use self::many_to_one_ring_buffer::ManyToOneRingBuffer;

pub mod one_to_one_ring_buffer;
pub mod many_to_one_ring_buffer;

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
    fn check_capacity(capacity: Index){
        if !bit_util::is_power_of_two(capacity){
            panic!("Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity={}", capacity)
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

    const HEADER_LENGTH: Index = size_of::<Index>() as Index * 2;
    pub(crate) const ALIGNMENT: Index = RecordDescriptor::HEADER_LENGTH;
    const PADDING_MSG_TYPE_ID: Index = -1;

    #[inline]
    fn length_offset(record_offset: Index) -> Index
    {
        record_offset
    }

    #[inline]
    fn type_offset(record_offset: Index) -> Index {
        record_offset + size_of::<Index>() as Index
    }

    #[inline]
    fn encoded_msg_offset(record_offset: Index) -> Index
    {
        record_offset + RecordDescriptor::HEADER_LENGTH
    }

    #[inline]
    fn make_header(length: i32, msg_type_id: i32) -> i64 {
         (((msg_type_id as i64) & 0xFFFFFFFF) << 32) | (length as i64 & 0xFFFFFFFF)
    }

    #[inline]
    fn record_length(header: i64) -> i32
    {
        header as i32
    }

    #[inline] fn message_type_id(header: i64) -> i32
    {
        (header >> 32) as i32
    }

    #[inline] fn check_msg_type_id(msg_type_id: i32)
    {
        if msg_type_id < 1 {
            panic!("Message type id must be greater than zero, msgTypeId={}", msg_type_id);
        }
    }
}


pub trait RingBuffer {

    fn capacity(&self) -> Index;

    fn write(&self, msg_type_id: i32, src_buffer: &AtomicBuffer, src_index: Index, length: Index) -> bool;

    fn read<'a, F>(&'a self, handler: F, message_count_limit: u32) -> u32
    where F: FnMut(i32, &'a AtomicBuffer, Index, Index);
}

pub trait MessageHandler {
    fn on_message(&self, msg_type_id: i32, buffer: &AtomicBuffer, index: Index, length: Index);
}