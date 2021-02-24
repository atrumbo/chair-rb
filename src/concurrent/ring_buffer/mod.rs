use crate::concurrent::AtomicBuffer;
use crate::util::bit_util;
use crate::util::bit_util::CACHE_LINE_LENGTH;
use crate::util::Index;
use std::mem::size_of;

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



pub struct OneToOneRingBuffer {
    buffer: AtomicBuffer,
    capacity: Index,
    max_msg_length: Index,
    head_position_index: Index,
    head_cache_position_index: Index,
    tail_position_index: Index,
    correlation_id_counter_index: Index,
    consumer_heartbeat_index: Index,
}

unsafe impl Send for OneToOneRingBuffer {}
unsafe impl Sync for OneToOneRingBuffer {}


impl OneToOneRingBuffer {
    #[inline]
    fn check_msg_length(&self, length: Index) {
        if length > self.max_msg_length {
            panic!("encoded message exceeds maxMsgLength of {}  length={}", self.max_msg_length, length)
        }
    }

    pub fn new(buffer: AtomicBuffer) -> OneToOneRingBuffer{

        let capacity = buffer.capacity() - RingBufferDescriptor::TRAILER_LENGTH;

        RingBufferDescriptor::check_capacity(capacity);

        OneToOneRingBuffer {
            buffer,
            capacity,
            max_msg_length: capacity / 8,
            tail_position_index: capacity + RingBufferDescriptor::TAIL_POSITION_OFFSET,
            head_cache_position_index: capacity + RingBufferDescriptor::HEAD_CACHE_POSITION_OFFSET,
            head_position_index: capacity + RingBufferDescriptor::HEAD_POSITION_OFFSET,
            correlation_id_counter_index: capacity + RingBufferDescriptor::CORRELATION_COUNTER_OFFSET,
            consumer_heartbeat_index: capacity + RingBufferDescriptor::CONSUMER_HEARTBEAT_OFFSET,
        }

    }
}

impl RingBuffer for OneToOneRingBuffer {
    fn capacity(&self) -> Index {
        self.capacity
    }

    fn write(&self, msg_type_id: i32, src_buffer: &AtomicBuffer, src_index: i32, length: i32) -> bool {
        RecordDescriptor::check_msg_type_id(msg_type_id);
        self.check_msg_length(length);

        let record_length: Index = length + RecordDescriptor::HEADER_LENGTH;
        let required_capacity: Index = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let mask: i64 = self.capacity as i64 - 1;

        let mut head = self.buffer.get_i64(self.head_cache_position_index);
        let tail: i64 = self.buffer.get_i64(self.tail_position_index);
        let available_capacity: Index = self.capacity - (tail - head) as Index;

        if required_capacity > available_capacity {
            head = self.buffer.get_int64_volatile(self.head_position_index);

            if required_capacity > (self.capacity - (tail - head) as Index)
            {
                return false;
            }
            self.buffer.put_i64(self.head_cache_position_index, head);
        }

        let mut padding: Index = 0;
        let mut record_index: Index = (tail & mask) as Index;
        let to_buffer_end_length: Index = self.capacity - record_index;

        if required_capacity > to_buffer_end_length
        {
            let mut head_index: Index = (head & mask) as Index;

            if required_capacity > head_index
            {
                head = self.buffer.get_int64_volatile(self.head_position_index);
                head_index = (head & mask) as Index;

                if required_capacity > head_index
                {
                    return false;
                }

                self.buffer.put_i64_ordered(self.head_cache_position_index, head);
            }

            padding = to_buffer_end_length;
        }

        if 0 != padding
        {
            self.buffer.put_i64_ordered(record_index, RecordDescriptor::make_header(padding, RecordDescriptor::PADDING_MSG_TYPE_ID));
            record_index = 0;
        }

        self.buffer.put_bytes(RecordDescriptor::encoded_msg_offset(record_index), src_buffer, src_index, length);
        self.buffer.put_i64_ordered(record_index, RecordDescriptor::make_header(record_length, msg_type_id));
        self.buffer.put_i64_ordered(self.tail_position_index, tail + (required_capacity + padding) as i64);

        return true;
    }

    fn read<'a, F>(&'a self, mut handler: F, message_count_limit: u32) -> u32 where F: FnMut(i32, &'a AtomicBuffer, Index, Index) {
        let head: i64 = self.buffer.get_i64(self.head_position_index);
        let head_index = (head & (self.capacity - 1) as i64) as Index;
        let contiguous_block_length: Index = self.capacity - head_index;
        let mut messages_read = 0;
        let mut bytes_read = 0;

        // auto cleanup = util::InvokeOnScopeExit {
        // [&]()
        // {
        // if (bytes_read != 0)
        // {
        // m_buffer.setMemory(head_index, static_cast<std::size_t>(bytes_read), 0);
        // m_buffer.putInt64Ordered(m_headPositionIndex, head + bytes_read);
        // }
        // }};
        //
        while (bytes_read < contiguous_block_length) && (messages_read < message_count_limit) {
            let record_index: Index = head_index + bytes_read;
            let header: i64 = self.buffer.get_int64_volatile(record_index);
            let record_length: Index = RecordDescriptor::record_length(header);

            if record_length <= 0
            {
                break;
            }

            bytes_read += bit_util::align(record_length, RecordDescriptor::ALIGNMENT);

            let msg_type_id: Index = RecordDescriptor::message_type_id(header);
            if RecordDescriptor::PADDING_MSG_TYPE_ID == msg_type_id
            {
                continue;
            }

            messages_read += 1;
            handler(msg_type_id,
                               &self.buffer,
                               RecordDescriptor::encoded_msg_offset(record_index),
                               record_length - RecordDescriptor::HEADER_LENGTH);
        }
        // TODO: Moved here for now need error handling from on message or a scope guard
        if bytes_read != 0
        {
            self.buffer.set_memory(head_index, bytes_read, 0);
            self.buffer.put_i64_ordered(self.head_position_index, head + bytes_read as i64);
        }
        messages_read
    }
}