use super::*;
use std::cell::Cell;

pub struct ManyToOneRingBuffer {
    buffer: AtomicBuffer,
    capacity: Index,
    max_msg_length: Index,
    head_position_index: Index,
    head_cache_position_index: Index,
    tail_position_index: Index,
    correlation_id_counter_index: Index,
    consumer_heartbeat_index: Index,
}

unsafe impl Send for ManyToOneRingBuffer {}

unsafe impl Sync for ManyToOneRingBuffer {}

impl ManyToOneRingBuffer {
    const INSUFFICIENT_CAPACITY: Index = -2;

    #[inline]
    fn check_msg_length(&self, length: Index) {
        if length > self.max_msg_length {
            panic!("encoded message exceeds maxMsgLength of {} length={}", self.max_msg_length, length)
        }
    }

    pub fn new(buffer: AtomicBuffer) -> ManyToOneRingBuffer {
        let capacity = buffer.capacity() - RingBufferDescriptor::TRAILER_LENGTH;

        RingBufferDescriptor::check_capacity(capacity);

        ManyToOneRingBuffer {
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

    fn claim_capacity(&self, required_capacity: Index) -> Index {
        unimplemented!()
    }
}

impl RingBuffer for ManyToOneRingBuffer {
    fn capacity(&self) -> i32 {
        self.capacity
    }

    fn write(&self, msg_type_id: i32, src_buffer: &AtomicBuffer, src_index: i32, length: i32) -> bool {
        let mut is_successful = false;

        RecordDescriptor::check_msg_type_id(msg_type_id);
        self.check_msg_length(length);

        let record_length: Index = length + RecordDescriptor::HEADER_LENGTH;
        let required_capacity: Index = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let record_index: Index = self.claim_capacity(required_capacity);

        if ManyToOneRingBuffer::INSUFFICIENT_CAPACITY != record_index
        {
            self.buffer.put_ordered(record_index, RecordDescriptor::make_header(-record_length, msg_type_id));
            self.buffer.put_bytes(RecordDescriptor::encoded_msg_offset(record_index), src_buffer, src_index, length);
            self.buffer.put_ordered(RecordDescriptor::length_offset(record_index), record_length);

            is_successful = true;
        }
        is_successful
    }

    fn read<'a, F>(&'a self, mut handler: F, message_count_limit: u32) -> u32 where F: FnMut(i32, &'a AtomicBuffer, Index, Index) {
        let head: i64 = self.buffer.get_i64(self.head_position_index);
        let head_index = (head & (self.capacity - 1) as i64) as Index;
        let contiguous_block_length: Index = self.capacity - head_index;
        let mut messages_read = 0;
        let bytes_read = Cell::new(0);

        defer! {
            let read = bytes_read.get();
            if read != 0
            {
                self.buffer.set_memory(head_index, read, 0);
                self.buffer.put_i64_ordered(self.head_position_index, head + read as i64);
            }
        }

        while (bytes_read.get() < contiguous_block_length) && (messages_read < message_count_limit) {
            let record_index: Index = head_index + bytes_read.get();
            let header: i64 = self.buffer.get_int64_volatile(record_index);
            let record_length: Index = RecordDescriptor::record_length(header);

            if record_length <= 0
            {
                break;
            }

            bytes_read.set(bytes_read.get() + bit_util::align(record_length, RecordDescriptor::ALIGNMENT));

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

        messages_read
    }

    fn max_msg_length(&self) -> i32 {
        self.max_msg_length
    }
}


