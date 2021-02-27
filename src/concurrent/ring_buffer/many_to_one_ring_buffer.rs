use super::*;

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
    #[inline]
    fn check_msg_length(&self, length: Index) {
        if length > self.max_msg_length {
            panic!("encoded message exceeds maxMsgLength of {}  length={}", self.max_msg_length, length)
        }
    }

    pub fn new(buffer: AtomicBuffer) -> ManyToOneRingBuffer{

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
}