use super::*;

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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::Align16;
    use std::mem::size_of;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    const CAPACITY: i32 = 1024;
    const BUFFER_SZ: usize = (CAPACITY + RingBufferDescriptor::TRAILER_LENGTH) as usize;
    const ODD_BUFFER_SZ: i32 = (CAPACITY - 1) + RingBufferDescriptor::TRAILER_LENGTH;

    const MSG_TYPE_ID: i32 = 101;
    const HEAD_COUNTER_INDEX: Index = 1024 + RingBufferDescriptor::HEAD_POSITION_OFFSET;
    const TAIL_COUNTER_INDEX: Index = 1024 + RingBufferDescriptor::TAIL_POSITION_OFFSET;

    #[test]
    fn one_to_one_rb_test() {
        // let context = OneToOneRingBufferTestContext::new();
        // assert_eq!(mem::align_of_val_raw(&context), 16);

        let mut buf: [u8; 1024] = [0; 1024];
        let buffer = AtomicBuffer::wrap(buf.as_mut_ptr(), buf.len() as u32);
        let ring_buffer = OneToOneRingBuffer::new(buffer);

        let mut src_buf: [u8; 128] = [0; 128];
        let src_buffer = AtomicBuffer::wrap(src_buf.as_mut_ptr(), src_buf.len() as u32);
        src_buffer.put_i64(0, 42);

        assert!(ring_buffer.write(1, &src_buffer, 0, size_of::<i64>() as i32));
        assert!(ring_buffer.write(1, &src_buffer, 0, size_of::<i64>() as i32));

        let mut times_called = 0;

        let messages_read = ring_buffer.read(|_,_,_,_| times_called+=1, 10);

        assert_eq!(messages_read, times_called);
        assert_eq!(2, times_called);

    }


    #[test]
    fn should_reject_write_when_insufficient_space() {

        let length: Index = 100;
        let head: i64 = 0;
        let tail: i64 = head + (CAPACITY - bit_util::align(length - RecordDescriptor::ALIGNMENT, RecordDescriptor::ALIGNMENT)) as i64;
        let src_index: Index = 0;

        let mut buffer: Align16<[u8; BUFFER_SZ]> = Align16::new([0; BUFFER_SZ]);

        assert_eq!(buffer.as_ptr() as usize % 16,  0);
        assert_eq!(buffer.aligned.as_ptr() as usize % 16,  0);

        let mut src_buffer: Align16<[u8; BUFFER_SZ]> = Align16::new([0;BUFFER_SZ]);

        assert_eq!(src_buffer.as_ptr() as usize % 16,  0);

        let ab = AtomicBuffer::wrap(buffer.as_mut_ptr(), buffer.len() as u32);
        let src_ab = AtomicBuffer::wrap(src_buffer.as_mut_ptr(), src_buffer.len() as u32);


        ab.put_i64(HEAD_COUNTER_INDEX, head);
        assert_eq!(head, ab.get_i64(HEAD_COUNTER_INDEX));

        ab.put_i64(TAIL_COUNTER_INDEX, tail);
        assert_eq!(tail, ab.get_i64(TAIL_COUNTER_INDEX));

        let ring_buffer= OneToOneRingBuffer::new(ab);
        assert!(!ring_buffer.write(MSG_TYPE_ID, &src_ab, src_index, length));

        assert_eq!(ab.get_i64(TAIL_COUNTER_INDEX), tail);
    }

    #[test]
    fn one_to_one_rb_test_threaded() {
        let mut buf: Align16<[u8; 17152]> = Align16::new([0; 17152]);
        let buffer = AtomicBuffer::wrap(buf.as_mut_ptr(), buf.len() as u32);
        // let ring_buffer = OneToOneRingBuffer::new(buffer);
        let ring_buffer = Arc::new(OneToOneRingBuffer::new(buffer));
        let rb = Arc::clone(&ring_buffer);

        let handle = thread::spawn(move || {
            let mut src_buf: [u8; 128] = [0; 128];
            let src_buffer = AtomicBuffer::wrap(src_buf.as_mut_ptr(), src_buf.len() as u32);

            let mut sent = 0;
            for i in 0..10_000_000_i32 {
                // println!("hi number {} from the spawned thread!", i);
                src_buffer.put_i64(0, i as i64);
                loop {
                    if rb.write(1, &src_buffer, 0, size_of::<i64>() as i32) {
                        sent += 1;
                        break;
                    }
                }
            }
            src_buffer.put_i64(0, 42);
            println!("ProducerThread: Sending poison after publishing {} messages to ring buffer", sent);
            loop{
                if rb.write(42, &src_buffer, 0, size_of::<i64>() as i32) {
                    break;
                }
            }

        });

        let mut times_called = 0;

        let mut is_poison = false;
        let mut prev_value = 0;

        thread::sleep(Duration::from_millis(10));

        loop {
            if is_poison {
                break;
            }

            ring_buffer.read(|msg_type_id, src_buffer, src_index , _src_length| {
                times_called += 1;

                let value = src_buffer.get_i64(src_index);
                // assert_eq!(value, prev_value + 1);
                prev_value = value;

                if msg_type_id == 42 {
                    is_poison = true;
                    println!("ConsumerThread: Got Poison - msg_type_id: {} value: {}, received total {}", msg_type_id, value, times_called);
                }

            }, 10);
        }

        handle.join().unwrap();
    }

}