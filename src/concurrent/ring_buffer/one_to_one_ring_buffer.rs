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

use super::*;
use std::cell::Cell;

pub struct OneToOneRingBuffer {
    buffer: AtomicBuffer,
    capacity: Index,
    max_msg_length: Index,
    head_position_index: Index,
    head_cache_position_index: Index,
    tail_position_index: Index,
    correlation_id_counter_index: Index,
    _consumer_heartbeat_index: Index,
}

unsafe impl Send for OneToOneRingBuffer {}

unsafe impl Sync for OneToOneRingBuffer {}

impl OneToOneRingBuffer {
    #[inline]
    fn check_msg_length(&self, length: Index) {
        if length > self.max_msg_length {
            panic!(
                "encoded message exceeds maxMsgLength of {} length={}",
                self.max_msg_length, length
            )
        }
    }

    pub fn new(buffer: AtomicBuffer) -> OneToOneRingBuffer {
        let capacity = buffer.capacity() - RingBufferDescriptor::TRAILER_LENGTH;

        RingBufferDescriptor::check_capacity(capacity);

        OneToOneRingBuffer {
            buffer,
            capacity,
            max_msg_length: capacity / 8,
            tail_position_index: capacity + RingBufferDescriptor::TAIL_POSITION_OFFSET,
            head_cache_position_index: capacity + RingBufferDescriptor::HEAD_CACHE_POSITION_OFFSET,
            head_position_index: capacity + RingBufferDescriptor::HEAD_POSITION_OFFSET,
            correlation_id_counter_index: capacity
                + RingBufferDescriptor::CORRELATION_COUNTER_OFFSET,
            _consumer_heartbeat_index: capacity + RingBufferDescriptor::CONSUMER_HEARTBEAT_OFFSET,
        }
    }
}

impl RingBuffer for OneToOneRingBuffer {
    fn capacity(&self) -> Index {
        self.capacity
    }

    fn write(
        &self,
        msg_type_id: i32,
        src_buffer: &AtomicBuffer,
        src_index: i32,
        length: i32,
    ) -> bool {
        RecordDescriptor::check_msg_type_id(msg_type_id);
        self.check_msg_length(length);

        let record_length: Index = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length: Index = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let required_capacity: Index = aligned_record_length + RecordDescriptor::HEADER_LENGTH;
        let mask: i64 = self.capacity as i64 - 1;

        let mut head = self.buffer.get_i64(self.head_cache_position_index);
        let tail: i64 = self.buffer.get_i64(self.tail_position_index);
        let available_capacity: Index = self.capacity - (tail - head) as Index;

        if required_capacity > available_capacity {
            head = self.buffer.get_i64_volatile(self.head_position_index);

            if required_capacity > (self.capacity - (tail - head) as Index) {
                return false;
            }
            self.buffer.put_i64(self.head_cache_position_index, head);
        }

        let mut padding: Index = 0;
        let mut record_index: Index = (tail & mask) as Index;
        let to_buffer_end_length: Index = self.capacity - record_index;

        if required_capacity > to_buffer_end_length {
            let mut head_index: Index = (head & mask) as Index;

            if required_capacity > head_index {
                head = self.buffer.get_i64_volatile(self.head_position_index);
                head_index = (head & mask) as Index;

                if required_capacity > head_index {
                    return false;
                }

                self.buffer
                    .put_i64_ordered(self.head_cache_position_index, head);
            }

            padding = to_buffer_end_length;
        }

        self.buffer.put_i64_ordered(self.tail_position_index, tail + (aligned_record_length + padding) as i64);

        if 0 != padding {
            self.buffer.put_i64(0, 0);

            self.buffer.put_i64_ordered(
                record_index,
                RecordDescriptor::make_header(padding, RecordDescriptor::PADDING_MSG_TYPE_ID),
            );
            record_index = 0;
        }

        self.buffer.put_i64(record_index + aligned_record_length, 0); // pre-zero next message header

        self.buffer.put_bytes(
            RecordDescriptor::encoded_msg_offset(record_index),
            src_buffer,
            src_index,
            length,
        );
        self.buffer.put_i64_ordered(
            record_index,
            RecordDescriptor::make_header(record_length, msg_type_id),
        );

        return true;
    }

    fn read<'a, F>(&'a self, mut handler: F, message_count_limit: u32) -> u32
    where
        F: FnMut(i32, &'a AtomicBuffer, Index, Index),
    {
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

        while (bytes_read.get() < contiguous_block_length) && (messages_read < message_count_limit)
        {
            let record_index: Index = head_index + bytes_read.get();
            let header: i64 = self.buffer.get_i64_volatile(record_index);
            let record_length: Index = RecordDescriptor::record_length(header);

            if record_length <= 0 {
                break;
            }

            bytes_read.set(
                bytes_read.get() + bit_util::align(record_length, RecordDescriptor::ALIGNMENT),
            );

            let msg_type_id: Index = RecordDescriptor::message_type_id(header);
            if RecordDescriptor::PADDING_MSG_TYPE_ID == msg_type_id {
                continue;
            }

            messages_read += 1;
            handler(
                msg_type_id,
                &self.buffer,
                RecordDescriptor::encoded_msg_offset(record_index),
                record_length - RecordDescriptor::HEADER_LENGTH,
            );
        }

        messages_read
    }

    fn max_msg_length(&self) -> i32 {
        self.max_msg_length
    }

    fn next_correlation_id(&self) -> i64 {
        return self
            .buffer
            .get_and_add_i64(self.correlation_id_counter_index, 1);
    }

    fn unblock(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::Align16;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::{panic, thread};

    const CAPACITY: i32 = 1024;
    const BUFFER_SZ: usize = (CAPACITY + RingBufferDescriptor::TRAILER_LENGTH) as usize;
    const ODD_BUFFER_SZ: usize =
        (CAPACITY - 1) as usize + RingBufferDescriptor::TRAILER_LENGTH as usize;

    const MSG_TYPE_ID: i32 = 101;
    const HEAD_COUNTER_INDEX: Index = 1024 + RingBufferDescriptor::HEAD_POSITION_OFFSET;
    const TAIL_COUNTER_INDEX: Index = 1024 + RingBufferDescriptor::TAIL_POSITION_OFFSET;

    struct OneToOneRingBufferTest {
        _buffer: Align16<Vec<u8>>,
        _src_buffer: Align16<Vec<u8>>,
        ab: AtomicBuffer,
        src_ab: AtomicBuffer,
        ring_buffer: OneToOneRingBuffer,
    }

    impl OneToOneRingBufferTest {
        fn new(buffer_size: usize) -> OneToOneRingBufferTest {
            let mut buffer = Align16::new(vec![0 as u8; buffer_size]);
            let mut src_buffer = Align16::new(vec![0 as u8; buffer_size]);
            let ab = AtomicBuffer::wrap(&mut buffer);
            let src_ab = AtomicBuffer::wrap(&mut src_buffer);
            let ring_buffer = OneToOneRingBuffer::new(ab);

            OneToOneRingBufferTest {
                _buffer: buffer,
                _src_buffer: src_buffer,
                ab,
                src_ab,
                ring_buffer,
            }
        }
    }

    impl Default for OneToOneRingBufferTest {
        fn default() -> Self {
            Self::new(BUFFER_SZ)
        }
    }

    #[test]
    fn should_calculate_capacity_for_buffer() {
        let context = OneToOneRingBufferTest::default();
        assert_eq!(context.ab.capacity(), BUFFER_SZ as i32);
        assert_eq!(
            context.ring_buffer.capacity(),
            BUFFER_SZ as i32 - RingBufferDescriptor::TRAILER_LENGTH
        );
    }

    #[test]
    #[should_panic(
        expected = "Capacity must be a positive power of 2 + TRAILER_LENGTH: capacity=1023"
    )]
    fn should_panic_for_capacity_not_power_of_two() {
        let mut test_buffer = Align16::new([0 as u8; ODD_BUFFER_SZ]);
        let ab = AtomicBuffer::wrap(&mut *test_buffer);
        let _ring_buffer = OneToOneRingBuffer::new(ab);
    }

    #[test]
    #[should_panic(expected = "encoded message exceeds maxMsgLength of 128 length=129")]
    fn should_panic_when_max_message_size_exceeded() {
        let context = OneToOneRingBufferTest::default();
        context.ring_buffer.write(
            MSG_TYPE_ID,
            &context.src_ab,
            0,
            context.ring_buffer.max_msg_length() + 1,
        );
    }

    #[test]
    fn should_write_to_empty_buffer() {
        let context = OneToOneRingBufferTest::default();
        let tail = 0;
        let tail_index = 0;
        let length = 8;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let src_index = 0;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);

        assert!(context
            .ring_buffer
            .write(MSG_TYPE_ID, &context.src_ab, src_index, length));

        assert_eq!(
            context
                .ab
                .get::<i32>(RecordDescriptor::length_offset(tail_index)),
            record_length
        );
        assert_eq!(
            context
                .ab
                .get::<i32>(RecordDescriptor::type_offset(tail_index)),
            MSG_TYPE_ID
        );
        assert_eq!(
            context.ab.get::<i32>(TAIL_COUNTER_INDEX),
            tail + aligned_record_length
        );
    }

    #[test]
    fn should_reject_write_when_insufficient_space() {
        let context = OneToOneRingBufferTest::default();
        let length = 100;
        let head = 0;
        let tail = head
            + (CAPACITY
                - bit_util::align(
                    length - RecordDescriptor::ALIGNMENT,
                    RecordDescriptor::ALIGNMENT,
                )) as i64;
        let src_index = 0;

        context.ab.put_i64(HEAD_COUNTER_INDEX, head);
        assert_eq!(head, context.ab.get_i64(HEAD_COUNTER_INDEX));

        context.ab.put_i64(TAIL_COUNTER_INDEX, tail);
        assert_eq!(tail, context.ab.get_i64(TAIL_COUNTER_INDEX));
        assert!(!context
            .ring_buffer
            .write(MSG_TYPE_ID, &context.src_ab, src_index, length));

        assert_eq!(context.ab.get_i64(TAIL_COUNTER_INDEX), tail);
    }

    #[test]
    fn should_reject_write_when_buffer_full() {
        let context = OneToOneRingBufferTest::default();
        let length = 8;
        let head: i64 = 0;
        let tail: i64 = head + CAPACITY as i64;
        let src_index = 0;

        context.ab.put_i64(HEAD_COUNTER_INDEX, head);
        context.ab.put_i64(TAIL_COUNTER_INDEX, tail);

        assert!(!context
            .ring_buffer
            .write(MSG_TYPE_ID, &context.src_ab, src_index, length));
        assert_eq!(context.ab.get::<i64>(TAIL_COUNTER_INDEX), tail);
    }

    #[test]
    fn should_insert_padding_record_plus_message_on_buffer_wrap() {
        let context = OneToOneRingBufferTest::default();
        let length = 100;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let tail = CAPACITY - RecordDescriptor::ALIGNMENT;
        let head = tail - (RecordDescriptor::ALIGNMENT * 4);
        let src_index = 0;

        context.ab.put_i64(HEAD_COUNTER_INDEX, head as i64);
        context.ab.put_i64(TAIL_COUNTER_INDEX, tail as i64);

        assert!(context
            .ring_buffer
            .write(MSG_TYPE_ID, &context.src_ab, src_index, length));

        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::type_offset(tail)),
            RecordDescriptor::PADDING_MSG_TYPE_ID
        );
        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::length_offset(tail)),
            RecordDescriptor::ALIGNMENT
        );

        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::length_offset(0)),
            record_length
        );
        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::type_offset(0)),
            MSG_TYPE_ID
        );
        assert_eq!(
            context.ab.get::<i64>(TAIL_COUNTER_INDEX) as i32,
            tail + aligned_record_length + RecordDescriptor::ALIGNMENT
        );
    }

    #[test]
    fn should_insert_padding_record_plus_message_on_buffer_wrap_with_head_equal_to_tail() {
        let context = OneToOneRingBufferTest::default();
        let length = 100;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let tail = CAPACITY - RecordDescriptor::ALIGNMENT;
        let head = tail;
        let src_index = 0;

        context.ab.put::<i64>(HEAD_COUNTER_INDEX, head as i64);
        context.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        assert!(context
            .ring_buffer
            .write(MSG_TYPE_ID, &context.src_ab, src_index, length));

        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::type_offset(tail)),
            RecordDescriptor::PADDING_MSG_TYPE_ID
        );
        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::length_offset(tail)),
            RecordDescriptor::ALIGNMENT
        );

        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::length_offset(0)),
            record_length
        );
        assert_eq!(
            context.ab.get::<i32>(RecordDescriptor::type_offset(0)),
            MSG_TYPE_ID
        );
        assert_eq!(
            context.ab.get::<i32>(TAIL_COUNTER_INDEX),
            tail + aligned_record_length + RecordDescriptor::ALIGNMENT
        );
    }

    #[test]
    fn should_read_nothing_from_empty_buffer() {
        let context = OneToOneRingBufferTest::default();
        let tail = 0;
        let head = 0;

        context.ab.put::<i64>(HEAD_COUNTER_INDEX, head);
        context.ab.put::<i64>(TAIL_COUNTER_INDEX, tail);

        let mut times_called = 0;
        let messages_read = context
            .ring_buffer
            .read(|_, _, _, _| times_called += 1, u32::max_value());

        assert_eq!(messages_read, 0);
        assert_eq!(times_called, 0);
    }

    #[test]
    fn should_read_single_message() {
        let context = OneToOneRingBufferTest::default();
        let length = 8;
        let head = 0;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let tail = aligned_record_length;

        context.ab.put::<i64>(HEAD_COUNTER_INDEX, head);
        context.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        context
            .ab
            .put::<i32>(RecordDescriptor::type_offset(0), MSG_TYPE_ID);
        context
            .ab
            .put::<i32>(RecordDescriptor::length_offset(0), record_length);

        let mut times_called = 0;
        let messages_read = context
            .ring_buffer
            .read(|_, _, _, _| times_called += 1, u32::max_value());
        assert_eq!(messages_read, 1);
        assert_eq!(times_called, 1);
        assert_eq!(
            context.ab.get::<i64>(HEAD_COUNTER_INDEX),
            head + aligned_record_length as i64
        );

        for i in (0..RecordDescriptor::ALIGNMENT).step_by(4) {
            assert_eq!(
                context.ab.get::<i32>(i),
                0,
                "buffer has not been zeroed between indexes {} - {}",
                i,
                i + 3
            );
        }
    }

    #[test]
    fn should_not_read_single_message_part_way_through_writing() {
        let context = OneToOneRingBufferTest::default();
        let length = 8;
        let head = 0;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let end_tail = aligned_record_length;

        context.ab.put::<i64>(TAIL_COUNTER_INDEX, end_tail as i64);
        context
            .ab
            .put::<i32>(RecordDescriptor::type_offset(0), MSG_TYPE_ID);
        context
            .ab
            .put::<i32>(RecordDescriptor::length_offset(0), -record_length);

        let mut times_called = 0;
        let messages_read = context
            .ring_buffer
            .read(|_, _, _, _| times_called += 1, u32::max_value());

        assert_eq!(messages_read, 0);
        assert_eq!(times_called, 0);
        assert_eq!(context.ab.get::<i64>(HEAD_COUNTER_INDEX), head);
    }

    #[test]
    fn should_read_two_messages() {
        let context = OneToOneRingBufferTest::default();
        let length = 8;
        let head = 0;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let tail = aligned_record_length * 2;

        context.ab.put::<i64>(HEAD_COUNTER_INDEX, head);
        context.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        context
            .ab
            .put::<i32>(RecordDescriptor::type_offset(0), MSG_TYPE_ID);
        context
            .ab
            .put::<i32>(RecordDescriptor::length_offset(0), record_length);

        context.ab.put::<i32>(
            RecordDescriptor::type_offset(0 + aligned_record_length),
            MSG_TYPE_ID,
        );
        context.ab.put::<i32>(
            RecordDescriptor::length_offset(0 + aligned_record_length),
            record_length,
        );

        let mut times_called = 0;
        let messages_read = context
            .ring_buffer
            .read(|_, _, _, _| times_called += 1, u32::max_value());

        assert_eq!(messages_read, 2);
        assert_eq!(times_called, 2);
        assert_eq!(
            context.ab.get::<i64>(HEAD_COUNTER_INDEX),
            head + (aligned_record_length + aligned_record_length) as i64
        );

        for i in (0..RecordDescriptor::ALIGNMENT * 2).step_by(4) {
            assert_eq!(
                context.ab.get::<i32>(i),
                0,
                "buffer has not been zeroed between indexes {} - {}",
                i,
                i + 3
            );
        }
    }

    #[test]
    fn should_limit_read_of_messages() {
        let context = OneToOneRingBufferTest::default();
        let length = 8;
        let head = 0;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let tail = aligned_record_length * 2;

        context.ab.put::<i64>(HEAD_COUNTER_INDEX, head);
        context.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        context
            .ab
            .put::<i32>(RecordDescriptor::type_offset(0), MSG_TYPE_ID);
        context
            .ab
            .put::<i32>(RecordDescriptor::length_offset(0), record_length);

        context.ab.put::<i32>(
            RecordDescriptor::type_offset(0 + aligned_record_length),
            MSG_TYPE_ID,
        );
        context.ab.put::<i32>(
            RecordDescriptor::length_offset(0 + aligned_record_length),
            record_length,
        );

        let mut times_called = 0;
        let messages_read = context.ring_buffer.read(|_, _, _, _| times_called += 1, 1);

        assert_eq!(messages_read, 1);
        assert_eq!(times_called, 1);
        assert_eq!(
            context.ab.get::<i64>(HEAD_COUNTER_INDEX),
            head + aligned_record_length as i64
        );

        for i in (0..RecordDescriptor::ALIGNMENT).step_by(4) {
            assert_eq!(
                context.ab.get::<i32>(i),
                0,
                "buffer has not been zeroed between indexes {} - {}",
                i,
                i + 3
            );
        }
        assert_eq!(
            context
                .ab
                .get::<i32>(RecordDescriptor::length_offset(aligned_record_length)),
            record_length
        );
    }

    #[test]
    fn should_cope_with_panic_from_handler() {
        let context = OneToOneRingBufferTest::default();
        let length = 8;
        let head = 0;
        let record_length = length + RecordDescriptor::HEADER_LENGTH;
        let aligned_record_length = bit_util::align(record_length, RecordDescriptor::ALIGNMENT);
        let tail = aligned_record_length * 2;

        context.ab.put::<i64>(HEAD_COUNTER_INDEX, head);
        context.ab.put::<i64>(TAIL_COUNTER_INDEX, tail as i64);

        context
            .ab
            .put::<i32>(RecordDescriptor::type_offset(0), MSG_TYPE_ID);
        context
            .ab
            .put::<i32>(RecordDescriptor::length_offset(0), record_length);

        context.ab.put::<i32>(
            RecordDescriptor::type_offset(0 + aligned_record_length),
            MSG_TYPE_ID,
        );
        context.ab.put::<i32>(
            RecordDescriptor::length_offset(0 + aligned_record_length),
            record_length,
        );

        let mut times_called = 0;

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let _messages_read = context.ring_buffer.read(
                |_, _, _, _| {
                    times_called += 1;
                    if 2 == times_called {
                        panic!("expected exception")
                    }
                },
                u32::max_value(),
            );
        }));

        assert_eq!(times_called, 2);
        assert!(result.is_err());
        assert_eq!(
            context.ab.get::<i64>(HEAD_COUNTER_INDEX),
            head + (aligned_record_length + aligned_record_length) as i64
        );

        for i in (0..RecordDescriptor::ALIGNMENT * 2).step_by(4) {
            assert_eq!(
                context.ab.get::<i32>(i),
                0,
                "buffer has not been zeroed between indexes {} - {}",
                i,
                i + 3
            );
        }
    }

    const NUM_MESSAGES: i32 = 10 * 1000 * 1000;
    const NUM_IDS_PER_THREAD: i32 = 10 * 1000 * 1000;

    #[test]
    fn should_provide_correlation_ids() {
        let mut spsc_buffer = Align16::new([0 as u8; BUFFER_SZ]);
        let spsc_ab = AtomicBuffer::wrap(&mut *spsc_buffer);
        let ring_buffer = Arc::new(OneToOneRingBuffer::new(spsc_ab));

        let count_down = Arc::new(AtomicUsize::new(2));

        let mut threads = vec![];

        for _ in 0..2 {
            let count_down_clone = count_down.clone();
            let rb = ring_buffer.clone();
            threads.push(thread::spawn(move || {
                count_down_clone.fetch_sub(1, Ordering::SeqCst);
                while count_down_clone.load(Ordering::Acquire) > 0 {
                    std::thread::yield_now();
                }

                for _ in 0..NUM_IDS_PER_THREAD {
                    rb.next_correlation_id();
                }
            }));
        }

        for thread in threads {
            assert!(thread.join().is_ok());
        }

        assert_eq!(
            ring_buffer.next_correlation_id(),
            (NUM_IDS_PER_THREAD * 2) as i64
        );
    }

    #[test]
    fn should_exchange_messages() {
        let mut spsc_buffer = Align16::new([0 as u8; BUFFER_SZ]);
        let spsc_ab = AtomicBuffer::wrap(&mut *spsc_buffer);
        let ring_buffer = Arc::new(OneToOneRingBuffer::new(spsc_ab));

        let rb = ring_buffer.clone();
        let thread = thread::spawn(move || {
            let mut src_buffer = Align16::new([0 as u8; BUFFER_SZ]);
            let src_ab = AtomicBuffer::wrap(&mut *src_buffer);

            for m in 0..NUM_IDS_PER_THREAD {
                src_ab.put::<i32>(0, m);
                while !rb.write(MSG_TYPE_ID, &src_ab, 0, 4) {
                    std::thread::yield_now();
                }
            }
        });

        let mut msg_count: u32 = 0;
        let mut counts = 0;

        while msg_count < NUM_MESSAGES as u32 {
            let read_count = ring_buffer.read(
                |msg_type_id, buffer, index, length| {
                    let message_number = buffer.get::<i32>(index);

                    assert_eq!(length, 4);
                    assert_eq!(msg_type_id, MSG_TYPE_ID);

                    assert_eq!(counts, message_number);
                    counts += 1;
                },
                u32::max_value(),
            );

            if 0 == read_count {
                std::thread::yield_now();
            }

            msg_count += read_count;
        }
        assert!(thread.join().is_ok());
    }
}
