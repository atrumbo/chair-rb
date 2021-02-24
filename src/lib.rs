use crate::concurrent::{NoOpIdleStrategy, Idle, IdleStrategy, BusySpinIdleStrategy};

mod concurrent;
mod util;

pub fn public_function() -> u8 {
    println!("called rary's `public_function()`");
    let idle = Idle{ strategy: NoOpIdleStrategy{}};

    idle.reset();
    idle.idle_work(0);
    idle.idle();

    let i2 : Box<dyn IdleStrategy> = Box::new(NoOpIdleStrategy);
    i2.idle();

    let i3 : &dyn IdleStrategy = &NoOpIdleStrategy{};
    i3.idle();

    let i4 = BusySpinIdleStrategy{};
    i4.idle_work(3);

    42
}

#[cfg(test)]
mod tests {
    use crate::concurrent::AtomicBuffer;
    use crate::concurrent::ring_buffer::{RingBuffer, RingBufferDescriptor, RecordDescriptor};
    use crate::concurrent::ring_buffer::OneToOneRingBuffer;
    use std::mem::size_of;
    use std::thread;
    use std::time::Duration;
    use std::sync::Arc;
    use crate::util::Index;
    use crate::util;
    use std::ops::{DerefMut, Deref};

    const CAPACITY: i32 = 1024;
    const BUFFER_SZ: usize = (CAPACITY + RingBufferDescriptor::TRAILER_LENGTH) as usize;
    const ODD_BUFFER_SZ: i32 = (CAPACITY - 1) + RingBufferDescriptor::TRAILER_LENGTH;

    const MSG_TYPE_ID: i32 = 101;
    const HEAD_COUNTER_INDEX: Index = 1024 + RingBufferDescriptor::HEAD_POSITION_OFFSET;
    const TAIL_COUNTER_INDEX: Index = 1024 + RingBufferDescriptor::TAIL_POSITION_OFFSET;

    //
    // struct OneToOneRingBufferTestContext {
    //     buffer: [u8; 1024],
    //     src_buffer: [u8; 1024],
    //     // ab: AtomicBuffer,
    //     // src_ab: AtomicBuffer,
    //     // ring_buffer: OneToOneRingBuffer,
    // }
    //
    // impl OneToOneRingBufferTestContext {
    //     fn new() -> OneToOneRingBufferTestContext {
    //         OneToOneRingBufferTestContext {
    //             buffer: [0; 1024],
    //             src_buffer: [0;1024],
    //             // ab: AtomicBuffer::wrap(buffer.as_mut_ptr(), buffer.len() as u32),
    //             // src_ab: AtomicBuffer::wrap(src_buffer.as_mut_ptr(), src_buffer.len() as u32),
    //             // ring_buffer: OneToOneRingBuffer::new(ab)
    //         }
    //     }
    // }

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

    #[repr(align(16))]
    struct Align16<T> {
        aligned: T
    }

    impl <T> Align16<T> {
        fn new(aligned: T) -> Align16<T> {
            Align16::<T>{ aligned }
        }
    }

    impl<T> Deref for Align16<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.aligned
        }
    }

    impl<T> DerefMut for Align16<T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.aligned
        }
    }

    #[test]
    fn should_reject_write_when_insufficient_space() {

        let length: Index = 100;
        let head: i64 = 0;
        let tail: i64 = head + (CAPACITY - util::bit_util::align(length - RecordDescriptor::ALIGNMENT, RecordDescriptor::ALIGNMENT)) as i64;
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
