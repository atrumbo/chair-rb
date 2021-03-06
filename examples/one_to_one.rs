use chair_rb::concurrent::ring_buffer::{OneToOneRingBuffer, RingBuffer, RingBufferDescriptor};
use chair_rb::concurrent::AtomicBuffer;
use chair_rb::mem::Align16;
use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use std::time::Instant;


const MESSAGES_TO_PRODUCE: u32 = 1_000_000_000;
const POISON_MESSAGE_TYPE: i32 = 42;

fn main() {
    println!("One to One Ring Buffer Example");
    let mut buf = Align16::new([0 as u8; 1024 + RingBufferDescriptor::TRAILER_LENGTH as usize]);
    let buffer = AtomicBuffer::wrap(&mut *buf);
    let ring_buffer = Arc::new(OneToOneRingBuffer::new(buffer));
    let rb = Arc::clone(&ring_buffer);

    let handle = thread::spawn(move || {
        let mut src_buf: [u8; 128] = [0; 128];
        let src_buffer = AtomicBuffer::wrap(&mut src_buf);

        let mut sent = 0;
        while sent < MESSAGES_TO_PRODUCE {
            src_buffer.put_i64(0, sent as i64);

            while !rb.write(1, &src_buffer, 0, size_of::<i64>() as i32) {}
            sent += 1;

            if sent % (MESSAGES_TO_PRODUCE / 10) == 0 {
                println!("Producer - Written {} message to ring buffer", sent);
            }

        }
        src_buffer.put_i64(0, POISON_MESSAGE_TYPE as i64);
        println!(
            "Producer - Sending poison after publishing {} messages to ring buffer",
            sent
        );

        while !rb.write(POISON_MESSAGE_TYPE, &src_buffer, 0, size_of::<i64>() as i32) {}

    });

    let mut times_called = 0;

    let mut is_poison = false;

    let instant = Instant::now();

    while !is_poison {

        ring_buffer.read(
            |msg_type_id, src_buffer, src_index, _src_length| {
                times_called += 1;

                if times_called % (MESSAGES_TO_PRODUCE / 10) == 0 {
                    println!("Consumer - Read {} messages from ring buffer", times_called);
                }

                let value = src_buffer.get_i64(src_index);

                if msg_type_id == 42 {
                    is_poison = true;
                    println!(
                        "Consumer - Got Poison - msg_type_id: {} value: {}, received total {}",
                        msg_type_id, value, times_called
                    );
                }
            },
            u32::max_value(),
        );
    }

    let elapsed = instant.elapsed();
    println!(
        "Time to receive {} messages {} milliseconds {} message per second",
        times_called,
        elapsed.as_millis(),
        times_called as u64 / elapsed.as_secs()
    );

    handle.join().unwrap();
}
