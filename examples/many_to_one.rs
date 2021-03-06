use chair_rb::concurrent::ring_buffer::{ManyToOneRingBuffer, RingBuffer, RingBufferDescriptor};
use chair_rb::concurrent::AtomicBuffer;
use chair_rb::mem::Align16;
use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::sync::atomic::{AtomicI32, Ordering};


const PRODUCER_COUNT: u32 = 3;
const MESSAGES_PER_PRODUCER: u32 = 100_000_000;
const POISON_MESSAGE_TYPE: i32 = 42;

fn main() {
    println!("Many to One Ring Buffer Example");
    let mut buf = Align16::new([0 as u8; 1024 + RingBufferDescriptor::TRAILER_LENGTH as usize]);
    let buffer = AtomicBuffer::wrap(&mut *buf);
    let ring_buffer = Arc::new(ManyToOneRingBuffer::new(buffer));
    let producer_id = Arc::new(AtomicI32::new(1));

    let mut threads = vec![];

    for _ in 0..PRODUCER_COUNT {
        let producer_id = producer_id.clone();
        let ring_buffer = ring_buffer.clone();
        threads.push( thread::spawn(move || {
            let mut src_buf: [u8; 128] = [0; 128];
            let src_buffer = AtomicBuffer::wrap(&mut src_buf);

            let producer_id = producer_id.fetch_add(1, Ordering::SeqCst);

            let mut sent = 0;

            while sent < MESSAGES_PER_PRODUCER {
                src_buffer.put_i64(0, sent as i64);

                while !ring_buffer.write(producer_id, &src_buffer, 0, size_of::<i64>() as i32) {
                    thread::yield_now();
                }
                sent += 1;

                if sent % (MESSAGES_PER_PRODUCER / 10) == 0 {
                    println!("Producer #{}\t- Written {} message to ring buffer", producer_id, sent);
                }



            }
            src_buffer.put_i64(0, POISON_MESSAGE_TYPE as i64);
            println!(
                "Producer #{}\t- Sending poison after publishing {} messages to ring buffer",
                producer_id, sent
            );

            while !ring_buffer.write(POISON_MESSAGE_TYPE, &src_buffer, 0, size_of::<i64>() as i32) {
                thread::yield_now();
            }

        }));
    }

    let mut times_called = 0;

    let mut poison_count = 0;

    let instant = Instant::now();

    while poison_count < PRODUCER_COUNT {

        ring_buffer.read(
            |msg_type_id, src_buffer, src_index, _src_length| {
                times_called += 1;

                if times_called % (MESSAGES_PER_PRODUCER / 10) == 0 {
                    println!("Consumer\t- Read {} messages from ring buffer", times_called);
                }

                let value = src_buffer.get_i64(src_index);

                if msg_type_id == POISON_MESSAGE_TYPE {
                    poison_count += 1;
                    println!(
                        "Consumer\t- Got Poison - msg_type_id: {} value: {}, received total {}",
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

    for thread in threads {
        thread.join().unwrap();
    }
}
