use std::mem::size_of;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use rust_agrona_rb::mem::Align16;
use rust_agrona_rb::concurrent::AtomicBuffer;
use rust_agrona_rb::concurrent::ring_buffer::{OneToOneRingBuffer, RingBuffer};


fn main() {
    println!("Hello from an example!");
    let mut buf: Align16<[u8; 1024 + 768]> = Align16::new([0; 1024+768]);
    let buffer = AtomicBuffer::wrap(buf.as_mut_ptr(), buf.len() as u32);
    // let ring_buffer = OneToOneRingBuffer::new(buffer);
    let ring_buffer = Arc::new(OneToOneRingBuffer::new(buffer));
    let rb = Arc::clone(&ring_buffer);

    let handle = thread::spawn(move || {
        let mut src_buf: [u8; 128] = [0; 128];
        let src_buffer = AtomicBuffer::wrap(src_buf.as_mut_ptr(), src_buf.len() as u32);

        let mut sent = 0;
        loop {
            // println!("hi number {} from the spawned thread!", i);
            src_buffer.put_i64(0, sent as i64);
            loop {
                if rb.write(1, &src_buffer, 0, size_of::<i64>() as i32) {
                    sent += 1;
                    break;
                }
            }
            if sent % 100_000_000 == 0 {
                println!("Written {} message to ring buffer", sent);
            }

            if sent == 1_000_000_000 {
                break;
            }
        }
        src_buffer.put_i64(0, 42);
        println!("ProducerThread: Sending poison after publishing {} messages to ringbuffer", sent);
        loop{
            if rb.write(42, &src_buffer, 0, size_of::<i64>() as i32) {
                break;
            }
        }

    });

    let mut times_called = 0;

    let mut is_poison = false;

    let instant = Instant::now();

    loop {
        if is_poison {
            break;
        }

        ring_buffer.read(|msg_type_id, src_buffer, src_index , _src_length| {
            times_called += 1;

            if times_called % 100_000_000 == 0 {
                println!("Read {} messages from ringbuffer", times_called);
            }

            let value = src_buffer.get_i64(src_index);

            if msg_type_id == 42 {
                is_poison = true;
                println!("ConsumerThread: Got Poison - msg_type_id: {} value: {}, received total {}", msg_type_id, value, times_called);
            }

        },u32::max_value());
    }

    let elapsed = instant.elapsed();
    println!("Time to receive {} messages {} milliseconds", times_called, elapsed.as_millis());

    handle.join().unwrap();
}