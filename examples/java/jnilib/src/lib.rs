use jni::JNIEnv;
use jni::objects::{JClass, JByteBuffer};
use jni::sys::{jboolean, jint};

use chair_rb::concurrent::ring_buffer::{OneToOneRingBuffer, RingBuffer};
use chair_rb::concurrent::AtomicBuffer;

use std::mem::size_of;
use std::sync::Arc;
use std::thread;
const POISON_MESSAGE_TYPE: i32 = 42;

/*
 * Class:     ChairRB
 * Method:    createAndStartProducer
 * Signature: ([B)Z
 */
#[no_mangle]
pub extern "system" fn Java_ChairRB_createAndStartOneToOneProducer(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                             _class: JClass,
                                             byte_buffer: JByteBuffer,
                                             mesages_to_produce: jint)
                                             -> jboolean {


    // First, we have to get the byte[] out of java.
    let buffer = env.get_direct_buffer_address(byte_buffer).unwrap();
    println!("Input byte buffer size: {}", buffer.len());

    let ab = AtomicBuffer::wrap(&mut *buffer);
    let ring_buffer = Arc::new(OneToOneRingBuffer::new(ab));
    let rb = Arc::clone(&ring_buffer);

    thread::spawn(move || {
        let mut src_buf: [u8; 128] = [0; 128];
        let src_buffer = AtomicBuffer::wrap(&mut src_buf);

        let mut sent = 0;
        while sent < mesages_to_produce {
            src_buffer.put_i64(0, sent as i64);

            while !rb.write(1, &src_buffer, 0, size_of::<i64>() as i32) {}
            sent += 1;

            if sent % (mesages_to_produce / 10) == 0 {
                println!("Rust Producer - Written {} message to ring buffer", sent);
            }
        }
        src_buffer.put_i64(0, POISON_MESSAGE_TYPE as i64);
        println!(
            "Producer - Sending poison after publishing {} messages to ring buffer",
            sent
        );

        while !rb.write(POISON_MESSAGE_TYPE, &src_buffer, 0, size_of::<i64>() as i32) {}
    });

    true as jboolean
}
                                             