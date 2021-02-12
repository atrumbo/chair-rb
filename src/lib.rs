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
    use crate::concurrent::ring_buffer::RingBuffer;
    use crate::concurrent::ring_buffer::OneToOneRingBuffer;
    use std::mem::size_of;

    #[test]
    fn one_to_one_rb_test() {
        let mut buf: [u8; 1024] = [0; 1024];
        let buffer = AtomicBuffer::wrap(buf.as_mut_ptr(), buf.len() as u32);
        let ring_buffer = OneToOneRingBuffer::new(buffer);

        let mut src_buf: [u8; 128] = [0; 128];
        let src_buffer = AtomicBuffer::wrap(src_buf.as_mut_ptr(), buf.len() as u32);
        src_buffer.put_i64(0, 42);

        let wrote = ring_buffer.write(1, &src_buffer, 0, size_of::<i64>() as i32);
        let wrote = ring_buffer.write(1, &src_buffer, 0, size_of::<i64>() as i32);
        assert!(wrote);

        let mut times_called = 0;

        let messages_read = ring_buffer.read(|x,y,z, zz| times_called+=1, 10);

        assert_eq!(messages_read, times_called);
        assert_eq!(2, times_called);

    }

}
