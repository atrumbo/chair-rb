use crate::util::Index;
use std::mem::size_of;

mod ring_buffer;
mod atomic;

pub trait IdleStrategy{
    fn idle_work(&self, work_count: i8);
    fn idle(&self);
    fn reset(&self);
}

pub struct Idle<S> {
    pub strategy: S
}

impl<S> Idle<S> where S: IdleStrategy {
    #[inline]
    pub fn idle_work(&self, work_count: i8){
        self.strategy.idle_work(work_count);
    }

    #[inline]
    pub fn idle(&self){
        self.strategy.idle();
    }

    #[inline]
    pub fn reset(&self){
        self.strategy.reset();
    }
}

pub struct NoOpIdleStrategy;

impl IdleStrategy for NoOpIdleStrategy {

    #[inline]
    fn idle_work(&self, _work_count: i8) {}

    #[inline]
    fn idle(&self) {}

    #[inline]
    fn reset(&self) {}
}

pub struct BusySpinIdleStrategy;

impl IdleStrategy for BusySpinIdleStrategy {

    #[inline]
    fn idle_work(&self, work_count: i8) {
        if work_count < 0 {
            atomic::cpu_pause();
        }

    }

    #[inline]
    fn idle(&self) {
        atomic::cpu_pause();
    }

    #[inline]
    fn reset(&self) {
    }
}



pub struct AtomicBuffer {
    buffer : *mut u8,
    length : u32
}

impl AtomicBuffer {

    pub fn capacity(&self) -> Index {
        self.length as Index
    }

    pub fn wrap(buffer: *mut u8, length: u32) -> AtomicBuffer{
        AtomicBuffer{buffer, length}
    }

    pub fn get_i64(&self, index: Index) -> i64 {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            *self.buffer.offset(index as isize) as i64
        }
    }

    pub fn put_i64(&self, index: Index, value: i64) {
        unimplemented!()
    }

    pub fn put_i64_ordered(&self, index: Index, value: i64) {
        unimplemented!()
    }

    pub fn get_int64_volatile(&self, index: Index) -> i64 {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            atomic::get_int64_volatile(self.buffer.offset(index as isize) as *const i64)
        }
    }

    pub fn put_bytes(&self, index: Index, src_buffer: &AtomicBuffer, src_index: Index, length: Index) {
        unimplemented!()
    }

    pub fn set_memory(&self, p0: i32, p1: i32, p2: i32) {
        unimplemented!()
    }

    #[cfg(disable_bounds_check)]
    #[inline]
    fn bounds_check (&self, _index: Index, _length: u64) {}

    #[cfg(not(disable_bounds_check))]
    #[inline]
    fn bounds_check (&self, index: Index, length: usize) {
        if index < 0 || (self.length as usize - index as usize)  < length {
            panic!("Index out of bounds")
        }
    }
}
