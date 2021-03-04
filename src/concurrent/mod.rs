use crate::util::Index;
use std::mem::size_of;

pub mod ring_buffer;
pub mod atomic;

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


#[derive(Debug, Clone, Copy)]
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

    pub fn get<T: Copy>(&self, index: Index) -> T {
        self.bounds_check(index, size_of::<T>());
        unsafe {
            *(self.buffer.offset(index as isize) as *const T)
        }
    }

    pub fn get_i64(&self, index: Index) -> i64 {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            *(self.buffer.offset(index as isize) as *const i64)
        }
    }

    pub fn get_and_add_i64(&self, index: Index, increment: i64) -> i64 {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            atomic::get_and_add_i64(self.buffer.offset(index as isize) as *const i64, increment)
        }
    }

    pub fn put_i64(&self, index: Index, value: i64) {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            std::ptr::write(self.buffer.offset(index as isize) as *mut i64, value);
        }
    }

    pub fn put<T>(&self, index: Index, value: T) {
        self.bounds_check(index, size_of::<T>());
        unsafe {
            std::ptr::write(self.buffer.offset(index as isize) as *mut T, value);
        }
    }


    pub fn put_ordered<T>(&self, index: Index, value: T){
        self.bounds_check(index, size_of::<T>());
        unsafe {
            atomic::put_ordered(self.buffer.offset(index as isize) as *mut T, value);
        }
    }

    pub fn put_i64_ordered(&self, index: Index, value: i64) {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            atomic::put_ordered(self.buffer.offset(index as isize) as *mut i64, value);
        }
    }

    pub fn get_i64_volatile(&self, index: Index) -> i64 {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            atomic::get_volatile(self.buffer.offset(index as isize) as *const i64)
        }
    }

    pub fn get_i32_volatile(&self, index: Index) -> i32 {
        self.bounds_check(index, size_of::<i32>());
        unsafe {
            atomic::get_volatile(self.buffer.offset(index as isize) as *const i32)
        }
    }

    pub fn put_bytes(&self, index: Index, src_buffer: &AtomicBuffer, src_index: Index, length: Index) {
        self.bounds_check(index, length as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(src_buffer.buffer.offset(src_index as isize), self.buffer.offset(index as isize), length as usize);
        }
    }

    pub fn set_memory(&self, index: Index, length: Index, value: u8) {
        self.bounds_check(index, length as usize);
        unsafe {
            std::ptr::write_bytes(self.buffer.offset(index as isize), value, length as usize);
        }
    }

    pub fn compare_and_set_i64(&self, index: Index, expected_value: i64, updated_value: i64) -> bool {
        self.bounds_check(index, size_of::<i64>());
        unsafe {
            let original = atomic::cmpxchg(self.buffer.offset(index as isize) as *const i64, expected_value, updated_value);
            return original == expected_value;
        }
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
