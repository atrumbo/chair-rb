use std::sync::atomic::{AtomicI64, Ordering};

#[inline]
pub fn thread_fence() {
    std::sync::atomic::fence(Ordering::AcqRel);
}

#[inline]
pub fn fence() {
    std::sync::atomic::fence(Ordering::SeqCst);
}

#[inline]
pub fn acquire() {
    std::sync::atomic::fence(Ordering::Acquire)
}

#[inline]
pub fn release() {
    std::sync::atomic::fence(Ordering::Release)
}

#[inline]
pub fn cpu_pause() {}

#[inline]
pub unsafe fn get_volatile<T>(source: *const T) -> T {
    let sequence: T = std::ptr::read_volatile(source);
    acquire();
    sequence
}

#[inline]
pub unsafe fn put_ordered<T>(dest: *mut T, value: T) {
    release();
    std::ptr::write(dest, value);
}

#[inline]
pub unsafe fn get_and_add_i64(src: *const i64, increment: i64) -> i64 {
    (&*(src as *const AtomicI64)).fetch_add(increment, Ordering::SeqCst)
}

#[inline]
pub unsafe fn compare_exchange(address: *const i64, expected: i64, desired: i64) -> i64 {
    match (&*(address as *const AtomicI64)).compare_exchange(
        expected,
        desired,
        Ordering::SeqCst,
        Ordering::SeqCst,
    ) {
        Ok(x) => x,
        Err(x) => x,
    }
}
