use std::sync::atomic::Ordering;

#[inline]
pub fn thread_fence(){
    std::sync::atomic::fence(Ordering::AcqRel);
}

#[inline]
pub fn fence(){
    std::sync::atomic::fence(Ordering::SeqCst);
}

#[inline]
pub fn acquire(){
    std::sync::atomic::fence(Ordering::Acquire)
}

#[inline]
pub fn release(){
    std::sync::atomic::fence(Ordering::Release)
}

#[inline]
pub fn cpu_pause(){
}

#[inline]
pub unsafe fn get_int64_volatile(source: *const i64) -> i64 {
    let sequence : i64 = std::ptr::read_volatile(source);
    acquire();
    sequence
}