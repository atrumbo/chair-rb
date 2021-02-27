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