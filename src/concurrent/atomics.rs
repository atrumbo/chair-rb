#[allow(unused_macros)]
macro_rules! get_volatile {
    ($dst:expr, $src:expr) => {
    loop {
            $dst = $src;
            unsafe {
                 // llvm_asm!("" ::: "memory" : "volatile")
                 asm!("", "memory", options("volatile"))
            }
            break;
        }
    }
}

#[allow(unused_macros)]
macro_rules! put_ordered {
    ($dst:expr, $src:expr) => {
    loop {
            unsafe {
                 llvm_asm!("" ::: "memory" : "volatile")
            }
            $dst = $src;
            break;
        }
    }
}

// #[allow(unused_macros)]
// macro_rules! put_volatile {
//     ($dst:expr, $src:expr) => {
//     loop {
//             unsafe {
//                  llvm_asm!("" ::: "memory" : "volatile")
//             }
//             $dst = $src;
//             unsafe {
//                  llvm_asm!("lock; addl $0, 0(%%rsp)" ::: "cc", "memory" : "volatile")
//             }
//             break;
//         }
//     }
// }
// #define AERON_PUT_VOLATILE(dst, src) \
// do \
// { \
// __asm__ volatile("" ::: "memory"); \
// dst = src; \
// __asm__ volatile("lock; addl $0, 0(%%rsp)" ::: "cc", "memory"); \
// } \
// while (false) \


#[cfg(test)]
mod tests {

    #[test]
    fn get_volatile_test() {
        let x = 42;
        let y;
        get_volatile!(y,x);
        assert_eq!(y, 42);
    }

    #[test]
    fn put_ordered_test() {
        let x = 42;
        let y;
        put_ordered!(y,x);
        assert_eq!(y, 42);
    }

    // #[test]
    // fn put_volatile_test() {
    //     let x = 42;
    //     let y;
    //     put_volatile!(y,x);
    //     assert_eq!(y, 42);
    // }

}
