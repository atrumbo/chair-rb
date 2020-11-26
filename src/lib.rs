#![feature(asm)]
#![feature(llvm_asm)]

mod concurrent;

pub fn public_function() -> u8 {
    println!("called rary's `public_function()`");
    42
}

#[cfg(test)]
mod tests {
    use crate::public_function;

    #[test]
    fn it_works() {
        assert_eq!(public_function(), 42);
    }
}
