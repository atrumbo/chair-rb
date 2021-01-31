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
    use crate::public_function;

    #[test]
    fn it_works() {
        assert_eq!(public_function(), 42);
    }
}
