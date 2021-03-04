pub type Index = i32;

pub mod bit_util {
    use crate::util::Index;

    pub const CACHE_LINE_LENGTH: i32 = 64;

    #[inline]
    pub fn is_power_of_two(value: Index) -> bool {
        value > 0 && ((value & (!value + 1)) == value)
    }

    #[inline]
    pub fn align(value: Index, alignment: Index) -> Index {
        (value + (alignment - 1)) & !(alignment - 1)
    }
}
