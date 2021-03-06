use std::ops::{Deref, DerefMut};

#[repr(align(16))]
pub struct Align16<T> {
    pub(crate) aligned: T,
}

impl<T> Align16<T> {
    pub fn new(aligned: T) -> Align16<T> {
        Align16::<T> { aligned }
    }
}

impl<T> Deref for Align16<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.aligned
    }
}

impl<T> DerefMut for Align16<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.aligned
    }
}
