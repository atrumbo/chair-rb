/*
 * Copyright 2021 Andrew Trumbo
 * This work is a derivative of:
 * https://github.com/real-logic/aeron/
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
