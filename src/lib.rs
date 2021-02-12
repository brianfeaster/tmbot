mod serror;
mod log;
mod json;

use ::std::str::{from_utf8};
pub use crate::serror::*;
pub use crate::log::*;
pub use crate::json::*;

pub fn num2heart (n :i32) -> &'static str {
    if n < -4 { return from_utf8(b"\xCE\xBB").unwrap(); } // lambda
    if n <= -1{ return from_utf8(b"\xF0\x9F\x96\xA4").unwrap(); } // black heart
    if n == 0 { return from_utf8(b"\xF0\x9F\x92\x94").unwrap(); } // red broken heart
    if n == 1 { return from_utf8(b"\xE2\x9D\xA4\xEF\xB8\x8F").unwrap(); } // red heart
    if n == 2 { return from_utf8(b"\xF0\x9F\xA7\xA1").unwrap(); } // orange heart
    if n == 3 { return from_utf8(b"\xF0\x9F\x92\x9B").unwrap(); } // yellow heart
    if n == 4 { return from_utf8(b"\xF0\x9F\x92\x9A").unwrap(); } // green heart
    if n == 5 { return from_utf8(b"\xF0\x9F\x92\x99").unwrap(); } // blue heart
    if n == 6 { return from_utf8(b"\xF0\x9F\x92\x9C").unwrap(); } // violet heart
    if n == 7 { return from_utf8(b"\xF0\x9F\x92\x97").unwrap(); } // pink growing heart
    if n == 8 { return from_utf8(b"\xF0\x9F\x92\x96").unwrap(); } // pink sparkling heart
    if n == 9 { return from_utf8(b"\xF0\x9F\x92\x93").unwrap(); } // beating heart
    if n == 10 { return from_utf8(b"\xF0\x9F\x92\x98").unwrap(); } // heart with arrow
    if n == 11 { return from_utf8(b"\xF0\x9F\x92\x9D").unwrap(); } // heart with ribbon
    if n == 12 { return from_utf8(b"\xF0\x9F\x92\x9E").unwrap(); } // revolving hearts
    return from_utf8(b"\xF0\x9F\x8D\x86").unwrap(); // egg plant
}
