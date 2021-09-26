use ::std::{
    thread,
    time::{Duration},
    str::{from_utf8},
    error::{Error},
};
pub use ::serde_json::{Value};
use log::*;

////////////////////////////////////////////////////////////////////////////////
/// Types

pub type Bresult<T> = Result<T, Box<dyn Error>>;

////////////////////////////////////////////////////////////////////////////////
/// Logging

pub fn ginfo<T:  std::fmt::Debug>(e: T) -> bool { info!("{:?}",  e); true }
pub fn gwarn<T:  std::fmt::Debug>(e: T) -> bool { warn!("{:?}",  e); true }
pub fn gerror<T: std::fmt::Debug>(e: T) -> bool { error!("{:?}", e); true }

#[macro_export(local_inner_macros)]
macro_rules! ginfod {
    ($pre:expr, $arg:expr) => (
        info!("{} {}",
            $pre,
            std::format!("{:?}", $arg).replace("\n","").replace("\\\\", "\\").replace("\\\"", "\""))
    )
}

#[macro_export(local_inner_macros)]
macro_rules! gerrord {
    ($pre:expr, $arg:expr) => (
        error!("{} {}",
            $pre,
            std::format!("{:?}", $arg).replace("\n","").replace("\\\\", "\\").replace("\\\"", "\""))
    )
}

#[macro_export(local_inner_macros)]
macro_rules! glog {
    ($arg:expr) => {
        match &$arg {
            Ok(o)  => ::log::info!("{:?}", o),
            Err(e) => ::log::error!("{:?}", e)
        }
    }
}

#[macro_export(local_inner_macros)]
macro_rules! glogd {
    ($pre:expr, $arg:expr) => {
        match &$arg {
            Ok(o)  => ::log::info!("{} {:?}", $pre, o),
            Err(e) => ::log::error!("{} {:?}", $pre, e)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// UTF-8

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

////////////////////////////////////////////////////////////////////////////////
/// JSON

pub fn bytes2json (body: &[u8]) -> Result<Value, Box<dyn Error>> {
    Ok( serde_json::from_str( from_utf8(&body)? )? )
}

////////////////////////////////////////

pub fn getin<'t> (mut j :&'t Value, keys :&[&str]) -> &'t Value {
    for k in keys { j = &j[*k] }
    j
}

pub fn getin_i64 (json :&Value, keys :&[&str]) -> Result<i64, String> {
    getin(json, keys)
    .as_i64()
    .ok_or( format!("Unable to parse {:?} as_i64", keys) )
}

pub fn getin_f64 (json :&Value, keys :&[&str]) -> Result<f64, String> {
    getin(json, keys)
    .as_f64()
    .ok_or( format!("Unable to parse {:?} as_f64", keys) )
}

pub fn getin_str <'t> (json :&'t Value, keys :&[&str]) -> Result<String, String> {
    getin(json, keys)
    .as_str()
    .map_or(
        Err(format!("Unable to parse {:?} as_str", keys)),
        |j| Ok(j.to_string()) )
}


////////////////////////////////////////////////////////////////////////////////
/// Useful

pub fn sleep_secs(secs: f64) {
    thread::sleep(Duration::from_millis( (secs*1000.0) as u64));
}