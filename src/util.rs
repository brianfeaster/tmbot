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

//info!("HEARTS {}", &(-6..=14).map( num2heart ).collect::<Vec<&str>>().join(""));
pub fn n2heart2 (n :usize) -> String {
    match n%2 {
        0 => from_utf8(b"\xF0\x9F\x96\xA4"), // black heart
        1 => from_utf8(b"\xF0\x9F\x92\x94"), // red broken heart
        _ => Ok("?")
    }.unwrap().to_string()
}

pub fn n2heart13 (n :usize) -> String {
    match n%13 {
        0 => from_utf8(b"\xF0\x9F\x96\xA4"), // black heart
        1 => from_utf8(b"\xE2\x9D\xA4\xEF\xB8\x8F"), // red heart
        2 => from_utf8(b"\xF0\x9F\xA7\xA1"), // orange heart
        3 => from_utf8(b"\xF0\x9F\x92\x9B"), // yellow heart
        4 => from_utf8(b"\xF0\x9F\x92\x9A"), // green heart
        5 => from_utf8(b"\xF0\x9F\x92\x99"), // blue heart
        6 => from_utf8(b"\xF0\x9F\x92\x9C"), // violet heart
        7 => from_utf8(b"\xF0\x9F\x92\x93"), // beating heart
        8 => from_utf8(b"\xF0\x9F\x92\x97"), // pink growing heart
        9 => from_utf8(b"\xF0\x9F\x92\x9E"), // revolving hearts
        10 => from_utf8(b"\xF0\x9F\x92\x98"), // heart with arrow
        11 => from_utf8(b"\xF0\x9F\x92\x96"), // pink sparkling heart
        12 => from_utf8(b"\xF0\x9F\x92\x9D"), // heart with ribbon
        _ => Ok("?")
    }.unwrap().to_string()
}

pub fn num2heart (mut n :i64) -> String {
    let mut sheart = String::new();
    if n < 0 {
        let mut n = -n as usize;
        loop {
            sheart.insert_str(0, &n2heart2(n));
            n /= 2;
            if n < 1 { return sheart }
        }
    }

    loop {
        sheart.insert_str(0, &n2heart13(n as usize));
        n /= 13;
        if n < 1 { break }
    }
    sheart
}

////////////////////////////////////////////////////////////////////////////////
/// JSON

pub fn bytes2json (body: &[u8]) -> Result<Value, Box<dyn Error>> {
    Ok( serde_json::from_str( from_utf8(&body)? )? )
}

////////////////////////////////////////

pub fn getin<'a> (mut j :&'a Value, keys :&[&str]) -> &'a Value {
    for k in keys { j = &j[*k] }
    j
}

pub fn getin_i64 (json :&Value, keys :&[&str]) -> Result<i64, String> {
    getin(json, keys)
    .as_i64()
    .ok_or( format!("Unable to parse {:?} as_i64", keys) )
}

pub fn getin_i64_or (default:i64, json :&Value, keys :&[&str]) -> i64 {
    getin(json, keys).as_i64().unwrap_or(default)
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

#[macro_export(local_inner_macros)]
macro_rules! IF {
    ($predicate:expr, $trueblock:expr, $falseblock:expr) => (
        if $predicate { $trueblock } else { $falseblock }
    )
}