pub use log::{error, info, warn};
pub use regex::Regex;
pub use serde_json::{json, Value};
pub use std::{collections::HashMap, error::Error, str::from_utf8, thread, time::Duration};

////////////////////////////////////////////////////////////////////////////////
/// Types

pub type Bresult<T> = Result<T, Box<dyn Error>>;

////////////////////////////////////////////////////////////////////////////////
/// General Logging

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
            Ok(o)  => ::log::info!("{} => {:?}", $pre, o),
            Err(e) => ::log::error!("{} => {:?}", $pre, e)
        }
    }
}

#[macro_export(local_inner_macros)]
macro_rules! fmthere {
    ($s:expr) => {
        std::format!("{}{} {}", std::module_path!(), std::line!(), $s)
    }
}

////////////////////////////////////////////////////////////////////////////////
/// UTF-8

fn n2heart2 (n :usize) -> String {
    match n%2 {
        0 => from_utf8(b"\xF0\x9F\x96\xA4"), // black heart
        1 => from_utf8(b"\xF0\x9F\x92\x94"), // red broken heart
        _ => Ok("?")
    }
    .map_err(|e| {error!("{:?}", e); e})
    .unwrap_or("?").to_string()
}

fn n2heart13 (n :usize) -> String {
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
    }
    .map_err(|e| {error!("{:?}", e); e})
    .unwrap_or("?").to_string()
}

//info!("HEARTS {}", &(-6..=14).map( num2heart ).collect::<Vec<&str>>().join(""));
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
// JSON

pub fn bytes2json(body: &[u8]) -> Bresult<Value> {
    Ok(serde_json::from_str(from_utf8(&body)?)?)
}

pub fn getin<'a>(v: &'a Value, ptr: &str) -> &'a Value {
    match v.pointer(ptr) {
        Some(v) => &v,
        None => &Value::Null
    }
}

pub fn getin_i64(json: &Value, ptr: &str) -> Result<i64, String> {
    getin(json, ptr)
    .as_i64()
    .ok_or( format!("Unable to parse {:?} as_i64", ptr) )
}

pub fn getin_i64_or(default: i64, json: &Value, ptr :&str) -> i64 {
    getin(json, ptr).as_i64().unwrap_or(default)
}

pub fn getin_f64(json: &Value, ptr: &str) -> Result<f64, String> {
    getin(json, ptr)
    .as_f64()
    .ok_or( format!("Unable to parse {:?} as_f64", ptr) )
}

pub fn getin_str(json: &Value, ptr: &str) -> Result<String, String> {
    getin(json, ptr)
    .as_str()
    .map_or(
        Err(format!("Unable to parse {:?} as_str", ptr)),
        |j| Ok(j.to_string()) )
}

pub fn getin_string(json: &Value, ptr: &str) -> Result<String, String> {
    json.pointer(ptr)
    .ok_or(format!("json: bad path {}", ptr))
    .map(|v|
        v.as_str()
        .map_or(
            v.to_string(),
            |v| v.to_string()))
}

////////////////////////////////////////////////////////////////////////////////
// Regex enhancements

// Return hashmap of the regex capture groups, if any.
pub fn regex_to_hashmap (re: &str, msg: &str) -> Bresult<HashMap<String, String>> {
    let regex = Regex::new(re)?;
    let captures = regex.captures(msg).ok_or("no regex captures found")?;
    Ok(regex
        .capture_names()
        .enumerate()
        .filter_map(|(i, capname)| { // Over capture group names (or indices)
            capname.map_or(
                captures
                    .get(i) // Get match via index.  This could be null which is filter by filter_map
                    .map(|capmatch| (i.to_string(), capmatch.as_str().into())),
                |capname| {
                    captures
                        .name(capname) // Get match via capture name.  Could be null which is filter by filter_map
                        .map(|capmatch| (capname.into(), capmatch.as_str().into()))
                },
            )
        })
        .collect())
}

// Return vector of the regex capture groups, if any.
pub fn regex_to_vec (re: &str, msg: &str) -> Bresult<Vec<Option<String>>> {
    Ok(Regex::new(re)?  // Result<Regex>
        .captures(msg)  // Option<Captures>
        .map(|caps|
            caps.iter() // Iter::Option<match>
                .map(|o_match|
                    o_match.map(|mtch| mtch.as_str().into()))
                .collect()) // Option<Vec<Option<String>>r
        .unwrap_or(Vec::new()))
}

// Return vector of the regex capture groups, Err("") if no match
pub fn must_regex_to_vec(re: &str, msg: &str) -> Bresult<Vec<Option<String>>> {
    Ok(Regex::new(re)?  // Result<Regex>?
        .captures(msg)  // Option<Captures>
        .ok_or("")?     // Result<Captures>?
        .iter()         // Iter<Option<Match>>
        .map(|om|       // Option<Match>
            om.map(|m|  // Match
                m.as_str().into())) // Iter<Option<String>>
        .collect::<Vec<Option<String>>>()) // Vec<Option<String>>
}

/// Regex captured strings 'as' impls
pub trait ReAs {
    fn as_i64 (&self, i:usize) -> Bresult<i64>;
    fn as_f64 (&self, i:usize) -> Bresult<f64>;
    fn as_str (&self, i:usize) -> Bresult<&str>;
    fn as_string (&self, i:usize) -> Bresult<String>;
}

impl ReAs for Vec<Option<String>> {
    fn as_i64 (&self, i:usize) -> Bresult<i64> {
        Ok(self.get(i).ok_or("Can't index vector")?
           .as_ref().ok_or("Can't parse i64 from None")?
           .parse::<i64>()?)
    }
    fn as_f64 (&self, i:usize) -> Bresult<f64> {
        Ok(self.get(i).ok_or("Can't index vector")?
           .as_ref().ok_or("Can't parse f64 from None")?
           .parse::<f64>()?)
    }
    fn as_str (&self, i:usize) -> Bresult<&str> {
        Ok(self.get(i)
            .ok_or("can't index vector")?.as_ref()
            .ok_or("can't infer str from None")? )
    }
    fn as_string (&self, i:usize) -> Bresult<String> {
        self.as_str(i).map( String::from )
    }
}


////////////////////////////////////////////////////////////////////////////////
/// Useful

pub fn sleep_secs(secs: f64) {
    thread::sleep(Duration::from_millis( (secs*1000.0) as u64));
}

#[macro_export(local_inner_macros)]
macro_rules! IF {
    ($p:expr, $t:expr, $f:expr) => (if $p { $t } else { $f })
}
