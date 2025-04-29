pub use actix_web::{
    http::header::{HeaderMap, CONTENT_TYPE, USER_AGENT},
    web, HttpRequest,
};
pub use awc::{Client, ClientBuilder, ClientRequest, ClientResponse, Connector};
pub use log::{error, info, warn};
use openssl::ssl::{SslConnector, SslMethod};
pub use regex::Regex;
pub use serde_json::{json, Value};
pub use std::{
    collections::HashMap, error::Error, str::from_utf8, sync::OnceLock, thread, time::Duration,
};
pub use ansi_colors::*;

////////////////////////////////////////////////////////////////////////////////
// ANSI color constants

pub mod ansi_colors {
#![allow(dead_code)]
    pub const BS :&str = "\x08";

    pub const SAVE :&str = "\x1b7";
    pub const RSTR :&str = "\x1b8";

    pub const RST :&str = "\x1b[0m";
    pub const BLD :&str = "\x1b[1m";
    pub const NRM :&str = "\x1b[22m";

    pub const BLK :&str = "\x1b[30m";
    pub const RED :&str = "\x1b[31m";
    pub const GRN :&str = "\x1b[32m";
    pub const YEL :&str = "\x1b[33m";
    pub const BLU :&str = "\x1b[34m";
    pub const MAG :&str = "\x1b[35m";
    pub const CYN :&str = "\x1b[36m";
    pub const GRY :&str = "\x1b[37m";

    pub const BLD_BLK :&str = "\x1b[1;30m";
    pub const BLD_RED :&str = "\x1b[1;31m";
    pub const BLD_GRN :&str = "\x1b[1;32m";
    pub const BLD_YEL :&str = "\x1b[1;33m";
    pub const BLD_BLU :&str = "\x1b[1;34m";
    pub const BLD_MAG :&str = "\x1b[1;35m";
    pub const BLD_CYN :&str = "\x1b[1;36m";
    pub const BLD_GRY :&str = "\x1b[1;37m";

    pub const B_BLK :&str = "\x1b[40m";
    pub const B_RED :&str = "\x1b[41m";
    pub const B_GRN :&str = "\x1b[42m";
    pub const B_YEL :&str = "\x1b[43m";
    pub const B_BLU :&str = "\x1b[44m";
    pub const B_MAG :&str = "\x1b[45m";
    pub const B_CYN :&str = "\x1b[46m";
    pub const B_GRY :&str = "\x1b[47m";

    pub const B_BLD_BLK :&str = "\x1b[100m";
    pub const B_BLD_RED :&str = "\x1b[101m";
    pub const B_BLD_GRN :&str = "\x1b[102m";
    pub const B_BLD_YEL :&str = "\x1b[103m";
    pub const B_BLD_BLU :&str = "\x1b[104m";
    pub const B_BLD_MAG :&str = "\x1b[105m";
    pub const B_BLD_CYN :&str = "\x1b[106m";
    pub const B_BLD_GRY :&str = "\x1b[107m";
}

////////////////////////////////////////////////////////////////////////////////
// Types

pub type Bresult<T> = Result<T, Box<dyn Error>>;

////////////////////////////////////////////////////////////////////////////////
// Useful

pub fn sleep_secs(secs: f64) {
    thread::sleep(Duration::from_millis( (secs*1000.0) as u64));
}

#[macro_export]
macro_rules! IF {
    ($p:expr, $t:expr, $f:expr) => (if $p { $t } else { $f })
}

////////////////////////////////////////////////////////////////////////////////
// Logging

#[macro_export]
macro_rules! glog {
    ($arg:expr) => {
        match &$arg {
            Ok(o)  => info!("{:?}", o),
            Err(e) => error!("{:?}", e)
        }
    }
}

#[macro_export]
macro_rules! glogd {
    ($pre:expr, $arg:expr) => {
        match &$arg {
            Ok(o)  => info!("{} -> {:?}", format!($pre), o),
            Err(e) => error!("{} -> {:?}", format!($pre), e)
        }
    }
}

#[macro_export]
macro_rules! fmthere {
    ($s:expr) => {
        std::format!("{}{} {}", std::module_path!(), std::line!(), $s)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Unicode

pub fn n2heart2 (n :usize) -> String {
    match n%2 {
        0 => from_utf8(b"\xF0\x9F\x96\xA4"), // black heart
        1 => from_utf8(b"\xF0\x9F\x92\x94"), // red broken heart
        _ => Ok("?")
    }
    .map_err(|e| {error!("{:?}", e); e})
    .unwrap_or("?").to_string()
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

pub fn trimmedQuotes(s: &str) -> &str {
    IF!(2<s.len() && s.ends_with("\"") && s.starts_with("\""),
        &s[1 .. s.len()-1],
        s)
}

////////////////////////////////////////////////////////////////////////////////
// Json

pub fn bytes2json(body: &[u8]) -> Bresult<Value> {
    Ok(serde_json::from_str(from_utf8(&body)?)?)
}

pub fn getin<'a>(v: &'a Value, ptr: &str) -> &'a Value {
    v.pointer(ptr).unwrap_or(&Value::Null)
}

pub fn getin_ary<'a>(json: &'a Value, ptr: &str) -> Result<&'a Vec<Value>, String> {
    getin(json, ptr)
    .as_array()
    .ok_or( format!("getin_ary {}", ptr) )
}

pub fn getin_i64(json: &Value, ptr: &str) -> Result<i64, String> {
    getin(json, ptr)
    .as_i64()
    .ok_or( format!("getin_i64 {}", ptr) )
}

pub fn getin_i64_or(default: i64, json: &Value, ptr :&str) -> i64 {
    getin(json, ptr).as_i64().unwrap_or(default)
}

pub fn getin_f64(json: &Value, ptr: &str) -> Result<f64, String> {
    getin(json, ptr)
    .as_f64()
    .ok_or( format!("getin_f64 {}", ptr) )
}

pub fn getin_str(json: &Value, ptr: &str) -> Result<String, String> {
    getin(json, ptr)
    .as_str()
    .map_or_else(
        || Err(format!("getin_str {}", ptr)),
        |j| Ok(j.to_string()))
}

pub fn getin_string(json: &Value, ptr: &str) -> Result<String, String> {
    json.pointer(ptr)
    .ok_or(format!("getin_string {}", ptr))
    .map(|v|
        v.as_str()
        .map_or(
            v.to_string(),
            |vs| vs.to_string()))
}

////////////////////////////////////////////////////////////////////////////////
// Regex

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
pub fn re_to_vec (re: &Regex, msg: &str) -> Bresult<Vec<Option<String>>> {
    Ok(re
        .captures(msg)  // Option<Captures>
        .map(|caps|
            caps.iter() // Iter::Option<match>
                .map(|o_match|
                    o_match.map(|mtch| mtch.as_str().into()))
                .collect()) // Option<Vec<Option<String>>r
        .unwrap_or(Vec::new()))
}

// Return vector of the regex capture groups, Err("") if no match
pub fn must_re_to_vec(re: &Regex, msg: &str) -> Bresult<Vec<Option<String>>> {
    Ok(re
        .captures(msg)  // Option<Captures>
        .ok_or("")?     // Result<Captures>?
        .iter()         // Iter<Option<Match>>
        .map(|om|       // Option<Match>
            om.map(|m|  // Match
                m.as_str().into())) // Iter<Option<String>>
        .collect::<Vec<Option<String>>>()) // Vec<Option<String>>
}

// Regex captured strings impls
pub trait ReAs {
    fn as_i64 (&self, i:usize) -> Bresult<i64>;
    fn as_f64 (&self, i:usize) -> Bresult<f64>;
    fn as_str (&self, i:usize) -> Bresult<&str>;
    fn as_str_or (&self, s: &'static str, i:usize) -> &str;
    fn as_string (&self, i:usize) -> Bresult<String>;
    fn as_string_or (&self, s: &str, i:usize) -> String;
}

impl ReAs for Vec<Option<String>> {
    fn as_i64(&self, i: usize) -> Bresult<i64> {
        Ok(self.get(i).ok_or("Can't index vector")?
           .as_ref().ok_or("Can't parse i64 from None")?
           .parse::<i64>()?)
    }
    fn as_f64(&self, i: usize) -> Bresult<f64> {
        Ok(self.get(i).ok_or("Can't index vector")?
           .as_ref().ok_or("Can't parse f64 from None")?
           .parse::<f64>()?)
    }
    fn as_str(&self, i: usize) -> Bresult<&str> {
        Ok(self.get(i)
            .ok_or("can't index vector")?.as_ref()
            .ok_or("can't infer str from None")? )
    }
    fn as_str_or(&self, s: &'static str, i: usize) -> &str {
        self.as_str(i).unwrap_or(s)
    }
    fn as_string(&self, i: usize) -> Bresult<String> {
        self.as_str(i).map( String::from )
    }
    fn as_string_or(&self, s: &str, i: usize) -> String {
        self.as_str(i).unwrap_or(s).to_string()
    }
}

#[macro_export]
macro_rules! regex {
    ($re:expr) => {{
        static RE: OnceLock<Regex> = OnceLock::new();
        RE.get_or_init(|| Regex::new($re).unwrap())
    }}
}


////////////////////////////////////////////////////////////////////////////////
// Http

// ClientBuilder -> Client -> SendClientRequest -> ClientResponse
pub fn newHttpsClient() -> Bresult<Client> {
    Ok(ClientBuilder::new()
        .connector(Connector::new()
            .openssl(SslConnector::builder(SslMethod::tls())?.build())
            .timeout(std::time::Duration::new(60, 0)))
        .add_default_header((USER_AGENT, "TMBot"))
        .timeout(std::time::Duration::new(60, 0))
        .finish())
}

#[macro_export]
macro_rules! httpResponseOk {
    () => {{
        let r = HttpResponse::Ok().finish();
        info!("{BLD_MAG}{}{}",
            r.status(),
            headersPretty(r.headers()));
        r
    }};
}

#[macro_export]
macro_rules! httpResponseNotFound {
    () => {{
        let r = HttpResponse::NotFound().finish();
        error!("{BLD_MAG}{}{}\n",
            r.status(),
            headersPretty(r.headers()));
        r
    }};
}

////////////////////////////////////////////////////////////////////////////////
// Logging

pub fn jsonPretty(json: &str) -> String {
    json.replace("{\"", &format!("{{\"{BLD}"))
        .replace("\n\"", &format!("\n\"{BLD}")) // Telegram inserts a newline for some reason.
        .replace(",\"", &format!(",\"{BLD}"))
        .replace("\":", &format!("{NRM}\":"))
        .replace("\n", &format!(" {SAVE}{BS}{B_YEL} {RSTR}"))
}
pub fn urlPretty(urlstr: &str) -> String {
    urlstr
        .replace("?", &format!("?{BLD}"))
        .replace("&", &format!("&{BLD}"))
        .replace("=", &format!("{NRM}="))
}

pub fn headersPretty(hm: &HeaderMap) -> String {
    hm.iter()
    .map(|(k,v)| format!("{RST} {k} {BLD_BLK}{}", v.to_str().unwrap_or("?")))
    .collect::<Vec<String>>()
    .join("") + RST
}

pub fn httpReqPretty(req: &HttpRequest, body: &web::Bytes) -> String {
    format!("{BLD_MAG}{} {:?} {RST}{YEL}{B_BLD_BLK}{}{}",
        req.peer_addr()
            .map(|sa| sa.ip().to_string())
            .as_deref()
            .unwrap_or("?"),
        req.uri(),
        from_utf8(body)
            .map(jsonPretty)
            .unwrap_or_else(|_| format!("{:?}", body)),
        headersPretty(req.headers())
    )
}

pub fn reqPretty(req: &ClientRequest, text: &str) -> String {
    format!("<= {BLU}{:?} {} {} {YEL}{B_BLD_BLK}{}{}",
        req.get_version(),
        req.get_method(),
        urlPretty(&req.get_uri().to_string()),
        jsonPretty(text),
        headersPretty(&req.headers()))
}

pub fn resPretty<T> (res: &ClientResponse<T>, body: &str) -> String {
    format!("=> {BLU}{:?} {} {YEL}{B_BLD_BLK}{}{}",
        res.version(),
        res.status(),
        jsonPretty(body),
        headersPretty(&res.headers()))
}
