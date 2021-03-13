use ::std::str::from_utf8;
pub use ::serde_json::{self as sj, Value};
pub use crate::serror::*;


/// 
pub fn bytes2json (body: &[u8]) -> Result<Value, Serror> {
    let json = sj::from_str( from_utf8(&body)? )?;
    Ok(json)
}

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