use ::std::str::from_utf8;
//use ::actix_web::web;
use ::log::*;
pub use ::json::JsonValue;
pub use crate::serror::*;


/// 
pub fn bytes2json (body: &[u8]) -> Result<JsonValue, Serror> {
    let json = json::parse( from_utf8(&body)? )?;
    info!("Json \x1b[1;35m{}\x1b[0m", json);
    Ok(json)
}

fn getin<'t> (mut j :&'t JsonValue, keys :&[&str]) -> &'t JsonValue {
    for k in keys { j = &j[*k] }
    j
}

pub fn getin_i64 (json :&JsonValue, keys :&[&str]) -> Result<i64, String> {
    getin(json, keys).as_i64().ok_or( format!("Unable to parse {:?} as_i64", keys).to_string() )
}
pub fn getin_str <'t> (json :&'t JsonValue, keys :&[&str]) -> Result<&'t str, String> {
    getin(json, keys).as_str().ok_or( format!("Unable to parse {:?} as_str", keys).to_string() )
}