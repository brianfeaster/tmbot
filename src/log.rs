use log::*;

/*
use ::serde::{Serialize, Deserialize};
use ::serde_json::{Value, from_str, to_string_pretty};
const  OFF :&str = "\x1b[0m";
const  MAG :&str = "\x1b[36m";
const  GRN :&str = "\x1b[32m";
const BGRN :&str = "\x1b[1;32m";
*/
pub fn ginfo<T: std::fmt::Debug>(e: T) { info!("{:?}", e); }
pub fn gerror<T: std::fmt::Debug>(e: T) { error!("{:?}", e); }

pub fn ginfod<T: std::fmt::Debug>(h:&str, e: T) { info!("{} {}", h, format!("{:?}", e).replace("\n","").replace("\\\\", "\\").replace("\\\"", "\"")); }
pub fn gerrord<T: std::fmt::Debug>(h:&str, e: T) { error!("{} {}", h, format!("{:?}", e).replace("\n","").replace("\\\\", "\\").replace("\\\"", "\"")); }

pub fn glogd<
    R: std::fmt::Debug,
    T: std::fmt::Debug
> (
    h:&str,
    e: Result<R, T>
) {
    match e {
        Ok(r) => ginfod(h, r),
       Err(r) => gerrord(h, r)
    }
}
