use log::*;

/*
use ::serde::{Serialize, Deserialize};
use ::serde_json::{Value, from_str, to_string_pretty};
const  OFF :&str = "\x1b[0m";
const  MAG :&str = "\x1b[36m";
const  GRN :&str = "\x1b[32m";
const BGRN :&str = "\x1b[1;32m";
*/
pub fn ginfo<T:  std::fmt::Debug>(e: T) -> bool { info!("{:?}",  e); true }
pub fn gwarn<T:  std::fmt::Debug>(e: T) -> bool { warn!("{:?}",  e); true }
pub fn gerror<T: std::fmt::Debug>(e: T) -> bool { error!("{:?}", e); true }

pub fn ginfod<T:  std::fmt::Debug>(h:&str, e: T) { info!("{} {}",  h, format!("{:?}", e).replace("\n","").replace("\\\\", "\\").replace("\\\"", "\"")); }
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
