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
macro_rules! glogd {
    ($pre:expr, $arg:expr) => ( {
        let r=$arg;
        match &r {
            Ok(r) => info!("{} {:?}", $pre, r),
            Err(r) => error!("{} {:?}", $pre, r)
        }
        //r
    } )
}