#[derive(Debug)]

pub enum Serror {
   Message(String),
   Msg(&'static str),
   ParseIntError(std::num::ParseIntError),
   StdIoError(std::io::Error),
   Utf8Error(std::str::Utf8Error),
   //JsonError(json::Error),
   JsonError(serde_json::Error),
   SslErrorStack(openssl::error::ErrorStack),
   SetLoggerError(log::SetLoggerError),
   SerUrlEnc(serde_urlencoded::ser::Error),
   SendRequestError(actix_web::client::SendRequestError),
   PayloadError(actix_web::error::PayloadError),
   SqliteError(sqlite::Error),
   SysTimeError(std::time::SystemTimeError),
   ParseFloatErr(std::num::ParseFloatError),
   RegexError(regex::Error),

}

impl From<String> for Serror {
    fn from(s: String) -> Self { Serror::Message(s) }
}
impl From<&'static str> for Serror {
    fn from(s: &'static str) -> Self { Serror::Msg(s) }
}
impl From<std::num::ParseIntError> for Serror {
    fn from(e: std::num::ParseIntError) -> Self { Serror::ParseIntError(e) }
}
impl From<std::io::Error> for Serror {
    fn from(e: std::io::Error) -> Self { Serror::StdIoError(e) }
}
impl From<std::str::Utf8Error> for Serror {
    fn from(e: std::str::Utf8Error) -> Self { Serror::Utf8Error(e) }
}
impl From<serde_json::Error> for Serror {
    fn from(e: serde_json::Error) -> Self { Serror::JsonError(e) }
}
impl From<openssl::error::ErrorStack> for Serror {
    fn from(e: openssl::error::ErrorStack) -> Self { Serror::SslErrorStack(e) }
}
impl From<log::SetLoggerError> for Serror {
    fn from(e: log::SetLoggerError) -> Self { Serror::SetLoggerError(e) }
}
impl From<serde_urlencoded::ser::Error> for Serror {
    fn from(e: serde_urlencoded::ser::Error) -> Self { Serror::SerUrlEnc(e) }
}
impl From<actix_web::client::SendRequestError> for Serror {
    fn from(e: actix_web::client::SendRequestError) -> Self { Serror::SendRequestError(e) }
}
impl From<actix_web::error::PayloadError> for Serror {
    fn from(e: actix_web::error::PayloadError) -> Self { Serror::PayloadError(e) }
}
impl From<sqlite::Error> for Serror {
    fn from(e: sqlite::Error) -> Self { Serror::SqliteError(e) }
}
impl From<std::time::SystemTimeError> for Serror {
    fn from(e: std::time::SystemTimeError) -> Self { Serror::SysTimeError(e) }
}
impl From<std::num::ParseFloatError> for Serror {
    fn from(e: std::num::ParseFloatError) -> Self { Serror::ParseFloatErr(e) }
}
impl From<regex::Error> for Serror {
    fn from(e: regex::Error) -> Self { Serror::RegexError(e) }
}

/*
impl Serror {
    pub fn str(&self) -> String {
        match self {
            Serror::SqliteError(e) => format!("A {:?}", e),
            _ => format!("B {:?}", self)
        }
    }
}
*/