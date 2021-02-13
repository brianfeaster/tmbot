#[derive(Debug)]
pub enum Serror {
   StdIoError(std::io::Error),
   JsonError(json::Error),
   Utf8Error(std::str::Utf8Error),
   SetLoggerError(log::SetLoggerError),
   SslErrorStack(openssl::error::ErrorStack),
   Message(String),
   ParseIntError(std::num::ParseIntError),
   Msg(&'static str),
   Err(&'static str),
   SerUrlEnc(serde_urlencoded::ser::Error),
   SendRequestError(actix_web::client::SendRequestError)
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
impl From<json::Error> for Serror {
    fn from(e: json::Error) -> Self { Serror::JsonError(e) }
}
impl From<&'static str> for Serror {
    fn from(s: &'static str) -> Self { Serror::Msg(s) }
}
impl From<String> for Serror {
    fn from(s: String) -> Self { Serror::Message(s) }
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