use std::io::Write;
use log::{error, Record, Level::{Error, Warn, Info, Debug, Trace}};
use env_logger::fmt::{Formatter, Color};

/// Logger output serializer.
fn _logger_formatter (buf :&mut Formatter, rec :&Record) -> std::io::Result<()> {
    let mut style = buf.style();
    style.set_color(
            match rec.level() {
                Error => Color::Red,
                Warn  => Color::Yellow,
                Info  => Color::Green,
                Debug => Color::Cyan,
                Trace => Color::Magenta
            });
    let pre = style.value(format!("{} {}:{}", rec.level(), rec.target(), rec.line().unwrap()));
    writeln!(buf, "{} {:?}", pre, rec.args())
}

/// Initialize logger
fn logger_init () {
    env_logger::builder()
    //.format_timestamp(None).init()
    //.filter_level(log::LevelFilter::max())
    .format(_logger_formatter)
    .init();
    //log::error!("error"); log::warn!("warn"); log::info!("info"); log::debug!("debug"); log::trace!("trace"); 
}

//#[actix_web::main] async fn main() { ::tmbot::launch().await; }

fn main () {
    logger_init();
    error!("main => {:?}", ::tmbot::main_launch()); // Should never return
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use regex::Regex;
    use std::collections::HashSet;
    #[test]
    fn it_works() {
        assert_eq!(
            tmbot::doquotes_scan_tickers(
                "a$ b$",
                Regex::new(r"^[@^]?[A-Z_a-z][-.0-9=A-Z_a-z]*$").unwrap()
            ).unwrap(),
            ["A","B"].iter().map(|e|e.to_string()).collect::<HashSet<String>>() );
    }
}