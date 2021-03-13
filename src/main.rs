use std::io::Write;
use log::{Record, Level::{Error, Warn, Info, Debug, Trace}};
use env_logger::fmt::{Formatter, Color};

fn logger_formatter (buf :&mut Formatter, rec :&Record) -> std::io::Result<()> {
    let mut style = buf.style();
    style.set_color(
        match rec.level() {
            Error=>Color::Red, Warn=>Color::Yellow,
            Info=>Color::Green, Debug=>Color::Cyan,
            Trace=>Color::Magenta
        });
    writeln!(buf, "{} {:?}",
        style.value( format!("{} {}:{}", rec.level(), rec.target(), rec.line().unwrap()) ),
        rec.args())
}

fn logger_init () {
    env_logger::builder()
    //.format_timestamp(None).init()
    //.filter_level(log::LevelFilter::max())
    .format(logger_formatter)
    .init();
    //log::error!("error"); log::warn!("warn"); log::info!("info"); log::debug!("debug"); log::trace!("trace"); 
}

#[actix_web::main]
async fn main() {
    logger_init();
    let r = tmbot::mainstart().await;
    log::info!("{:?}", r);
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    #[test]
    fn it_works() {
        assert_eq!(
            tmbot::extract_tickers("a$ b$"),
            ["A","B"].iter().map(|e|e.to_string()).collect::<HashSet<String>>() );
    }
}
