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
    if true {
        logger_init();
        let r = tmbot::mainstart().await;
        ::tmbot::glogd!("hmm", r);
    } else {
        checkfloats();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Float rounding testng

use std::mem::transmute;

fn round(f: f64) -> f64 {
    //(unsafe { transmute::<u64, f64>(transmute::<f64, u64>(f) + 1) } * 100.0).round() / 100.0
    unsafe { transmute::<u64, f64>(transmute::<f64, u64>(f) + 0) }
}

fn checkfloats() {
    let mut errors = 0;
    let mut i = 000000_000i64;
    while i <=  999999_999i64 && errors < 10 {
        let n = i / 1000i64;
        let d = i % 1000i64;

        // Fixed point string
        let s = format!("{}.{:03}", n, d);
        // Fixed point rounded string
        let sr = {
            let mut n = n;
            let mut d = if d % 10 < 5 { d / 10 } else { d / 10 + 1 };
            if 100 <= d {
                n += 1;
                d -= 100;
            }
            format!("{}.{:02}", n, d)
        };

        // Float
        let f: f64 = s.parse::<f64>().map_err( |e| { println!("{:?} {:?}", e, s); e }).unwrap();
        // Float rounded string
        let fr = format!("{:.2}", round(f));

        if sr != fr {
            errors += 1;
            println!("{} '{}'/'{}'  {}/'{}'  {:.60}", i, s, sr, f, fr, f);
        }

        i += 1;
    }
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
