use env_logger::fmt::{Formatter, style::{Color, AnsiColor::*, Ansi256Color}};

use log::{error, Level, Record};
use std::io::Write;
use ::tmbot::main_launch;

fn logger_formatter(fmt: &mut Formatter, rec: &Record) -> std::io::Result<()> {
    let level = rec.level();
    let style = fmt
        .default_level_style(level)
        .fg_color(match level {
            Level::Error => Some(Color::Ansi256(Ansi256Color(9))),
            Level::Warn  => Some(Color::Ansi256(Ansi256Color(11))),
            Level::Info  => Some(Color::Ansi(Green)),
            Level::Debug => Some(Color::Ansi(Magenta)),
            Level::Trace => Some(Color::Ansi(Cyan)),
        });
    writeln!(fmt, "{style}{}{} {:?}", /*rec.level()*/ rec.target(), rec.line().unwrap_or(0), rec.args())
}

fn main() {
    env_logger::builder().format(logger_formatter).init();
    main_launch()
        .map(|r| println!("{}", r))
        .map_err(|e| error!("{}", e))
        .ok();
    //tmbot::sleep_secs(1.0);
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
