use env_logger::fmt::{Color, Formatter};
use log::{error, Level, Record};
use std::io::Write;
use ::tmbot::main_launch;

fn logger_formatter(fmt: &mut Formatter, rec: &Record) -> std::io::Result<()> {
    let mut style = fmt.style();
    let pre = style
        .set_color(match rec.level() {
            Level::Error => Color::Ansi256(9),
            Level::Warn  => Color::Ansi256(11),
            Level::Info  => Color::Green,
            Level::Debug => Color::Magenta,
            Level::Trace => Color::Cyan,
        })
        .value(format!("{}{}", rec.target(), rec.line().unwrap_or(0)));
    writeln!(fmt, "{} {:?}", pre, rec.args())
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
