use env_logger::fmt::{Color, Formatter};
use log::{
    info,
    Level::{Debug, Error, Info, Trace, Warn},
    Record
};
use std::io::Write;

fn logger_formatter(buf: &mut Formatter, rec: &Record) -> std::io::Result<()> {
    let mut style = buf.style();
    let pre = style
        .set_color(match rec.level() {
            Error => Color::Red,
            Warn => Color::Yellow,
            Info => Color::Green,
            Debug => Color::Magenta,
            Trace => Color::Cyan,
        })
        .value(format!("{}{}", rec.target(), rec.line().unwrap_or(0)));
    writeln!(buf, "{} {:?}", pre, rec.args())
}

fn main() {
    env_logger::builder().format(logger_formatter).init();
    info!("::main tmbot::main_launch() => {:?}", tmbot::main_launch()); // Should never return
    tmbot::util::sleep_secs(0.1);
    println!()
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
