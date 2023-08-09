use env_logger::fmt::{Color, Formatter};
use log::{
    warn,
    Level::{Debug, Error, Info, Trace, Warn},
    Record
};
use std::io::{self, Write};

fn logger_formatter(buf: &mut Formatter, rec: &Record) -> io::Result<()> {
    let mut style = buf.style();
    style.set_color(
            match rec.level() {
                Error => Color::Red,
                Warn  => Color::Yellow,
                Info  => Color::Green,
                Debug => Color::Magenta,
                Trace => Color::Cyan
            });
    let pre = style.value(format!("{}{}", rec.target(), rec.line().unwrap_or(0)));
    writeln!(buf, "{} {:?}", pre, rec.args())
}

fn logger_init() {
    env_logger::builder().format(logger_formatter).init();
}

fn main() {
    logger_init();
    warn!("main tmbot::main_launch() => {:?}", ::tmbot::main_launch()); // Should never return
    ::tmbot::util::sleep_secs(0.1);
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
