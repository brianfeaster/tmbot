//! # External Chat Service Robot
mod util;  pub use crate::util::*;
mod comm;  use crate::comm::*;
mod srvs;  use crate::srvs::*;
mod db;
use ::std::{env,
    error::Error,
    mem::transmute,
    collections::{HashMap, HashSet},
    str::{from_utf8},
    fs::{read_to_string, write},
    sync::{Arc, Mutex} };
use ::log::*;
use ::regex::{Regex};
use ::datetime::{Instant, Duration, LocalDate, LocalTime, LocalDateTime, DatePiece,
    Weekday::{Sunday, Saturday} };
use ::openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use ::actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route,
    client::{Client, Connector} };

////////////////////////////////////////////////////////////////////////////////

const QUOTE_DELAY_MINUTES :i64 = 1;

////////////////////////////////////////////////////////////////////////////////
/// Rust primitives enahancements

trait AsI64 { fn as_i64 (&self, i:usize) -> Bresult<i64>; }
impl AsI64 for Vec<Option<String>> {
    fn as_i64 (&self, i:usize) -> Bresult<i64> {
        Ok(self.get(i).ok_or("Can't index vector")?
           .as_ref().ok_or("Can't parse i64 from None")?
           .parse::<i64>()?)
    }
}

trait AsF64 { fn as_f64 (&self, i:usize) -> Bresult<f64>; }
impl AsF64 for Vec<Option<String>> {
    fn as_f64 (&self, i:usize) -> Bresult<f64> {
        Ok(self.get(i).ok_or("Can't index vector")?
           .as_ref().ok_or("Can't parse f64 from None")?
           .parse::<f64>()?)
    }
}

trait AsStr { fn as_istr (&self, i:usize) -> Bresult<&str>; }
impl AsStr for Vec<Option<String>> {
    fn as_istr (&self, i:usize) -> Bresult<&str> {
        Ok(self.get(i)
            .ok_or("can't index vector")?.as_ref()
            .ok_or("can't infer str from None")? )
    }
}

trait AsString { fn as_string (&self, i:usize) -> Bresult<String>; }
impl AsString for Vec<Option<String>> {
    fn as_string (&self, i:usize) -> Bresult<String> {
        Ok(self.get(i)
            .ok_or("can't index vector")?.as_ref()
            .ok_or("can't infer str from None")?
            .to_string() )
    }
}

trait GetI64 { fn get_i64 (&self, k:&str) -> Bresult<i64>; }
impl GetI64 for HashMap<String, String> {
    fn get_i64 (&self, k:&str) -> Bresult<i64> {
        Ok(self
            .get(k).ok_or(format!("Can't find key '{}'", k))?
            .parse::<i64>()?)
    }
}
trait GetF64 { fn get_f64 (&self, k:&str) -> Bresult<f64>; }
impl GetF64 for HashMap<String, String> {
    fn get_f64 (&self, k:&str) -> Bresult<f64> {
        Ok(self
            .get(k).ok_or(format!("Can't find key '{}'", k))?
            .parse::<f64>()?)
    }
}

trait GetStr { fn get_str (&self, k:&str) -> Bresult<String>; }
impl GetStr for HashMap<String, String> {
    fn get_str (&self, k:&str) -> Bresult<String> {
        Ok(self
            .get(k)
            .ok_or(format!("Can't find key '{}'", k))?
            .to_string() )
    }
}

trait GetChar { fn get_char (&self, k:&str) -> Bresult<char>; }
impl GetChar for HashMap<String, String> {
    fn get_char (&self, k:&str) -> Bresult<char> {
        Ok(self
            .get(k)
            .ok_or(format!("Can't find key '{}'", k))?
            .to_string()
            .chars()
            .nth(0)
            .ok_or(format!("string lacks character key '{}'", k))? )

    }
}

/*
// Trying to implement without using ? for the learns.
impl AsStr for Vec<Option<String>> {
    fn as_istr (&self, i:usize) -> Bresult<&str> {
        self.get(i) // Option< Option<String> >
        .ok_or("can't index vector".into()) // Result< Option<String>>, Bresult<> >
        .map_or_else(
            |e| e, // the previous error
            |o| o.ok_or("can't infer str from None".into())
                .map( |r| &r.to_string()) ) // Result< String, Bresult >
    }
}
*/

////////////////////////////////////////////////////////////////////////////////
/// Abstracted primitive types helpers

/// Decide if a ticker's price should be refreshed given its last lookup time.
///    Market   PreMarket   Regular  AfterHours       Closed
///       PST   0900Zpre..  1430Z..  2100Z..0100Zaft  0100Z..9000Z
///       PDT   0800Z..     1330Z..  2000Z..2400Z     0000Z..0800Z
///  Duration   5.5h        6.5h     4h               8h
fn most_recent_trading_hours (
    env: &Env,
    now: LocalDateTime,
) -> Bresult<(LocalDateTime, LocalDateTime)> {
    // Absolute time (UTC) when US pre-markets open 1AMPT / 0900Z|0800Z
    let start_time = LocalTime::hm(9-env.dst_hours_adjust, 0)?; 
    let start_duration = Duration::of(start_time.to_seconds());
    let market_hours_duration = Duration::of( LocalTime::hm(16, 30)?.to_seconds() ); // Add 30 minutes for delayed orders

    // Consider the current trading date relative to now. Since pre-markets
    // start at 0900Z (0800Z during daylight savings) treat that as the
    // start of the trading day so subtract that many hours (shift to midnight)
    // to shift the yesterday/today cutoff.
    let now_norm_date :LocalDate = (now - start_duration).date();

    // How many days ago (in seconds) were the markets open?
    let since_last_market_duration =
        Duration::of( LocalTime::hm(24, 0)?.to_seconds() )
        * match now_norm_date.weekday() { Sunday=>2, Saturday=>1, _=>0 };

    // Absolute time the markets opened/closed last. Could be future time.
    let time_open = LocalDateTime::new(now_norm_date, start_time) - since_last_market_duration;
    let time_close = time_open + market_hours_duration; // Add extra 30m for delayed market orders.

    info!("most_recent_trading_hours  now:{:?}  now_norm_date:{:?}  since_last_market_duration:{:?}  time_open:{:?}  time_close:{:?}",
        now, now_norm_date, since_last_market_duration, time_open, time_close);

    Ok((time_open, time_close))
}

fn trading_hours_p (env:&Env, now:i64) -> Bresult<bool>{
    let now = LocalDateTime::at(now);
    let (time_open, time_close) = most_recent_trading_hours(env, now)?;
    Ok(time_open <= now && now <= time_close)
}

fn update_ticker_p (env:&Env, cached:i64, now:i64, traded_all_day:bool) -> Bresult<bool> {
    info!("update_ticker_p  cached:{}  now:{}  traded_all_day:{}", cached, now, traded_all_day);

    if traded_all_day { return Ok(cached+(env.quote_delay_minutes*60) < now) }

    let lookup_throttle_duration = Duration::of( 60 * env.quote_delay_minutes ); // Lookup tickers every 2 minutes

    // Do everything in DateTime
    let cached = LocalDateTime::at(cached);
    let now = LocalDateTime::at(now);

    let (time_open, time_close) = most_recent_trading_hours(env, now)?;

    info!("update_ticker_p  cached:{:?}  now:{:?}  time_open:{:?}  time_close:{:?}",
        cached, now, time_open, time_close);

    Ok(cached <= time_close
        && time_open <= now
        && (cached + lookup_throttle_duration < now  ||  time_close <= now))
}

/// Round a float at the specified decimal offset
///  println!("{:?}", num::Float::integer_decode(n) );
fn roundfloat (num:f64, dec:i32) -> f64 {
    let fac = 10f64.powi(dec);
    let num_incremented = unsafe { transmute::<u64, f64>(transmute::<f64, u64>(num) + 1) };
    (num_incremented * fac).round() / fac
}

// Round for numbers that were serialized funny: -0.00999999 => -0.01, -1.3400116 => -1.34
fn roundqty (num:f64) -> f64 { roundfloat(num, 4) }
fn roundcents (num:f64) -> f64 { roundfloat(num, 2) }

fn round (f:f64) -> String {
    if 0.0 == f {
        format!(".00")
    } else if f < 1.0 {
        format!("{:.4}", roundqty(f))
            .trim_start_matches('0').to_string()
    } else {
        format!("{:.2}", roundcents(f))
    }
}
// number to -> .1234 1.12 999.99 1.99k
fn roundkilofy (f:f64) -> String {
    if 0.0 == f {
        format!(".00")
    } else if 1000.0 <= f {
        format!("{:.2}k", roundcents(f/1000.0))
    } else if f < 1.0 {
        format!("{:.4}", roundqty(f))
            .trim_start_matches('0').to_string()
    } else {
        format!("{:.2}", roundcents(f))
            .trim_start_matches('0').to_string()
    }
}

// Trim trailing . and 0
fn num_simp (num:&str) -> String {
    num
    .trim_start_matches("0")
    .trim_end_matches("0")
    .trim_end_matches(".")
    .into()
}

// Try to squish number to 3 digits: "100%"  "99.9%"  "10.0%"  "9.99%"  "0.99%""
fn percent_squish (num:f64) -> String {
    if 100.0 <= num{ return format!("{:.0}", num) }
    if 10.0 <= num { return format!("{:.1}", num) }
    if 1.0 <= num  { return format!("{:.2}", num) }
    if num < 0.001 { return format!("{:.1}", num) }
    format!("{:.3}", num).trim_start_matches('0').into() // .0001 ... .9999
}

// Stringify float to two decimal places unless under 10
// where it's 4 decimal places with trailing 0s truncate.
fn money_pretty (n:f64) -> String {
    if n < 10.0 {
        let mut np = format!("{:.4}", n);
        np = regex_to_hashmap(r"^(.*)0$", &np).map_or(np, |c| c["1"].to_string() );
        regex_to_hashmap(r"^(.*)0$", &np).map_or(np, |c| c["1"].to_string() )
    } else {
        format!("{:.2}", n)
    }
}

fn percentify (a: f64, b: f64) -> f64 {
    (b-a)/a*100.0
}

// (5,"a","bb") => "a..bb"
fn _pad_between(width: usize, a:&str, b:&str) -> String {
    let lenb = b.len();
    format!("{: <pad$}{}", a, b, pad=width-lenb)
}

/// Return hashmap of the regex capture groups, if any.
fn regex_to_hashmap (re: &str, msg: &str) -> Option<HashMap<String, String>> {
    let regex = Regex::new(re).unwrap();
    regex.captures(msg).map(|captures| { // Consider capture groups or return None
        regex
            .capture_names()
            .enumerate()
            .filter_map(|(i, capname)| { // Over capture group names (or indices)
                capname.map_or(
                    captures
                        .get(i) // Get match via index.  This could be null which is filter by filter_map
                        .map(|capmatch| (i.to_string(), capmatch.as_str().into())),
                    |capname| {
                        captures
                            .name(capname) // Get match via capture name.  Could be null which is filter by filter_map
                            .map(|capmatch| (capname.into(), capmatch.as_str().into()))
                    },
                )
            })
            .collect()
    })
}

fn regex_to_vec (re: &str, msg: &str) -> Bresult<Vec<Option<String>>> {
    Regex::new(re)?
    .captures(msg) // An Option<Captures>
    .map_or(Ok(Vec::new()), // Return Ok empty vec if None...
        |captures|          // ...or return Ok Vec of Option<Vec>
        Ok(captures.iter() // Iterator over Option<Match>
            .map( |o_match| // None or Some<String>
                    o_match.map( |mtch| mtch.as_str().into() ) )
            .collect()))
}

// Transforms @user_nickname to {USER_ID}
fn deref_ticker (s :&str) -> Option<String> {
    // Expects:  @shrewm   Returns: Some(308188500)
    if &s[0..1] == "@" {
        // Quote is ID of whomever this nick matches
        getsql!("SELECT id FROM entitys WHERE name=?", &s[1..])
            .ok()
            .filter( |v| 1 == v.len() )
            .map( |v| v[0].get("id").unwrap().to_string() )
    } else { None }
}

// Transforms {USER_ID} to @user_nickname
fn reference_ticker (t :&str) -> String {
    if is_self_stonk(t) {
        getsql!("SELECT name FROM entitys WHERE id=?", t) // Result<Vec, Err>
        .map_err( |e| error!("{:?}", e) )
        .ok()
        .filter( |v| 1 == v.len() )
        .map( |v| format!("@{}", v[0].get("name").unwrap()) )
        .unwrap_or(t.to_string())
    } else {
        t.to_string()
    }
}

fn is_self_stonk (s: &str) -> bool {
    Regex::new(r"^[0-9]+$").unwrap().find(s).is_some()
}

////////////////////////////////////////////////////////////////////////////////
/// Helpers on complex types

fn get_bank_balance (id :i64) -> Bresult<f64> {
    let res = getsql!("SELECT * FROM accounts WHERE id=?", id)?;
    Ok(
        if res.is_empty() {
            let sql = getsql!("INSERT INTO accounts VALUES (?, ?)", id, 1000.0)?;
            info!("{:?}", sql);
            1000.0
        } else {
            res[0]
            .get("balance".into())
            .ok_or("balance missing from accounts table")?
            .parse::<f64>()
            .or( Err("can't parse balance field from accounts table") )?
        }
    )
}

pub fn sql_table_order_insert (id:i64, ticker:&str, qty:f64, price:f64, time:i64) -> Bresult<Vec<HashMap<String, String>>> {
    getsql!("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", id, ticker, qty, price, time)
}

////////////////////////////////////////////////////////////////////////////////
/// Blobs

#[derive(Clone, Debug)]
pub struct Env {
    url_api: String,
    chat_id_default: i64,
    quote_delay_minutes: i64,
    dst_hours_adjust: i8,
    // Previous message editing
    message_id_read: i64,
    message_id_write: i64,
    message_buff_write: String,
    fmt_quote: String,
    fmt_position: String,
}

impl Env {
    fn copy (&self) -> Self {
        Self{
            url_api: self.url_api.to_string(),
            chat_id_default: self.chat_id_default,
            quote_delay_minutes: self.quote_delay_minutes,
            dst_hours_adjust: self.dst_hours_adjust,
            message_id_read: self.message_id_read,
            message_id_write: self.message_id_write,
            message_buff_write: self.message_buff_write.to_string(),
            fmt_quote: self.fmt_quote.to_string(),
            fmt_position: self.fmt_position.to_string(),
        }
    }
}

type AMEnv = Arc<Mutex<Env>>;

////////////////////////////////////////

#[derive(Debug)]
pub struct Cmd {
    env: Env,
    id: i64, // Entity that sent the message message.from.id
    at: i64, // Group (or entity/DM) that the message was sent in message.chat.id
    to: i64, // To whom the message is addressed (contains a message.reply_to_message.from.id) defaults to id
    id_level: i64,
    at_level: i64,
    to_level: i64,
    message_id: i64,
    fmt_quote: String,
    fmt_position: String,
    msg: String
}

impl Cmd {
    fn copy (&self) -> Self {
        Self{
            env: self.env.copy(),
            id:  self.id,
            at:  self.at,
            to:  self.to,
            id_level:  self.id_level,
            at_level:  self.at_level,
            to_level:  self.to_level,
            message_id: self.message_id,
            fmt_quote: self.fmt_quote.to_string(),
            fmt_position: self.fmt_position.to_string(),
            msg: self.msg.to_string()
        }
    }
    fn level (&self, level:i64) -> MsgCmd { MsgCmd::from(self).level(level) }
    fn _to (&self, to:i64) -> MsgCmd { MsgCmd::from(self)._to(to) }
    /// Creates a Cmd object from the useful details of a Telegram message.
    fn parse_cmd(env :&Env, body: &web::Bytes) -> Bresult<Self> {

        let json: Value = bytes2json(&body)?;
        let inline_query = &json["inline_query"];

        let edited_message = &json["edited_message"];
        let message = if edited_message.is_object() { edited_message } else { &json["message"] };
        let message_id = getin_i64(message, &["message_id"]).unwrap_or(0);

        let (id, at, to, msg) =
            if inline_query.is_object() {
                let id = getin_i64(inline_query, &["from", "id"])?;
                let msg = getin_str(inline_query, &["query"])?.to_string();
                (id, id, id, msg) // Inline queries are strictly DMs (no associated "at" groups nor reply "to" messages)
            } else if message.is_object() {
                let id = getin_i64(message, &["from", "id"])?;
                let at = getin_i64(message, &["chat", "id"])?;
                let msg = getin_str(message, &["text"])?.to_string();
                if let Ok(to) = getin_i64(&message, &["reply_to_message", "from", "id"]) {
                    (id, at, to, msg) // Reply to message
                } else {
                    (id, at, id, msg) // TODO: should "to" be the "at" group and not the "id" sender?  Check existing logic first.
                }
            } else { Err("Nothing to do.")? };

        // Create hash map id -> echoLevel
        let getsqlres = getsql!("SELECT id, echo FROM entitys NATURAL JOIN modes")?;
        let echo_levels =
            getsqlres.iter()
            .map( |hm| // tuple -> hashmap
                 ( hm.get_i64("id").unwrap(),
                   hm.get_i64("echo").unwrap() ) )
            .collect::<HashMap<i64,i64>>();

        let res = getsql!(r#"SELECT entitys.id, COALESCE(quote, "") AS quote, coalesce(position, "") AS position FROM entitys LEFT JOIN formats ON entitys.id = formats.id WHERE entitys.id=?"#, id)?;

        let (mut fmt_quote, mut fmt_position) =
            if res.is_empty() {
                Err( format!("{} missing from entitys", id))?
            } else {
                (res[0].get_str("quote")?, res[0].get_str("position")? )
            };
        if fmt_quote.is_empty() { fmt_quote = env.fmt_quote.to_string() }
        if fmt_position.is_empty() { fmt_position = env.fmt_position.to_string() }
        info!("fmt_quote {:?}  fmt_position {:?}", fmt_quote, fmt_position);

        Ok(Self {
            env:env.copy(),
            id, at, to,
            id_level: echo_levels.get(&id).map(|v|*v).unwrap_or(2_i64),
            at_level: echo_levels.get(&at).map(|v|*v).unwrap_or(2_i64),
            to_level: echo_levels.get(&to).map(|v|*v).unwrap_or(2_i64),
            message_id,
            fmt_quote, fmt_position,
            msg
        })
    }
} // Cmd

////////////////////////////////////////

#[derive(Debug)]
pub struct Quote {
    pub ticker: String,
    pub price: f64, // Current known price
    pub last: f64,  // Previous day's closing regular market price
    pub amount: f64, // Delta change since last
    pub percent: f64, // Delta % change since last
    pub market: String, // 'p're 'r'egular 'p'ost
    pub hours: i64, // 16 or 24 (hours per day market trades)
    pub exchange: String,// Keep track of this to filter "PNK" exchanged securities.
    pub title: String, // Full title/name of security
    pub updated: bool // Was this generated or pulled from cache
}

impl Quote {
    // Used by:  Quote::get_market_quote
    async fn new_market_quote (ticker: &str) -> Bresult<Self> {
        let json = srvs::get_ticker_raw(ticker).await?;

        let details = getin(&json, &["context", "dispatcher", "stores", "QuoteSummaryStore", "price"]);
        if details.is_null() { Err("Unable to find quote data in json key 'QuoteSummaryStore'")? }
        info!("{}", details);

        let title_raw =
            &getin_str(&details, &["longName"])
            .or_else( |_e| getin_str(&details, &["shortName"]) )?;
        let mut title :&str = title_raw;
        let title = loop { // Repeatedly strip title of useless things
            let title_new = title
                .trim_end_matches(".")
                .trim_end_matches(" ")
                .trim_end_matches(",")
                .trim_end_matches("Inc")
                .trim_end_matches("Corp")
                .trim_end_matches("Corporation")
                .trim_end_matches("Holdings")
                .trim_end_matches("Brands")
                .trim_end_matches("Company")
                .trim_end_matches("USD");
            if title == title_new { break title_new.to_string() }
            title = title_new;
        };
        info!("title pretty {} => {}", title_raw, &title);

        let exchange = getin_str(&details, &["exchange"])?;

        let hours = getin(&details, &["volume24Hr"]);
        let hours :i64 = if hours.is_object() && 0 != hours.as_object().unwrap().keys().len() { 24 } else { 16 };

        let previous_close   = getin_f64(&details, &["regularMarketPreviousClose", "raw"]).unwrap_or(0.0);
        let pre_market_price = getin_f64(&details, &["preMarketPrice", "raw"]).unwrap_or(0.0);
        let reg_market_price = getin_f64(&details, &["regularMarketPrice", "raw"]).unwrap_or(0.0);
        let pst_market_price = getin_f64(&details, &["postMarketPrice", "raw"]).unwrap_or(0.0);

        // This array will be sorted on market time for the latest market data.  Prices
        // are relative to the previous day's market close, including pre and post markets,
        // because most online charts I've come across ignore previous day's after market
        // closing price for next day deltas.  Non-regular markets are basically forgotten
        // after the fact.
        let mut details = [
            (pre_market_price, reg_market_price, "p", getin_i64(&details, &["preMarketTime"]).unwrap_or(0)),
            (reg_market_price, previous_close,   "r", getin_i64(&details, &["regularMarketTime"]).unwrap_or(0)),
            (pst_market_price, previous_close,   "a", getin_i64(&details, &["postMarketTime"]).unwrap_or(0))];

        /* // Log all prices for sysadmin requires "use ::datetime::ISO"
        use ::datetime::ISO;
        error!("{} \"{}\" ({}) {}hrs\n{:.2} {:.2} {} {:.2}%\n{:.2} {:.2} {} {:.2}%\n{:.2} {:.2} {} {:.2}%",
            ticker, title, exchange, hours,
            LocalDateTime::from_instant(Instant::at(details[0].3)).iso(), details[0].0, details[0].1, details[0].2,
            LocalDateTime::from_instant(Instant::at(details[1].3)).iso(), details[1].0, details[1].1, details[1].2,
            LocalDateTime::from_instant(Instant::at(details[2].3)).iso(), details[2].0, details[2].1, details[2].2); */

        details.sort_by( |a,b| b.3.cmp(&a.3) ); // Find latest quote details

        let price = details[0].0;
        let last = details[0].1;
        Ok(Quote{
            ticker:  ticker.to_string(),
            price, last,
            amount:  roundqty(price-last),
            percent: percentify(last,price),
            market:  details[0].2.to_string(),
            hours, exchange, title,
            updated: true})
    } // Quote::new_market_quote 
}

impl Quote {
    async fn get_market_quote (cmd:&Cmd, ticker:&str) -> Bresult<Self> {
        // Make sure not given referenced stonk. Expected symbols:  GME  308188500   Illegal: @shrewm
        if &ticker[0..1] == "@" { Err("Illegal ticker")? }

        let now :i64 = Instant::now().seconds();
        let is_self_stonk = is_self_stonk(ticker);

        let res = getsql!("SELECT ticker, price, last, market, hours, exchange, time, title FROM stonks WHERE ticker=?", ticker)?;
        let is_in_table = !res.is_empty();

        let is_cache_valid = 
            is_in_table && (is_self_stonk || { 
                let hm = &res[0];
                let timesecs       = hm.get_i64("time")?;
                let traded_all_day = 24 == hm.get_i64("hours")?;
                !update_ticker_p(&cmd.env, timesecs, now, traded_all_day)?
            });

        //info!("is_self_stonk {:?}", is_self_stonk);
        //info!("is_in_table {:?}", is_in_table);
        //info!("is_cache_valid {:?}", is_cache_valid);
        //if is_in_table { info!("{:?}", &res[0]) }
        let ticker =
            if is_cache_valid { // Is in cache so use it
                let hm = &res[0];
                let price = hm.get_f64("price")?;
                let last = hm.get_f64("last")?;
                Quote{
                    ticker:   hm.get_str("ticker")?,
                    price, last,
                    amount:   roundqty(price-last),
                    percent:  percentify(last,price),
                    market:   hm.get_str("market")?,
                    hours:    hm.get_i64("hours")?,
                    exchange: hm.get_str("exchange")?,
                    title:    hm.get_str("title")?,
                    updated:  false}
            } else if is_self_stonk { // FNFT is not in cache so create
                Quote {
                    ticker:  ticker.to_string(),
                    price:   0.0,
                    last:    0.0,
                    amount:  0.0,
                    percent: 0.0,
                    market:  "r".to_string(),
                    hours:   24,
                    exchange:"™BOT".to_string(),
                    title:   "FNFT".to_string(),
                    updated: true
                }
            } else { // Quote not in cache so query internet
                Quote::new_market_quote(ticker).await?
            };

        if !is_self_stonk { // Cached FNFTs are only updated during trading/settling.
            if is_in_table {
                getsql!("UPDATE stonks SET price=?, last=?, market=?, time=? WHERE ticker=?",
                    ticker.price, ticker.last, &*ticker.market, now, ticker.ticker.as_str())?
            } else {
                getsql!("INSERT INTO stonks VALUES(?,?,?,?,?,?,?,?)",
                    &*ticker.ticker, ticker.price, ticker.last, &*ticker.market, ticker.hours, &*ticker.exchange, now, &*ticker.title)?
            };
        }

        Ok(ticker)
    } // Quote::get_market_quote
}

fn fmt_decode_to (c: &str, s: &mut String) {
    match c {
        "%" => s.push_str("%"),
        "b" => s.push_str("*"),
        "i" => s.push_str("_"),
        "u" => s.push_str("__"),
        "s" => s.push_str("~"),
        "q" => s.push_str("`"),
        "n" => s.push_str("\n"),
        c => { s.push_str("%"); s.push_str(c) }
    }
}

fn amt_as_glyph (amt: f64) -> (&'static str, &'static str) {
    if 0.0 < amt {
        (from_utf8(b"\xF0\x9F\x9F\xA2").unwrap(), from_utf8(b"\xE2\x86\x91").unwrap()) // Green circle, Arrow up
    } else if 0.0 == amt {
        (from_utf8(b"\xF0\x9F\x94\xB7").unwrap(), " ") // Blue diamond, nothing
    } else {
        (from_utf8(b"\xF0\x9F\x9F\xA5").unwrap(), from_utf8(b"\xE2\x86\x93").unwrap()) // Red square, Arrow down
    }
}

impl Quote {
    // Formats a ticker given a format string.
    // Used to generate: 🟢ETH-USD@2087.83! ↑48.49 2.38% Ethereum USD CCC
    // Called by:  get_quote_pretty
    fn format_quote (self: &Self, fmt: &str) -> Bresult<String> {

        let gain_glyphs = amt_as_glyph(self.amount);

        Ok(Regex::new("(?s)(%([A-Za-z%])|.)").unwrap()
            .captures_iter(fmt)
            .fold(String::new(), |mut s, cap| {
                if let Some(m) = cap.get(2) { match m.as_str() {
                    "A" => s.push_str(gain_glyphs.1), // Arrow
                    "B" => s.push_str(&format!("{}", money_pretty(self.amount.abs()))), // inter-day delta
                    "C" => s.push_str(&percent_squish(self.percent.abs())), // inter-day percent
                    "D" => s.push_str(gain_glyphs.0), // red/green light
                    "E" => s.push_str(&reference_ticker(&self.ticker).replacen("_", "\\_", 10000)),  // ticker symbol
                    "F" => s.push_str(&format!("{}", money_pretty(self.price))), // current stonk value
                    "G" => s.push_str(&self.title),  // ticker company title
                    "H" => s.push_str( // Market regular, pre, after
                        if self.hours == 24 { "∞" }
                        else { match &*self.market { "r"=>"", "p"=>"ρ", "a"=>"α", _=>"?" } },
                    ),
                    "I" => s.push_str( if self.updated { "·" } else { "" } ),
                    c => fmt_decode_to(c, &mut s) }
                } else {
                    s.push_str(&cap[1])
                }
                s
            } ) )
    } // Quote.format_quote
}

////////////////////////////////////////

#[derive(Debug)]
struct Position {
    quote: Option<Quote>,
    id:    i64,
    ticker:String,
    qty:   f64,
    price: f64,
}

impl Position {
    async fn update_quote(&mut self, cmd: &Cmd) -> Bresult<&Self> {
        self.quote = Some(Quote::get_market_quote(cmd, &self.ticker).await?);
        Ok(self)
    }
}

impl Position {
    // Return vector instead of option in case of box/short positions.
    fn get_position (id: i64, ticker: &str) -> Bresult<Vec<Position>> {
        let positions = getsql!("SELECT qty,price FROM positions WHERE id=? AND ticker=?", id, ticker )?;
        let mut res = Vec::new();
        for pos in positions {
            res.push(Position {
                quote:  None,
                id,
                ticker: ticker.to_string(),
                qty:    pos.get_f64("qty")?,
                price:  pos.get_f64("price")?
            })
        }
        Ok(res)
    }
}

impl Position {
    fn get_users_positions (id:i64) -> Bresult<Vec<Position>> {
        let positions = getsql!("SELECT ticker,qty,price FROM positions WHERE id=?", id)?;
        let mut res = Vec::new();
        for pos in positions {
            res.push(Position {
                quote: None,
                id,
                ticker: pos.get_str("ticker")?,
                qty: pos.get_f64("qty")?,
                price: pos.get_f64("price")?
            })
        }
        Ok(res)
    }
}

impl Position {
    async fn query (id:i64, ticker:&str, cmd: &Cmd) -> Bresult<Position> {
        let mut hm = Position::get_position(id, ticker)?;

        if 2 <= hm.len() {
            Err(format!("For {} ticker {} has {} positions, expect 0 or 1", id, ticker, hm.len()))?
        }

        let mut hm = // Consider the quote or a 0 quantity quote
            if 0 == hm.len() {
                Self {
                    quote: None,
                    id,
                    ticker: ticker.to_string(),
                    qty: 0.0,
                    price: 0.0 }
            } else {
                hm.pop().unwrap()
            };
        hm.update_quote(cmd).await?;
        Ok(hm)
    }
}

impl Position {
    // Used by: do_trade_buy/sell -> execute_buy/sell, do_portfolio
    fn format_position (&mut self, fmt :&str) -> Bresult<String> {
        let qty = self.qty;
        let cost = self.price;
        let price = self.quote.as_ref().unwrap().price;
        let last = self.quote.as_ref().unwrap().last;

        let basis = qty*cost;
        let value = qty*price;
        let last_value = qty*last;

        let gain = value - basis;
        let day_gain = value - last_value;

        let gain_percent = percentify(cost, price).abs();
        let day_gain_percent = percentify(last, price).abs();

        let gain_glyphs = amt_as_glyph(price-cost);
        let day_gain_glyphs = amt_as_glyph(price-last);

        Ok(Regex::new("(?s)(%([A-Za-z%])|.)").unwrap()
        .captures_iter(fmt)
        .fold( String::new(), |mut s, cap| {
            if let Some(m) = cap.get(2) { match m.as_str() {
                "A" => s.push_str( &format!("{:.2}", roundcents(value)) ), // value
                "B" => s.push_str( &round(gain.abs())), // gain
                "C" => s.push_str( gain_glyphs.1), // Arrow
                "D" => s.push_str( &percent_squish(gain_percent)), // gain%
                "E" => s.push_str( gain_glyphs.0 ), // Color
                "F" => s.push_str( &reference_ticker(&self.ticker).replacen("_", "\\_", 10000) ), // Ticker
                "G" => s.push_str( &money_pretty(price) ), // latet stonks value
                "H" => s.push_str( &if 0.0 == qty { "0".to_string() } else { qty.to_string().trim_start_matches('0').to_string()} ), // qty
                "I" => s.push_str( &roundkilofy(cost) ), // cost
                "J" => s.push_str( day_gain_glyphs.0 ), // day color
                "K" => s.push_str( &money_pretty(day_gain) ), // inter-day delta
                "L" => s.push_str( day_gain_glyphs.1 ), // day arrow
                "M" => s.push_str( &percent_squish(day_gain_percent) ), // inter-day percent
                c => fmt_decode_to(c, &mut s) }
            } else {
                s.push_str(&cap[1]);
            }
            s
        } ) )
    }
}

////////////////////////////////////////

#[derive(Debug)]
struct Trade {
    id: i64,
    ticker: String,
    action: char, // '+' buy or '-' sell
    is_dollars: bool, // amt represents dollars or fractional quantity
    amt: Option<f64>
}

impl Trade {
    fn new (id:i64, msg:&str) -> Option<Self> {
        //                         _____ticker____  _+-_  _$_   ____________amt_______________
        match regex_to_hashmap(r"^([A-Za-z0-9^.-]+)([+-])([$])?([0-9]+\.?|([0-9]*\.[0-9]{1,4}))?$", msg) {
            Some(caps) => 
                Some(Self {
                    id,
                    ticker:     caps.get("1").unwrap().to_uppercase(),
                    action:     caps.get("2").unwrap().chars().nth(0).unwrap(),
                    is_dollars: caps.get("3").is_some(),
                    amt:        caps.get("4").map(|m| m.parse::<f64>().unwrap()) }),
                None => return None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Bot's Do Handler Helpers
////////////////////////////////////////////////////////////////////////////////

// For do_quotes Return set of tickers that need attention
pub fn extract_tickers (txt :&str) -> HashSet<String> {
    let mut tickers = HashSet::new();
    let re = Regex::new(r"^[@^]?[A-Z_a-z][-.0-9=A-Z_a-z]*$").unwrap(); // BRK.A ^GSPC BTC-USD don't end in - so a bad-$ trade doesn't trigger this
    for s in txt.split(" ") {
        let w = s.split("$").collect::<Vec<&str>>();
        if 2 == w.len() {
            let mut idx = 42;
            if w[0]!=""  &&  w[1]=="" { idx = 0; } // "$" is to the right of ticker symbol
            if w[0]==""  &&  w[1]!="" { idx = 1; } // "$" is to the left of ticker symbol
            if 42!=idx && re.find(w[idx]).is_some() { // ticker characters only
                let ticker = if &w[idx][0..1] == "@" {
                    w[idx].to_string()
                } else {
                    w[idx].to_string().to_uppercase()
                };
                tickers.insert(ticker );
            }
        }
    }
    tickers
} // extract_tickers

// for do_quotes
async fn get_quote_pretty (cmd :&Cmd, ticker :&str) -> Bresult<String> {
// Expects:  GME 308188500 @shrewm.   Includes local level 2 quotes as well.  Used by do_quotes only
    let ticker = deref_ticker(ticker).unwrap_or(ticker.to_string());
    let bidask =
        if is_self_stonk(&ticker) {
            let mut asks = "*Asks:*".to_string();
            for ask in getsql!("SELECT -qty AS qty, price FROM exchange WHERE qty<0 AND ticker=? order by price;", &*ticker)? {
                asks.push_str(&format!(" `{}@{}`",
                    num_simp(ask.get("qty").unwrap()),
                    ask.get_f64("price")?,
                ));
            }
            let mut bids = "*Bids:*".to_string();
            for bid in getsql!("SELECT qty, price FROM exchange WHERE 0<qty AND ticker=? order by price desc;", &*ticker)? {
                bids.push_str(&format!(" `{}@{}`",
                    num_simp(bid.get("qty").unwrap()),
                    bid.get_f64("price")?,
                ));
            }
            format!("\n{}\n{}", asks, bids).to_string()
        } else {
            "".to_string()
        };

    Ok(
        format!("{}{}",
            Quote::get_market_quote(cmd, &ticker).await?
                .format_quote(&cmd.fmt_quote)?,
            bidask) )
} // get_quote_pretty


////////////////////////////////////////////////////////////////////////////////
// Bot's Do Handlers -- The Meat And Potatos.  The Bread N Butter.  The Works.
////////////////////////////////////////////////////////////////////////////////


async fn do_echo (cmd :&Cmd) -> Bresult<&'static str> {
    let caps :Vec<Option<String>> = regex_to_vec("^/echo ?([0-9]+)?", &cmd.msg)?;
    if caps.is_empty() { return Ok("SKIP") }
    match caps.as_i64(1) {
        Ok(echo) => { // Update existing echo level
            if 2 < echo {
                let msg = "*echo level must be 0…2*";
                send_msg_markdown_id(cmd.into(), &msg).await?;
                Err(msg)?
            }
            getsql!("UPDATE modes set echo=? WHERE id=?", echo, cmd.at)?;
            send_msg_markdown(cmd.level(0), &format!("`echo {} set verbosity {:.0}%`", echo, echo as f64/0.02)).await?;
        }
        Err(_) => { // Create or return existing echo level
            let rows = getsql!("SELECT echo FROM modes WHERE id=?", cmd.at)?;
            let mut echo = 2;
            if rows.len() == 0 { // Create/set echo level for this channel
                getsql!("INSERT INTO modes values(?, ?)", cmd.at, echo)?;
            } else {
                echo = rows[0].get_i64("echo")?;
            }
            send_msg_markdown(cmd.level(0), &format!("`echo {} verbosity at {:.0}%`", echo, echo as f64/0.02)).await?;
        }
    }

    Ok("COMPLETED.")
}

pub async fn do_help (cmd:&Cmd) -> Bresult<&'static str> {
    if Regex::new(r"/help").unwrap().find(&cmd.msg).is_none() { return Ok("SKIP") }
    let delay = cmd.env.quote_delay_minutes;
    send_msg_markdown(cmd.into(), &format!(
"`          ™Bot Commands          `
`/echo 2` `Echo level (verbose 2…0 quiet)`
`word: ` `Definition lookup`
`+1    ` `Like someone's post (via reply)`
`+?    ` `Like leaderboard`
`/stonks` `Your Stonkfolio`
`/orders` `Your @shares and bid/ask orders`
`/yolo  ` `Stonks leaderboard`
`gme$   ` `Quote ({}min delay)`
`gme+   ` `Buy max GME shares`
`gme-   ` `Sell all GME shares`
`gme+3  ` `Buy 3 shares (min qty 0.0001)`
`gme-5  ` `Sell 5 share`
`gme+$18` `Buy $18 worth (min $0.01)`
`gme-$.9` `Sell 90¢ worth`
`@usr+2@3` `Bid/buy 2sh of '@usr' at $3`
`@usr-5@4` `Ask/sell 5sh of '@usr' at $4`
`/fmt [?]     ` `Show format strings, help`
`/fmt [qp] ...` `Set quote/position fmt str`", delay)).await?;
    Ok("COMPLETED.")
}

async fn do_curse (cmd:&Cmd) -> Bresult<&'static str> {
    if Regex::new(r"/curse").unwrap().find(&cmd.msg).is_none() { return Ok("SKIP") }

    send_msg_markdown(cmd.into(), 
        ["shit", "piss", "fuck", "cunt", "cocksucker", "motherfucker", "tits"][::rand::random::<usize>()%7]
    ).await?;

    Ok("COMPLETED.")
}


// Trying to delay an action
async fn do_repeat (cmd :&Cmd) -> Bresult<&'static str> {
    let caps =
        match Regex::new(r"^/repeat ([0-9]+) (.*)$").unwrap().captures(&cmd.msg) {
            None => return Ok("SKIP".into()),
            Some(caps) => caps
        };

    let delay = caps[1].parse::<u64>()?;
    //let msg = caps[2].to_string();
    //let delay = ::tokio::time::delay_for(Duration::from_millis(delay));

    let msgcmd :MsgCmd = cmd.into();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(delay));
        error!("*************{:?}", msgcmd);
        //let res = futures::executor::block_on(send_msg(msgcmd, &format!("{} {}", delay, msg)));
        //info!("send_msg => {:?}", res)
    });

    //println!("join = {:?}", ::futures::join!(delay, sendmsg));
    Ok("COMPLETED.")
}
async fn do_like (cmd:&Cmd) -> Bresult<String> {

    let amt =
        match Regex::new(r"^([+-])1")?.captures(&cmd.msg) {
            None => return Ok("SKIP".into()),
            Some(cap) =>
                if cmd.id == cmd.to { return Ok("SKIP self plussed".into()); }
                else if &cap[1] == "+" { 1 } else { -1 }
    };

    // Load database of users

    let mut people :HashMap<i64, String> = HashMap::new();

    for l in read_to_string("tmbot/users.txt").unwrap().lines() {
        let mut v = l.split(" ");
        let id = v.next().ok_or("User DB malformed.")?.parse::<i64>().unwrap();
        let name = v.next().ok_or("User DB malformed.")?.to_string();
        people.insert(id, name);
    }
    info!("{:?}", people);

    // Load/update/save likes

    let likes = read_to_string( format!("tmbot/{}", cmd.to) )
        .unwrap_or("0".to_string())
        .lines()
        .nth(0).unwrap()
        .parse::<i32>()
        .unwrap() + amt;

    info!("update likes in filesystem {:?} by {} to {:?}  write => {:?}",
        cmd.to, amt, likes,
        write( format!("tmbot/{}", cmd.to), likes.to_string()));

    let sfrom = cmd.id.to_string();
    let sto = cmd.to.to_string();
    let fromname = people.get(&cmd.id).unwrap_or(&sfrom);
    let toname   = people.get(&cmd.to).unwrap_or(&sto);
    let text = format!("{}{}{}", fromname, num2heart(likes), toname);
    send_msg(cmd.into(), &text).await?;

    Ok("COMPLETED.".into())
}

async fn do_like_info (cmd :&Cmd) -> Bresult<&'static str> {

    if cmd.msg != "+?" { return Ok("SKIP"); }

    let mut likes = Vec::new();
    // Over each user in file
    for l in read_to_string("tmbot/users.txt").unwrap().lines() {
        let mut v = l.split(" ");
        let id = v.next().ok_or("User DB malformed.")?;
        let name = v.next().ok_or("User DB malformed.")?.to_string();
        // Read the user's count file
        let count =
            read_to_string( "tmbot/".to_string() + &id )
            .unwrap_or("0".to_string())
            .trim()
            .parse::<i32>().or(Err("user like count parse i32 error"))?;
        likes.push((count, name));
    }

    let mut text = String::new();
    likes.sort_by(|a,b| b.0.cmp(&a.0) );
    // %3c %2f b %3e </b>
    for (likes,nom) in likes {
        text.push_str(&format!("{}{} ", nom, num2heart(likes)));
    }
    //info!("HEARTS -> msg tmbot {:?}", send_msg(env, chat_id, &(-6..=14).map( |n| num2heart(n) ).collect::<Vec<&str>>().join("")).await);
    send_msg(cmd.level(1), &text).await?;
    Ok("COMPLETED.")
}

async fn do_def (cmd :&Cmd) -> Bresult<&'static str> {

    let cap = Regex::new(r"^([A-Za-z-]+):$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let word = &cap.unwrap()[1];

    info!("looking up {:?}", word);

    let mut msg = String::new();

    // Definitions

    let defs = get_definition(word).await?;

    if defs.is_empty() {
        send_msg_markdown_id(cmd.into(), &format!("*{}* def is empty", word)).await?;
    } else {
        msg.push_str( &format!("*{}", word) );
        if 1 == defs.len() { // If multiple definitions, leave off the colon
            msg.push_str( &format!(":* {}", defs[0].to_string().replacen("`", "\\`", 10000)));
        } else {
            msg.push_str( &format!(" ({})* {}", 1, defs[0].to_string().replacen("`", "\\`", 10000)));
            for i in 1..std::cmp::min(4, defs.len()) {
                msg.push_str( &format!(" *({})* {}", i+1, defs[i].to_string().replacen("`", "\\`", 10000)));
            }
        }
    }

    // Synonym

    let mut syns = get_syns(word).await?;

    if syns.is_empty() {
        send_msg_markdown(cmd.into(), &format!("*{}* synonyms is empty", word)).await?;
    } else {
        if msg.is_empty() {
            msg.push_str( &format!("*{}:* _", word) );
        } else {
            msg.push_str("\n_");
        }
        syns.truncate(12);
        msg.push_str( &syns.join(", ") );
        msg.push_str("_");
    }

    if !msg.is_empty() {
        send_msg_markdown(cmd.level(1), &msg).await?;
    }
    Ok("COMPLETED.")
}

async fn do_sql (cmd :&Cmd) -> Bresult<&'static str> {

    if cmd.id != 308188500 { return Ok("do_sql invalid user"); }
    let rev = regex_to_vec("^(.*)ß$", &cmd.msg)?;
    let expr :&str = if rev.is_empty() { return Ok("SKIP") } else { rev.as_istr(1)? };

    let results =
        match getsql!(expr) {
            Err(e)  => {
                send_msg(cmd.into(), &format!("{:?}", e)).await?;
                return Err(e)
            }
            Ok(r) => r
        };

    // SQL rows to single string.
    let msg =
        results.iter().fold(
            String::new(),
            |mut b, vv| {
                vv.iter().for_each( |(key,val)| {b+=&format!(" {}:{}", key, val);} );
                b+="\n";
                b
            } );

    if msg.is_empty() { send_msg(cmd.into(), "empty results").await?; }
    else { send_msg(cmd.into(), &msg).await?; }

    Ok("COMPLETED.")
}

async fn do_quotes (cmd :&mut Cmd) -> Bresult<&'static str> {

    let tickers = extract_tickers(&cmd.msg);
    if tickers.is_empty() { return Ok("SKIP") }

    if cmd.env.message_id_read != cmd.message_id {
        cmd.env.message_id_read = cmd.message_id;
        cmd.env.message_id_write = send_msg(cmd.level(1), "…").await?;
        cmd.env.message_buff_write.clear();
    }

    let mut didwork = false;
    for ticker in &tickers {
        // Catch error and continue looking up tickers
        match get_quote_pretty(cmd, ticker).await {
            Ok(res) => {
                info!("get_quote_pretty {:?}", res);
                didwork = true;
                cmd.env.message_buff_write.push_str(&res);
                cmd.env.message_buff_write.push('\n');
                send_edit_msg_markdown(cmd.level(1), cmd.env.message_id_write, &cmd.env.message_buff_write).await?;
            }
            e => { glogd!("get_quote_pretty => ", e); }
        }
    }

    // Notify if no results (message not saved)
    if !didwork {
        send_edit_msg_markdown(
            cmd.level(1),
            cmd.env.message_id_write,
            &format!("{}\nno results", &cmd.env.message_buff_write)
        ).await?;
    }
    Ok("COMPLETED.")
}

async fn do_portfolio (cmd :&mut Cmd) -> Bresult<&'static str> {
    if Regex::new(r"/STONKS").unwrap().find(&cmd.msg.to_uppercase()).is_none() { return Ok("SKIP"); }

    if cmd.env.message_id_read != cmd.message_id {
        cmd.env.message_id_read = cmd.message_id;
        cmd.env.message_id_write = send_msg(cmd.level(1), "…").await?;
        cmd.env.message_buff_write.clear();
    }

    let mut total = 0.0;
    for mut pos in Position::get_users_positions(cmd.id)? { 
        if !is_self_stonk(&pos.ticker) {
            pos.update_quote(cmd).await?;
            info!("{} position {:?}", cmd.id, pos);
            cmd.env.message_buff_write.push_str(&pos.format_position(&cmd.fmt_position)?);
            send_edit_msg_markdown(cmd.into(), cmd.env.message_id_write, &cmd.env.message_buff_write).await?;
            total += pos.qty * pos.quote.unwrap().price;
        }
    }

    let cash = get_bank_balance(cmd.id)?;
    cmd.env.message_buff_write.push_str(&format!("\n`{:7.2}``Cash`    `YOLO``{:.2}`\n", roundcents(cash), roundcents(total+cash)));

    send_edit_msg_markdown(cmd.into(), cmd.env.message_id_write, &cmd.env.message_buff_write).await?;
    Ok("COMPLETED.")
}

async fn do_yolo (cmd :&Cmd) -> Bresult<&'static str> {

    // Handle: /yolo
    if Regex::new(r"/YOLO").unwrap().find(&cmd.msg.to_uppercase()).is_none() { return Ok("SKIP"); }

    let working_message = "working...".to_string();
    let message_id = send_msg(cmd.level(1), &working_message).await?;

    // Update all user-positioned tickers
    for row in getsql!("\
        SELECT ticker \
        FROM positions \
        WHERE positions.ticker NOT LIKE '0%' \
          AND positions.ticker NOT LIKE '1%' \
          AND positions.ticker NOT LIKE '2%' \
          AND positions.ticker NOT LIKE '3%' \
          AND positions.ticker NOT LIKE '4%' \
          AND positions.ticker NOT LIKE '5%' \
          AND positions.ticker NOT LIKE '6%' \
          AND positions.ticker NOT LIKE '7%' \
          AND positions.ticker NOT LIKE '8%' \
          AND positions.ticker NOT LIKE '9%' \
          AND positions.ticker NOT LIKE '@%' \
        GROUP BY ticker")? {
        let ticker = row.get("ticker").unwrap();
        //working_message.push_str(ticker);
        //working_message.push_str("...");
        //send_edit_msg(cmd, message_id, &working_message).await?;
        info!("Stonk \x1b[33m{:?}", Quote::get_market_quote(cmd, ticker).await?);
    }

    /* Everyone's YOLO including non positioned YOLOers
    let sql = "\
    SELECT name, round(sum(value),2) AS yolo \
    FROM ( \
            SELECT id, qty*stonks.price AS value \
            FROM positions \
            LEFT JOIN stonks ON positions.ticker = stonks.ticker \
        UNION \
            SELECT id, balance as value FROM accounts \
        ) \
    NATURAL JOIN entitys \
    GROUP BY name \
    ORDER BY yolo DESC"; */

    // Everyone's YOLO including non positioned YOLOers
    let sql = "SELECT name, ROUND(value + balance, 2) AS yolo \
               FROM (SELECT positions.id, SUM(qty*stonks.price) AS value \
                     FROM positions \
                     LEFT JOIN stonks ON stonks.ticker = positions.ticker \
                     WHERE positions.ticker NOT LIKE '0%' \
                       AND positions.ticker NOT LIKE '1%' \
                       AND positions.ticker NOT LIKE '2%' \
                       AND positions.ticker NOT LIKE '3%' \
                       AND positions.ticker NOT LIKE '4%' \
                       AND positions.ticker NOT LIKE '5%' \
                       AND positions.ticker NOT LIKE '6%' \
                       AND positions.ticker NOT LIKE '7%' \
                       AND positions.ticker NOT LIKE '8%' \
                       AND positions.ticker NOT LIKE '9%' \
                       AND positions.ticker NOT LIKE '@%' \
                     GROUP BY id) \
               NATURAL JOIN accounts \
               NATURAL JOIN entitys \
               ORDER BY yolo DESC";
    let results = getsql!(&sql)?;

    // Build and send response string

    let mut msg = "*YOLOlians*".to_string();
    for row in results {
        msg.push_str( &format!(" `{:.2}@{}`",
            row.get_f64("yolo")?,
            row.get("name").unwrap()) );
    }
    send_edit_msg_markdown(cmd.level(1), message_id, &msg).await?;
    Ok("COMPLETED.")
}

// Returns quantiy and new bank balance if all good.  Otherwise an error string.
fn verify_qty (mut qty:f64, price:f64, bank_balance:f64) -> Result<(f64,f64), &'static str> {
    qty = roundqty(qty);
    let basis  = qty * price;
    let new_balance = bank_balance - basis;
    info!("\x1b[1mbuy? {} @ {} = {}  BANK {} -> {}", qty, price, basis,  bank_balance, new_balance);
    if qty < 0.0001 { return Err("Amount too low") }
    if new_balance < 0.0 { return Err("Need more cash") }
    Ok((qty, new_balance))
}

////////////////////////////////////////////////////////////////////////////////
/// Stonk Buy

#[derive(Debug)]
struct TradeBuy { qty:f64, price:f64, bank_balance: f64, hours: i64, trade: Trade }

impl TradeBuy {
    async fn new (trade:Trade, cmd:&Cmd) -> Bresult<Self> {
        let quote = Quote::get_market_quote(cmd, &trade.ticker).await?;

        if quote.exchange == "PNK" {
            send_msg_markdown(cmd.into(), "`OTC / PinkSheet Verboten Stonken`").await?;
            Err("OTC / PinkSheet Verboten Stonken")?
        }
        let price = quote.price;
        let bank_balance = get_bank_balance(trade.id)?;
        let hours = quote.hours;
        let qty = match trade.amt {
            Some(amt) => if trade.is_dollars { amt/price } else { amt },
            None => roundqty(bank_balance / price) // Buy as much as possible
        };
        Ok( Self{qty, price, bank_balance, hours, trade} )
    }
}

#[derive(Debug)]
struct TradeBuyCalc { qty:f64, new_balance:f64, new_qty:f64, new_basis:f64, position:Position, new_position_p:bool, tradebuy:TradeBuy }

impl TradeBuyCalc {
    async fn compute_position (obj:TradeBuy, cmd:&Cmd) -> Bresult<Self> {
        let (qty, new_balance) =
            match
                verify_qty(obj.qty, obj.price, obj.bank_balance)
                .or_else( |_e| verify_qty(obj.qty-0.0001, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0002, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0003, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0004, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0005, obj.price, obj.bank_balance) ) {
                Err(e) => {  // Message user problem and log
                    send_msg_id(cmd.into(), &e).await?;
                    return Err(e.into())
                },
                Ok(r) => r
            };
        //let position = Position::query(obj.trade.id, &obj.trade.ticker, cmd).await?;
        let position = Position::query(obj.trade.id, &obj.trade.ticker, cmd).await?;
        let new_position_p = position.qty == 0.0;

        let qty_old = position.qty;
        let price_old = position.price;
        let price_new = position.quote.as_ref().unwrap().price;
        let (new_qty, new_basis) = {
            //let qty_old = position.qty;
            //let price_old = position.quote.unwrap().price;
            let new_basis = (qty * price_new + qty_old * price_old) / (qty + qty_old);
            let new_qty = roundqty(qty + qty_old);
            (new_qty, new_basis)
        };
        Ok(Self{qty, new_balance, new_qty, new_basis, position, new_position_p, tradebuy:obj})
    }
}

#[derive(Debug)]
struct ExecuteBuy { msg:String, tradebuycalc:TradeBuyCalc }

impl ExecuteBuy {
    async fn execute (mut obj:TradeBuyCalc, cmd:&Cmd) -> Bresult<Self> {

        let now = Instant::now().seconds();

        if obj.tradebuy.hours!=24 && !trading_hours_p(&cmd.env, now)? {
            return Ok(Self{msg:"Unable to trade after hours".into(), tradebuycalc:obj});
        }

        let id = obj.tradebuy.trade.id;
        let ticker = &obj.tradebuy.trade.ticker;
        let price = obj.tradebuy.price;

        sql_table_order_insert(id, ticker, obj.qty, price, now)?;
        let mut msg = format!("*Bought:*");

        if obj.new_position_p {
            getsql!("INSERT INTO positions VALUES (?, ?, ?, ?)", id, &**ticker, obj.new_qty, obj.new_basis)?;
        } else {
            msg += &format!("  `{:.2}``{}` *{}*_@{}_", obj.qty*price, ticker, obj.qty, price);
            info!("\x1b[1madd to existing position:  {} @ {}  ->  {} @ {}", obj.position.qty, obj.position.quote.as_ref().unwrap().price, obj.new_qty, obj.new_basis);
            getsql!("UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=?", obj.new_qty, obj.new_basis, id, &**ticker)?;
        }
        obj.position.qty = obj.new_qty;
        obj.position.price = obj.new_basis;
        msg += &obj.position.format_position(&cmd.env.fmt_position)?;

        getsql!("UPDATE accounts SET balance=? WHERE id=?", obj.new_balance, id)?;

        Ok(Self{msg, tradebuycalc:obj})
    }
}

async fn do_trade_buy (cmd :&Cmd) -> Bresult<&'static str> {
    let trade = Trade::new(cmd.id, &cmd.msg);
    if trade.as_ref().map_or(true, |trade| trade.action != '+') { return Ok("SKIP") }

    let res =
        TradeBuy::new(trade.unwrap(), cmd).await
        .map(|tradebuy| TradeBuyCalc::compute_position(tradebuy, cmd))?.await
        .map(|tradebuycalc| ExecuteBuy::execute(tradebuycalc, cmd))?.await
        .map(|res| { info!("\x1b[1;31mResult {:#?}", &res); res })?;

    send_msg_markdown(cmd.into(), &res.msg).await?; // Report to group
    Ok("COMPLETED.")
}

////////////////////////////////////////////////////////////////////////////////
/// Stonk Sell

#[derive(Debug)]
struct TradeSell {
    position: Position,
    qty: f64,
    price: f64,
    bank_balance: f64,
    new_balance: f64,
    new_qty: f64,
    hours: i64,
    trade: Trade
}

impl TradeSell {
    async fn new (trade:Trade, cmd:&Cmd) -> Bresult<Self> {
        let id = trade.id;
        let ticker = &trade.ticker;

        let mut positions = Position::get_position(id, ticker)?;
        if positions.is_empty() {
            send_msg_id(cmd.into(), "You lack a valid position.").await?;
            Err("expect 1 position in table")?
        }
        let mut position = positions.pop().unwrap();
        position.update_quote(cmd).await?;

        let pos_qty = position.qty;
        let quote = &position.quote.as_ref().unwrap();
        let price = quote.price;
        let hours = quote.hours;

        let mut qty =
            roundqty(match trade.amt {
                Some(amt) => if trade.is_dollars { amt / price } else { amt }, // Convert dollars to shares maybe
                None => pos_qty // no amount set, so set to entire qty
            });
        if qty <= 0.0 {
            send_msg_id(cmd.into(), "Quantity too low.").await?;
            Err("sell qty too low")?
        }

        let bank_balance = get_bank_balance(id)?;
        let mut gain = qty*price;
        let mut new_balance = bank_balance+gain;

        info!("\x1b[1msell? {}/{} @ {} = {}  CASH {} -> {}", qty, pos_qty, price, gain,  bank_balance, new_balance);

        // If equal to the rounded position value, snap qty to exact position
        if qty != pos_qty && roundcents(gain) == roundcents(pos_qty*price) {
            qty = pos_qty;
            gain = qty*price;
            new_balance = bank_balance + gain;
            info!("\x1b[1msell? {}/{} @ {} = {}  BANK {} -> {}", qty, pos_qty, price, gain,  bank_balance, new_balance);
        }

        if pos_qty < qty {
            send_msg_id(cmd.into(), "You can't sell more than you own.").await?;
            return Err("not enough shares to sell".into());
        }

        let new_qty = roundqty(pos_qty-qty);

        Ok( Self{position, qty, price, bank_balance, new_balance, new_qty, hours, trade} )
    }
}

#[derive(Debug)]
struct ExecuteSell {
    msg: String,
    tradesell: TradeSell
}

impl ExecuteSell {
    async fn execute (mut obj:TradeSell, cmd:&Cmd) -> Bresult<Self> {
        let now = Instant::now().seconds();

        if obj.hours!=24 && !trading_hours_p(&cmd.env, now)? {
            return Ok(Self{msg:"Unable to trade after hours".into(), tradesell:obj});
        }

        let id = obj.trade.id;
        let ticker = &obj.trade.ticker;
        let qty = obj.qty;
        let price = obj.price;
        let new_qty = obj.new_qty;
        sql_table_order_insert(id, ticker, -qty, price, now)?;
        let mut msg = format!("*Sold:*");
        if new_qty == 0.0 {
            getsql!("DELETE FROM positions WHERE id=? AND ticker=?", id, &**ticker)?;
            msg += &obj.position.format_position(&cmd.fmt_position)?;
        } else {
            msg += &format!("  `{:.2}``{}` *{}*_@{}_{}",
                qty*price, ticker, qty, price,
                &obj.position.format_position(&cmd.fmt_position)?);
            getsql!("UPDATE positions SET qty=? WHERE id=? AND ticker=?", new_qty, id, &**ticker)?;
        }
        getsql!("UPDATE accounts SET balance=? WHERE id=?", obj.new_balance, id)?;
        Ok(Self{msg, tradesell:obj})
    }
}

async fn do_trade_sell (cmd :&Cmd) -> Bresult<&'static str> {
    let trade = Trade::new(cmd.id, &cmd.msg);
    if trade.as_ref().map_or(true, |trade| trade.action != '-') { return Ok("SKIP") }

    let res =
        TradeSell::new(trade.unwrap(), cmd).await
        .map(|tradesell| ExecuteSell::execute(tradesell, cmd))?.await?;

    info!("\x1b[1;31mResult {:#?}", res);
    send_msg_markdown(cmd.into(), &res.msg).await?; // Report to group
    Ok("COMPLETED.")
}


////////////////////////////////////////////////////////////////////////////////
/// General Exchange Market Place
/*
    Create an ask quote (+price) on the exchange table, lower is better.
            [[ 0.01  0.30  0.50  0.99   1.00  1.55  2.00  9.00 ]]
    Best BID price (next buyer)--^      ^--Best ASK price (next seller)
    Positive quantity                      Negative quantity
*/ 

#[derive(Debug)]
struct ExQuote {
    id: i64,
    thing: String,
    qty: f64,
    price: f64,
    ticker: String,
    now: i64
}

impl ExQuote {
    fn scan (id:i64, msg:&str) -> Bresult<Option<Self>> {
         //                         ____ticker____  _____________qty____________________  $@  ___________price______________
        let caps = regex_to_vec(r"^(@[A-Za-z^.-_]+)([+-]([0-9]+[.]?|[0-9]*[.][0-9]{1,4}))[$@]([0-9]+[.]?|[0-9]*[.][0-9]{1,2})$", msg)?;
        if caps.is_empty() { return Ok(None) }

        let thing = caps.as_string(1)?;
        let qty = roundqty(caps.as_f64(2)?);
        let price = caps.as_f64(4)?;
        let now = Instant::now().seconds();
        deref_ticker(&thing) // Option<String>
        .map_or(
             Ok(None),
            |ticker| // String
                Ok(Some(Self{id, thing, qty, price, ticker, now})) ) // Result<Option<ExQuote>, Err>
    }
}

#[derive(Debug)]
struct QuoteCancelMine {
    qty: f64,
    myasks: Vec<HashMap<String,String>>,
    myasksqty: f64,
    mybids: Vec<HashMap<String,String>>,
    exquote: ExQuote,
    msg: String
}
impl QuoteCancelMine {
    fn doit (exquote :ExQuote) -> Bresult<Self> {
        let id = exquote.id;
        let ticker = &exquote.ticker;
        let mut qty = exquote.qty;
        let price = exquote.price;
        let mut msg = String::new();

        let mut myasks = getsql!("SELECT * FROM exchange WHERE id=? AND ticker=? AND qty<0.0 ORDER BY price", id, &**ticker)?;
        let myasksqty = -roundqty(myasks.iter().map( |ask| ask.get_f64("qty").unwrap() ).sum::<f64>());
        let mut mybids = getsql!("SELECT * FROM exchange WHERE id=? AND ticker=? AND 0.0<=qty ORDER BY price DESC", id, &**ticker)?;

        if qty < 0.0 { // This is an ask exquote
            // Remove/decrement a matching bid in the current settled market
            if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get_f64("price").unwrap() ) {
                let bidqty = roundqty(mybid.get_f64("qty")?);
                if bidqty <= -qty {
                    getsql!("DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", id, &**ticker, bidqty, price)?;
                    mybid.insert("*UPDATED*".into(), "*REMOVED*".into());
                    qty = roundqty(qty + bidqty);
                    msg += &format!("\n*Removed bid:* `{}+{}@{}`", exquote.thing, bidqty, price);
                } else {
                    let newbidqty = roundqty(bidqty+qty);
                    getsql!("UPDATE exchange SET qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newbidqty, id, &**ticker, bidqty, price)?;
                    mybid.insert("*UPDATED*".into(), format!("*QTY={:.}*", newbidqty));
                    qty = 0.0;
                    msg += &format!("\n*Updated bid:* `{}+{}@{}` -> `{}@{}`", exquote.thing, bidqty, price, newbidqty, price);
                }
            }
        } else if 0.0 < qty { // This is a bid exquote
            // Remove/decrement a matching ask in the current settled market
            if let Some(myask) = myasks.iter_mut().find( |b| price == b.get_f64("price").unwrap() ) {
                let askqty = roundqty(myask.get_f64("qty")?);
                if -askqty <= qty {
                    getsql!("DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", id, &**ticker, askqty, price)?;
                    myask.insert("*UPDATED*".into(), "*REMOVED*".into());
                    qty = roundqty(qty + askqty);
                    msg += &format!("\n*Removed ask:* `{}{}@{}`", exquote.thing, askqty, price);
                } else {
                    let newaskqty = roundqty(askqty+qty);
                    getsql!("UPDATE exchange SET qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newaskqty, id, &**ticker, askqty, price)?;
                    myask.insert("*UPDATED*".into(), format!("*QTY={:.}*", newaskqty));
                    qty = 0.0;
                    msg += &format!("\n*Updated ask:* `{}{}@{}` -> `{}@{}`", exquote.thing, askqty, price, newaskqty, price);
                }
            }

        }

        Ok(Self{qty, myasks, myasksqty, mybids, exquote, msg})
    }
}

#[derive(Debug)]
struct QuoteExecute {
    qty: f64,
    bids: Vec<HashMap<String,String>>,
    quotecancelmine: QuoteCancelMine,
    msg: String
}

impl QuoteExecute {
    async fn doit(mut obj :QuoteCancelMine, cmd:&Cmd) -> Bresult<Self> {
        let id = obj.exquote.id;
        let ticker = &obj.exquote.ticker;
        let price = obj.exquote.price;
        let mut qty = obj.qty;
        let now = obj.exquote.now;
        let myasks = &mut obj.myasks;
        let mybids = &mut obj.mybids;
        let asksqty = obj.myasksqty;

        // Other bids / asks (not mine) global since it's being updated and returned for logging
        let mut bids = getsql!("SELECT * FROM exchange WHERE id!=? AND ticker=? AND 0.0<qty ORDER BY price DESC", id, &**ticker)?;
        let mut asks = getsql!("SELECT * FROM exchange WHERE id!=? AND ticker=? AND qty<0.0 ORDER BY price", id, &**ticker)?;
        let mut msg = obj.msg.to_string();
        let position = Position::query(id, ticker, cmd).await?;
        let posqty = position.qty;
        let posprice = position.price;

        if qty < 0.0 { // This is an ask exquote
            if posqty < -qty + asksqty { // does it exceed my current ask qty?
                msg += "\nYou lack that available quantity to sell.";
            } else {
                for abid in bids.iter_mut().filter( |b| price <= b.get_f64("price").unwrap() ) {
                    let aid = abid.get_i64("id")?;
                    let aqty = roundqty(abid.get_f64("qty")?);
                    let aprice = abid.get_f64("price")?;
                    let xqty = aqty.min(-qty); // quantity exchanged is the smaller of the two (could be equal)

                    if xqty == aqty { // bid to be entirely settled
                        getsql!("DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", aid, &**ticker, aqty, aprice)?;
                        abid.insert("*UPDATED*".into(), "*REMOVED*".into());
                    } else {
                        getsql!("UPDATE exchange set qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", aqty-xqty, aid, &**ticker, aqty, aprice)?;
                        abid.insert("*UPDATED*".into(), format!("*qty={}*", aqty-xqty));
                    }

                    // Update order table with the purchase and buy info
                    sql_table_order_insert(id, ticker, -xqty, aprice, now)?;
                    sql_table_order_insert(aid, ticker, xqty, aprice, now)?;

                    // Update each user's account
                    let value = xqty * aprice;
                    getsql!("UPDATE accounts SET balance=balance+? WHERE id=?",  value, id)?;
                    getsql!("UPDATE accounts SET balance=balance+? WHERE id=?", -value, aid)?;

                    msg += &format!("\n*Settled:*\n{} `{}{:+}@{}` <-> `${}` {}",
                            reference_ticker(&id.to_string()), obj.exquote.thing, xqty, aprice,
                            value, reference_ticker(&aid.to_string()));

                    // Update my position
                    let newposqty = roundqty(posqty - xqty);
                    if 0.0 == newposqty {
                        getsql!("DELETE FROM positions WHERE id=? AND ticker=? AND qty=?", id, &**ticker, posqty)?;
                    } else {
                        getsql!("UPDATE positions SET qty=? WHERE id=? AND ticker=? AND qty=?", newposqty, id, &**ticker, posqty)?;
                    }

                    // Update buyer's position
                    let aposition = Position::query(aid, ticker, cmd).await?;
                    let aposqty = aposition.qty;
                    let aposprice = aposition.price;

                    if 0.0 == aposqty {
                        getsql!("INSERT INTO positions values(?, ?, ?, ?)", aid, &**ticker, xqty, value)?;
                    } else {
                        let newaposqty = roundqty(aposqty+aqty);
                        let newaposcost = (aposprice + value) / (aposqty + xqty);
                        getsql!("UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=? AND qty=?", newaposqty, newaposcost, aid, &**ticker, aposqty)?;
                    }

                    // Update stonk exquote value
                    let last_price = Quote::get_market_quote(cmd, ticker).await?.price;
                    getsql!("UPDATE stonks SET price=?, last=?, time=? WHERE ticker=?", aprice, last_price, now, &**ticker)?;

                    qty = roundqty(qty+xqty);
                    if 0.0 <= qty { break }
                }

                // create or increment in exchange table my ask.  This could also be the case if no bids were executed.
                if qty < 0.0 {
                    if let Some(myask) = myasks.iter_mut().find( |a| price == a.get_f64("price").unwrap() ) {
                        let oldqty = myask.get_f64("qty").unwrap();
                        let newqty = roundqty(oldqty + qty); // both negative, so "increased" ask qty
                        let mytime = myask.get_i64("time").unwrap();
                        getsql!("UPDATE exchange set qty=? WHERE id=? AND ticker=? AND price=? AND time=?", newqty, id, &**ticker, price, mytime)?;
                        myask.insert("*UPDATED*".into(), format!("qty={}", newqty));
                        msg += &format!("\n*Updated ask:* `{}{}@{}` -> `{}@{}`", obj.exquote.thing, oldqty, price, newqty, price);
                    } else {
                        getsql!("INSERT INTO exchange VALUES (?, ?, ?, ?, ?)", id, &**ticker, &*format!("{:.4}", qty), &*format!("{:.4}", price), now)?;
                        // Add a fake entry to local myasks vector for logging's sake
                        let mut hm = HashMap::<String,String>::new();
                        hm.insert("*UPDATED*".to_string(), format!("*INSERTED* {} {} {} {} {}", id, ticker, qty, price, now));
                        myasks.push(hm);
                        msg += &format!("\n*Created ask:* `{}{}@{}`", obj.exquote.thing, qty, price);
                    }
                }
            }
        } else if 0.0 < qty { // This is a bid exquote (want to buy someone)
            // Limited by available cash but that's up to the current best ask price and sum of ask costs.
            let basis = qty * price;
            let bank_balance = get_bank_balance(id)?;
            let mybidsprice =
                mybids.iter().map( |bid|
                    bid.get_f64("qty").unwrap()
                    * bid.get_f64("price").unwrap() )
                .sum::<f64>();

            if bank_balance < mybidsprice + basis { // Verify not over spending as this order could become a bid
                msg += "Available cash lacking for this bid.";
            } else {
                for aask in asks.iter_mut().filter( |a| a.get_f64("price").unwrap() <= price ) {
                    //error!("{:?}", mybid);
                    let aid = aask.get_i64("id")?;
                    let aqty = roundqty(aask.get_f64("qty")?); // This is a negative value
                    let aprice = aask.get_f64("price")?;
                    //error!("my ask qty {}  asksqty sum {}  bid qty {}", qty, asksqty, aqty);

                    let xqty = roundqty(qty.min(-aqty)); // quantity exchanged is the smaller of the two (could be equal)

                    if xqty == -aqty { // ask to be entirely settled
                        getsql!("DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", aid, &**ticker, aqty, aprice)?;
                        aask.insert("*UPDATED*".into(), "*REMOVED*".into());
                    } else {
                        let newqty = aqty+xqty;
                        getsql!("UPDATE exchange set qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newqty, aid, &**ticker, aqty, aprice)?;
                        aask.insert("*UPDATED*".into(), format!("*qty={}*", newqty));
                    }

                    //error!("new order {} {} {} {} {}", id, ticker, aqty, price, now);
                    // Update order table with the purchase and buy info
                    sql_table_order_insert(id, ticker, xqty, aprice, now)?;
                    sql_table_order_insert(aid, ticker, -xqty, aprice, now)?;

                    // Update each user's account
                    let value = xqty * aprice;
                    getsql!("UPDATE accounts SET balance=balance+? WHERE id=?", -value, id)?;
                    getsql!("UPDATE accounts SET balance=balance+? WHERE id=?",  value, aid)?;

                    msg += &format!("\n*Settled:*\n{} `${}` <-> `{}{:+}@{}` {}",
                            reference_ticker(&id.to_string()), value,
                            obj.exquote.thing, xqty, aprice, reference_ticker(&aid.to_string()));

                    // Update my position
                    if 0.0 == posqty {
                        getsql!("INSERT INTO positions values(?, ?, ?, ?)", id, &**ticker, xqty, value)?;
                    } else {
                        let newposqty = roundqty(posqty+xqty);
                        let newposcost = (posprice + value) / (posqty + xqty);
                        getsql!("UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=? AND qty=?", newposqty, newposcost, id, &**ticker, posqty)?;
                    }

                    // Update their position
                    let aposition = Position::query(aid, ticker, cmd).await?;
                    let aposqty = aposition.qty;
                    let newaposqty = roundqty(aposqty - xqty);
                    if 0.0 == newaposqty {
                        getsql!("DELETE FROM positions WHERE id=? AND ticker=? AND qty=?", aid, &**ticker, aposqty)?;
                    } else {
                        getsql!("UPDATE positions SET qty=? WHERE id=? AND ticker=? AND qty=?", newaposqty, aid, &**ticker, aposqty)?;
                    }

                    // Update self-stonk exquote value
                    let last_price = Quote::get_market_quote(cmd, ticker).await?.price;
                    getsql!("UPDATE stonks SET price=?, last=?, time=?, WHERE ticker=?", aprice, last_price, now, &**ticker)?;

                    qty = roundqty(qty-xqty);
                    if qty <= 0.0 { break }
                }

                // create or increment in exchange table my bid.  This could also be the case if no asks were executed.
                if 0.0 < qty{
                    if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get_f64("price").unwrap() ) {
                        let oldqty = mybid.get_f64("qty").unwrap();
                        let newqty = roundqty(oldqty + qty);
                        let mytime = mybid.get_i64("time").unwrap();
                        getsql!("UPDATE exchange set qty=? WHERE id=? AND ticker=? AND price=? AND time=?", newqty, id, &**ticker, price, mytime)?;
                        mybid.insert("*UPDATED*".into(), format!("qty={}", newqty));
                        msg += &format!("\n*Updated bid:* `{}+{}@{}` -> `{}@{}`", obj.exquote.thing, oldqty, price, newqty, price);
                    } else {
                        getsql!("INSERT INTO exchange VALUES (?, ?, ?, ?, ?)", id, &**ticker, &*format!("{:.4}",qty), &*format!("{:.4}",price), now)?;
                        // Add a fake entry to local mybids vector for logging's sake
                        let mut hm = HashMap::<String,String>::new();
                        hm.insert("*UPDATED*".to_string(), format!("*INSERTED* {} {} {} {} {}", id, ticker, qty, price, now));
                        mybids.push(hm);
                        msg += &format!("\n*Created bid:* `{}+{}@{}`", obj.exquote.thing, qty, price);
                    }
                }
            }
        }
        Ok(Self{qty, bids, quotecancelmine: obj, msg})
    }
}

async fn do_exchange_bidask (cmd :&Cmd) -> Bresult<&'static str> {

    let exquote = ExQuote::scan(cmd.id, &cmd.msg)?;
    let exquote = if exquote.is_none() { return Ok("SKIP") } else { exquote.unwrap() };

    let ret =
        QuoteCancelMine::doit(exquote)
        .map(|obj| QuoteExecute::doit(obj, cmd) )?.await?;
    info!("\x1b[1;31mResult {:#?}", ret);
    if 0!=ret.msg.len() {
        send_msg_markdown(cmd.into(),
            &ret.msg
            .replacen("_", "\\_", 1000)
            .replacen(">", "\\>", 1000)
        ).await?;
    } // Report to group
    Ok("COMPLETED.")
}

async fn do_orders (cmd :&Cmd) -> Bresult<&'static str> {
    if Regex::new(r"/ORDERS").unwrap().find(&cmd.msg.to_uppercase()).is_none() { return Ok("SKIP"); }
    let id = cmd.id;
    let mut asks = String::from("");
    let mut bids = String::from("");
    for order in getsql!("SELECT * FROM exchange WHERE id=?", id)? {
        let ticker = order.get("ticker").unwrap();
        let stonk = reference_ticker(ticker).replacen("_", "\\_", 10000);
        let qty = order.get_f64("qty")?;
        let price = order.get("price").unwrap();
        if qty < 0.0 {
            asks += &format!("\n{}{:+}@{}", stonk, qty, price);
        } else {
            bids += &format!("\n{}{:+}@{}", stonk, qty, price);
        }
    }

    let mut msg = String::new();
    let mut total = 0.0;

    // Include all self-stonks positions (mine and others)
    let sql = format!("
        SELECT {} AS id, id AS ticker, 0.0 AS qty, 0.0 AS price \
        FROM entitys \
        WHERE 0<id \
        AND id NOT IN (SELECT ticker FROM positions WHERE id={} GROUP BY ticker)
            UNION \
        SELECT * FROM positions WHERE id={} AND ticker IN (SELECT id FROM entitys WHERE 0<id)", id, id, id);
    for pos in getsql!(sql)? {
        if is_self_stonk(pos.get("ticker").unwrap()) {
            let mut pos = Position {
                quote: None,
                id: pos.get_i64("id")?,
                ticker: pos.get_str("ticker")?,
                qty: pos.get_f64("qty")?,
                price: pos.get_f64("price")?
            };
            pos.update_quote(cmd).await?;
            msg += &pos.format_position(&cmd.fmt_position)?;
            total += pos.qty * pos.quote.unwrap().price;
        }
    }

    let cash = get_bank_balance(cmd.id)?;
    msg += &format!("\n`{:7.2}``Cash`    `YOLO``{:.2}`",
        roundcents(cash),
        roundcents(total+cash));

    if 0 < msg.len() { msg += "\n" }
    if bids.len() == 0 && asks.len() == 0 {
        msg += "*NO BIDS/ASKS*"
    } else {
        if asks.len() != 0 {
            msg += "*ASKS:*";
            msg += &asks;
        } else {
            msg += "*ASKS:* none";
        }
        msg += "\n";
        if  bids.len() != 0 {
            msg += "*BIDS:*";
            msg += &bids;
        } else {
            msg += "*BIDS* none";
        }
    }
    info!("{:?}", &msg);
    send_msg_markdown(cmd.into(), &msg).await?;
    Ok("COMPLETED.")
}

async fn do_fmt (cmd :&Cmd) -> Bresult<&'static str> {
    let cap = Regex::new(r"^/fmt( ([qp?])[ ]?(.*)?)?$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let cap = cap.unwrap();

    if cap.get(1) == None {
        let res = getsql!(r#"SELECT COALESCE(quote, "") as quote, COALESCE(position, "") as position FROM entitys LEFT JOIN formats ON entitys.id = formats.id WHERE entitys.id=?"#, cmd.id)?;
        let fmt_quote =
            if res.is_empty() || res[0].get_str("quote")?.is_empty() {
                format!("DEFAULT {:?}", cmd.fmt_quote.to_string())
            } else {
                res[0].get_str("quote")?
            };
        let fmt_position =
            if res.is_empty() || res[0].get_str("position")?.is_empty() {
                format!("DEFAULT {:?}", cmd.fmt_position.to_string())
            } else {
                res[0].get_str("position")?
            };
        send_msg(cmd.into(), &format!("fmt_quote {}\nfmt_position {}", fmt_quote, fmt_position)).await?;
        return Ok("COMPLETED.")
    }

    let c = match &cap[2] {
        "?" => {
            send_msg_markdown(
                cmd.into(),
"` ™Bot Quote Formatting `
`%A` `Gain Arrow`
`%B` `Gain`
`%C` `Gain %`
`%D` `Gain Color`
`%E` `Ticker Symbol`
`%F` `Stock Price`
`%G` `Company Title`
`%H` `Market 'p're 'a'fter '∞'`
`%I` `Updated indicator`
`%[%nbiusq]` `% newline bold italics underline strikeout quote`
"
            ).await?;
            send_msg_markdown(
                cmd.into(),
"` ™Bot Position Formatting `
`%A` `Value`
`%B` `Gain`
`%C` `Gain Arrow`
`%D` `Gain %`
`%E` `Gain Color`
`%F` `Ticker Symbol`
`%G` `Stock Price`
`%H` `Share Count`
`%I` `Cost Basis per Share`
`%J` `Day Color`
`%K` `Day Gain`
`%L` `Day Arrow`
`%M` `Day Gain %`
`%[%nbiusq]` `% newline bold italics underline strikeout quote`
"
            ).await?;
            return Ok("COMPLETED.");
        },
        "q" => "quote",
        _ => "position"
    };
    let s = &cap[3];

    info!("set format {} to {:?}", c, s);

    let res = getsql!("SELECT id FROM formats WHERE id=?", cmd.id)?;
    if res.is_empty() {
        getsql!(r#"INSERT INTO formats VALUES (?, "", "")"#, cmd.id)?;
    }

    // notify user the change
    send_msg(cmd.into(), &format!("fmt_{} {}", c, if s.is_empty() { "DEFAULT".to_string() } else { format!("{:?}", s) })).await?;

    // make change to DB
    getsql!(r#"UPDATE formats SET ?=? WHERE id=?"#, c, &*s.replacen("\"", "\"\"", 10000), cmd.id)?;

    Ok("COMPLETED.")
}


async fn do_rebalance (cmd :&Cmd) -> Bresult<&'static str> {
    let caps = regex_to_vec(
        r"^/REBALANCE( ([0-9]+[.]?|([0-9]*[.][0-9]{1,2})))?(( [@^]?[A-Z_a-z][-.0-9=A-Z_a-z]* [0-9]+)+)",
        &cmd.msg.to_uppercase() )?;
    if caps.is_empty() { return Ok("SKIP") }

    let mut total :f64 = caps.as_f64(2).unwrap_or(0.0);

    let percents =
        Regex::new(r" ([@^]?[A-Z_a-z][-.0-9=A-Z_a-z]*) ([0-9]+[.]?|([0-9]*[.][0-9]{1,2}))")?
        .captures_iter(&caps.as_string(4)?)
        .map(|cap| (
            cap.get(1).unwrap().as_str().to_string(),
            cap.get(2).unwrap().as_str().parse::<f64>().unwrap() / 100.0
        ))
        .collect::<HashMap<String, f64>>();
    //error!("{:?}", percents);

    let mut positions = getsql!(format!(
        r#"SELECT pos.ticker, ROUND(pos.qty*stonks.price,2) AS value
        FROM (SELECT ticker,qty FROM positions WHERE id={} AND ticker IN ("{}")) pos
        LEFT JOIN stonks ON pos.ticker=stonks.ticker"#,
        cmd.id,
        percents.keys().map(|e|e.to_string()).collect::<Vec<String>>().join("\",\"")))?;

    total += positions.iter().map(|hm|hm.get_f64("value").unwrap()).sum::<f64>();
    info!("rebalance total {}", total);

    for i in 0..positions.len() {
        let ticker = positions[i].get_str("ticker")?;
        let value = positions[i].get_f64("value")?;
        let diff = roundfloat(percents.get(&ticker).unwrap() * total - value, 2);
        positions[i].insert("diff".to_string(), diff.to_string());
    }
    for i in 0..positions.len() {
        if positions[i].get_str("diff")?.chars().nth(0).unwrap() == '-'  {
            info!("rebalance position {:?}", positions[i]);
            send_msg(
                cmd.level(1),
                &format!("Suggested trade: {}-${}",
                    &positions[i].get_str("ticker")?,
                    &positions[i].get_str("diff")?[1..])).await?;
        }
    }
    for i in 0..positions.len() {
        if positions[i].get_str("diff")?.chars().nth(0).unwrap() != '-'  {
            info!("rebalance position {:?}", positions[i]);
            send_msg(
                cmd.level(1),
                &format!("Suggested trade: {}+${}",
                    &positions[i].get_str("ticker")?,
                    &positions[i].get_str("diff")?)).await?;
        }
    }

    Ok("COMPLETED.")
}


async fn do_all(env:&mut Env, body: &web::Bytes) -> Bresult<()> {
    let mut cmd = Cmd::parse_cmd(env, body)?;
    info!("\x1b[33m{:?}", &cmd);

    glogd!("do_echo =>",      do_echo(&cmd).await);
    glogd!("do_help =>",      do_help(&cmd).await);
    glogd!("do_curse =>",     do_curse(&cmd).await);
    glogd!("do_repeat =>",    do_repeat(&cmd).await);
    glogd!("do_like =>",      do_like(&cmd).await);
    glogd!("do_like_info =>", do_like_info(&cmd).await);
    glogd!("do_def =>",       do_def(&cmd).await);
    glogd!("do_sql =>",       do_sql(&cmd).await);
    glogd!("do_quotes => ",   do_quotes(&mut cmd).await);
    glogd!("do_portfolio =>", do_portfolio(&mut cmd).await);
    glogd!("do_yolo =>",      do_yolo(&cmd).await);
    glogd!("do_trade_buy =>", do_trade_buy(&cmd).await);
    glogd!("do_trade_sell =>",do_trade_sell(&cmd).await);
    glogd!("do_exchange_bidask =>", do_exchange_bidask(&cmd).await);
    glogd!("do_orders =>", do_orders(&cmd).await);
    glogd!("do_fmt =>",       do_fmt(&cmd).await);
    glogd!("do_rebalance =>", do_rebalance(&cmd).await);

    // Copy back important altered state for next time.
    env.message_id_read = cmd.env.message_id_read;
    env.message_id_write = cmd.env.message_id_write;
    env.message_buff_write.clear();
    env.message_buff_write.push_str(&cmd.env.message_buff_write);
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////

fn do_schema() -> Bresult<()> {
    let sql = ::sqlite::open("tmbot.sqlite")?;

    sql.execute("
        CREATE TABLE entitys (
            id INTEGER NOT NULL UNIQUE,
            name  TEXT NOT NULL);
    ").map_or_else(gwarn, ginfo);

    sql.execute("
        CREATE TABLE modes (
            id   INTEGER NOT NULL UNIQUE,
            echo INTEGER NOT NULL);
    ").map_or_else(gwarn, ginfo);

    sql.execute("
        CREATE TABLE formats (
            id    INTEGER NOT NULL UNIQUE,
            quote    TEXT NOT NULL,
            position TEXT NOT NULL);
    ").map_or_else(gwarn, ginfo);

    for l in read_to_string("tmbot/users.txt").unwrap().lines() {
        let mut v = l.split(" ");
        let id = v.next().ok_or("User DB malformed.")?;
        let name = v.next().ok_or("User DB malformed.")?.to_string();
        sql.execute( format!("INSERT INTO entitys VALUES ( {}, {:?} )", id, name)).map_or_else(gwarn, ginfo);
        if id.parse::<i64>().unwrap() < 0 {
            sql.execute( format!("INSERT INTO modes VALUES ({}, 2)", id) ).map_or_else(gwarn, ginfo);
        }
    }

    sql.execute("
        CREATE TABLE accounts (
            id    INTEGER  NOT NULL UNIQUE,
            balance FLOAT  NOT NULL);
    ").map_or_else(gwarn, ginfo);

    sql.execute("
        --DROP TABLE stonks;
        CREATE TABLE stonks (
            ticker   TEXT NOT NULL UNIQUE,
            price   FLOAT NOT NULL,
            last    FLOAT NOT NULL,
            market   TEXT NOT NULL,
            hours INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            time  INTEGER NOT NULL,
            title    TEXT NOT NULL);
    ").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO stonks VALUES ( 'TWNK', 14.97, 1613630678, '14.97')").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO stonks VALUES ( 'GOOG', 2128.31, 1613630678, '2,128.31')").map_or_else(gwarn, ginfo);
    //sql.execute("UPDATE stonks SET time=1613630678 WHERE ticker='TWNK'").map_or_else(gwarn, ginfo);
    //sql.execute("UPDATE stonks SET time=1613630678 WHERE ticker='GOOG'").map_or_else(gwarn, ginfo);

    sql.execute("
        --DROP TABLE orders;
        CREATE TABLE orders (
            id   INTEGER  NOT NULL,
            ticker  TEXT  NOT NULL,
            qty    FLOAT  NOT NULL,
            price  FLOAT  NOT NULL,
            time INTEGER  NOT NULL);
    ").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO orders VALUES ( 241726795, 'TWNK', 500, 14.95, 1613544000 )").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO orders VALUES ( 241726795, 'GOOG', 0.25, 2121.90, 1613544278 )").map_or_else(gwarn, ginfo);

    sql.execute("
        CREATE TABLE positions (
            id   INTEGER NOT NULL,
            ticker  TEXT NOT NULL,
            qty    FLOAT NOT NULL,
            price  FLOAT NOT NULL);
    ").map_or_else(gwarn, ginfo);

    sql.execute("
        CREATE TABLE exchange (
            id   INTEGER NOT NULL,
            ticker  TEXT NOT NULL,
            qty    FLOAT NOT NULL,
            price  FLOAT NOT NULL,
            time INTEGER NOT NULL);
    ").map_or_else(gwarn, ginfo);

    Ok(())
}


fn log_header () {
    info!("\x1b[0;31;40m _____ __  __ ____        _   ");
    info!("\x1b[0;33;40m|_   _|  \\/  | __ )  ___ | |_™ ");
    info!("\x1b[0;32;40m  | | | |\\/| |  _ \\ / _ \\| __|");
    info!("\x1b[0;34;40m  | | | |  | | |_) | (_) | |_ ");
    info!("\x1b[0;35;40m  |_| |_|  |_|____/ \\___/ \\__|");
}

////////////////////////////////////////////////////////////////////////////////

async fn main_dispatch (req: HttpRequest, body: web::Bytes) -> HttpResponse {
    log_header();

    let mut env = req.app_data::<web::Data<AMEnv>>().unwrap().lock().unwrap(); // TODO: want to edit this for state

    info!("\x1b[1m{:?}", &env);
    info!("\x1b[35m{}", format!("{:?}", &req.connection_info()).replace(": ", ":").replace("\"", "").replace(",", ""));
    info!("\x1b[35m{}", format!("{:?}", &req).replace("\n", "").replace(r#"": ""#, ":").replace("\"", "").replace("   ", ""));
    info!("body:\x1b[35m{}", from_utf8(&body).unwrap_or(&format!("{:?}", body)));

    do_all(&mut env, &body)
        .await
        .unwrap_or_else(|r| error!("{:?}", r));

    error!("{:#?}", env);
    info!("End.");

    HttpResponse::from("")
}

pub async fn launch() -> Bresult<()> {
    let mut ssl_acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ssl_acceptor_builder.set_private_key_file("key.pem", SslFiletype::PEM)?;
    ssl_acceptor_builder.set_certificate_chain_file("cert.pem")?;

    let botkey           = env::args().nth(1).ok_or("args[1] missing")?;
    let chat_id_default  = env::args().nth(2).ok_or("args[2] missing")?.parse::<i64>()?;
    let dst_hours_adjust = env::args().nth(3).ok_or("args[3] missing")?.parse::<i8>()?;

    if !true { do_schema()? } // Create DB
    if !true { // Hacks and other test code
        let r = fun();
        match r {
            Ok(o) => Ok(o),
            Err(e) => {
                info!("{:?}", e);
                //info!("{:?}", e.is::<std::fmt::Error>());
                //info!("{:?}", e.is::<std::io::Error>());
                Err(e.into())
            }
        }
    } else {

    let amenv =  // Shared between all calls.
        Arc::new(Mutex::new( Env{
            url_api:             "https://api.telegram.org/bot".to_string() + &botkey,
            chat_id_default:     chat_id_default,
            dst_hours_adjust:    dst_hours_adjust,
            quote_delay_minutes: QUOTE_DELAY_MINUTES,
            message_id_read:     0, // TODO edited_message mechanism is broken especially when /echo=1
            message_id_write:    0,
            message_buff_write:  String::new(),
            fmt_quote:           "%q%B%A%C%% %D%E@%F %G%H%I%q".to_string(),
            fmt_position:        "%n%q%A %C%B %D%%%q %q%E%F@%G%q %q%H@%I%q".to_string(),
        } ) );

    Ok( HttpServer::new(
        move ||
        App::new()
        .data(amenv.clone())
        .service(
            web::resource("*")
            .route(
                Route::new()
                .to(main_dispatch) ) ) )
    .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
    .run().await? )

} }

////////////////////////////////////////////////////////////////////////////////
use macros::*;

#[derive(Debug, Hello)]
struct Greetings {
    z:i32,
    err:Box<dyn Error>
}

fn _fun_macros() -> Result<(), Box<dyn std::error::Error>> {
    let g = Greetings{z:42, err:"".into() };
    //Err("oh noe".into())
    //return Err(std::fmt::Error{});
    Err(std::io::Error::from_raw_os_error(0))?;
    Ok(g.hello())
}

fn fun () -> Bresult<()> {
    for a in getsql!("SELECT * FROM stonks WHERE title like ?", "%'%")? {
      println!("{:?}", a);
    }
    Ok(())
}
   
/*
  DROP   TABLE users
  CREATE TABLE users (name TEXT, age INTEGER)
  INSERT INTO  users VALUES ('Alice', 42)
  DELETE FROM  users WHERE age=42 AND name="Oskafunoioi"
  UPDATE       ukjsers SET a=1, b=2  WHERE c=3
*/