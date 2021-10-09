//! # External Chat Service Robot
mod util;  pub use crate::util::*;
mod comm;  use crate::comm::*;
mod srvs;  use crate::srvs::*;
mod db;    use crate::db::*;
use ::std::{
    env,
    cmp::Ordering,
    boxed::Box,
    error::Error,
    mem::transmute,
    collections::{HashMap, HashSet},
    str::{from_utf8, FromStr},
    sync::{Arc, Mutex},
 };
use ::log::*;
use ::regex::{Regex};
use ::datetime::{
    Instant, Duration, LocalDate, LocalTime, LocalDateTime, DatePiece, TimePiece,
    Weekday::{Sunday, Saturday} };
use ::openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use ::actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route,
    client::{Client, Connector} };
use ::macros::*;

////////////////////////////////////////////////////////////////////////////////

const QUOTE_DELAY_SECS :i64 = 60;
const FORMAT_STRING_QUOTE    :&str = "%q%A%B %C%% %D%E@%F %G%H%I%q";
const FORMAT_STRING_POSITION :&str = "%n%q%A %C%B %D%%%q %q%E%F@%G%q %q%H@%I%q";

//////////////////////////////////////////////////////////////////////////////
// Rust primitives enahancements
trait GetI64Or    { fn get_i64_or    (&self, default:i64,  key:&str) -> i64; }
trait GetF64Or    { fn get_f64_or    (&self, default:f64,  key:&str) -> f64; }
trait GetStringOr { fn get_string_or (&self, default:&str, key:&str) -> String; }

impl GetI64Or for HashMap<String, ::sqlite::Value> {
    fn get_i64_or (&self, default:i64, key:&str) -> i64 {
        self.get(key).and_then( ::sqlite::Value::as_integer ).unwrap_or(default)
    }
}

impl GetF64Or for HashMap<String, ::sqlite::Value> {
    fn get_f64_or (&self, default:f64, key:&str) -> f64 {
        self.get(key).and_then( ::sqlite::Value::as_float ).unwrap_or(default)
    }
}

impl GetStringOr for HashMap<String, ::sqlite::Value> {
    fn get_string_or (&self, default:&str, key:&str) -> String {
        self.get(key).and_then( ::sqlite::Value::as_string ).unwrap_or(default).to_string()
    }
}

//////////////////////////////////////// 

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

trait AsStr { fn as_str (&self, i:usize) -> Bresult<&str>; }
impl AsStr for Vec<Option<String>> {
    fn as_str (&self, i:usize) -> Bresult<&str> {
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

////////////////////////////////////////

trait GetI64 { fn get_i64 (&self, k:&str) -> Bresult<i64>; }
impl GetI64 for HashMap<String, String> {
    fn get_i64 (&self, k:&str) -> Bresult<i64> {
        Ok(self
            .get(k).ok_or(format!("Can't find key '{}'", k))?
            .parse::<i64>()?)
    }
}
impl GetI64 for HashMap<String, ::sqlite::Value> {
    fn get_i64 (&self, k:&str) -> Bresult<i64> {
        Ok(self
            .get(k).ok_or(format!("Can't find key '{}'", k))?
            .as_integer().ok_or(format!("Not an integer '{}'", k))?)
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

trait GetStr { fn get_str (&self, key:&str) -> Bresult<String>; }
impl GetStr for HashMap<String, String> {
    fn get_str (&self, key:&str) -> Bresult<String> {
        Ok(self
            .get(key)
            .ok_or(format!("Can't find key '{}'", key))?
            .to_string() )
    }
}
impl GetStr for HashMap<String, ::sqlite::Value> {
    fn get_str (&self, key:&str) -> Bresult<String> {
        Ok(self
            .get(key).ok_or(format!("Can't find key '{}'", key))?
            .as_string().ok_or(format!("Not a string '{}'", key))?
            .to_string())
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
    fn as_str (&self, i:usize) -> Bresult<&str> {
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
    dst_hours_adjust: i8,
    now: LocalDateTime,
) -> Bresult<(LocalDateTime, LocalDateTime)> {
    // Absolute time (UTC) when US pre-markets open 1AMPT / 0900Z|0800Z
    let start_time = LocalTime::hm(9 - dst_hours_adjust, 0)?;  // TODO create function describing the hour maths
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

fn trading_hours_p (dst_hours_adjust:i8, now:i64) -> Bresult<bool>{
    let now = LocalDateTime::at(now);
    let (time_open, time_close) = most_recent_trading_hours(dst_hours_adjust, now)?;
    Ok(time_open <= now && now <= time_close)
}

fn update_ticker_p (
    envstruct: &EnvStruct,
    cached: i64,
    now:    i64,
    traded_all_day: bool
) -> Bresult<bool> {
    info!("update_ticker_p  cached:{}  now:{}  traded_all_day:{}", cached, now, traded_all_day);

    if traded_all_day { return Ok(cached+(envstruct.quote_delay_secs) < now) }

    let lookup_throttle_duration = Duration::of( 60 * envstruct.quote_delay_secs ); // Lookup tickers every 2 minutes

    // Do everything in DateTime
    let cached = LocalDateTime::at(cached);
    let now = LocalDateTime::at(now);

    let (time_open, time_close) = most_recent_trading_hours(envstruct.dst_hours_adjust, now)?;

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
    if n < 0.0001 {
        format!("0.0")
    } else if n < 1.0 {
        let mut np = format!("{:.4}", n);
        np = regex_to_hashmap(r"^0(.*)$", &np).map_or(np, |c| c["1"].to_string() ); // strip leading 0
        np = regex_to_hashmap(r"^(.*)0$", &np).map_or(np, |c| c["1"].to_string() ); // strip trailing 0
        regex_to_hashmap(r"^(.*)0$", &np).map_or(np, |c| c["1"].to_string() ) // strip trailing 0
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
fn deref_ticker (dbconn:&Connection, s:&str) -> Option<String> {
    // Expects:  @shrewm   Returns: Some(308188500)
    if &s[0..1] == "@" {
        // Quote is ID of whomever this nick matches
        getsql!(dbconn, "SELECT id FROM entitys WHERE name=?", &s[1..])
            .ok()
            .filter( |v| 1 == v.len() )
            .map( |v| v[0].get("id").unwrap().to_string() )
    } else { None }
}

// Transforms {USER_ID} to @user_nickname
fn reference_ticker (dbconn:&Connection, t :&str) -> String {
    if is_self_stonk(t) {
        getsql!(dbconn, "SELECT name FROM entitys WHERE id=?", t) // Result<Vec, Err>
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

pub fn time2datetimestr (time:i64) -> String {
    let dt = LocalDateTime::at(time);
    format!("{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        dt.year(), 1+dt.month().months_from_january(),
        dt.day(), dt.hour(), dt.minute(), dt.second() )
}

pub fn time2timestr (time:i64) -> String {
    let dt = LocalDateTime::at(time);
    format!("{:02}:{:02}:{:02}", dt.hour(), dt.minute(), dt.second() )
}

////////////////////////////////////////////////////////////////////////////////
/// Helpers on complex types

fn get_bank_balance (cmdstruct:&CmdStruct) -> Bresult<f64> {
    let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
    let id = cmdstruct.id;
    let res = getsql!(dbconn, "SELECT * FROM accounts WHERE id=?", id)?;
    Ok(
        if res.is_empty() {
            let sql = getsql!(dbconn, "INSERT INTO accounts VALUES (?, ?)", id, 1000.0)?;
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

pub fn sql_table_order_insert (dbconn:&Connection, id:i64, ticker:&str, qty:f64, price:f64, time:i64) -> Bresult<Vec<HashMap<String, String>>> {
    getsql!(dbconn, "INSERT INTO orders VALUES (?, ?, ?, ?, ?)", id, ticker, qty, price, time)
}

fn create_schema() -> Bresult<()> {
    let conn = Connection::new(&"tmbot.sqlite")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS entitys (
            id INTEGER NOT NULL UNIQUE,
            name  TEXT NOT NULL);")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS modes (
            id   INTEGER NOT NULL UNIQUE,
            echo INTEGER NOT NULL);")?;

    //for l in read_to_string("tmbot/users.txt").unwrap().lines() {
    //    let mut v = l.split(" ");
    //    let id = v.next().ok_or("User DB malformed.")?.parse::<i64>()?;
    //    let name = v.next().ok_or("User DB malformed.")?.to_string();
    //    getsql!(&conn,"INSERT INTO entitys VALUES (?, ?)", id, &*name)?;
    //    if id < 0 {
    //        getsql!(&conn,"INSERT INTO modes VALUES (?, 2)", id)?;
    //    }
    //}

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS formats (
            id    INTEGER NOT NULL UNIQUE,
            quote    TEXT NOT NULL,
            position TEXT NOT NULL);
    ")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS accounts (
            id    INTEGER  NOT NULL UNIQUE,
            balance FLOAT  NOT NULL);
    ")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS stonks (
            ticker   TEXT NOT NULL UNIQUE,
            price   FLOAT NOT NULL,
            last    FLOAT NOT NULL,
            market   TEXT NOT NULL,
            hours INTEGER NOT NULL,
            exchange TEXT NOT NULL,
            time  INTEGER NOT NULL,
            title    TEXT NOT NULL);
    ")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS orders (
            id   INTEGER  NOT NULL,
            ticker  TEXT  NOT NULL,
            qty    FLOAT  NOT NULL,
            price  FLOAT  NOT NULL,
            time INTEGER  NOT NULL);
    ")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS positions (
            id   INTEGER NOT NULL,
            ticker  TEXT NOT NULL,
            qty    FLOAT NOT NULL,
            price  FLOAT NOT NULL);
    ")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS exchange (
            id   INTEGER NOT NULL,
            ticker  TEXT NOT NULL,
            qty    FLOAT NOT NULL,
            price  FLOAT NOT NULL,
            time INTEGER NOT NULL);
    ")?;

    // time 0 midnight
    // time 44 44seconds   33m 33 minutes   33m44 33 min and 44 seconds
    // when = 0:once 1:daily
    // 1h33m44
    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS schedules (
            id   INTEGER NOT NULL,
            at   INTEGER NOT NULL,
            time INTEGER NOT NULL,
            cmd     TEXT NOT NULL);")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS likes (
            id    INTEGER NOT NULL UNIQUE,
            likes INTEGER NOT NULL);")?;

    Ok(())
} // create_schema

////////////////////////////////////////////////////////////////////////////////
/// Blobs

#[derive(Debug)]
pub struct Entity {
    id: i64,
    name: String,
    balance: f64,
    echo: i64,
    likes: i64,
    quote: String,
    position: String
}

#[derive(Debug)]
pub struct EnvStruct {
    url_api:          String, // Telgram API URL
    dbconn:           Connection, // SQLite connection
    quote_delay_secs: i64,    // Delay between remote stock quote queries
    dst_hours_adjust: i8,     // 1 = DST, 0 = ST
    time_scheduler:   i64,    // Time the scheduler last ran
    entitys:          HashMap<i64, Entity>
}

type Env = Arc<Mutex<EnvStruct>>;

impl From<EnvStruct> for Env {
    fn from (envstruct:EnvStruct) -> Self {
        Arc::new(Mutex::new(envstruct))
    }
}

impl EnvStruct {
    fn fmt_str_quote (&self, id:i64) -> String {
        let s = &self.entitys.get(&id).unwrap().quote;
        if s.is_empty() { FORMAT_STRING_QUOTE } else { s }.to_string()
    }
    fn fmt_str_position (&self, id:i64) -> String {
        let s = &self.entitys.get(&id).unwrap().position;
        if s.is_empty() { FORMAT_STRING_POSITION } else { s }.to_string()
    }
}

impl EnvStruct {
fn new (
    mut argv: std::env::Args
) -> Bresult<Env> {
    let url_api = format!(
        "https://api.telegram.org/bot{}",
        env::var_os( &argv.nth(1).ok_or("args[1] missing")? )
            .ok_or("can't resolve telegram api key")?
            .to_str()
            .unwrap());
    let dbconn = Connection::new(&argv.next().ok_or("args[2] missing")?)?; 
    let mut entitys = HashMap::new();
    for hm in
        getsqlraw!(dbconn,
            r"SELECT entitys.id, entitys.name, accounts.balance, modes.echo, likes.likes, formats.quote, formats.position
            FROM entitys
            LEFT JOIN accounts ON entitys.id = accounts.id
            LEFT JOIN modes    ON entitys.id = modes.id
            LEFT JOIN likes    ON entitys.id = likes.id
            LEFT JOIN formats  ON entitys.id = formats.id")?
    {
        //warn!("{:#?}", hm);
        let id = hm.get_i64("id")?;
        entitys.insert(id,
            Entity{
                id,
                name:    hm.get_str("name")?,
                balance: hm.get_f64_or(0.0, "balance"),
                echo:    hm.get_i64_or(2, "echo"),
                likes:   hm.get_i64_or(0, "likes"),
                quote:   hm.get_string_or("", "quote"),
                position:hm.get_string_or("", "position")} );
    }
    Ok(Env::from(EnvStruct{
        url_api, dbconn,
        dst_hours_adjust:   argv.next().ok_or("args[3] missing")?.parse::<i8>()?,
        quote_delay_secs:   QUOTE_DELAY_SECS,
        time_scheduler:     Instant::now().seconds(),
        entitys
    }))
} }

////////////////////////////////////////

#[derive(Debug)]
pub struct CmdStruct { // Represents in incoming Telegram message and session/service state.
    env: Env,
    telegram: Telegram,
    // Telegram incoming message details
    now: i64,
    id: i64, // Entity who sent msg                    message.from.id
    at: i64, // Channel (group/entity) msg sent to     message.chat.id
    to: i64, // Entity replied to ('at' if non-reply)  message.reply_to_message.from.id
    message_id: i64,  // Message's unique id in channel.  TODO map this to msg_id so edits are reflected to the same response msg.
    message: String, // The incoming message
    id_level: i64, // Echo level [0,1,2] for each id.  Higher can't write to lower.
    at_level: i64,
    // Telegram outgoing response message details
    msg_id: Option<i64>, // previous message_id (for response updating/editing)
    msg: String
}

impl<'a> From<&'a CmdStruct> for MsgCmd<'a> {
    fn from (cmdstruct:&'a CmdStruct) -> MsgCmd<'a> {
        MsgCmd {
            id:       cmdstruct.id,
            at:       cmdstruct.at,
            id_level: cmdstruct.id_level,
            at_level: cmdstruct.at_level,
            dm_id:  None,
            markdown: false,
            msg_id: cmdstruct.msg_id,
            msg: &cmdstruct.msg
        }
    }
}
impl<'a> From<&'a mut CmdStruct> for MsgCmd<'a> {
    fn from (cmdstruct:&'a mut CmdStruct) -> MsgCmd<'a> {
        MsgCmd{
            id:       cmdstruct.id,
            at:       cmdstruct.at,
            id_level: cmdstruct.id_level,
            at_level: cmdstruct.at_level,
            dm_id:  None,
            markdown: false,
            msg_id: cmdstruct.msg_id,
            msg: &cmdstruct.msg
        }
    }
}

impl CmdStruct {
    // Creates a Cmd object from Env and Telegram message body.
    fn newcmdstruct(env:Env, body: &web::Bytes) -> Bresult<CmdStruct> {
        let json: Value = bytes2json(&body)?;
        let inline_query = &json["inline_query"];
        let edited_message = &json["edited_message"];
        let message = if edited_message.is_object() { edited_message } else { &json["message"] };
        let message_id = getin_i64(message, &["message_id"]).unwrap_or(0);
        let (id, at, to, message) =
            if inline_query.is_object() { // Inline queries are DMs so no other associated channels
                let id = getin_i64(inline_query, &["from", "id"])?;
                let message = getin_str(inline_query, &["query"])?.to_string();
                (id, id, id, message)
            } else if message.is_object() { // An incoming message could be a reply or normal.
                let id = getin_i64(message, &["from", "id"])?;
                let at = getin_i64(message, &["chat", "id"])?;
                let to = getin_i64_or(at, &message, &["reply_to_message", "from", "id"]);
                let message = getin_str(message, &["text"])?.to_string();
                (id, at, to, message) // Normal message
            } else { Err("Nothing to do.")? };

        CmdStruct::new_cmdstruct(&env, Instant::now().seconds(), id, at, to, message_id, &message)
    }

    // create a basic CmdStruct
    fn new_cmdstruct(env:&Env, now:i64, id:i64, at:i64, to:i64, message_id:i64, message:&str) -> Bresult<CmdStruct> {
        let envstruct = env.lock().unwrap();
        Ok(CmdStruct{
            env: env.clone(),
            telegram: Telegram::new(envstruct.url_api.to_string())?,
            now, id, at, to, message_id,
            message: message.to_string(),
            id_level: envstruct.entitys.get(&id).unwrap().echo,
            at_level: envstruct.entitys.get(&at).unwrap().echo,
            msg_id: None,
            msg: String::new()
        })
    }

    fn push_msg (&mut self, s:&str) -> &mut Self {
        self.msg.push_str(s);
        self
    }
    fn set_msg (&mut self, s:&str) -> &mut Self {
        self.msg.clear();
        self.msg.push_str(s);
        self
    }
    async fn send_msg (&self) -> Bresult<i64> {
        self.telegram.send_msg(self.into()).await
    }
    async fn send_msg_id (&self) -> Bresult<i64> {
        self.telegram.send_msg( MsgCmd::from(self).dm(self.id) ).await
    }
    async fn send_msg_markdown (&self) -> Bresult<i64> {
        self.telegram.send_msg( MsgCmd::from(self).markdown() ).await
    }
    async fn send_msg_id_markdown (&self) -> Bresult<i64> {
        self.telegram.send_msg( MsgCmd::from(self).dm(self.id).markdown() ).await
    }
    async fn _send_msg_dm_markdown (&self, id:i64) -> Bresult<i64> {
        self.telegram.send_msg( MsgCmd::from(self).dm(id).markdown() ).await
    }
} // CmdStruct

////////////////////////////////////////

#[derive(Debug)]
pub struct Quote {
    pub env: Env,
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

impl Quote { // Query internet for ticker details
    async fn new_market_quote (env:Env, ticker: &str) -> Bresult<Self> {
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
            env,
            ticker:  ticker.to_string(),
            price, last,
            amount:  roundqty(price-last),
            percent: percentify(last,price),
            market:  details[0].2.to_string(),
            hours, exchange, title,
            updated: true})
    } // Quote::new_market_quote
}

impl Quote {// Query local cache or internet for ticker details
    async fn get_market_quote (env:Env, ticker:&str, now:i64) -> Bresult<Self> {
        // Make sure not given referenced stonk. Expected symbols:  GME  308188500   Illegal: @shrewm
        if &ticker[0..1] == "@" { Err("Illegal ticker")? }
        let is_self_stonk = is_self_stonk(ticker);

        let (res, is_in_table, is_cache_valid) = {
            let envstruct = env.lock().unwrap();
            let res = getsql!(
                envstruct.dbconn,
                "SELECT ticker, price, last, market, hours, exchange, time, title FROM stonks WHERE ticker=?",
                ticker)?;
            let is_in_table = !res.is_empty();
            let is_cache_valid =
                is_in_table && (is_self_stonk || {
                    let hm = &res[0];
                    let timesecs       = hm.get_i64("time")?;
                    let traded_all_day = 24 == hm.get_i64("hours")?;
                    !update_ticker_p(&envstruct, timesecs, now, traded_all_day)?
                });
            (res, is_in_table, is_cache_valid)
        };

        let quote =
            if is_cache_valid { // Is in cache so use it
                let hm = &res[0];
                let price = hm.get_f64("price")?;
                let last = hm.get_f64("last")?;
                Quote{
                    env: env.clone(),
                    ticker: hm.get_str("ticker")?,
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
                    env: env.clone(),
                    ticker: ticker.to_string(),
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
                Quote::new_market_quote(env.clone(), ticker).await?
            };
        info!("quote = {:?}", quote);

        if !is_self_stonk { // Cached FNFTs are only updated during trading/settling.
            let dbconn = &env.lock().unwrap().dbconn;
            if !is_in_table {
                getsql!(dbconn, "INSERT INTO stonks VALUES(?,?,?,?,?,?,?,?)",
                    &*quote.ticker, quote.price, quote.last, &*quote.market, quote.hours, &*quote.exchange, now, &*quote.title)?;
            } else if !is_cache_valid {
                getsql!(dbconn, "UPDATE stonks SET price=?, last=?, market=?, time=? WHERE ticker=?",
                    quote.price, quote.last, &*quote.market, now, quote.ticker.as_str())?;
            }
        }

        Ok(quote)
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

impl Quote { // Format the quote/ticker using its format string IE: 🟢ETH-USD@2087.83! ↑48.49 2.38% Ethereum USD CCC
    fn format_quote (&self, id:i64) -> Bresult<String> {
        let envstruct = &self.env.lock().unwrap();
        let gain_glyphs = amt_as_glyph(self.amount);
        Ok(Regex::new("(?s)(%([A-Za-z%])|.)").unwrap()
            .captures_iter(&envstruct.fmt_str_quote(id))
            .fold(String::new(), |mut s, cap| {
                if let Some(m) = cap.get(2) {
                    match m.as_str() {
                    "A" => s.push_str(gain_glyphs.1), // Arrow
                    "B" => s.push_str(&format!("{}", money_pretty(self.amount.abs()))), // inter-day delta
                    "C" => s.push_str(&percent_squish(self.percent.abs())), // inter-day percent
                    "D" => s.push_str(gain_glyphs.0), // red/green light
                    "E" => s.push_str(&reference_ticker(&envstruct.dbconn, &self.ticker).replacen("_", "\\_", 10000)),  // ticker symbol
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
            } )
        )
    }
}

////////////////////////////////////////

#[derive(Debug)]
struct Position {
    quote: Option<Quote>,
    id:    i64,
    ticker:String,
    qty:   f64,
    price: f64,
    fmt_position: String
}

impl Position {
    async fn update_quote(&mut self, cmdstruct: &CmdStruct) -> Bresult<&Position> {
        self.quote = Some(Quote::get_market_quote(cmdstruct.env.clone(), &self.ticker, cmdstruct.now).await?);
        Ok(self)
    }
}

impl Position {
    // Return vector instead of option in case of box/short positions.
    fn get_position (cmdstruct:&CmdStruct, id: i64, ticker: &str) -> Bresult<Vec<Position>> {
        let envstruct = cmdstruct.env.lock().unwrap();
        let dbconn = &envstruct.dbconn;
        Ok(getsql!(dbconn, "SELECT qty,price FROM positions WHERE id=? AND ticker=?", id, ticker )?
        .iter()
        .map( |pos|
            Position {
                quote:  None,
                id,
                ticker: ticker.to_string(),
                qty:    pos.get_f64("qty").unwrap(),
                price:  pos.get_f64("price").unwrap(),
                fmt_position: envstruct.fmt_str_position(id)
            } )
        .collect())
    }
}

impl Position {
    fn get_users_positions (cmdstruct: &CmdStruct) -> Bresult<Vec<Position>> {
        let id = cmdstruct.id;
        let envstruct = cmdstruct.env.lock().unwrap();
        let dbconn = &envstruct.dbconn;
        Ok(getsql!(dbconn, "SELECT ticker,qty,price FROM positions WHERE id=?", id)?
        .iter()
        .map( |pos|
            Position {
                quote: None,
                id,
                ticker: pos.get_str("ticker").unwrap(),
                qty: pos.get_f64("qty").unwrap(),
                price: pos.get_f64("price").unwrap(),
                fmt_position: envstruct.fmt_str_position(id)
            } )
        .collect())
    }
}

impl Position {
    async fn query(cmdstruct:&CmdStruct, id:i64, ticker:&str) -> Bresult<Position> {
        let mut hm = Position::get_position(cmdstruct, id, ticker)?;

        if 2 <= hm.len() {
            Err(format!("For {} ticker {} has {} positions, expect 0 or 1", id, ticker, hm.len()))?
        }

        let mut hm = // Consider the quote or a 0 quantity quote
            if 0 == hm.len() {
                Position {
                    quote: None,
                    id,
                    ticker: ticker.to_string(),
                    qty: 0.0,
                    price: 0.0,
                    fmt_position: cmdstruct.env.lock().unwrap().fmt_str_position(id) }
            } else {
                hm.pop().unwrap()
            };
        hm.update_quote(cmdstruct).await?;
        Ok(hm)
    }
}

impl Position { // Format the position using its format string.
    fn format_position (&mut self, dbconn:&Connection) -> Bresult<String> {
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
        .captures_iter(&self.fmt_position)
        .fold( String::new(), |mut s, cap| {
            if let Some(m) = cap.get(2) { match m.as_str() {
                "A" => s.push_str( &format!("{:.2}", roundcents(value)) ), // value
                "B" => s.push_str( &round(gain.abs())), // gain
                "C" => s.push_str( gain_glyphs.1), // Arrow
                "D" => s.push_str( &percent_squish(gain_percent)), // gain%
                "E" => s.push_str( gain_glyphs.0 ), // Color
                "F" => s.push_str( &reference_ticker(dbconn, &self.ticker).replacen("_", "\\_", 10000) ), // Ticker
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
struct Trade<'a> {
    cmdstruct: &'a mut CmdStruct,
    ticker: String,
    action: char, // '+':buy '-':sell
    is_dollars: bool, // amt is dollars or quantity
    amt: Option<f64>
}

impl<'a> Trade<'a> {
    fn new (cmdstruct:&'a mut CmdStruct) -> Option<Self> {
        //                         _____ticker____  _+-_  _$_   ____________amt_______________
        match regex_to_hashmap(r"^([A-Za-z0-9^.-]+)([+-])([$])?([0-9]+\.?|([0-9]*\.[0-9]{1,4}))?$", &cmdstruct.message) {
            Some(caps) =>
                Some(Self {
                    cmdstruct,
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
// Valid "ticker" strings:  GME, 308188500, @shrewm, local level 2 quotes.  Used by do_quotes only
async fn get_quote_pretty (cmdstruct:&CmdStruct, ticker :&str) -> Bresult<String> {
    let (ticker, bidask) = { // Sub block so the Cmd lock is released/dropped before .await
        let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
        let ticker = deref_ticker(dbconn, ticker).unwrap_or(ticker.to_string());
        let bidask =
            if is_self_stonk(&ticker) {
                let mut asks = format!("*Asks:*");
                for ask in getsql!(dbconn, "SELECT -qty AS qty, price FROM exchange WHERE qty<0 AND ticker=? order by price;", &*ticker)? {
                    asks.push_str(
                        &format!(" `{}@{}`",
                        num_simp(ask.get("qty").unwrap()),
                        ask.get_f64("price")?));
                }
                let mut bids = format!("*Bids:*");
                for bid in getsql!(dbconn, "SELECT qty, price FROM exchange WHERE 0<qty AND ticker=? order by price desc;", &*ticker)? {
                    bids.push_str(
                        &format!(" `{}@{}`",
                        num_simp(bid.get("qty").unwrap()),
                        bid.get_f64("price")?));
                }
                format!("\n{}\n{}", asks, bids)
            } else {
                "".to_string()
            };
        (ticker, bidask)
    };

    let quote = Quote::get_market_quote(cmdstruct.env.clone(), &ticker, cmdstruct.now).await?;

    Ok(format!("{}{}", quote.format_quote(cmdstruct.id)?, bidask))
} // get_quote_pretty


////////////////////////////////////////////////////////////////////////////////
// Bot's Do Handlers -- The Meat And Potatos.  The Bread N Butter.  The Works.
////////////////////////////////////////////////////////////////////////////////

async fn do_echo (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {

    let caps :Vec<Option<String>> = regex_to_vec("^/echo ?([0-9]+)?", &cmdstruct.message)?;
    if caps.is_empty() { return Ok("SKIP") }

    let msg = match caps.as_i64(1) {
        Ok(echo) => { // Update existing echo level
            if 2 < echo {
                let msg = "*echo level must be 0…2*";
                cmdstruct.push_msg(&msg).send_msg().await?;
                Err(msg)?
            }
            let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
            getsql!(dbconn, "UPDATE modes set echo=? WHERE id=?", echo, cmdstruct.at)?;
            format!("`echo {} set verbosity {:.0}%`", echo, echo as f64/0.02)
        }
        Err(_) => { // Create or return existing echo level
            let mut echo = 2;
            let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
            let rows = getsql!(dbconn, "SELECT echo FROM modes WHERE id=?", cmdstruct.at)?;
            if rows.len() == 0 { // Create/set echo level for this channel
                getsql!(dbconn, "INSERT INTO modes values(?, ?)", cmdstruct.at, echo)?;
            } else {
                echo = rows[0].get_i64("echo")?;
            }
            format!("`echo {} verbosity at {:.0}%`", echo, echo as f64/0.02)
        }
    };
    cmdstruct.push_msg(&msg).send_msg().await?;
    Ok("COMPLETED.")
}


pub async fn do_help (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {
    //let cmdl = cmd.lock().unwrap();
    //let cmdstruct = cmdl.deref();
    if regex_to_vec(r"/help", &cmdstruct.message)?.is_empty() { return Ok("SKIP") }
    let delay = cmdstruct.env.lock().unwrap().quote_delay_secs;
    let msg = format!(
r#"`          ™Bot Commands          `
`/echo 2` `Echo level (verbose 2…0 quiet)`
`/say hi` `™Bot will say "hi"`
`word: ` `Definition lookup`
`+1    ` `Like someone's post (via reply)`
`+?    ` `Like leaderboard`
`/stonks [sort]` `Your Stonkfolio`
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
`/rebalance -9.99 AMC 40 QQQ 60 ...`
   `rebalances AMC/40% QQQ/60%, opt adj -$9.99`
`/fmt [?]     ` `Show format strings, help`
`/fmt [qp] ...` `Set quote/position fmt str`
`/schedule [time]` `List jobs, delete job at time`
`/schedule [ISO-8601] [1h][2m][3][*] CMD` `schedule CMD now or ISO-8601 GMT o'clock, offset 1h 2m 3s, * repeat daily`"#, delay);
    cmdstruct.push_msg(&msg).send_msg_markdown().await?;
    Ok("COMPLETED.")
}

async fn do_curse (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {
    if Regex::new(r"/curse").unwrap().find(&cmdstruct.message).is_none() { return Ok("SKIP") }
    cmdstruct
        .push_msg(["shit", "piss", "fuck", "cunt", "cocksucker", "motherfucker", "tits"][::rand::random::<usize>()%7])
        .send_msg()
        .await?;
    Ok("COMPLETED.")
}

async fn do_say (cmdstruct: &mut CmdStruct) -> Bresult<&'static str> {
    let rev = regex_to_vec(r"^/say (.*)$", &cmdstruct.message)?;
    if rev.is_empty() { return Ok("SKIP".into()) }
    cmdstruct.push_msg(rev.as_str(1)?).send_msg().await?;
    Ok("COMPLETED.")
}

async fn do_like (cmdstruct: &mut CmdStruct) -> Bresult<String> {
    let amt =
        match Regex::new(r"^([+-])1")?.captures(&cmdstruct.message) {
            None => return Ok("SKIP".into()),
            Some(cap) =>
                if cmdstruct.id == cmdstruct.to { return Ok("SKIP self plussed".into()); }
                else if &cap[1] == "+" { 1 } else { -1 }
    };


    let (fromname, likes, toname) = {
        let mut envstruct = cmdstruct.env.lock().unwrap();

        let likes :i64 = {
          let entity = envstruct.entitys.get_mut(&cmdstruct.to).unwrap();
          entity.likes = entity.likes + amt;
          entity.likes
        };

        let entitys = &envstruct.entitys;
        getsql!(&envstruct.dbconn, "INSERT OR REPLACE INTO likes VALUES (?, ?)", cmdstruct.to, likes)?;
        (entitys.get(&cmdstruct.id).unwrap().name.to_string(),
        likes,
        entitys.get(&cmdstruct.to).unwrap().name.to_string() )
    };

    cmdstruct
        .push_msg(&format!("{}{}{}", fromname, num2heart(likes), toname))
        .send_msg()
        .await?;
    Ok("COMPLETED.".into())
}

async fn do_like_info (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {
    if cmdstruct.message != "+?" { return Ok("SKIP"); }

    let mut likes :Vec<(i64, String)>=
        cmdstruct.env.lock().unwrap().entitys.iter().map( |(_,e)| (e.likes, e.name.clone()) ).collect();
    likes.sort_by(|a,b| b.0.cmp(&a.0));

    cmdstruct
        .push_msg(
            &likes
            .iter()
            .map( |(likecount, username)| format!("{}{} ", username, num2heart(*likecount)) )
            .collect::<Vec<String>>().join(""))
        .send_msg().await?;
    Ok("COMPLETED.")
}

async fn do_def (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {
    let cap = regex_to_vec(r"^([A-Za-z-]+):$", &cmdstruct.message)?;
    if cap.is_empty() { return Ok("SKIP"); }
    let word = cap.as_str(1)?;

    info!("looking up {:?}", word);

    let mut msg = String::new();

    // Definitions

    let defs = get_definition(&word).await?;

    if defs.is_empty() {
        cmdstruct
            .push_msg(&format!("*{}* def is empty", &word))
            .send_msg_id_markdown()
            .await?;
    } else {
        msg.push_str( &format!("*{}", &word) );
        if 1 == defs.len() { // If multiple definitions, leave off the colon
            msg.push_str( &format!(":* {}", defs[0].to_string().replacen("`", "\\`", 10000)));
        } else {
            msg.push_str( &format!(" ({})* {}", 1, defs[0].to_string().replacen("`", "\\`", 10000)));
            for i in 1..std::cmp::min(4, defs.len()) {
                msg.push_str( &format!(" *({})* {}", i+1, defs[i].to_string().replacen("`", "\\`", 10000)));
            }
        }
    }

    // Synonyms

    let mut syns = get_syns(&word).await?;

    if syns.is_empty() {
        cmdstruct
            .push_msg(&format!("*{}* syns is empty", &word))
            .send_msg_id_markdown()
            .await?;
    } else {
        if msg.is_empty() {
            msg.push_str( &format!("*{}:* _", &word) );
        } else {
            msg.push_str("\n_");
        }
        syns.truncate(12);
        msg.push_str( &syns.join(", ") );
        msg.push_str("_");
    }

    if !msg.is_empty() {
        cmdstruct.push_msg(&msg).send_msg_markdown().await?;
    }
    Ok("COMPLETED.")
}

async fn do_sql (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {
    if cmdstruct.id != 308188500 { return Ok("do_sql invalid user"); }
    let rev = regex_to_vec("^(.*)ß$", &cmdstruct.message)?;
    let sqlexpr = if rev.is_empty() { return Ok("SKIP") } else { rev.as_str(1)? };
    let sqlres = {
        let envstruct = cmdstruct.env.lock().unwrap();
        getsql!(&envstruct.dbconn, sqlexpr)
    };
    let results =
        match sqlres {
            Err(e) => {
                cmdstruct.push_msg(&format!("{}", e)).send_msg().await?;
                Err(e)?
            }
            Ok(r) => r
        };

    let msg = // SQL rows to single string.
        results.iter().fold(
            String::new(),
            |mut b, vv| {
                vv.iter().for_each( |(k,v)| b += &format!(" {}:{}", k, v) );
                b+"\n"
            } );

    cmdstruct
        .push_msg(if msg.is_empty() { &"empty results" } else { &msg })
        .send_msg().await?;
    Ok("COMPLETED.")
}

async fn do_quotes (cmdstruct :&mut CmdStruct) -> Bresult<&'static str> {
    let tickers = extract_tickers(&cmdstruct.message);
    if tickers.is_empty() { return Ok("SKIP") }

    // Send/update to the same message to reduce clutter
    cmdstruct.msg_id = Some(cmdstruct.push_msg("…").send_msg().await?);
    cmdstruct.set_msg("");

    let mut found_tickers = false;
    for ticker in &tickers {
        // Catch error and continue looking up tickers
        match get_quote_pretty(&cmdstruct, ticker).await {
            Ok(res) => {
                info!("get_quote_pretty {:?}", res);
                cmdstruct.push_msg(&format!("{}\n", res)).send_msg_markdown().await?;
                found_tickers = true;
            },
            e => { glogd!("get_quote_pretty => ", e); }
        }
    }

    // Notify if no results (message not saved)
    if !found_tickers {
        cmdstruct.set_msg(&"No quotes found").send_msg_markdown().await?;
    }
    Ok("COMPLETED.")
}

async fn do_portfolio (cmdstruct: &mut CmdStruct) -> Bresult<&'static str> {
    let caps = regex_to_vec(r"(?i)/stonks( sort)?", &cmdstruct.message)?;
    if caps.is_empty() { return Ok("SKIP"); }

    let dosort = !caps[1].is_none();
    let positions = Position::get_users_positions(&cmdstruct)?;

    cmdstruct.msg_id = Some(cmdstruct.push_msg("…").send_msg().await?);
    cmdstruct.set_msg("");

    let mut total = 0.0;
    let mut positions_table :Vec<(f64,String)> = Vec::new();
    for mut pos in positions {
        if !is_self_stonk(&pos.ticker) {
            pos.update_quote(&cmdstruct).await?;
            info!("{} position {:?}", cmdstruct.id, &pos);
            let pretty_position = pos.format_position(&cmdstruct.env.lock().unwrap().dbconn)?;

            if dosort {
                let qty = pos.qty;
                let cost = pos.price;
                let price = pos.quote.as_ref().unwrap().price;
                let basis = qty*cost;
                let value = qty*price;
                let gain = value - basis;
                positions_table.push( (gain, pretty_position) );
                positions_table.sort_by( |a,b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Less) );
                cmdstruct.set_msg( // Refresh message with sorted stonks
                    &positions_table.iter()
                        .map( |(_,s)| s.to_string())
                        .collect::<Vec<String>>().join(""));
            } else {
                cmdstruct.push_msg(&pretty_position);
            }

            total += pos.qty * pos.quote.unwrap().price;
            cmdstruct.send_msg_markdown().await?;
        }
    }

    let cash = get_bank_balance(&cmdstruct)?;
    cmdstruct
        .push_msg(&format!("\n`{:7.2}``Cash`    `YOLO``{:.2}`\n", roundcents(cash), roundcents(total+cash)))
        .send_msg_markdown()
        .await?;
    Ok("COMPLETED.")
}

// Handle: /yolo
async fn do_yolo (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {

    if regex_to_vec(r"/yolo", &cmdstruct.message)?.is_empty() { return Ok("SKIP"); }

    cmdstruct.msg_id = Some(cmdstruct.push_msg("working...").send_msg().await?);

    let rows = {
        let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
        getsql!(dbconn, "\
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
            GROUP BY ticker")?
    };

    // Update all user-positioned tickers
    for row in rows {
        let ticker = row.get("ticker").unwrap();
        //working_message.push_str(ticker);
        //working_message.push_str("...");
        //send_edit_msg(cmd, message_id, &working_message).await?;
        info!("Stonk \x1b[33m{:?}", Quote::get_market_quote(cmdstruct.env.clone(), ticker, cmdstruct.now).await?);
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

    let sql_results = {
        let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
        getsql!(dbconn, &sql)?
    };

    // Build and send response string

    let mut msg = "*YOLOlians*".to_string();
    for row in sql_results {
        msg.push_str( &format!(" `{:.2}@{}`",
            row.get_f64("yolo")?,
            row.get("name").unwrap()) );
    }
    cmdstruct.set_msg(&msg).send_msg_markdown().await?;
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
struct TradeBuy<'a> {
    qty:f64,
    price:f64,
    bank_balance: f64,
    hours: i64,
    trade: Trade<'a>
}

impl<'a> TradeBuy<'a> {
    async fn new (
        trade:Trade<'a>
    ) -> Bresult<TradeBuy<'a>> {
        let (qty, price, bank_balance, hours) = {
            let quote = Quote::get_market_quote(trade.cmdstruct.env.clone(), &trade.ticker, trade.cmdstruct.now).await?;
            if quote.exchange == "PNK" {
                trade.cmdstruct.push_msg("`OTC / PinkSheet untradeable`").send_msg_markdown().await?;
                Err("OTC / PinkSheet Verboten Stonken")?
            }
            let price = quote.price;
            let bank_balance = get_bank_balance( &trade.cmdstruct )?;
            let hours = quote.hours;
            let qty = match trade.amt {
                Some(amt) => if trade.is_dollars { amt/price } else { amt },
                None => roundqty(bank_balance / price) // Buy as much as possible
            };
            (qty, price, bank_balance, hours)
        };
        Ok( TradeBuy{qty, price, bank_balance, hours, trade} )
    }
}

#[derive(Debug)]
struct TradeBuyCalc<'a> { qty:f64, new_balance:f64, new_qty:f64, new_basis:f64, position:Position, new_position_p:bool, tradebuy:TradeBuy<'a> }

impl<'a> TradeBuyCalc<'a> {
    async fn compute_position (obj:TradeBuy<'a>) -> Bresult<TradeBuyCalc<'a>> {
        let (qty, new_balance, new_qty, new_basis, position, new_position_p) = {
            let (qty, new_balance) =
                match
                    verify_qty(obj.qty, obj.price, obj.bank_balance)
                    .or_else( |_e| verify_qty(obj.qty-0.0001, obj.price, obj.bank_balance) )
                    .or_else( |_e| verify_qty(obj.qty-0.0002, obj.price, obj.bank_balance) )
                    .or_else( |_e| verify_qty(obj.qty-0.0003, obj.price, obj.bank_balance) )
                    .or_else( |_e| verify_qty(obj.qty-0.0004, obj.price, obj.bank_balance) )
                    .or_else( |_e| verify_qty(obj.qty-0.0005, obj.price, obj.bank_balance) ) {
                    Err(e) => {  // Message user problem and log
                        obj.trade.cmdstruct.push_msg(&e).send_msg_markdown().await?;
                        return Err(e.into())
                    },
                    Ok(r) => r
                };
            let id = obj.trade.cmdstruct.id;
            let position = Position::query(&obj.trade.cmdstruct, id, &obj.trade.ticker).await?;
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
            (qty, new_balance, new_qty, new_basis, position, new_position_p)
        };
        Ok(Self{qty, new_balance, new_qty, new_basis, position, new_position_p, tradebuy:obj})
    }
}

#[derive(Debug)]
struct ExecuteBuy<'a> { msg:String, tradebuycalc:TradeBuyCalc<'a> }

impl<'a> ExecuteBuy<'a> {
    fn execute (mut obj:TradeBuyCalc<'a>) -> Bresult<Self> {
        let mut msg = {
            let now = obj.tradebuy.trade.cmdstruct.now;
            if obj.tradebuy.hours!=24
                && !trading_hours_p( obj.tradebuy.trade.cmdstruct.env.lock().unwrap().dst_hours_adjust, now)? {
                return Ok( ExecuteBuy {
                    msg: format!("Unable to buy {} after hours", obj.position.ticker),
                    tradebuycalc:obj
                } )
            }
            let dbconn = &obj.tradebuy.trade.cmdstruct.env.lock().unwrap().dbconn;
            let id = obj.tradebuy.trade.cmdstruct.id;
            let ticker = &obj.tradebuy.trade.ticker;
            let price = obj.tradebuy.price;

            sql_table_order_insert(dbconn, id, ticker, obj.qty, price, now)?;
            let mut msg = format!("*Bought:*");

            if obj.new_position_p {
                getsql!(dbconn, "INSERT INTO positions VALUES (?, ?, ?, ?)", id, &**ticker, obj.new_qty, obj.new_basis)?;
            } else {
                msg += &format!("  `{:.2}``{}` *{}*_@{}_", obj.qty*price, ticker, obj.qty, price);
                info!("\x1b[1madd to existing position:  {} @ {}  ->  {} @ {}", obj.position.qty, obj.position.quote.as_ref().unwrap().price, obj.new_qty, obj.new_basis);
                getsql!(dbconn, "UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=?", obj.new_qty, obj.new_basis, id, &**ticker)?;
            }
            obj.position.qty = obj.new_qty;
            obj.position.price = obj.new_basis;
            getsql!(dbconn, "UPDATE accounts SET balance=? WHERE id=?", obj.new_balance, id)?;
            msg
        };

        msg += &obj.position.format_position(&obj.tradebuy.trade.cmdstruct.env.lock().unwrap().dbconn)?;

        Ok(Self{msg, tradebuycalc: obj})
    }
}

async fn do_trade_buy (cmdstruct:&mut CmdStruct) -> Bresult<&'static str> {
    let trade = Trade::new(cmdstruct);
    if trade.as_ref().map_or(true, |trade| trade.action != '+') { return Ok("SKIP") }

    let res =
        TradeBuy::new(trade.unwrap()).await
        .map(TradeBuyCalc::compute_position)?.await
        .map(ExecuteBuy::execute)??;

    info!("\x1b[1;31mResult {:#?}", &res);

    res.tradebuycalc.tradebuy.trade.cmdstruct.push_msg(&res.msg).send_msg_markdown().await?; // Report to group
    Ok("COMPLETED.")
}

////////////////////////////////////////////////////////////////////////////////
/// Stonk Sell

#[derive(Debug)]
struct TradeSell<'a> {
    position: Position,
    qty: f64,
    price: f64,
    bank_balance: f64,
    new_balance: f64,
    new_qty: f64,
    hours: i64,
    trade: Trade<'a>
}

impl<'a> TradeSell<'a> {
    async fn new (trade:Trade<'a>) -> Bresult<TradeSell<'a>> {
        let (position, qty, price, bank_balance, new_balance, new_qty, hours) = {
            let id = trade.cmdstruct.id;
            let ticker = &trade.ticker;

            let mut positions = Position::get_position(&trade.cmdstruct, id, ticker)?;
            if positions.is_empty() {
                trade.cmdstruct.push_msg("You lack a valid position.").send_msg_markdown().await?;
                Err("expect 1 position in table")?
            }
            let mut position = positions.pop().unwrap();
            position.update_quote(&trade.cmdstruct).await?;

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
                trade.cmdstruct.push_msg("Quantity too low.").send_msg_markdown().await?;
                Err("sell qty too low")?
            }

            let bank_balance = get_bank_balance(&trade.cmdstruct)?;
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
                trade.cmdstruct.push_msg("You can't sell more than you own.").send_msg_markdown().await?;
                return Err("not enough shares to sell".into());
            }

            let new_qty = roundqty(pos_qty-qty);
            (position, qty, price, bank_balance, new_balance, new_qty, hours)
        };

        Ok( Self{position, qty, price, bank_balance, new_balance, new_qty, hours, trade} )
    }
}

#[derive(Debug)]
struct ExecuteSell<'a> {
    msg: String,
    tradesell: TradeSell<'a>
}

impl<'a> ExecuteSell<'a> {
    fn execute (mut obj:TradeSell<'a>) -> Bresult<Self> {
        let msg = {
            let now = obj.trade.cmdstruct.now;
            if obj.hours!=24
                && !trading_hours_p(obj.trade.cmdstruct.env.lock().unwrap().dst_hours_adjust, now)? {
                return Ok(Self{msg:format!("Unable to sell {} after hours", obj.position.ticker), tradesell:obj});
            }
            let dbconn = &obj.trade.cmdstruct.env.lock().unwrap().dbconn;
            let id = obj.trade.cmdstruct.id;
            let ticker = &obj.trade.ticker;
            let qty = obj.qty;
            let price = obj.price;
            sql_table_order_insert(dbconn, id, ticker, -qty, price, now)?;

            let new_qty = obj.new_qty;
            let mut msg = format!("*Sold:*");
            if new_qty == 0.0 {
                getsql!(dbconn, "DELETE FROM positions WHERE id=? AND ticker=?", id, &**ticker)?;
                msg += &obj.position.format_position(dbconn)?;
            } else {
                obj.position.qty = obj.new_qty; // so format_position is up to date
                getsql!(dbconn, "UPDATE positions SET qty=? WHERE id=? AND ticker=?", new_qty, id, &**ticker)?;
                msg += &format!("  `{:.2}``{}` *{}*_@{}_{}",
                    qty*price, ticker, qty, price,
                    &obj.position.format_position(dbconn)?);
            }
            getsql!(dbconn, "UPDATE accounts SET balance=? WHERE id=?", obj.new_balance, id)?;
            msg
        };
        Ok(Self{msg, tradesell:obj})
    }
}

async fn do_trade_sell (cmdstruct :&mut CmdStruct) -> Bresult<&'static str> {
    let trade = Trade::new(cmdstruct);
    if trade.as_ref().map_or(true, |trade| trade.action != '-') { return Ok("SKIP") }
    let trade = trade.unwrap();

    let res =
        TradeSell::new(trade).await
        .map(ExecuteSell::execute)??;

    info!("\x1b[1;31mResult {:#?}", res);
    res.tradesell.trade.cmdstruct.push_msg(&res.msg).send_msg_markdown().await?; // Report to group
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
struct ExQuote<'a> { // Local Exchange Quote Structure
    cmdstruct: &'a mut CmdStruct,
    id: i64,
    thing: String,
    qty: f64,
    price: f64,
    ticker: String,
    now: i64,
}

impl<'a> ExQuote<'a> {
    fn scan (cmdstruct: &'a mut CmdStruct) -> Bresult<Option<ExQuote<'a>>> {
         //                         ____ticker____  _____________qty____________________  $@  ___________price______________
        let caps = regex_to_vec(r"^(@[A-Za-z^.-_]+)([+-]([0-9]+[.]?|[0-9]*[.][0-9]{1,4}))[$@]([0-9]+[.]?|[0-9]*[.][0-9]{1,2})$", &cmdstruct.message)?;
        if caps.is_empty() { return Ok(None) }

        let thing = caps.as_string(1)?;
        let ticker = {
            let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
            deref_ticker(dbconn, &thing)
        };
        Ok(match ticker { // -> Option<String>
            None => None,
            Some(ticker) => {
                let qty   = roundqty(caps.as_f64(2)?);
                let price = caps.as_f64(4)?;
                let now   = Instant::now().seconds();
                let id    = cmdstruct.id;
                Some(ExQuote {cmdstruct, id, thing, qty, price, ticker, now } )
            }
        })
    }
}

#[derive(Debug)]
struct QuoteCancelMine<'a> {
    qty: f64,
    myasks: Vec<HashMap<String,String>>,
    myasksqty: f64,
    mybids: Vec<HashMap<String,String>>,
    exquote: ExQuote<'a>,
    msg: String
}

impl<'a> QuoteCancelMine<'a> {
    fn doit (exquote :ExQuote<'a>) -> Bresult<QuoteCancelMine<'a>> {
        let id = exquote.id;
        let ticker = &exquote.ticker;
        let mut qty = exquote.qty;
        let price = exquote.price;
        let mut msg = String::new();

        let (myasks, myasksqty, mybids) = {
            let cmdstruct = &exquote.cmdstruct;
            let dbconn = &cmdstruct.env.lock().unwrap().dbconn;

            let mut myasks = getsql!(dbconn, "SELECT * FROM exchange WHERE id=? AND ticker=? AND qty<0.0 ORDER BY price", id, &**ticker)?;
            let myasksqty = -roundqty(myasks.iter().map( |ask| ask.get_f64("qty").unwrap() ).sum::<f64>());
            let mut mybids = getsql!(dbconn, "SELECT * FROM exchange WHERE id=? AND ticker=? AND 0.0<=qty ORDER BY price DESC", id, &**ticker)?;

            if qty < 0.0 { // This is an ask exquote
                // Remove/decrement a matching bid in the current settled market
                if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get_f64("price").unwrap() ) {
                    let bidqty = roundqty(mybid.get_f64("qty")?);
                    if bidqty <= -qty {
                        getsql!(dbconn, "DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", id, &**ticker, bidqty, price)?;
                        mybid.insert("*UPDATED*".into(), "*REMOVED*".into());
                        qty = roundqty(qty + bidqty);
                        msg += &format!("\n*Removed bid:* `{}+{}@{}`", exquote.thing, bidqty, price);
                    } else {
                        let newbidqty = roundqty(bidqty+qty);
                        getsql!(dbconn, "UPDATE exchange SET qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newbidqty, id, &**ticker, bidqty, price)?;
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
                        getsql!(dbconn, "DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", id, &**ticker, askqty, price)?;
                        myask.insert("*UPDATED*".into(), "*REMOVED*".into());
                        qty = roundqty(qty + askqty);
                        msg += &format!("\n*Removed ask:* `{}{}@{}`", exquote.thing, askqty, price);
                    } else {
                        let newaskqty = roundqty(askqty+qty);
                        getsql!(dbconn, "UPDATE exchange SET qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newaskqty, id, &**ticker, askqty, price)?;
                        myask.insert("*UPDATED*".into(), format!("*QTY={:.}*", newaskqty));
                        qty = 0.0;
                        msg += &format!("\n*Updated ask:* `{}{}@{}` -> `{}@{}`", exquote.thing, askqty, price, newaskqty, price);
                    }
                }
            }
            (myasks, myasksqty, mybids)
        };

        Ok(Self{qty, myasks, myasksqty, mybids, exquote, msg})
    }
}

#[derive(Debug)]
struct QuoteExecute<'a> {
    qty: f64,
    bids: Vec<HashMap<String,String>>,
    quotecancelmine: QuoteCancelMine<'a>,
    msg: String
}

impl<'a> QuoteExecute<'a> {
    async fn doit (mut obj :QuoteCancelMine<'a>) -> Bresult<QuoteExecute<'a>> {
        let (qty, bids, msg) = {
            let id = obj.exquote.id;
            let ticker = &obj.exquote.ticker;
            let price = obj.exquote.price;
            let mut qty = obj.qty;
            let now = obj.exquote.now;
            let myasks = &mut obj.myasks;
            let mybids = &mut obj.mybids;
            let asksqty = obj.myasksqty;

            let (mut bids, mut asks) = {
                let dbconn = &obj.exquote.cmdstruct.env.lock().unwrap().dbconn;
                // Other bids / asks (not mine) global since it's being updated and returned for logging
                (
                    getsql!(dbconn,
                        "SELECT * FROM exchange WHERE id!=? AND ticker=? AND 0.0<qty ORDER BY price DESC",
                        id, &**ticker)?,
                    getsql!(dbconn,
                        "SELECT * FROM exchange WHERE id!=? AND ticker=? AND qty<0.0 ORDER BY price",
                        id, &**ticker)?
                )
            };

            let mut msg = obj.msg.to_string();
            let position = Position::query( &obj.exquote.cmdstruct, id, ticker).await?;
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

                        let dbconn = &obj.exquote.cmdstruct.env.lock().unwrap().dbconn;

                        if xqty == aqty { // bid to be entirely settled
                            getsql!(dbconn, "DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", aid, &**ticker, aqty, aprice)?;
                            abid.insert("*UPDATED*".into(), "*REMOVED*".into());
                        } else {
                            getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", aqty-xqty, aid, &**ticker, aqty, aprice)?;
                            abid.insert("*UPDATED*".into(), format!("*qty={}*", aqty-xqty));
                        }

                        // Update order table with the purchase and buy info
                        sql_table_order_insert(dbconn, id, ticker, -xqty, aprice, now)?;
                        sql_table_order_insert(dbconn, aid, ticker, xqty, aprice, now)?;

                        // Update each user's account
                        let value = xqty * aprice;
                        getsql!(dbconn, "UPDATE accounts SET balance=balance+? WHERE id=?",  value, id)?;
                        getsql!(dbconn, "UPDATE accounts SET balance=balance+? WHERE id=?", -value, aid)?;

                        msg += &format!("\n*Settled:*\n{} `{}{:+}@{}` <-> `${}` {}",
                                reference_ticker(dbconn, &id.to_string()), obj.exquote.thing, xqty, aprice,
                                value, reference_ticker(dbconn, &aid.to_string()));

                        // Update my position
                        let newposqty = roundqty(posqty - xqty);
                        if 0.0 == newposqty {
                            getsql!(dbconn, "DELETE FROM positions WHERE id=? AND ticker=? AND qty=?", id, &**ticker, posqty)?;
                        } else {
                            getsql!(dbconn, "UPDATE positions SET qty=? WHERE id=? AND ticker=? AND qty=?", newposqty, id, &**ticker, posqty)?;
                        }

                        // Update buyer's position
    /**/                let aposition = Position::query( &obj.exquote.cmdstruct, aid, ticker).await?;
                        let aposqty = aposition.qty;
                        let aposprice = aposition.price;

                        if 0.0 == aposqty {
                            getsql!(dbconn, "INSERT INTO positions values(?, ?, ?, ?)", aid, &**ticker, xqty, value)?;
                        } else {
                            let newaposqty = roundqty(aposqty+aqty);
                            let newaposcost = (aposprice + value) / (aposqty + xqty);
                            getsql!(dbconn, "UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=? AND qty=?", newaposqty, newaposcost, aid, &**ticker, aposqty)?;
                        }

                        // Update stonk exquote value
    /**/                let last_price = Quote::get_market_quote(obj.exquote.cmdstruct.env.clone(), ticker, obj.exquote.cmdstruct.now).await?.price;
                        getsql!(dbconn, "UPDATE stonks SET price=?, last=?, time=? WHERE ticker=?", aprice, last_price, now, &**ticker)?;

                        qty = roundqty(qty+xqty);
                        if 0.0 <= qty { break }
                    } // for

                    // create or increment in exchange table my ask.  This could also be the case if no bids were executed.
                    if qty < 0.0 {
                        let dbconn = &obj.exquote.cmdstruct.env.lock().unwrap().dbconn;
                        if let Some(myask) = myasks.iter_mut().find( |a| price == a.get_f64("price").unwrap() ) {
                            let oldqty = myask.get_f64("qty").unwrap();
                            let newqty = roundqty(oldqty + qty); // both negative, so "increased" ask qty
                            let mytime = myask.get_i64("time").unwrap();
                            getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND price=? AND time=?", newqty, id, &**ticker, price, mytime)?;
                            myask.insert("*UPDATED*".into(), format!("qty={}", newqty));
                            msg += &format!("\n*Updated ask:* `{}{}@{}` -> `{}@{}`", obj.exquote.thing, oldqty, price, newqty, price);
                        } else {
                            getsql!(dbconn, "INSERT INTO exchange VALUES (?, ?, ?, ?, ?)", id, &**ticker, &*format!("{:.4}", qty), &*format!("{:.4}", price), now)?;
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
                let bank_balance = get_bank_balance(obj.exquote.cmdstruct)?;
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

                        {
                            let dbconn = &obj.exquote.cmdstruct.env.lock().unwrap().dbconn;

                            if xqty == -aqty { // ask to be entirely settled
                                getsql!(dbconn, "DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", aid, &**ticker, aqty, aprice)?;
                                aask.insert("*UPDATED*".into(), "*REMOVED*".into());
                            } else {
                                let newqty = aqty+xqty;
                                getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newqty, aid, &**ticker, aqty, aprice)?;
                                aask.insert("*UPDATED*".into(), format!("*qty={}*", newqty));
                            }

                            //error!("new order {} {} {} {} {}", id, ticker, aqty, price, now);
                            // Update order table with the purchase and buy info
                            sql_table_order_insert(dbconn, id, ticker, xqty, aprice, now)?;
                            sql_table_order_insert(dbconn, aid, ticker, -xqty, aprice, now)?;

                            // Update each user's account
                            let value = xqty * aprice;
                            getsql!(dbconn, "UPDATE accounts SET balance=balance+? WHERE id=?", -value, id)?;
                            getsql!(dbconn, "UPDATE accounts SET balance=balance+? WHERE id=?",  value, aid)?;

                            msg += &format!("\n*Settled:*\n{} `${}` <-> `{}{:+}@{}` {}",
                                    reference_ticker(dbconn, &id.to_string()), value,
                                    obj.exquote.thing, xqty, aprice, reference_ticker(dbconn, &aid.to_string()));

                            // Update my position
                            if 0.0 == posqty {
                                getsql!(dbconn, "INSERT INTO positions values(?, ?, ?, ?)", id, &**ticker, xqty, value)?;
                            } else {
                                let newposqty = roundqty(posqty+xqty);
                                let newposcost = (posprice + value) / (posqty + xqty);
                                getsql!(dbconn, "UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=? AND qty=?",
                                    newposqty, newposcost, id, &**ticker, posqty)?;
                            }
                        }

                        {
                            // Update their position
        /**/                let aposition = Position::query( &obj.exquote.cmdstruct, aid, ticker).await?;
                            let aposqty = aposition.qty;
                            let newaposqty = roundqty(aposqty - xqty);
                            let dbconn = &obj.exquote.cmdstruct.env.lock().unwrap().dbconn;
                            if 0.0 == newaposqty {
                                getsql!(dbconn, "DELETE FROM positions WHERE id=? AND ticker=? AND qty=?", aid, &**ticker, aposqty)?;
                            } else {
                                getsql!(dbconn, "UPDATE positions SET qty=? WHERE id=? AND ticker=? AND qty=?", newaposqty, aid, &**ticker, aposqty)?;
                            }
                        }

                        // Update self-stonk exquote value
                        let last_price = Quote::get_market_quote(obj.exquote.cmdstruct.env.clone(), ticker, obj.exquote.cmdstruct.now).await?.price;
                        let dbconn = &obj.exquote.cmdstruct.env.lock().unwrap().dbconn;
                        getsql!(dbconn, "UPDATE stonks SET price=?, last=?, time=? WHERE ticker=?",
                            aprice, last_price, now, &**ticker)?;

                        qty = roundqty(qty-xqty);
                        if qty <= 0.0 { break }
                    } // for

                    // create or increment in exchange table my bid.  This could also be the case if no asks were executed.
                    if 0.0 < qty{
                        let dbconn = &obj.exquote.cmdstruct.env.lock().unwrap().dbconn;
                        if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get_f64("price").unwrap() ) {
                            let oldqty = mybid.get_f64("qty").unwrap();
                            let newqty = roundqty(oldqty + qty);
                            let mytime = mybid.get_i64("time").unwrap();
                            getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND price=? AND time=?", newqty, id, &**ticker, price, mytime)?;
                            mybid.insert("*UPDATED*".into(), format!("qty={}", newqty));
                            msg += &format!("\n*Updated bid:* `{}+{}@{}` -> `{}@{}`", obj.exquote.thing, oldqty, price, newqty, price);
                        } else {
                            getsql!(dbconn, "INSERT INTO exchange VALUES (?, ?, ?, ?, ?)", id, &**ticker, &*format!("{:.4}",qty), &*format!("{:.4}",price), now)?;
                            // Add a fake entry to local mybids vector for logging's sake
                            let mut hm = HashMap::<String,String>::new();
                            hm.insert("*UPDATED*".to_string(), format!("*INSERTED* {} {} {} {} {}", id, ticker, qty, price, now));
                            mybids.push(hm);
                            msg += &format!("\n*Created bid:* `{}+{}@{}`", obj.exquote.thing, qty, price);
                        }
                    }
                } // else
            } // if
            (qty, bids, msg)
        };
        Ok(Self{qty, bids, quotecancelmine: obj, msg})
    }
}

async fn do_exchange_bidask (cmdstruct :&mut CmdStruct) -> Bresult<&'static str> {

    let exquote = ExQuote::scan(cmdstruct)?;
    let exquote = if exquote.is_none() { return Ok("SKIP") } else { exquote.unwrap() };

    let ret =
        QuoteCancelMine::doit(exquote)
        .map(|obj| QuoteExecute::doit(obj) )?.await?;
    info!("\x1b[1;31mResult {:#?}", ret);
    if 0 != ret.msg.len() {
        ret.quotecancelmine.exquote.cmdstruct
            .push_msg(
                &ret.msg
                .replacen("_", "\\_", 1000)
                .replacen(">", "\\>", 1000))
            .send_msg_markdown()
            .await?;
    } // Report to group
    Ok("COMPLETED.")
}


async fn do_orders (cmdstruct: &mut CmdStruct) -> Bresult<&'static str> {
    let mut asks = String::from("");
    let mut bids = String::from("");
    let rows = {
        let envstruct = &cmdstruct.env.lock().unwrap();
        let dbconn = &envstruct.dbconn;
        if Regex::new(r"(?i)/orders").unwrap().find(&cmdstruct.message).is_none() { return Ok("SKIP"); }
        let id = cmdstruct.id;
        for order in getsql!(dbconn, "SELECT * FROM exchange WHERE id=?", id)? {
            let ticker = order.get("ticker").unwrap();
            let stonk = reference_ticker(dbconn, ticker).replacen("_", "\\_", 10000);
            let qty = order.get_f64("qty")?;
            let price = order.get("price").unwrap();
            if qty < 0.0 {
                asks += &format!("\n{}{:+}@{}", stonk, qty, price);
            } else {
                bids += &format!("\n{}{:+}@{}", stonk, qty, price);
            }
        }

        // Include all self-stonks positions (mine and others)
        let sql = format!("
            SELECT {} AS id, id AS ticker, 0.0 AS qty, 0.0 AS price \
            FROM entitys \
            WHERE 0<id \
            AND id NOT IN (SELECT ticker FROM positions WHERE id={} GROUP BY ticker)
                UNION \
            SELECT * FROM positions WHERE id={} AND ticker IN (SELECT id FROM entitys WHERE 0<id)", id, id, id);
        getsql!(dbconn, sql)?
    };

    let mut msg = String::new();
    let mut total = 0.0;
    for pos in rows {
        if is_self_stonk(pos.get("ticker").unwrap()) {
            let mut pos = {
                Position {
                    quote: None,
                    id: pos.get_i64("id")?,
                    ticker: pos.get_str("ticker")?,
                    qty: pos.get_f64("qty")?,
                    price: pos.get_f64("price")?,
                    fmt_position: cmdstruct.env.lock().unwrap().fmt_str_quote(cmdstruct.id)
                }
            };
            pos.update_quote(&cmdstruct).await?; // Locks cmdstruct.env

            let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
            msg += &pos.format_position(dbconn)?;
            total += pos.qty * pos.quote.unwrap().price;
        }
    }

    let cash = get_bank_balance(cmdstruct)?;
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

    cmdstruct.push_msg(&msg).send_msg_markdown().await?;
    Ok("COMPLETED.")
}

async fn send_format_strings_help (cmdstruct: &mut CmdStruct) -> Bresult<i64> {
    cmdstruct
        .push_msg(FORMAT_STRINGS_HELP)
        .send_msg_markdown()
        .await
}

const FORMAT_STRINGS_HELP: &str =
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

` ™Bot Position Formatting `
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
";

async fn do_fmt (cmdstruct :&mut CmdStruct) -> Bresult<&'static str> {

    let caps = regex_to_vec(r"^/fmt( ([qp?])[ ]?(.*)?)?$", &cmdstruct.message)?;
    //caps.iter().for_each( |c| println!("\x1b[1;35m{:?}", c));
    if caps.is_empty() { return Ok("SKIP"); }

    let id = cmdstruct.id;

    // "/fmt" show current format strings

    if caps.as_str(1).is_err() {
        let (fmt_quote, fmt_position) = {
            let envstruct = cmdstruct.env.lock().unwrap();
            (envstruct.fmt_str_quote(id).to_string(), envstruct.fmt_str_position(id).to_string())
        };
        return cmdstruct
            .push_msg(&format!("FORMAT STRINGS\nQuote{} {}\nPosition{} {}",
                IF!(fmt_quote==FORMAT_STRING_QUOTE, "*", ""), fmt_quote,
                IF!(fmt_position==FORMAT_STRING_POSITION, "*", ""), fmt_position))
            .send_msg().await
            .and(Ok("COMPLETED."))
    }

    // "/fmt [qp?] [*]" show current format strings

    let new_format_str = caps.as_str(3).unwrap_or("").to_string();
    let format_type = match caps.as_str(2)? {
        "q" => {
            cmdstruct.env.lock().unwrap().entitys.get_mut(&id).unwrap().quote = new_format_str.to_string();
            "quote"
        },
        "p" => {
            cmdstruct.env.lock().unwrap().entitys.get_mut(&id).unwrap().position = new_format_str.to_string();
            "position"
        },
          _ => return send_format_strings_help(cmdstruct).await.and(Ok("COMPLETED."))
    };

    // notify user the change
    cmdstruct
        .push_msg(
            &format!("Format {}\n{}",
                format_type,
                if new_format_str.is_empty() { "DEFAULT".to_string() } else { format!("{}", new_format_str) }))
        .send_msg()
        .await?;

    let dbconn = &cmdstruct.env.lock().unwrap().dbconn;

    // make change to DB
    getsql!(dbconn, "INSERT OR IGNORE INTO formats VALUES (?, '', '')", id)?;
    getsql!(dbconn, format!("UPDATE formats SET {}=? WHERE id=?", format_type),
        &*new_format_str.replacen("\"", "\"\"", 10000), id)?;

    Ok("COMPLETED.")
}


async fn do_rebalance (cmdstruct: &mut CmdStruct) -> Bresult<&'static str> {

    let caps = regex_to_vec(
        //             _______float to 2 decimal places______                                    ____________float_____________
        r"(?i)^/rebalance( (-?[0-9]+[.]?|(-?[0-9]*[.][0-9]{1,2})))?(( [@^]?[A-Z_a-z][-.0-9=A-Z_a-z]* (([0-9]*[.][0-9]+)|([0-9]+[.]?)))+)",
        &cmdstruct.message)?;
    if caps.is_empty() { return Ok("SKIP") }

    cmdstruct.msg_id = Some(
        cmdstruct
            .push_msg(&format!("Rebalancing:"))
            .send_msg()
            .await?);

    let percents = // HashMap of Ticker->Percent
        Regex::new(r" ([@^]?[A-Z_a-z][-.0-9=A-Z_a-z]*) (([0-9]*[.][0-9]+)|([0-9]+[.]?))")?
        .captures_iter(&caps.as_string(4)?)
        .map(|cap|
            ( cap.get(1).unwrap().as_str().to_uppercase(),
              cap.get(2).unwrap().as_str().parse::<f64>().unwrap() / 100.0 ) )
        .collect::<HashMap<String, f64>>();


    if { // Check/exit if after hours rebalancing
        let envstruct = &cmdstruct.env.lock().unwrap();
        !trading_hours_p(envstruct.dst_hours_adjust, cmdstruct.now)?
        &&
        getsql!(
            envstruct.dbconn,
            format!("SELECT hours FROM stonks WHERE hours!=24 AND ticker IN ('{}')",
                percents.keys().map(String::to_string).collect::<Vec<String>>().join("','")))?
        .len() != 0
    } {
        cmdstruct.push_msg(&"\nRebalance During Trading Hours Mon..Fri 1AM..5PM").send_msg().await?;
        return Ok("COMPLETED.");
    }

    for ticker in percents.keys() { // Refresh stonk quotes
        if !is_self_stonk(&ticker) {
            Quote::get_market_quote(cmdstruct.env.clone(), &ticker, cmdstruct.now).await.err().map_or(false, gwarn);
            // Update feedback message with ticker symbol
            cmdstruct.push_msg(&format!(" {}", ticker)).send_msg().await?;
        }
    }


    let mut positions = {
        let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
        getsql!(dbconn, format!("
            SELECT
                positions.ticker,
                positions.qty,
                stonks.price,
                positions.qty*stonks.price AS value
            FROM positions
            LEFT JOIN stonks ON positions.ticker=stonks.ticker
            WHERE id={} AND positions.ticker IN ('{}')",
            cmdstruct.id,
            percents.keys().map(String::to_string).collect::<Vec<String>>().join("','")
        ))?
    };

    // Sum the optional offset amount and stonk values
    let mut total :f64 = caps.as_f64(2).unwrap_or(0.0);
    positions.iter().for_each(|hm| total += hm.get_f64("value").unwrap());
    info!("rebalance total {}", total);

    if 0==positions.len() {
        cmdstruct.push_msg("no valid tickers").send_msg().await?;
    } else {
        for i in 0..positions.len() {
            let ticker = positions[i].get_str("ticker")?;
            let value = positions[i].get_f64("value")?;
            let mut diff = roundfloat(percents.get(&ticker).unwrap() * total - value, 2);
            if -0.01 < diff && diff < 0.01 { diff = 0.0; } // under 1¢ diffs will be skipped
            positions[i].insert("diff".to_string(), diff.to_string()); // Add new key/val to Position HashMap
        }
        for i in 0..positions.len() {
            if positions[i].get_str("diff")?.chars().nth(0).unwrap() == '-'  {
                info!("rebalance position {:?}", positions[i]);
                let ticker = &positions[i].get_str("ticker")?;
                let diffstr = &positions[i].get_str("diff")?[1..];
                cmdstruct.push_msg("\n");
                if "0" == diffstr {
                    cmdstruct.push_msg(&format!("{} is balanced", ticker)).send_msg().await?;
                } else {
                    let message = format!("{}-${}", ticker, &diffstr);
                    cmdstruct.message = message;
                    glogd!(" do_trade_sell =>", do_trade_sell(cmdstruct).await); // Recurse on same cmsstruct but mutated message
                }
                //cmdstruct.send_msg_markdown().await?;
            }
        }
        for i in 0..positions.len() {
            if positions[i].get_str("diff")?.chars().nth(0).unwrap() != '-'  {
                info!("rebalance position {:?}", positions[i]);
                let ticker = &positions[i].get_str("ticker")?;
                let mut diffstr = positions[i].get_str("diff")?;
                if diffstr != "0" {
                    let bank_balance = { get_bank_balance(cmdstruct)?  };
                    // The last buy might be so off, so skip or adjust to account value
                    if bank_balance < diffstr.parse::<f64>()? {
                        if 0.01 <= bank_balance {
                            diffstr = format!("{}", (bank_balance*100.0) as i64 as f64 / 100.0);
                        } else {
                            diffstr = format!("0");
                        }
                        warn!("updated diff position {} {}", ticker, diffstr);
                    }
                }
                cmdstruct.push_msg("\n");
                if "0" == diffstr {
                    cmdstruct.push_msg(&format!("{} is balanced", ticker)).send_msg().await?;
                } else {
                    let message = format!("{}+${}", ticker, &diffstr);
                    //cmdstruct.push_msg(&format!("\n{}\n", message));
                    cmdstruct.message = message;
                    glogd!(" do_trade_buy =>", do_trade_buy(cmdstruct).await); // Recurse on same cmsstruct but mutated message
                }
                //cmdstruct.send_msg_markdown().await?;
            }
        }
    }
    Ok("COMPLETED.")
}

async fn do_schedule (cmdstruct: &mut CmdStruct) -> Bresult<&'static str> {
    let caps =
        regex_to_vec(
            r"(?xi)^/schedule
            (
                \ +
                (?: ### Fixed GMT
                    ( \d+ - \d{1,2} - \d{1,2} T )?
                    ( \d{1,2} : \d{1,2} : \d{1,2} )
                    (?: Z | [-+]0{1,4})
                )?
                (?: ### Duration
                    \ *
                    (-)?
                    (?: (\d+)h)?
                    (?: (\d+)m)?
                    (\d+)?
                    ([*])?
                )?
                #### Command
                (?:
                    \ *
                    (.+)
                )?
            )?",
            &cmdstruct.message )?;
    //caps.iter().for_each( |c| println!("\x1b[1;35m{:?}", c));
    if caps.is_empty() { return Ok("SKIP") }

    // "/schedule" Show all jobs
    if caps.get(1).unwrap().is_none() {
        let mut res = {
            let dbconn = &cmdstruct.env.lock().unwrap().dbconn;
            getsql!(dbconn, "SELECT name, time, cmd FROM schedules LEFT JOIN entitys ON schedules.at = entitys.id WHERE schedules.id=?", cmdstruct.id)?
        };
        if res.is_empty() {
            cmdstruct.push_msg("No Scheduled Jobs").send_msg_markdown().await?;
            return Ok("COMPLETED.")
        }
        res.sort_by( |a,b| a.get_i64("time").unwrap().cmp(&b.get_i64("time").unwrap()) );
        let buff =
            "Scheduled Jobs:\n".to_string() +
            &res.iter()
            .map( |row| {
                let time = row.get_i64("time").unwrap();
                format!("`{}Z` `{}` `{}`",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time),
                    row.get_str("name").unwrap(),
                    row.get_str("cmd").unwrap())
            } )
            .collect::<Vec<String>>()
            .join("\n");
        cmdstruct.push_msg(&buff).send_msg_markdown().await?;
        return Ok("COMPLETED.")
    }

    let msg = {
        let id = cmdstruct.id;
        let at = cmdstruct.at;
        let now = cmdstruct.now;
        let dbconn = &cmdstruct.env.lock().unwrap().dbconn;

        let mut daily = caps.as_str(8).map_or(false,|_|true);
        let mut time =
            if let Ok(datestr) = caps.as_str(2) {
                LocalDateTime
                    ::from_str( &format!("{}{}", datestr, caps.as_str(3)?).to_uppercase() )?
                    .to_instant()
                    .seconds()
            } else if let Ok(timestr) = caps.as_str(3) {
                daily = true;
                LocalTime
                    ::from_str( timestr )?
                    .to_seconds()
            } else {
                now
            }
            + (
                caps.as_i64(5).unwrap_or(0) * 60 * 60
                + caps.as_i64(6).unwrap_or(0) * 60
                + caps.as_i64(7).unwrap_or(0)
            ) * caps.as_str(4).map_or(1,|_|-1);

        if daily { time = time.rem_euclid(86400)}

        // Save job
        if let Ok(command) = caps.as_str(9) {
                info!("now:{} [id:{} at:{} time:{} cmd:{:?}]",
                    now, id, at, time, command);
                glog!(getsql!(dbconn, "INSERT INTO schedules VALUES (?, ?, ?, ?)",
                    id, at, time, command));
                format!("Scheduled: `{}Z` `{}`",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time),
                    command)
        } else { // Delete job
            if getsql!(dbconn, "SELECT * FROM schedules WHERE time=?", time)?.len()
                != getsql!(dbconn, "DELETE FROM schedules WHERE time=?", time)?.len()
            {
                format!("`{}Z` removed",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time))
            } else {
                format!("`{}Z` not found",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time))
            }
        }
    };

    cmdstruct.push_msg(&msg).send_msg_markdown().await?;

    Ok("COMPLETED.")
}

////////////////////////////////////////////////////////////////////////////////

async fn do_all (cmdstruct:&mut CmdStruct) -> Bresult<()> {
    info!("\x1b[33m{:?}", cmdstruct);

    let res = do_schedule(cmdstruct).await;
    glogd!("do_schedule =>", res);
    match res {
        Err(e) => {
            cmdstruct.push_msg(&format!("Scheduler {}", e)).send_msg_id().await?; }
        Ok(o) =>  {
            // Stop evaluating if this is a successfull scheduled job
            if o == "COMPLETED." { return Ok(()) }
        }
    }

    glogd!("do_echo =>",       do_echo(cmdstruct).await);
    glogd!("do_help =>",       do_help(cmdstruct).await);
    glogd!("do_curse =>",      do_curse(cmdstruct).await);
    glogd!("do_say =>",        do_say(cmdstruct).await);
    glogd!("do_like =>",       do_like(cmdstruct).await);
    glogd!("do_like_info =>",  do_like_info(cmdstruct).await);
    glogd!("do_def =>",        do_def(cmdstruct).await);
    glogd!("do_sql =>",        do_sql(cmdstruct).await);
    glogd!("do_quotes => ",    do_quotes(cmdstruct).await);
    glogd!("do_yolo =>",       do_yolo(cmdstruct).await);
    glogd!("do_portfolio =>",  do_portfolio(cmdstruct).await);
    glogd!("do_trade_buy =>",  do_trade_buy(cmdstruct).await);
    glogd!("do_trade_sell =>", do_trade_sell(cmdstruct).await);
    glogd!("do_exchange_bidask =>", do_exchange_bidask(cmdstruct).await);
    glogd!("do_orders =>",     do_orders(cmdstruct).await);
    glogd!("do_fmt =>",        do_fmt(cmdstruct).await);
    glogd!("do_rebalance =>",  do_rebalance(cmdstruct).await);
    Ok(())
}

fn log_header () {
    info!("\x1b[0;31;40m _____ __  __ ____        _   ");
    info!("\x1b[0;33;40m|_   _|  \\/  | __ )  ___ | |_™ ");
    info!("\x1b[0;32;40m  | | | |\\/| |  _ \\ / _ \\| __|");
    info!("\x1b[0;34;40m  | | | |  | | |_) | (_) | |_ ");
    info!("\x1b[0;35;40m  |_| |_|  |_|____/ \\___/ \\__|");
}

async fn main_dispatch (req: HttpRequest, body: web::Bytes) -> HttpResponse {
    log_header();

    info!("\x1b[35m{}",
        format!("{:?}", &req.connection_info())
        .replace(": ", ":")
        .replace("\"", "")
        .replace(",", ""));

    info!("\x1b[35m{}  Body:\x1b[1;35m{}",
        format!("{:?}", &req)
            .replace("\n", "")
            .replace(r#"": ""#, ":")
            .replace("\"", "")
            .replace("   ", ""),
        from_utf8(&body)
            .unwrap_or(&format!("{:?}", body)) );

    let env :Env = req.app_data::<web::Data<Env>>().unwrap().get_ref().clone();
    info!("\x1b[1m{:?}", env.lock().unwrap());

    std::thread::spawn( move || {
        let envc = env.clone();
        actix_web::rt::System::new("tmbot").block_on(async move {
            match CmdStruct::newcmdstruct(envc, &body) {
                Ok(mut cmdstruct) => do_all(&mut cmdstruct).await.unwrap_or_else(|r| error!("{:?}", r)),
                e => glog!(e)
            }
        });
        info!("\x1b[33m{:?}\n\x1b[0;1mEnd.", env.lock().unwrap());
    });
    "".into()
}

pub fn launch_scheduler(env:Env) -> Bresult<()> {
    info!("Spawned Scheduler Event Thread, 10s delay");
    std::thread::spawn( move ||
    loop {
        sleep_secs(10.0);
        let now = Instant::now().seconds();
        let jobs = {
            let envstruct = env.lock().unwrap();
            let res = getsqlquiet!(&envstruct.dbconn,
                "SELECT id, at, time, cmd FROM schedules WHERE (?<=time AND time<?) or (?<=time AND time<?) ORDER BY time",
                envstruct.time_scheduler, now, // one-time jobs
                envstruct.time_scheduler % 86400, now % 86400); // daily jobs
            if res.is_err() { glog!(res); continue }
            res.unwrap()
        };

        if 0 < jobs.len() {
            info!("\x1b[1mSchedules start...");
            ginfo(&jobs);
            let env = env.clone();
            actix_web::rt::System::new("tmbot").block_on( async move {
            for job in jobs {
                let id = job.get_i64("id").unwrap();
                let at = job.get_i64("at").unwrap();
                let command = job.get_str("cmd").unwrap();
                let mut cmdstruct = match CmdStruct::new_cmdstruct(&env, now, id, at, at, 0, &command) {
                    Ok(cmdstruct) => cmdstruct,
                    e => { glog!(e); continue }
                };
                glog!(do_all(&mut cmdstruct).await);
                let time = job.get_i64("time").unwrap();
                if 86400 <= time { // Delete the non-daily job
                    let dbconn = &env.lock().unwrap().dbconn;
                    getsql!(dbconn, "DELETE FROM schedules WHERE id=? AND at=? AND time=? AND cmd=?", id, at, time, &*command).unwrap();
                }
            } } ); // for rows, async, block_on
            info!("\x1b[1mSchedules end.");
        }
        env.lock().unwrap().time_scheduler = now;
    } ); // loop, ||, thread
    Ok(())
}

pub fn launch_server(env:Env) -> Bresult<()> {
    let mut ssl_acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ssl_acceptor_builder.set_private_key_file("key.pem", SslFiletype::PEM)?;
    ssl_acceptor_builder.set_certificate_chain_file("cert.pem")?;
    let srv =
        HttpServer::new( move ||
            App::new()
            .data(env.clone())
            .service(
                web::resource("*")
                .route( Route::new().to(main_dispatch) ) ) )
        .workers(2)
        .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?;
    Ok(actix_web::rt::System::new("tmbot").block_on(async move {
        println!("launch() => {:?}", srv.run().await)
    })) // This should never return
} // launch_server

pub fn main_launch() -> Bresult<()> {
    let argv = env::args();
    if 1 == argv.len() {
        Err(format!("{:?} missing: KEY ID DST", argv))?
    }
    if !true { glogd!("create_schema => ", create_schema()); } // Create DB
    if !true { fun(argv) } // Hacks and other test code
    else {
        let env = EnvStruct::new(argv)?;
        glogd!("scheduler() =>", launch_scheduler(env.clone()));
        launch_server(env)
    }
}

////////////////////////////////////////////////////////////////////////////////

fn fun (argv: std::env::Args) -> Bresult<()>  {
    let _env = EnvStruct::new(argv)?;
    //info!("{:#?}", env);

    (-6..=28).for_each( |c| info!("{}", num2heart(c)) );

    Ok(())
}

#[derive(Debug, Hello)]
struct Greetings {
    z:i32,
    err:Box<dyn Error>
}

fn _fun_macros() -> Bresult<()> {
    let g = Greetings{z:42, err:"".into() };
    Err(std::io::Error::from_raw_os_error(0))?;
    Ok(g.hello())
}