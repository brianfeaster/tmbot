#![allow(non_snake_case)]

//! # External Chat Service Robot

mod comm;
mod db;
mod srvs;
pub mod util;
mod tmlog;

use crate::comm::*;
use crate::db::*;
use crate::srvs::*;
use crate::util::*;

use actix::{prelude::*, Actor, StreamHandler};
use actix_web::{rt, web, App, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use datetime::{
    DatePiece, Duration, Instant, LocalDate, LocalDateTime, LocalTime, TimePiece, Weekday::*,
};
use regex::Regex;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    env,
    mem::transmute,
    str::{from_utf8, FromStr},
    sync::{Arc, Mutex},
    thread
};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

////////////////////////////////////////////////////////////////////////////////

const QUOTE_DELAY_SECS :i64 = 30;
const FORMAT_STRING_QUOTE    :&str = "%q%A%B %C%% %D%E@%F %G%H%I%q";
const FORMAT_STRING_POSITION :&str = "%n%q%A %C%B %D%%%q %q%E%F@%G%q %q%H@%I%q";


////////////////////////////////////////////////////////////////////////////////
// Abstracted primitive types helpers

// Decide if a ticker's price should be refreshed given its last lookup time.
//  PacificTZ  PreMarket  Regular  AfterHours  Closed
//  PST        0900z      1430z    2100z       0100z..9000z
//  PDT        0800z      1330z    2000z       0000z..0800z
//  Duration   5.5h       6.5h     4h          8h
fn most_recent_trading_hours (
    dst_hours_adjust: i8,
    now: LocalDateTime,
) -> Bresult<(LocalDateTime, LocalDateTime)> {
    // Absolute time (UTC) when US pre-markets open 1AMPT / 0900Z|0800Z
    let start_time = LocalTime::hm(9 - dst_hours_adjust, 0)?;  // TODO create function describing the hour maths
    let start_duration = Duration::of(start_time.to_seconds());
    let market_hours_duration = Duration::of( LocalTime::hm(16, 5)?.to_seconds() ); // Add 5 minutes for delayed orders

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

    let lookup_throttle_duration = Duration::of(envstruct.quote_delay_secs); // Lookup tickers every 2 minutes

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
fn num_simp (num:String) -> String {
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
        format!("{}", n)
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

// Transforms @user_nickname to {USER_ID}
fn deref_ticker (dbconn: &Connection, s:&str) -> Bresult<String> {
    // Expects:  "@shrewm"   Returns: Some(308188500)
    if &s[0..1] == "@" {
        let rows = getsql!(dbconn, "SELECT id FROM entitys WHERE name=?", &s[1..])?;
        if 1 != rows.len() { Err("deref_ticker() multiple names matched")? }
        Ok(rows[0].get_string("id")?)
    } else {
        Err("deref_ticker expected form '@.*'".into())
    }
}

// Transform a valid ticker/id into "@nickname", else leave as-is
fn reference_ticker<'a> (envstruct: &'a EnvStruct, tkr: &'a str) -> &'a str {
    if is_self_stonk(tkr) {
        match envstruct.entity_ticker2name(tkr) {
            Ok(name) => name,
            Err(e) => { warn!("{:?}", e); tkr }
        }
    } else {
        tkr
    }
}

fn is_self_stonk (s: &str) -> bool {
    Regex::new(r"^[0-9]+$").unwrap().find(s).is_some()
}

fn time2datetimestr (time:i64) -> String {
    let dt = LocalDateTime::at(time);
    format!("{}-{:02}-{:02}T{:02}:{:02}:{:02}",
        dt.year(), 1+dt.month().months_from_january(),
        dt.day(), dt.hour(), dt.minute(), dt.second() )
}

fn time2timestr (time:i64) -> String {
    let dt = LocalDateTime::at(time);
    format!("{:02}:{:02}:{:02}", dt.hour(), dt.minute(), dt.second() )
}

////////////////////////////////////////////////////////////////////////////////
/// Helpers on complex types

fn sql_table_order_insert (dbconn: &Connection, id:i64, ticker:&str, qty:f64, price:f64, time:i64) -> Bresult<()> {
    getsql!(dbconn,
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        id, ticker, qty, price, time)?;
    Ok(())
}

fn create_schema() -> Bresult<()> {
    let conn = Connection::new("tmbot.sqlite".into())?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS entitys (
            id INTEGER NOT NULL UNIQUE,
            name  TEXT NOT NULL);")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS modes (
            id   INTEGER NOT NULL UNIQUE,
            echo INTEGER NOT NULL);")?;

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

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS schedules (
            id    INTEGER NOT NULL,
            at    INTEGER NOT NULL,
            topic INTEGER NOT NULL,
            time  INTEGER NOT NULL,
            days     TEXT NOT NULL,
            cmd      TEXT NOT NULL);")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS likes (
            id    INTEGER NOT NULL UNIQUE,
            likes INTEGER NOT NULL);")?;

    getsql!(&conn,"
        CREATE TABLE IF NOT EXISTS aliases (
            alias TEXT NOT NULL UNIQUE,
            cmd   TEXT NOT NULL);")?;
    Ok(())
} // create_schema

////////////////////////////////////////////////////////////////////////////////
/// Blobs

#[derive(Debug)]
struct Entity {
    _id: i64,
    name: String,
    balance: f64, // Available cash.  Could be negative as long as -2*balance < portfolioValue
    echo: i64,
    likes: i64,
    // format strings
    quote: String,
    position: String,
    // web portal
    _uuid: String
}

#[derive(Debug)]
pub struct EnvStruct {
    url_api:          String, // Telgram API URL
    telegram_key:     String,
    telegram_cert:    String,
    tmbot_key:        String,
    tmbot_cert:       String,
    dbconn:           Connection, // SQLite connection
    quote_delay_secs: i64,    // Delay between remote stock quote queries
    dst_hours_adjust: i8,     // 1 = DST, 0 = ST
    time_scheduler:   i64,    // Time the scheduler last ran
    entitys:          HashMap<i64, Entity>,
}

type Env = Arc<Mutex<EnvStruct>>;

impl From<EnvStruct> for Env {
    fn from (envstruct: EnvStruct) -> Self {
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
    fn entity_balance (&self, id:i64) -> Bresult<f64> {
        Ok(self.entitys.get(&id).ok_or(format!("entity_balance() no id {}", id))?.balance)
    }
    fn entity_id2name (&self, id:i64) -> Bresult<&str> {
        Ok(&self.entitys.get(&id).ok_or(format!("entity_name() no id {}", id))?.name)
    }
    fn entity_ticker2name (&self, tkr:&str) -> Bresult<&str> {
        Ok(&self.entitys.get(&tkr.parse::<i64>()?).ok_or(format!("entity_name() no id {}", tkr))?.name)
    }
    fn entity_balance_set (&mut self, id:i64, new_balance:f64) -> Bresult<()> {
        getsql!(self.dbconn, "UPDATE accounts SET balance=? WHERE id=?", new_balance, id)?;
        self.entitys.get_mut(&id).unwrap().balance = new_balance;
        Ok(())
    }
    fn entity_balance_inc (&mut self, id:i64, balance_adjust:f64) -> Bresult<()> {
        getsql!(self.dbconn, "UPDATE accounts SET balance=balance+? WHERE id=?", balance_adjust, id)?;
        self.entitys.get_mut(&id).unwrap().balance += balance_adjust;
        Ok(())
    }
    fn entity_likes_inc (&mut self, at: i64, adj: i64) -> Bresult<i64> {
        let likes = self.entitys.get_mut(&at).map_or(1, |obj| { obj.likes+=adj; obj.likes });
        getsql!(self.dbconn, "INSERT OR REPLACE INTO likes VALUES (?, ?)", at, likes)?;
        Ok(likes)
    }
    fn _entity_uuid_set (&mut self, id: i64, pw: usize) -> Bresult<()> {
        self.entitys
            .get_mut(&id)
            .map_or(Err("no such entity for uuid".into()), |e| { e._uuid = pw.to_string(); Ok(()) } )
    }
} // impl EnvStruct

impl EnvStruct {
fn new(mut argv: std::env::Args) -> Bresult<Env> {
    let url_api = format!("https://api.telegram.org/bot{}",
        env::var_os("TELEGRAM_API_TOKEN").ok_or("TELEGRAM_API_TOKEN")?.into_string().unwrap());
    let telegram_key  = env::var_os("TELEGRAM_KEY").ok_or("TELEGRAM_KEY")?.into_string().unwrap();
    let telegram_cert = env::var_os("TELEGRAM_CERT").ok_or("TELEGRAM_CERT")?.into_string().unwrap();
    let tmbot_key  = env::var_os("TMBOT_KEY").ok_or("TMBOT_KEY")?.into_string().unwrap();
    let tmbot_cert = env::var_os("TMBOT_CERT").ok_or("TMBOT_CERT")?.into_string().unwrap();
    let tmbot_db  = env::var_os("TMBOT_DB").ok_or("TMBOT_DB")?.into_string().unwrap();
    let dbconn = Connection::new( tmbot_db )?;
    let mut entitys = HashMap::new();
    for hm in
        getsql!(dbconn,
            r"SELECT entitys.id, entitys.name, accounts.balance, modes.echo, likes.likes, formats.quote, formats.position
            FROM entitys
            LEFT JOIN accounts ON entitys.id = accounts.id
            LEFT JOIN modes    ON entitys.id = modes.id
            LEFT JOIN likes    ON entitys.id = likes.id
            LEFT JOIN formats  ON entitys.id = formats.id")?
    {
        //warn!("{:#?}", hm);
        let _id = hm.get_i64("id")?;
        entitys.insert(_id,
            Entity{
                _id,
                name:    hm.get_string("name")?,
                balance: hm.get_f64_or(0.0, "balance"),
                echo:    hm.get_i64_or(2, "echo"),
                likes:   hm.get_i64_or(0, "likes"),
                quote:   hm.get_string_or("", "quote"),
                position:hm.get_string_or("", "position"),
                _uuid:    String::new()
            });
    }
    entitys.insert(0, Entity{
                _id:       0,
                name:     "nil".to_string(),
                balance:  0.0,
                echo:     2,
                likes:    0,
                quote:    String::new(),
                position: String::new(),
                _uuid:     String::new()
    });
    Ok(EnvStruct{
        url_api, telegram_key, telegram_cert, tmbot_key, tmbot_cert, dbconn,
        dst_hours_adjust:   argv.nth(1).and_then(|s| s.parse::<i8>().ok()).ok_or(fmthere!("args[1] DST"))?,
        quote_delay_secs:   QUOTE_DELAY_SECS,
        time_scheduler:     Instant::now().seconds(),
        entitys,
    }.into())
} }

////////////////////////////////////////

// Separate structure from Env due to a !send Rc in Telegram
#[derive(Debug)]
struct CmdStruct { // Represents an incoming Telegram message and session/service state.
    env: Env,
    telegram: Telegram, // Not in Env since threads can't share this due to an internal Rc
    // Telegram incoming message details
    now: i64,
    id: i64, // Entity who sent msg                    message.from.id
    at: i64, // Channel (group/entity) msg sent to     message.chat.id
    to: i64, // Entity replied to ('at' if non-reply)  message.reply_to_message.from.id
    topic: String, // topic id
    //message_id: i64,  // Message's unique id in channel.  TODO map this to msg_id so edits are reflected to the same response msg.
    message: String, // The incoming message
    id_level: i64, // Echo level [0,1,2] for each id.  Higher can't write to lower.  Generally kept at 2 for most people and bots.
    at_level: i64, // Lowering channel's echo level results in less chatter, assuming speaker echo level is larger.
    // Telegram outgoing/response message details
    markdown: bool,
    dm: Option<i64>, // direct message non-overrideable (normally sent to at if levels concur, othrwise id)
    msg_id: Option<i64>, // existing/previous message_id to overwrite
    msg: String
}

impl CmdStruct {
    fn from_json(env: Env, json: &Value) -> Bresult<CmdStruct> {
        let inline_query = &json["inline_query"];
        let edited_message = &json["edited_message"];
        let message = if edited_message.is_object() { edited_message } else { &json["message"] };
        let now = Instant::now().seconds();
        //let message_id = getin_i64(message, "/message_id").unwrap_or(0);
        if inline_query.is_object() { // Inline queries are DMs so no other associated channels
            let id = getin_i64(inline_query, "/from/id")?;
            let message = getin_str(inline_query, "/query")?;
            CmdStruct::new(env, now, id, id, id, "".into(), message)
        } else if message.is_object() { // An incoming message could be a reply or normal.
            let id = getin_i64(message, "/from/id")?;
            let at = getin_i64(message, "/chat/id")?;
            let to = getin_i64_or(at, &message, "/reply_to_message/from/id");
            let topic =
                if getin(message, "/is_topic_message").is_null() {
                    "".to_string()
                } else {
                    getin_string(message, "/message_thread_id")?
                };
            let message = getin_str(message, "/text")?;
            CmdStruct::new(env, now, id, at, to, topic, message)
        } else { Err("Invalid messenger JSON")? }
    }
    fn new(env: Env, now:i64, id:i64, at:i64, to:i64, topic:String, message:String) -> Bresult<CmdStruct> {
        let (telegram, id_level, at_level) =  {
            let envstruct = env.lock().map_err(|e| <std::sync::PoisonError<_> as std::string::ToString>::to_string(&e))?;
            ( Telegram::new(&envstruct.url_api)?,
              envstruct.entitys.get(&id).ok_or(format!("id {} missing from entitys", id))?.echo,
              envstruct.entitys.get(&at).ok_or(format!("at {} missing from entitys", at))?.echo )
        };
        Ok(CmdStruct{
            env, telegram, now, id, at, to, topic,
            message: message.to_string(),
            id_level, at_level,
            markdown: false,
            dm: None,
            msg_id: None,
            msg: String::new()
        })
    }

    // Switch to markdown mode
    fn markdown (&mut self) -> &mut Self {
        self.markdown = true;
        self
    }
    // Append more text to message
    fn push_msg (&mut self, s:&str) -> &mut Self {
        self.msg.push_str(s);
        self
    }
    // Reset message with text
    fn set_msg (&mut self, s:&str) -> &mut Self {
        self.msg.clear();
        self.msg.push_str(s);
        self
    }
    // Send new message
    async fn send_msg (&mut self) -> Bresult<()> {
        self.msg_id = None;
        Telegram::send_msg(self).await
    }
    // Edit last message (last msg_id is always cached)
    async fn edit_msg (&mut self) -> Bresult<()> {
        Telegram::send_msg(self).await
    }
    // Send new message to self (TODO: implement edit message to self?)
    async fn send_msg_id (&mut self) -> Bresult<()> {
        self.msg_id = None;
        self.dm = Some(self.id);
        let ret = Telegram::send_msg(self).await;
        self.dm = None;
        ret
    }
    // Buying power is summed long positions (positive) plus double the bank
    // balance (pos or neg) plus 3 times summed short positions (negative).
    // Buying long decreased cash while selling short increases cash.
    // Thus the invariant/buying-power is (otherwise the margin is exceeded/called):
    // 0 < 3*ShortPositions + LongPositions + 2*CashBalance
    async fn buying_power (&mut self) -> Bresult<f64> {
        let positions = Position::get_users_positions(self)?;
        let mut long = 0.0;
        let mut short = 0.0;
        for mut pos in positions {
            if !is_self_stonk(&pos.ticker) {
                pos.update_quote(&self).await?;
                let price = pos.quote.ok_or("buying_power: no quote")?.price;
                let qty = pos.qty;
                if qty < 0.0 {
                    short += qty * price
                } else {
                    long += qty * price
                }
            }
        }
        let cash = getenvstruct!(self)?.entity_balance(self.id)?;
        let bp = 2.0*cash + 3.0*short + long;
        info!("buying_power == cash {:.2}*2 + short {:.2}*3 + long {:.2} = BP {:.2}", cash, short, long, bp);
        Ok(bp)
    }

}

impl MsgDetails for CmdStruct {
    // Get id of channel to message
    fn telegram (&self) -> &Telegram {
        &self.telegram
    }
    fn at (&self) -> i64 {
        if let Some(dm) = self.dm {
            dm // Forced message to id
        } else if self.id_level <= self.at_level {
            self.at // Message the channel if sender has better/equal privs than channel
        } else {
            self.id // Message the "id" channel
        }
    }
    // Is message to send markdown?
    fn markdown (&self) -> bool { self.markdown }
    // Message might be from a channel topic
    fn topic (&self) -> &str { if self.dm.is_some() { "" } else { &self.topic } }
    // Optional edit message id
    fn msg_id (&self) -> Option<i64> { self.msg_id }
    fn set_msg_id (&mut self, i: i64) { self.msg_id = Some(i) }
    // The text to send
    fn msg (&self) -> &str { &self.msg }
}


// This is a macro so logs reveal caller details
#[macro_export]
macro_rules! getenvstruct {($cmdstruct:expr) => {
    $cmdstruct.env
        .try_lock()
        .map_err(|e|
            format!("{}.env.try_lock() -> {:?}", stringify!($cmdstruct), e))
}}

////////////////////////////////////////

#[derive(Debug)]
struct Quote {
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
    async fn new_market_quote (env: Env, ticker: &str) -> Bresult<Self> {
        let json = srvs::get_ticker_raw(ticker).await?;
        //warn!("{}", json);
        let details =
            getin(&json, "/chart/result")
            .get(0)
            .ok_or("chart.result failed on json response")
            .map( |j| getin(j, "/meta") )?;
        info!("{}", details);

        let title_raw =
            &getin_str(&details, "/longName")
            .or_else( |_e| getin_str(&details, "/shortName") )
            .or_else( |_e| getin_str(&details, "/symbol") )
            .unwrap_or( "???".to_string() );
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
        info!("clean title: '{}' -> '{}'", title_raw, &title);

        let exchange = getin_str(&details, "/exchangeName")?;

        let timeStart = getin_i64(&details, "/currentTradingPeriod/regular/start").unwrap_or(0);
        let timeEnd   = getin_i64(&details, "/currentTradingPeriod/regular/end").unwrap_or(0);
        let seconds = timeEnd - timeStart;
        let hours :i64 = if 23400 < seconds { 24 } else { 16 };

        let previous_close   = getin_f64(&details, "/previousClose").unwrap_or(0.0);
        let pre_market_price = getin_f64(&details, "/previousClose").unwrap_or(0.0);
        let reg_market_price = getin_f64(&details, "/regularMarketPrice").unwrap_or(0.0);
        let pst_market_price = getin_f64(&details, "/previousClose").unwrap_or(0.0);

        // This array will be sorted on market time for the latest market data.  Prices
        // are relative to the previous day's market close, including pre and post markets,
        // because most online charts I've come across ignore previous day's after market
        // closing price for next day deltas.  Non-regular markets are basically forgotten
        // after the fact.
        let details = [
            (pre_market_price, reg_market_price, "p", getin_i64(&details, "/currentTradingPeriod/pre/start").unwrap_or(0)),
            (reg_market_price, previous_close,   "r", getin_i64(&details, "/currentTradingPeriod/regular/start").unwrap_or(0)),
            (pst_market_price, previous_close,   "a", getin_i64(&details, "/currentTradingPeriod/post/start").unwrap_or(0))];

        /* // Log all prices for sysadmin requires "use ::datetime::ISO"
        use ::datetime::ISO;
        error!("{} \"{}\" ({}) {}hrs\n{:.2} {:.2} {} {:.2}%\n{:.2} {:.2} {} {:.2}%\n{:.2} {:.2} {} {:.2}%",
            ticker, title, exchange, hours,
            LocalDateTime::from_instant(Instant::at(details[0].3)).iso(), details[0].0, details[0].1, details[0].2,
            LocalDateTime::from_instant(Instant::at(details[1].3)).iso(), details[1].0, details[1].1, details[1].2,
            LocalDateTime::from_instant(Instant::at(details[2].3)).iso(), details[2].0, details[2].1, details[2].2); */

        //details.sort_by( |a,b| b.3.cmp(&a.3) ); // Find latest quote details

        let price = details[1].0;
        let last = details[1].1;
        Ok(Quote{
            env,
            ticker:  ticker.to_string(),
            price, last,
            amount:  roundqty(price-last),
            percent: percentify(last,price),
            market:  details[1].2.to_string(),
            hours, exchange, title,
            updated: true})
    } // Quote::new_market_quote

    /*
    async fn _new_market_quote_1 (env:Env, ticker: &str) -> Bresult<Self> {
        let json = srvs::_get_ticker_raw_1(ticker).await?;

        let details = getin(&json, "/context/dispatcher/stores/QuoteSummaryStore/price");
        if details.is_null() { Err("Unable to find quote data in json key 'QuoteSummaryStore'")? }
        info!("{}", details);

        let title_raw =
            &getin_str(&details, "/longName")
            .or_else( |_e| getin_str(&details, "/shortName") )?;
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
        info!("cleane title: '{}' -> '{}'", title_raw, &title);

        let exchange = getin_str(&details, "/exchange")?;

        let hours = getin(&details, "/volume24Hr");
        let hours :i64 = if hours.is_object() && 0 != hours.as_object().unwrap().keys().len() { 24 } else { 16 };

        let previous_close   = getin_f64(&details, "/regularMarketPreviousClose/raw").unwrap_or(0.0);
        let pre_market_price = getin_f64(&details, "/preMarketPrice/raw").unwrap_or(0.0);
        let reg_market_price = getin_f64(&details, "/regularMarketPrice/raw").unwrap_or(0.0);
        let pst_market_price = getin_f64(&details, "/postMarketPrice/raw").unwrap_or(0.0);

        // This array will be sorted on market time for the latest market data.  Prices
        // are relative to the previous day's market close, including pre and post markets,
        // because most online charts I've come across ignore previous day's after market
        // closing price for next day deltas.  Non-regular markets are basically forgotten
        // after the fact.
        let mut details = [
            (pre_market_price, reg_market_price, "p", getin_i64(&details, "/preMarketTime").unwrap_or(0)),
            (reg_market_price, previous_close,   "r", getin_i64(&details, "/regularMarketTime").unwrap_or(0)),
            (pst_market_price, previous_close,   "a", getin_i64(&details, "/postMarketTime").unwrap_or(0))];

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
    } // Quote::_new_market_quote_1
    */
}

impl Quote {// Query local cache or internet for ticker details
    async fn get_market_quote (cmdstruct: &CmdStruct, ticker: &str) -> Bresult<Self> {
        // Make sure not given referenced stonk. Expected symbols:  GME  308188500   Illegal: @shrewm
        if &ticker[0..1] == "@" { Err("Illegal ticker")? }
        let is_self_stonk = is_self_stonk(ticker);

        let (res, is_in_table, is_cache_valid) = {
            let envstruct = getenvstruct!(cmdstruct)?;
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
                    !update_ticker_p(&envstruct, timesecs, cmdstruct.now, traded_all_day)?
                });
            (res, is_in_table, is_cache_valid)
        };

        let quote =
            if is_cache_valid { // Is in cache so use it
                let hm = &res[0];
                let price = hm.get_f64("price")?;
                let last = hm.get_f64("last")?;
                Quote{
                    env: cmdstruct.env.clone(),
                    ticker: hm.get_string("ticker")?,
                    price, last,
                    amount:   roundqty(price-last),
                    percent:  percentify(last,price),
                    market:   hm.get_string("market")?,
                    hours:    hm.get_i64("hours")?,
                    exchange: hm.get_string("exchange")?,
                    title:    hm.get_string("title")?,
                    updated:  false}
            } else if is_self_stonk { // FNFT is not in cache so create
                Quote {
                    env: cmdstruct.env.clone(),
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
                Quote::new_market_quote(cmdstruct.env.clone(), ticker).await?
            };
        info!("quote = {:?}", quote);

        if !is_self_stonk { // Cached FNFTs are only updated during trading/settling.
            let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
            if !is_in_table {
                getsql!(dbconn, "INSERT INTO stonks VALUES(?,?,?,?,?,?,?,?)",
                    &*quote.ticker, quote.price, quote.last, &*quote.market, quote.hours, &*quote.exchange, cmdstruct.now, &*quote.title)?;
            } else if !is_cache_valid {
                getsql!(dbconn, "UPDATE stonks SET price=?, last=?, market=?, time=? WHERE ticker=?",
                    quote.price, quote.last, &*quote.market, cmdstruct.now, quote.ticker.as_str())?;
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

fn amt_as_glyph (qty: f64, amt: f64) -> (&'static str, &'static str) {
    if 0.0 == amt {
        (from_utf8(b"\xF0\x9F\x94\xB7").unwrap_or("?"), " ") // Blue diamond, nothing
    } else if (0.0 < amt) ^ (qty < 0.0) {
        (from_utf8(b"\xF0\x9F\x9F\xA2").unwrap_or("?"), from_utf8(b"\xE2\x86\x91").unwrap_or("?")) // Green circle, Arrow up
    } else {
        (from_utf8(b"\xF0\x9F\x9F\xA5").unwrap_or("?"), from_utf8(b"\xE2\x86\x93").unwrap_or("?")) // Red square, Arrow down
    }
}

impl Quote { // Format the quote/ticker using its format string IE: 🟢ETH-USD@2087.83! ↑48.49 2.38% Ethereum USD CCC
    fn format_quote (&self, id:i64) -> Bresult<String> {
        let envstruct = &self.env.lock().unwrap();
        let gain_glyphs = amt_as_glyph(0.0, self.amount);
        Ok(Regex::new("(?s)(%([A-Za-z%])|.)")? // (?s) dot accepts newline
            .captures_iter(&envstruct.fmt_str_quote(id))
            .fold(String::new(), |mut s, cap| {
                if let Some(m) = cap.get(2) {
                    match m.as_str() {
                    "A" => s.push_str(gain_glyphs.1), // Arrow
                    "B" => s.push_str(&format!("{}", money_pretty(self.amount.abs()))), // inter-day delta
                    "C" => s.push_str(&percent_squish(self.percent.abs())), // inter-day percent
                    "D" => s.push_str(gain_glyphs.0), // red/green light
                    "E" => s.push_str(&reference_ticker(envstruct, &self.ticker).replacen("_", "\\_", 10000)),  // ticker symbol
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
struct Position { // Represent a ledgered position, and optional quote
    ticker:String,
    qty:   f64,
    price: f64,
    quote: Option<Quote>
}

impl Position {
    async fn update_quote(&mut self, cmdstruct: &CmdStruct) -> Bresult<&mut Position> {
        self.quote = Some(Quote::get_market_quote(cmdstruct, &self.ticker).await?);
        Ok(self)
    }
}

impl Position {
    fn get_users_positions (cmdstruct: &mut CmdStruct) -> Bresult<Vec<Position>> {
        let id = cmdstruct.id;
        let envstruct = &mut getenvstruct!(cmdstruct)?;
        Position::get_user_positions_env(envstruct, id)
    }

    fn get_user_positions_env (envstruct: &mut EnvStruct, id: i64) -> Bresult<Vec<Position>> {
        let dbconn = &envstruct.dbconn;
        Ok(getsql!(dbconn, "SELECT ticker, qty, price FROM positions WHERE id=?", id)?
        .iter()
        .map( |pos|
            Position {
                //id,
                ticker: pos.get_string("ticker").unwrap(),
                qty:    pos.get_f64("qty").unwrap(),
                price:  pos.get_f64("price").unwrap(),
                quote:  None
            } )
        .collect())
    }
}

impl Position {
    // Return vector instead of option (maybe support more of the same position?)
    fn cached_position (cmdstruct:&CmdStruct, id: i64, ticker: &str) -> Bresult<Vec<Position>> {
        let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
        Ok(getsql!(dbconn, "SELECT qty,price FROM positions WHERE id=? AND ticker=?", id, ticker )?
        .iter()
        .map( |pos|
            Position {
                ticker: ticker.to_string(),
                qty:    pos.get_f64("qty").unwrap(),
                price:  pos.get_f64("price").unwrap(),
                quote:  None
            } )
        .collect())
    }
    async fn query_position (cmdstruct:&CmdStruct, id:i64, ticker:&str) -> Bresult<Position> {
        let mut vec = Position::cached_position(cmdstruct, id, ticker)?;
        let mut pos = // Consider the quote or a 0 quantity quote
            match vec.len() {
                0 => Position { ticker: ticker.to_string(), qty: 0.0, price: 0.0, quote: None },
                1 => vec.pop().ok_or("query_position: bad pop")?,
                _ => Err(format!("For {} ticker {} has {} positions, expect 0 or 1", id, ticker, vec.len()))?
            };
        pos.update_quote(cmdstruct).await?;
        Ok(pos)
    }
}

impl Position { // Format the position using its format string.
    fn format_position (&self, envstruct:&EnvStruct, id: i64) -> Bresult<String> {
        let qty = self.qty;
        let cost = self.price;
        let price = self.quote.as_ref().ok_or("format_position: bad quote")?.price;
        let last = self.quote.as_ref().ok_or("format_position: bad quote")?.last;

        let basis = qty*cost;
        let value = qty*price;
        let last_value = qty*last;

        let gain = value - basis;
        let day_gain = value - last_value;

        let gain_percent = if qty<0.0 { percentify(cost, price).abs() } else { percentify(cost, price).abs() };
        let day_gain_percent = percentify(last, price).abs();

        let gain_glyphs = amt_as_glyph(qty, price-cost);
        let day_gain_glyphs = amt_as_glyph(qty, price-last);

        Ok(Regex::new("(?s)(%([A-Za-z%])|.)")?
        .captures_iter(&envstruct.fmt_str_position(id) )
        .fold( String::new(), |mut s, cap| {
            if let Some(m) = cap.get(2) { match m.as_str() {
                "A" => s.push_str( &format!("{:.2}", roundcents(value)) ), // value
                "B" => s.push_str( &round(gain.abs())), // gain
                "C" => s.push_str( gain_glyphs.1), // Arrow
                "D" => s.push_str( &percent_squish(gain_percent)), // gain%
                "E" => s.push_str( gain_glyphs.0 ), // Color
                "F" => s.push_str( &reference_ticker(envstruct, &self.ticker).replacen("_", "\\_", 10000) ), // Ticker
                "G" => s.push_str( &money_pretty(price) ), // latet stonks value
                "H" =>
                    if qty == 0.0 { s.push_str("0") }
                    else { s.push_str( qty.to_string().trim_start_matches('0') ) },
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
    fn new_trade(cmdstruct: &'a mut CmdStruct) -> Bresult<Option<Trade>> {
        //                           _____ticker____  _+-_  _$_   ____________amt_______________
        let caps = regex_to_vec(
            r"(?xi)^
                ([A-Za-z0-9^.-]+)         ### ticker symbol
                ([+-])                    ### + buy, - sell
                ([$])?                    ### amount is dollars
                (\d+\.?|(\d*\.\d{1,4}))?  ### float amount
                $",
            &cmdstruct.message)?;
        if caps.is_empty() { return Ok(None) }
        Ok(Some(Trade{
            cmdstruct,
            ticker:     caps.as_str(1)?.to_uppercase(),
            action:     caps.as_str(2)?.chars().nth(0).ok_or("bad action")?,
            is_dollars: caps.as_str(3).is_ok(),
            amt:        caps.as_f64(4).ok()
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Bot's Do Handlers -- The Meat And Potatos.  The Bread N Butter.  The Works.
////////////////////////////////////////////////////////////////////////////////

async fn do_echo_lvl(cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let caps = must_regex_to_vec("^/echo ?([0-9]+)?", &cmdstruct.message)?;

    let msg = match caps.as_i64(1) {
        Ok(echo) => { // Update existing echo level
            if 2 < echo {
                let msg = "*echo level must be 0…2*";
                cmdstruct.push_msg(&msg).send_msg().await?;
                Err(msg)?
            }
            let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
            getsql!(dbconn, "UPDATE modes set echo=? WHERE id=?", echo, cmdstruct.at)?;
            format!("`echo {} set verbosity {:.0}%`", echo, echo as f64/0.02)
        },
        Err(_) => { // Create or return existing echo level
            let mut echo = 2;
            let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
            let rows = getsql!(dbconn, "SELECT echo FROM modes WHERE id=?", cmdstruct.at)?;
            if rows.len() == 0 { // Create/set echo level for this channel
                getsql!(dbconn, "INSERT INTO modes VALUES(?, ?)", cmdstruct.at, echo)?;
            } else {
                echo = rows[0].get_i64("echo")?;
            }
            format!("`echo {} verbosity at {:.0}%`", echo, echo as f64/0.02)
        }
    };
    cmdstruct.push_msg(&msg).send_msg().await?;
    Ok("COMPLETED.")
}

async fn do_help (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    must_regex_to_vec(r"/help", &cmdstruct.message)?;
    let delay = getenvstruct!(cmdstruct)?.quote_delay_secs;
    let msg = format!(
r#"`          ™Bot Commands          `
`/echo 2` `Echo level (verbose 2…0 quiet)`
`/say hi` `™Bot will say "hi"`
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
`/rebalance -9.99 AMC 40 QQQ 60 ...`
   `rebalances AMC/40% QQQ/60%, opt adj -$9.99`
`/fmt [?]     ` `Show format strings, help`
`/fmt [qp] ...` `Set quote/position fmt str`
`/schedule [time]` `List jobs, delete job at time`
`/schedule [ISO-8601] [-][1h][2m][3] [mtwhfsu*] CMD` `schedule CMD now or ISO-8601 GMT o'clock, offset 1h 2m 3s, repeat on day(s)`
`/httpsget URL` `https get request`
`/httpsbody URL TEXT` `https post request`
`/httpsjson URL JSON` `https post request`"#, delay);
    cmdstruct.markdown().push_msg(&msg).send_msg().await?;
    Ok("COMPLETED.")
}

async fn do_curse(cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    must_regex_to_vec("/curse", &cmdstruct.message)?;
    cmdstruct
        .push_msg(["shit", "piss", "fuck", "cunt", "cocksucker", "motherfucker", "tits"][::rand::random::<usize>()%7])
        .send_msg().await?;
    Ok("COMPLETED.")
}

async fn do_say (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let rev = must_regex_to_vec(r"(?s)^/say (.*)$", &cmdstruct.message)?;
    cmdstruct.push_msg(rev.as_str(1)?).send_msg().await?;
    Ok("COMPLETED.")
}

async fn do_like (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let cap = must_regex_to_vec(r"^([+-])1$", &cmdstruct.message)?;
    let adj =
        if cmdstruct.id == cmdstruct.to {
            return Ok("SKIP self plussed");
        } else if cap.as_str(1)? == "+" { 1 } else { -1 };

    let (fromname, likes, toname) = {
        let ref mut envstruct = getenvstruct!(cmdstruct)?;
        let likes = envstruct.entity_likes_inc(cmdstruct.to, adj)?;
        let entitys = &envstruct.entitys;
        (
            entitys.get(&cmdstruct.id).ok_or("bad id")?.name.to_string(),
            likes,
            entitys.get(&cmdstruct.to).ok_or("bad to")?.name.to_string()
        )
    };

    cmdstruct
        .push_msg(&format!("{}{}{}", fromname, num2heart(likes), toname))
        .send_msg().await?;
    Ok("COMPLETED.".into())
}

async fn do_like_info (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    if cmdstruct.message != "+?" { Err("")? }

    let mut likes :Vec<(i64, String)>=
        getenvstruct!(cmdstruct)?.entitys.iter().map( |(_,e)| (e.likes, e.name.clone()) ).collect();
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


async fn do_def (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let cap = must_regex_to_vec(r"^([A-Za-z-]+):$", &cmdstruct.message)?;
    let word = cap.as_str(1)?;

    info!("{BLD}::do_def(){RST} {:?}", word);

    cmdstruct.markdown();
    let mut msg = String::new();

    // Definitions

    let defs = get_definition(word).await?;

    if defs.is_empty() {
        cmdstruct
            .push_msg(&format!("*{}* def is empty", word))
            .send_msg_id().await?;
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

    // Synonyms

    let mut syns = get_syns(word).await?;

    if syns.is_empty() {
        cmdstruct
            .push_msg(&format!("*{}* syns is empty", word))
            .send_msg_id().await?;
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
        cmdstruct.push_msg(&msg).send_msg().await?;
    }
    Ok("COMPLETED.")
}

async fn do_httpsget (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let cap = must_regex_to_vec(r"^/httpsget\s+([^\s]+)$", &cmdstruct.message)?;
    let url = cap.as_str(1)?;

    let mut msg = String::new();
    let url2 = url.to_string();
    let body = httpsget(&url2).await?;

    if body.is_empty() {
        cmdstruct
            .push_msg(&format!("*{}* response body is empty", &url))
            .send_msg_id().await?;
    } else {
        msg.push_str( &format!("{}", &body) );
    }

    if !msg.is_empty() {
        cmdstruct.push_msg(&msg).send_msg().await?;
    }

    Ok("COMPLETED.")
}

async fn do_httpsbody(cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let cap = must_regex_to_vec(r#"(?s)^/httpsbody\s+([^\s]+) ?(.+)"#, &cmdstruct.message)?;
    let url = cap.as_str(1)?;
    let text = cap.get(2).and_then(|c|c.as_ref()).unwrap_or(&String::new()).to_string();

    let body = httpsbody(&url, text).await?;

    if body.is_empty() {
        cmdstruct
            .push_msg(&format!("*{}* body is empty or error", &url))
            .send_msg_id().await?
    } else {
        cmdstruct.push_msg(&body).send_msg().await?
    }

    Ok("COMPLETED.")
}

async fn do_httpsjson (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let cap = must_regex_to_vec(r#"(?sx) ^ /httpsjson \s+ ([^\s]+) \s* (.+)? $ "#, &cmdstruct.message)?;
    let url = cap.as_str(1)?;
    let string_json =
        cap.as_str(2)
        .map(|s|
            if 2 < s.len()
                && s.chars().nth(0).map_or(false, |c| c == '"')
                && s.chars().last().map_or(false, |c| c == '"')
            {
                s.replace("\n","\\n") // Replace escape newlines if a JSON string
            } else {
                s.to_string()
            })?;
    match httpsjson(url, string_json).await {
        Ok(mut res) => {
           if res.is_empty() { res.push_str("*empty response*") }
           cmdstruct.push_msg(&res).send_msg().await
        }
        Err(e) => cmdstruct.push_msg(&e.to_string()).send_msg_id().await
    }?;

    Ok("COMPLETED.")
}

async fn do_json(cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    match must_regex_to_vec(r#"(?s)^/json\s+(.+)$"#, &cmdstruct.message)?
    .as_str(1)
    .map(|s| {
        if 2 < s.len() && s.ends_with("\"") && s.starts_with("\"") {
            // escapeify newlines in string literals
            s.replace("\n", "\\n")
        } else {
            s.to_string()
        }
    })
    .and_then(|txt| bytes2json(txt.as_bytes()))
    .map(|s| s.to_string())
    .or_else(|e| Ok::<String, ()>(e.to_string()))
    {
        Ok(s) => { cmdstruct.push_msg(&s).send_msg().await?; },
        Err(_) => ()
    };
    Ok("COMPLETED.")
}

async fn do_sql (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let rev = must_regex_to_vec("^(.*)ß$", &cmdstruct.message)?;
    if cmdstruct.id != 308188500 { return Ok("Wrong User"); }
    let sqlexpr = if rev.is_empty() { return Ok("*no stmt*") } else { rev.as_str(1)? };
    let sqlres = {
        let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
        getsql!(dbconn, sqlexpr)
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
                vv.iter().for_each( |(k,_v)|
                    b += &format!(" {}:{}", k,
                      vv.to_string(k)
                      .unwrap_or_else( |e| {error!("{:?}", e); "?".into()} ) ) );
                b+"\n"
            } );

    cmdstruct
        .push_msg(if msg.is_empty() { &"empty results" } else { &msg })
        .send_msg().await?;
    Ok("COMPLETED.")
}

////////////////////////////////////////
// DO QUOTES

// Valid "ticker" strings:  GME, 308188500, @shrewm, local level 2 quotes
fn doquotes_scan_tickers (txt :&str, re:Regex) -> Bresult<HashSet<String>> {
    let mut tickers = HashSet::new();
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
    Ok(tickers)
}

async fn doquotes_pretty (cmdstruct: &CmdStruct, ticker: &str) -> Bresult<String> {
    let (ticker, bidask) = {
        let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
        let ticker = deref_ticker(dbconn, ticker).unwrap_or(ticker.to_string());
        let bidask = if is_self_stonk(&ticker) {
            let mut asks = format!("*Asks:*");
            for ask in getsql!(dbconn, "SELECT -qty AS qty, price FROM exchange WHERE qty<0 AND ticker=? order by price;", &*ticker).map_err(|e| { error!("{:?}", e); e} )? {
                asks.push_str(
                    &format!(" `{}@{}`",
                    num_simp( format!("{:.4}", ask.get_f64("qty")?) ),
                    ask.get_f64("price")?));
            }
            let mut bids = format!("*Bids:*");
            for bid in getsql!(dbconn, "SELECT qty, price FROM exchange WHERE 0<qty AND ticker=? order by price desc;", &*ticker).map_err(|e| { error!("{:?}", e); e} )? {
                bids.push_str(
                    &format!(" `{}@{}`",
                    num_simp( format!("{:.4}", bid.get_f64("qty")?) ),
                    bid.get_f64("price")?));
            }
            format!("\n{}\n{}", asks, bids)
        } else {
            "".to_string()
        };
        (ticker, bidask)
    };
    let quote = Quote::get_market_quote(cmdstruct, &ticker).await.map_err(|e| { error!("{:?}", e); e} )?;
    Ok(format!("{}{}", quote.format_quote(cmdstruct.id)?, bidask))
}

async fn do_quotes (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let tickers =
        doquotes_scan_tickers(
            &cmdstruct.message,
            Regex::new(r"^[@^]?[A-Z_a-z][-.0-9=A-Z_a-z]*$")?)?;
    if tickers.is_empty() { return Err("".into()); }

    cmdstruct.markdown().set_msg("…").send_msg().await?;
    cmdstruct.set_msg("");

    let mut found_tickers = false;
    for ticker in &tickers {
        // Catch error and continue looking up tickers
        match doquotes_pretty(&cmdstruct, ticker).await {
            Ok(res) => {
                info!("doquotes_pretty => {:?}", res);
                cmdstruct.push_msg(&format!("{}\n", res)).edit_msg().await?;
                found_tickers = true;
            },
            e => { glogd!("doquotes_pretty", e); }
        }
    }

    if !found_tickers {
        cmdstruct.set_msg(&"No quotes found").edit_msg().await?;
    }
    Ok("COMPLETED.")
}
// DO QUOTES
////////////////////////////////////////

async fn do_portfolio(cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let caps = must_regex_to_vec(r"(?i)/stonks( .+)?", &cmdstruct.message)?;

    let dosort = caps[1].is_none();
    let positions = Position::get_users_positions(cmdstruct)?;

    cmdstruct.markdown().push_msg("…").send_msg().await?;
    cmdstruct.set_msg("");

    let mut long = 0.0;
    let mut short = 0.0;
    let mut positions_table :Vec<(f64,String)> = Vec::new();
    for mut pos in positions {
        if !is_self_stonk(&pos.ticker) {
            pos.update_quote(&cmdstruct).await?;
            info!("{} position {:?}", cmdstruct.id, &pos);
            let quote = pos.quote.as_ref().ok_or("quote not acquired")?;
            let pretty_position = pos.format_position(&*getenvstruct!(cmdstruct)?, cmdstruct.id)?;
            if dosort {
                let gain = pos.qty*(quote.price - pos.price);
                positions_table.push( (gain, pretty_position) );
                positions_table.sort_by( |a,b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Less) );
                cmdstruct.set_msg( // Refresh message with sorted stonks
                    &positions_table.iter()
                        .map( |(_,s)| s.to_string())
                        .collect::<Vec<String>>().join(""));
            } else {
                cmdstruct.push_msg(&pretty_position);
            }

            if pos.qty < 0.0 {
                short += pos.qty * quote.price;
            } else {
                long += pos.qty * quote.price;
            }
            cmdstruct.edit_msg().await?;
        }
    }

    let cash = getenvstruct!(cmdstruct)?.entity_balance(cmdstruct.id)?;
    let bp = long + short*3.0 + cash*2.0;
    cmdstruct
        .push_msg(&format!("\n`{:.2}``CASH`  `{:.2}``BP`  `{:.2}``YOLO`\n",
            roundcents(IF!(cash<0.0, 0.0, cash)), roundcents(bp), roundcents(long+short+cash)))
        .edit_msg().await?;
    Ok("COMPLETED.")
}

// Handle: /yolo
async fn do_yolo (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    must_regex_to_vec(r"/yolo", &cmdstruct.message)?;

    cmdstruct.markdown().push_msg("...").send_msg().await?;

    let rows = {
        let envstruct = getenvstruct!(cmdstruct)?;
        let dbconn = &envstruct.dbconn;
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
        let ticker = row.get_string("ticker")?;
        info!("Stonk {YEL}{:?}", Quote::get_market_quote(cmdstruct, &ticker).await); // got rid of ?
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
        let envstruct = getenvstruct!(cmdstruct)?;
        getsql!(&envstruct.dbconn, &sql)?
    };

    // Build and send response string

    let mut msg = "*YOLOlians*".to_string();
    for row in sql_results {
        msg.push_str( &format!(" `{:.2}@{}`",
            row.get_f64("yolo")?,
            row.get_string("name")?) );
    }
    cmdstruct.set_msg(&msg).edit_msg().await?;
    Ok("COMPLETED.")
}

// Returns qty,newBalance if qty to buy doesn't exceed buying power
fn verify_qty (mut qty:f64, price:f64, bp:f64) -> Result<(f64,f64), &'static str> {
    qty = roundqty(qty);
    let basis = qty * price;
    let new_balance = bp - basis;
    info!("{BLD}buy? {} @ {} = {}  BANK {} -> {}", qty, price, basis, bp, new_balance);
    if qty < 0.0001 { return Err("Amount too low") }
    if new_balance < 0.0 { return Err("Need more buying power") }
    Ok((qty, new_balance))
}

////////////////////////////////////////////////////////////////////////////////
/// Stonk Buy

#[derive(Debug)]
struct TradeBuy<'a> {
    position: Position,
    qty:f64, // actual qty to trade
    bp: f64,
    trade: Trade<'a>
}

impl<'a> TradeBuy<'a> {
    async fn new_tradebuy(trade: Trade<'a>) -> Bresult<TradeBuy<'a>> {
        let position =
            Position::query_position(&trade.cmdstruct, trade.cmdstruct.id, &trade.ticker).await?;
        let quote = position.quote.as_ref().ok_or("quote not acquired")?;
        trade.cmdstruct.markdown();

        if quote.exchange == "PNK" {
            trade.cmdstruct.push_msg("`OTC/PinkSheet untradeable`").send_msg().await?;
            Err("OTC/PinkSheet untradeable")?
        }

        let bp = trade.cmdstruct.buying_power().await?;
        let qty = match trade.amt {
            Some(amt) => if trade.is_dollars { amt/quote.price } else { amt },
            None => {
                if position.qty < 0.0 {
                    -position.qty // Cover full short position
                } else {
                    roundqty(bp / quote.price) // Buy as much as possible
                }
            }
        };
        Ok( TradeBuy{position, qty, bp, trade} )
    }
}

#[derive(Debug)]
struct TradeBuyCalc<'a> { qty:f64, cost:f64, new_qty:f64, new_basis:f64, new_position_p:bool, tradebuy: TradeBuy<'a> }

impl<'a> TradeBuyCalc<'a> {
    async fn compute_position(obj: TradeBuy<'a>) -> Bresult<TradeBuyCalc<'a>> {
        let quote = obj.position.quote.as_ref().ok_or("quote not acquired")?;
        // TODO: Adjust for short settling?
        let bp = obj.bp + IF!(obj.position.qty<0.0, -obj.position.qty*quote.price, 0.0);
        // Try and fit quantity to buying power (reduce qty until <=)
        let (qty, _new_balance) =
            match
                verify_qty(obj.qty, quote.price, bp)
                .or_else( |_e| verify_qty(obj.qty-0.0001, quote.price, bp) )
                .or_else( |_e| verify_qty(obj.qty-0.0002, quote.price, bp) )
                .or_else( |_e| verify_qty(obj.qty-0.0003, quote.price, bp) )
                .or_else( |_e| verify_qty(obj.qty-0.0004, quote.price, bp) )
                .or_else( |_e| verify_qty(obj.qty-0.0005, quote.price, bp) ) {
                Err(e) => {  // Message user problem and log
                    obj.trade.cmdstruct.push_msg(&e).send_msg_id().await?;
                    return Err(e.into())
                },
                Ok(r) => r
            };
        let qty_old = obj.position.qty;
        let price_old = obj.position.price;
        let new_position_p = qty_old == 0.0;
        let price_new = quote.price;
        let cost = qty * price_new;
        let new_basis = (qty * price_new + qty_old * price_old) / (qty + qty_old);
        let new_qty = roundqty(qty + qty_old);

        Ok(Self{qty, cost, new_qty, new_basis, new_position_p, tradebuy:obj})
    }
}

#[derive(Debug)]
struct ExecuteBuy<'a> { msg:String, tradebuycalc: TradeBuyCalc<'a> }

impl<'a> ExecuteBuy<'a> {
    fn execute (mut obj: TradeBuyCalc<'a>) -> Bresult<Self> {
        let quote = obj.tradebuy.position.quote.as_ref().ok_or("quote not acquired")?;
        let now = obj.tradebuy.trade.cmdstruct.now;

        let dst_hours_adjust = getenvstruct!(obj.tradebuy.trade.cmdstruct)?.dst_hours_adjust;
        if quote.hours!=24 && !trading_hours_p(dst_hours_adjust, now)? {
            return Ok( ExecuteBuy {
                msg: format!("Unable to buy {} after hours", obj.tradebuy.trade.ticker),
                tradebuycalc:obj
            } )
        }

        let msg = {
            let mut envstruct = getenvstruct!(obj.tradebuy.trade.cmdstruct)?;
            let dbconn = &envstruct.dbconn;
            let id = obj.tradebuy.trade.cmdstruct.id;
            let ticker = &obj.tradebuy.trade.ticker;

            let price = quote.price;

            sql_table_order_insert(dbconn, id, ticker, obj.qty, price, now)?;
            let mut msg = format!("*Bought:*");
            msg += &format!("  `{:.2}``{}` *{}*_@{}_", obj.qty*price, ticker, obj.qty, price);

            if obj.new_position_p {
                getsql!(dbconn, "INSERT INTO positions VALUES (?, ?, ?, ?)", id, &**ticker, obj.new_qty, obj.new_basis)?;
            } else if obj.new_qty == 0.0 {
                getsql!(dbconn, "DELETE FROM positions WHERE id=? AND ticker=?", id, &**ticker)?;
            } else {
                info!("{BLD}add to existing position:  {} @ {}  ->  {} @ {}", obj.tradebuy.position.qty, price, obj.new_qty, obj.new_basis);
                getsql!(dbconn, "UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=?", obj.new_qty, obj.new_basis, id, &**ticker)?;
            }

            envstruct.entity_balance_inc(id, -obj.cost)?;

            if obj.new_qty != 0.0 {
                obj.tradebuy.position.qty = obj.new_qty; // TODO: Mutating previous monadic state
                obj.tradebuy.position.price = obj.new_basis;
                msg.push_str(&obj.tradebuy.position.format_position(&envstruct, id)?);
            }
            msg
        };

        Ok(Self{msg, tradebuycalc:obj})
    }
}

async fn do_trade_buy (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let trade = Trade::new_trade(cmdstruct)?;
    if trade.as_ref().map_or(true, |trade| trade.action != '+') { Err("")?  }

    let res =
        match TradeBuy::new_tradebuy(trade.unwrap()).await
        { Ok(tb) => TradeBuyCalc::compute_position(tb).await, Err(e) => Err(e) }
        .map(ExecuteBuy::execute)??;

    info!("{BLD_RED}Result {:#?}", &res);

    res.tradebuycalc.tradebuy.trade.cmdstruct.push_msg(&res.msg).edit_msg().await?; // Report to group
    Ok("COMPLETED.")
}

////////////////////////////////////////////////////////////////////////////////
/// Stonk Sell

#[derive(Debug)]
struct TradeSell<'a> {
    position: Position,
    short: bool,
    qty: f64, // calculated quantity to sell
    price: f64, // stonk price
    _bank_balance: f64,
    new_balance: f64, // eventual bank balance
    new_qty: f64,     // eventual position quantity
    trade: Trade<'a>,
    bp: f64
}

impl<'a> TradeSell<'a> {
    async fn new_tradesell (trade: Trade<'a>) -> Bresult<TradeSell<'a>> {
        let position = Position::query_position(trade.cmdstruct, trade.cmdstruct.id, &trade.ticker).await?;
        let quote = position.quote.as_ref().ok_or("quote not acquired")?;
        let price = quote.price;
        trade.cmdstruct.markdown();

        if quote.exchange == "PNK" {
            trade.cmdstruct.push_msg("`OTC / PinkSheet untradeable`").send_msg().await?;
            Err("OTC / PinkSheet Verboten Stonken")?
        }

        let bp = trade.cmdstruct.buying_power().await?;

        let qty =
            roundqty(match trade.amt {
                Some(amt) => IF!(trade.is_dollars, amt/price, amt), // Convert dollars to shares
                None =>
                    if position.qty <= 0.0 {
                        roundqty(bp / price) // Short entire bying power
                    } else {
                        position.qty // no amount set, so set to entire qty
                    }
            });

        if qty == 0.0 {
            trade.cmdstruct.push_msg("Quantity too low.").send_msg().await?;
            Err("sell qty too low")?
        }

        let short = position.qty <= 0.0;

        let (mut qty, _new_balance) =
            if !short { (qty, 0.0) } else {
            match
                verify_qty(qty, price, bp)
                .or_else( |_e| verify_qty(qty-0.0001, price, bp) )
                .or_else( |_e| verify_qty(qty-0.0002, price, bp) )
                .or_else( |_e| verify_qty(qty-0.0003, price, bp) )
                .or_else( |_e| verify_qty(qty-0.0004, price, bp) )
                .or_else( |_e| verify_qty(qty-0.0005, price, bp) ) {
                Err(e) => {  // Message user problem and log
                    trade.cmdstruct.push_msg(&e).send_msg().await?;
                    (qty, 0.0)
                },
                Ok(r) => r
            } };

        let mut gain = qty*price;
        let _bank_balance = getenvstruct!(trade.cmdstruct)?.entity_balance(trade.cmdstruct.id)?;
        let mut new_balance = _bank_balance+gain;

        info!("{BLD}sell? {} {}/{} @ {} = {}  CASH {} -> {}", IF!(short, "short", "long"), qty, position.qty, price, gain,  _bank_balance, new_balance);

        // If equal to the rounded position value, snap qty to exact position
        if qty != position.qty && roundcents(gain) == roundcents(position.qty*price) {
            qty = position.qty;
            gain = qty*price;
            new_balance = _bank_balance + gain;
            info!("{BLD}sell? {}/{} @ {} = {}  BANK {} -> {}", qty, position.qty, price, gain,  _bank_balance, new_balance);
        }

        if !short && position.qty < qty {
            trade.cmdstruct.push_msg("You can't sell more than you own.").send_msg().await?;
            return Err("not enough shares to sell".into());
        }

        let new_qty = roundqty(position.qty-qty);

        Ok( Self{ position, short, qty, price, _bank_balance, new_balance, new_qty, trade, bp} )
    }
}

#[derive(Debug)]
struct ExecuteSell<'a> {
    msg: String,
    tradesell: TradeSell<'a>
}

impl<'a> ExecuteSell<'a> {
    fn execute (mut obj: TradeSell<'a>) -> Bresult<Self> {
        let msg = {
            let now = obj.trade.cmdstruct.now;
            if obj.position.quote.as_ref().unwrap().hours != 24
                && !trading_hours_p(getenvstruct!(obj.trade.cmdstruct)?.dst_hours_adjust, now)? {
                return Ok(Self{msg:format!("Unable to sell {} after hours", obj.position.ticker), tradesell:obj});
            }
            let mut envstruct = getenvstruct!(obj.trade.cmdstruct)?;
            let dbconn = &envstruct.dbconn;
            let id = obj.trade.cmdstruct.id;
            let ticker = &obj.trade.ticker;
            let qty = obj.qty;
            let price = obj.price;

            let mut msg = IF!(obj.short, format!("*Short:*"), format!("*Sold:*"));

            let new_qty = obj.new_qty;
            if new_qty == 0.0 {
                sql_table_order_insert(dbconn, id, ticker, -qty, price, now)?;
                getsql!(dbconn, "DELETE FROM positions WHERE id=? AND ticker=?", id, &**ticker)?;
                msg += &obj.position.format_position(&envstruct, id)?;
                envstruct.entity_balance_set(id, obj.new_balance)?;
            } else if obj.position.qty == 0.0 {
                let amt = qty*price;
                if obj.bp < amt {
                    msg = format!("${} of {} exceeds buying power of ${}", money_pretty(amt), ticker, money_pretty(obj.bp));
                } else {
                    sql_table_order_insert(dbconn, id, ticker, -qty, price, now)?;
                    getsql!(dbconn, "INSERT INTO positions VALUES (?, ?, ?, ?)", id, &**ticker, obj.new_qty, obj.price)?;
                    obj.position.qty = obj.new_qty; // so format_position is up to date
                    obj.position.price = obj.price; // Update previous monad so position is printed correctly
                    msg += &format!("  `{:.2}``{}` *{}*_@{}_{}",
                        amt, ticker, qty, price,
                        &obj.position.format_position(&envstruct, id)?);
                    envstruct.entity_balance_set(id, obj.new_balance)?;
                }
            } else {
                let amt = qty*price;
                if obj.short && obj.bp < amt {
                    msg = format!("${} of {} exceeds buying power of ${}", money_pretty(amt), ticker, money_pretty(obj.bp));
                } else {
                    sql_table_order_insert(dbconn, id, ticker, -qty, price, now)?;
                    let new_basis = (amt + -obj.position.qty * obj.position.price) / -new_qty;
                    getsql!(dbconn, "UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=?", new_qty, new_basis, id, &**ticker)?;
                    obj.position.qty = obj.new_qty; // so format_position is up to date
                    obj.position.price = new_basis;
                    msg += &format!("  `{:.2}``{}` *{}*_@{}_{}",
                        amt, ticker, qty, price,
                        &obj.position.format_position(&envstruct, id)?);
                    envstruct.entity_balance_set(id, obj.new_balance)?;
                }
            }
            msg
        };
        Ok(Self{msg, tradesell:obj})
    }
}

async fn do_trade_sell (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let trade = Trade::new_trade(cmdstruct)?;
    if trade.as_ref().map_or(true, |trade| trade.action != '-') { Err("")? }

    let res =
        TradeSell::new_tradesell(trade.unwrap()).await
        .map(ExecuteSell::execute)??;

    info!("{BLD_RED}Result {:#?}", res);
    res.tradesell.trade.cmdstruct.push_msg(&res.msg).send_msg().await?; // Report to group
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
            let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
            deref_ticker(dbconn, &thing)?
        };
        let qty   = roundqty(caps.as_f64(2)?);
        let price = caps.as_f64(4)?;
        let now   = Instant::now().seconds();
        let id    = cmdstruct.id;
        Ok(Some(ExQuote {cmdstruct, id, thing, qty, price, ticker, now } ))
    }
}

#[derive(Debug)]
struct QuoteCancelMine<'a> {
    qty: f64,
    myasks: Vec<HashMap<String, ::sqlite::Value>>,
    myasksqty: f64,
    mybids: Vec<HashMap<String, ::sqlite::Value>>,
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
            let dbconn = & getenvstruct!(cmdstruct)?.dbconn;

            let mut myasks = getsql!(dbconn, "SELECT * FROM exchange WHERE id=? AND ticker=? AND qty<0.0 ORDER BY price", id, &**ticker)?;
            let myasksqty = -roundqty(myasks.iter().map( |ask| ask.get_f64("qty").unwrap() ).sum::<f64>());
            let mut mybids = getsql!(dbconn, "SELECT * FROM exchange WHERE id=? AND ticker=? AND 0.0<=qty ORDER BY price DESC", id, &**ticker)?;

            if qty < 0.0 { // This is an ask exquote
                // Remove/decrement a matching bid in the current settled market
                if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get_f64("price").unwrap() ) {
                    let bidqty = roundqty(mybid.get_f64("qty")?);
                    if bidqty <= -qty {
                        getsql!(dbconn, "DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", id, &**ticker, bidqty, price)?;
                        mybid.insert("*UPDATED*".into(), ::sqlite::Value::String("*REMOVED*".into()));
                        qty = roundqty(qty + bidqty);
                        msg += &format!("\n*Removed bid:* `{}+{}@{}`", exquote.thing, bidqty, price);
                    } else {
                        let newbidqty = roundqty(bidqty+qty);
                        getsql!(dbconn, "UPDATE exchange SET qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newbidqty, id, &**ticker, bidqty, price)?;
                        mybid.insert("*UPDATED*".into(), ::sqlite::Value::String(format!("*QTY={:.}*", newbidqty)));
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
                        myask.insert("*UPDATED*".into(), ::sqlite::Value::String("*REMOVED*".into()));
                        qty = roundqty(qty + askqty);
                        msg += &format!("\n*Removed ask:* `{}{}@{}`", exquote.thing, askqty, price);
                    } else {
                        let newaskqty = roundqty(askqty+qty);
                        getsql!(dbconn, "UPDATE exchange SET qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newaskqty, id, &**ticker, askqty, price)?;
                        myask.insert("*UPDATED*".into(), ::sqlite::Value::String(format!("*QTY={:.}*", newaskqty)));
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
    _qty: f64,
    _bids: Vec<HashMap<String, ::sqlite::Value>>,
    quotecancelmine: QuoteCancelMine<'a>,
    msg: String
}

impl<'a> QuoteExecute<'a> {
    async fn doit (mut obj: QuoteCancelMine<'a>) -> Bresult<QuoteExecute<'a>> {
        let (_qty, _bids, msg) = {
            let id = obj.exquote.id;
            let ticker = &obj.exquote.ticker;
            let price = obj.exquote.price;
            let mut qty = obj.qty;
            let now = obj.exquote.now;
            let myasks = &mut obj.myasks;
            let mybids = &mut obj.mybids;
            let asksqty = obj.myasksqty;

            let (mut bids, mut asks) = {
                let dbconn = &getenvstruct!(obj.exquote.cmdstruct)?.dbconn;
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
            let position = Position::query_position( &obj.exquote.cmdstruct, id, ticker).await?;
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

                        let mut envstruct = getenvstruct!(obj.exquote.cmdstruct)?;

                        {
                            let dbconn = &envstruct.dbconn;
                            if xqty == aqty { // bid to be entirely settled
                                getsql!(dbconn, "DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", aid, &**ticker, aqty, aprice)?;
                                abid.insert("*UPDATED*".into(), sqlite::Value::String("*REMOVED*".into()));
                            } else {
                                getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", aqty-xqty, aid, &**ticker, aqty, aprice)?;
                                abid.insert("*UPDATED*".into(), sqlite::Value::String(format!("*qty={}*", aqty-xqty)));
                            }

                            // Update order table with the purchase and buy info
                            sql_table_order_insert(dbconn, id, ticker, -xqty, aprice, now)?;
                            sql_table_order_insert(dbconn, aid, ticker, xqty, aprice, now)?;
                        }

                        // Update each user's account
                        let value = xqty * aprice;
                        envstruct.entity_balance_inc(id, value)?;
                        envstruct.entity_balance_inc(aid, -value)?;

                        let name = envstruct.entity_id2name(id);
                        let namea = envstruct.entity_id2name(aid);
                        msg += &format!("\n*Settled:*\n{} `{}{:+}@{}` <-> `${}` {}",
                                name.unwrap_or(&id.to_string()),
                                obj.exquote.thing, xqty, aprice,
                                value,
                                namea.unwrap_or(&aid.to_string()));

                        // Update my position
                        let newposqty = roundqty(posqty - xqty);
                        let dbconn = &envstruct.dbconn;
                        if 0.0 == newposqty {
                            getsql!(dbconn, "DELETE FROM positions WHERE id=? AND ticker=? AND qty=?", id, &**ticker, posqty)?;
                        } else {
                            getsql!(dbconn, "UPDATE positions SET qty=? WHERE id=? AND ticker=? AND qty=?", newposqty, id, &**ticker, posqty)?;
                        }

                        // Update buyer's position
    /**/                let aposition = Position::query_position( &obj.exquote.cmdstruct, aid, ticker).await?;
                        let aposqty = aposition.qty;
                        let aposprice = aposition.price;

                        if 0.0 == aposqty {
                            getsql!(dbconn, "INSERT INTO positions VALUES(?, ?, ?, ?)", aid, &**ticker, xqty, value)?;
                        } else {
                            let newaposqty = roundqty(aposqty+aqty);
                            let newaposcost = (aposprice + value) / (aposqty + xqty);
                            getsql!(dbconn, "UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=? AND qty=?", newaposqty, newaposcost, aid, &**ticker, aposqty)?;
                        }

                        // Update stonk exquote value
    /**/                let last_price = Quote::get_market_quote(obj.exquote.cmdstruct, ticker).await?.price;
                        getsql!(dbconn, "UPDATE stonks SET price=?, last=?, time=? WHERE ticker=?", aprice, last_price, now, &**ticker)?;

                        qty = roundqty(qty+xqty);
                        if 0.0 <= qty { break }
                    } // for

                    // create or increment in exchange table my ask.  This could also be the case if no bids were executed.
                    if qty < 0.0 {
                        let dbconn = &getenvstruct!(obj.exquote.cmdstruct)?.dbconn;
                        if let Some(myask) = myasks.iter_mut().find( |a| price == a.get_f64("price").unwrap() ) {
                            let oldqty = myask.get_f64("qty").unwrap();
                            let newqty = roundqty(oldqty + qty); // both negative, so "increased" ask qty
                            let mytime = myask.get_i64("time").unwrap();
                            getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND price=? AND time=?", newqty, id, &**ticker, price, mytime)?;
                            myask.insert("*UPDATED*".into(), ::sqlite::Value::String(format!("qty={}", newqty)));
                            msg += &format!("\n*Updated ask:* `{}{}@{}` -> `{}@{}`", obj.exquote.thing, oldqty, price, newqty, price);
                        } else {
                            getsql!(dbconn, "INSERT INTO exchange VALUES (?, ?, ?, ?, ?)", id, &**ticker, &*format!("{:.4}", qty), &*format!("{:.4}", price), now)?;
                            // Add a fake entry to local myasks vector for logging's sake
                            let mut hm = HashMap::<String, ::sqlite::Value>::new();
                            hm.insert("*UPDATED*".to_string(), ::sqlite::Value::String(format!("*INSERTED* {} {} {} {} {}", id, ticker, qty, price, now)));
                            myasks.push(hm);
                            msg += &format!("\n*Created ask:* `{}{}@{}`", obj.exquote.thing, qty, price);
                        }
                    }
                }
            } else if 0.0 < qty { // This is a bid exquote (want to buy someone)
                // Limited by available cash but that's up to the current best ask price and sum of ask costs.
                let basis = qty * price;
                let bank_balance = getenvstruct!(obj.exquote.cmdstruct)?.entity_balance(obj.exquote.id)?;
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
                            let mut envstruct = getenvstruct!(obj.exquote.cmdstruct)?;

                            {
                                let dbconn = &envstruct.dbconn;

                                if xqty == -aqty { // ask to be entirely settled
                                    getsql!(dbconn, "DELETE FROM exchange WHERE id=? AND ticker=? AND qty=? AND price=?", aid, &**ticker, aqty, aprice)?;
                                    aask.insert("*UPDATED*".into(), ::sqlite::Value::String("*REMOVED*".into()));
                                } else {
                                    let newqty = aqty+xqty;
                                    getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND qty=? AND price=?", newqty, aid, &**ticker, aqty, aprice)?;
                                    aask.insert("*UPDATED*".into(), ::sqlite::Value::String(format!("*qty={}*", newqty)));
                                }

                                //error!("new order {} {} {} {} {}", id, ticker, aqty, price, now);
                                // Update order table with the purchase and buy info
                                sql_table_order_insert(dbconn, id, ticker, xqty, aprice, now)?;
                                sql_table_order_insert(dbconn, aid, ticker, -xqty, aprice, now)?;
                            }

                            // Update each user's account
                            let value = xqty * aprice;
                            envstruct.entity_balance_inc(id, -value)?;
                            envstruct.entity_balance_inc(aid, value)?;

                            msg += &format!("\n*Settled:*\n{} `${}` <-> `{}{:+}@{}` {}",
                                    envstruct.entity_id2name(id).unwrap_or(&id.to_string()),
                                    value,
                                    obj.exquote.thing, xqty, aprice,
                                    envstruct.entity_id2name(aid).unwrap_or(&aid.to_string()));

                            // Update my position
                            let dbconn = &envstruct.dbconn;
                            if 0.0 == posqty {
                                getsql!(dbconn, "INSERT INTO positions VALUES(?, ?, ?, ?)", id, &**ticker, xqty, value)?;
                            } else {
                                let newposqty = roundqty(posqty+xqty);
                                let newposcost = (posprice + value) / (posqty + xqty);
                                getsql!(dbconn, "UPDATE positions SET qty=?, price=? WHERE id=? AND ticker=? AND qty=?",
                                    newposqty, newposcost, id, &**ticker, posqty)?;
                            }
                        }

                        {
                            // Update their position
        /**/                let aposition = Position::query_position( &obj.exquote.cmdstruct, aid, ticker).await?;
                            let aposqty = aposition.qty;
                            let newaposqty = roundqty(aposqty - xqty);
                            let dbconn = &getenvstruct!(obj.exquote.cmdstruct)?.dbconn;
                            if 0.0 == newaposqty {
                                getsql!(dbconn, "DELETE FROM positions WHERE id=? AND ticker=? AND qty=?", aid, &**ticker, aposqty)?;
                            } else {
                                getsql!(dbconn, "UPDATE positions SET qty=? WHERE id=? AND ticker=? AND qty=?", newaposqty, aid, &**ticker, aposqty)?;
                            }
                        }

                        // Update self-stonk exquote value
                        let last_price = Quote::get_market_quote(obj.exquote.cmdstruct, ticker).await?.price;
                        let dbconn = &getenvstruct!(obj.exquote.cmdstruct)?.dbconn;
                        getsql!(dbconn, "UPDATE stonks SET price=?, last=?, time=? WHERE ticker=?",
                            aprice, last_price, now, &**ticker)?;

                        qty = roundqty(qty-xqty);
                        if qty <= 0.0 { break }
                    } // for

                    // create or increment in exchange table my bid.  This could also be the case if no asks were executed.
                    if 0.0 < qty{
                        let dbconn = &getenvstruct!(obj.exquote.cmdstruct)?.dbconn;
                        if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get_f64("price").unwrap() ) {
                            let oldqty = mybid.get_f64("qty").unwrap();
                            let newqty = roundqty(oldqty + qty);
                            let mytime = mybid.get_i64("time").unwrap();
                            getsql!(dbconn, "UPDATE exchange set qty=? WHERE id=? AND ticker=? AND price=? AND time=?", newqty, id, &**ticker, price, mytime)?;
                            mybid.insert("*UPDATED*".into(), ::sqlite::Value::String(format!("qty={}", newqty)));
                            msg += &format!("\n*Updated bid:* `{}+{}@{}` -> `{}@{}`", obj.exquote.thing, oldqty, price, newqty, price);
                        } else {
                            getsql!(dbconn, "INSERT INTO exchange VALUES (?, ?, ?, ?, ?)", id, &**ticker, &*format!("{:.4}",qty), &*format!("{:.4}",price), now)?;
                            // Add a fake entry to local mybids vector for logging's sake
                            let mut hm = HashMap::<String, ::sqlite::Value>::new();
                            hm.insert("*UPDATED*".to_string(), ::sqlite::Value::String(format!("*INSERTED* {} {} {} {} {}", id, ticker, qty, price, now)));
                            mybids.push(hm);
                            msg += &format!("\n*Created bid:* `{}+{}@{}`", obj.exquote.thing, qty, price);
                        }
                    }
                } // else
            } // if
            (qty, bids, msg)
        };
        Ok(Self{_qty, _bids, quotecancelmine: obj, msg})
    }
}

async fn do_exchange_bidask (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let exquote = ExQuote::scan(cmdstruct)?;
    let exquote = if exquote.is_none() { Err("")? } else { exquote.unwrap() };

    let ret =
        QuoteCancelMine::doit(exquote)
        .map(|obj| QuoteExecute::doit(obj) )?.await?;
    info!("{BLD_RED}Result {:#?}", ret);
    if 0 != ret.msg.len() {
        ret.quotecancelmine.exquote.cmdstruct
            .markdown()
            .push_msg(
                &ret.msg
                .replacen("_", "\\_", 1000)
                .replacen(">", "\\>", 1000))
            .send_msg().await?;
    } // Report to group
    Ok("COMPLETED.")
}

async fn do_orders (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let id = cmdstruct.id;
    let mut asks = String::from("");
    let mut bids = String::from("");
    let rows = {
        let envstruct = &getenvstruct!(cmdstruct)?;
        let dbconn = &envstruct.dbconn;

        must_regex_to_vec(r"(?i)/orders", &cmdstruct.message)?;

        for order in getsql!(dbconn, "SELECT * FROM exchange WHERE id=?", id)? {
            let ticker = order.get_string("ticker")?;
            let stonk = reference_ticker(&envstruct, &ticker).replacen("_", "\\_", 10000);
            let qty = order.get_f64("qty")?;
            let price = order.get_f64("price")?;
            if qty < 0.0 {
                asks += &format!("\n{}{:+}@{}", stonk, qty, price);
            } else {
                bids += &format!("\n{}{:+}@{}", stonk, qty, price);
            }
        }

        // Include all self-stonks positions (mine and others)
        let sql = format!(r#"
            SELECT id||'' AS ticker, 0.0 AS qty, 0.0 AS price
            FROM entitys
            WHERE 0<id AND id NOT IN (SELECT ticker FROM positions WHERE id={})
        UNION
            SELECT ticker,qty,price
            FROM positions
            WHERE id={} AND ticker IN ( SELECT id FROM entitys WHERE 0<id)"#, id, id);
        getsql!(dbconn, sql)?
    };

    let mut msg = String::new();
    let mut total = 0.0;
    for pos in rows {
        if is_self_stonk(&pos.get_string("ticker")?) {
            let mut pos = {
                Position {
                    ticker: pos.get_string("ticker")?,
                    qty: pos.get_f64("qty")?,
                    price: pos.get_f64("price")?,
                    quote: None
                }
            };
            pos.update_quote(&cmdstruct).await?;
            msg += &pos.format_position(&mut*getenvstruct!(cmdstruct)?, id)?;
            total += pos.qty * pos.quote.unwrap().price;
        }
    }

    let cash = getenvstruct!(cmdstruct)?.entity_balance(id)?;
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

    cmdstruct.markdown().push_msg(&msg).send_msg().await?;
    Ok("COMPLETED.")
}
async fn send_format_strings_help (cmdstruct: &mut CmdStruct) -> Bresult<()> {
    cmdstruct
        .markdown()
        .push_msg(FORMAT_STRINGS_HELP)
        .edit_msg().await?;
    Ok(())
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

async fn do_fmt (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let caps = must_regex_to_vec(r"^/fmt( ([qp?])[ ]?(.*)?)?$", &cmdstruct.message)?;
    //caps.iter().for_each( |c| println!("{BLD_MAG}{:?}", c));

    let id = cmdstruct.id;

    // "/fmt" show current format strings

    if caps.as_str(1).is_err() {
        let (fmt_quote, fmt_position) = {
            let envstruct = getenvstruct!(cmdstruct)?;
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
            getenvstruct!(cmdstruct)?.entitys.get_mut(&id).unwrap().quote = new_format_str.to_string();
            "quote"
        },
        "p" => {
            getenvstruct!(cmdstruct)?.entitys.get_mut(&id).unwrap().position = new_format_str.to_string();
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
        .send_msg().await?;

    let dbconn = &getenvstruct!(cmdstruct)?.dbconn;

    // make change to DB
    getsql!(dbconn, "INSERT OR IGNORE INTO formats VALUES (?, '', '')", id)?;
    getsql!(dbconn, format!("UPDATE formats SET {}=? WHERE id=?", format_type),
        &*new_format_str.replacen("\"", "\"\"", 10000), id)?;

    Ok("COMPLETED.")
}

async fn do_rebalance (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let caps = must_regex_to_vec(
        //             _______float to 2 decimal places______                                    ____________float_____________
        r"(?i)^/rebalance( (-?[0-9]+[.]?|(-?[0-9]*[.][0-9]{1,2})))?(( [@^]?[A-Z_a-z][-.0-9=A-Z_a-z]* (([0-9]*[.][0-9]+)|([0-9]+[.]?)))+)",
        &cmdstruct.message)?;

    cmdstruct
        .markdown()
        .push_msg(&format!("Rebalancing:"))
        .send_msg().await?;

    let percents = // HashMap of Ticker->Percent
        Regex::new(r" ([@^]?[A-Z_a-z][-.0-9=A-Z_a-z]*) (([0-9]*[.][0-9]+)|([0-9]+[.]?))")?
        .captures_iter(&caps.as_string(4)?)
        .map(|cap|
            ( cap.get(1).unwrap().as_str().to_uppercase(),
              cap.get(2).unwrap().as_str().parse::<f64>().unwrap() / 100.0 ) )
        .collect::<HashMap<String, f64>>();


    if { // Check/exit if after hours rebalancing
        let envstruct = &getenvstruct!(cmdstruct)?;
        !trading_hours_p(envstruct.dst_hours_adjust, cmdstruct.now)?
        &&
        getsql!(
            envstruct.dbconn,
            format!("SELECT hours FROM stonks WHERE hours!=24 AND ticker IN ('{}')",
                percents.keys().map(String::to_string).collect::<Vec<String>>().join("','")))?
        .len() != 0
    } {
        cmdstruct.push_msg(&"\nRebalance During Trading Hours Mon..Fri 1AM..5PM").edit_msg().await?;
        return Ok("COMPLETED.");
    }

    for ticker in percents.keys() { // Refresh stonk quotes
        if !is_self_stonk(&ticker) {
            if Quote::get_market_quote(cmdstruct, &ticker).await.map_err( |e| warn!("ticker {} invalid {}", ticker, e) ).is_err() {
                cmdstruct.push_msg(&format!(" ~{}~", ticker)).edit_msg().await?;
            } else {
                // Update feedback message with ticker symbol
                cmdstruct.push_msg(&format!(" {}", ticker)).edit_msg().await?;
            }
        }
    }

    let mut positions = {
        let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
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
        cmdstruct.push_msg("no valid tickers").edit_msg().await?;
    } else {
        for i in 0..positions.len() {
            let ticker = positions[i].get_string("ticker")?;
            let value = positions[i].get_f64("value")?;
            let mut diff = roundfloat(percents.get(&ticker).unwrap() * total - value, 2);
            if -0.01 < diff && diff < 0.01 { diff = 0.0; } // under 1¢ diffs will be skipped
            positions[i].insert("diff".to_string(), ::sqlite::Value::String(diff.to_string())); // Add new key/val to Position HashMap
        }
        for i in 0..positions.len() {
            if positions[i].get_string("diff")?.chars().nth(0).unwrap() == '-'  {
                info!("rebalance position {:?}", positions[i]);
                let ticker = &positions[i].get_string("ticker")?;
                let diffstr = &positions[i].get_string("diff")?[1..];
                cmdstruct.push_msg("\n");
                if "0" == diffstr {
                    cmdstruct.push_msg(&format!("{} is balanced", ticker)).edit_msg().await?;
                } else {
                    let message = format!("{}-${}", ticker, &diffstr);
                    cmdstruct.message = message;
                    glogd!(" do_trade_sell", do_trade_sell(cmdstruct).await); // Recurse on same cmsstruct but mutated message
                }
                //cmdstruct.edit_msg()?;
            }
        }
        for i in 0..positions.len() {
            if positions[i].get_string("diff")?.chars().nth(0).unwrap() != '-'  {
                info!("rebalance position {:?}", positions[i]);
                let ticker = &positions[i].get_string("ticker")?;
                let mut diffstr = positions[i].get_string("diff")?;
                if diffstr != "0" {
                    let bp = cmdstruct.buying_power().await?;
                    // The last buy might be so off, so skip or adjust to account value
                    if bp < diffstr.parse::<f64>()? {
                        if 0.01 <= bp {
                            diffstr = format!("{}", (bp*100.0) as i64 as f64 / 100.0);
                        } else {
                            diffstr = format!("0");
                        }
                        warn!("updated diff position {} {}", ticker, diffstr);
                    }
                }
                cmdstruct.push_msg("\n");
                if "0" == diffstr {
                    cmdstruct.push_msg(&format!("{} is balanced", ticker)).edit_msg().await?;
                } else {
                    let message = format!("{}+${}", ticker, &diffstr);
                    //cmdstruct.push_msg(&format!("\n{}\n", message));
                    cmdstruct.message = message;
                    glogd!(" do_trade_buy", do_trade_buy(cmdstruct).await); // Recurse on same cmsstruct but mutated message
                }
                //cmdstruct.send_msg().await?;
            }
        }
    }
    Ok("COMPLETED.")
}

async fn do_schedule (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let caps =
        must_regex_to_vec(
            r"(?sxi)^/schedule
            (
                (?: ### Fixed GMT
                    \ +
                    ( \d+ - \d{1,2} - \d{1,2} T )?
                    ( \d{1,2} : \d{1,2} : \d{1,2} )
                    (?: Z | [-+]0{1,4})
                )?
                (?: ### Duration
                    \ + (-?  \d+ (?: h (?: \d+ m)? | m)? \d*)
                )?
                (?: ### days
                    \ + ([*]|[mtwhfsu]+)
                )?
                #### Command
                (?: \ + (.+))?
            )",
            &cmdstruct.message )?;

    cmdstruct.markdown();

    // "/schedule" Show all jobs
    if caps.as_str(1)?.is_empty() {
        let mut res = {
            let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
            getsql!(dbconn, "SELECT name, time, days, cmd FROM schedules LEFT JOIN entitys ON schedules.at = entitys.id WHERE schedules.id=?", cmdstruct.id)?
        };
        if res.is_empty() {
            cmdstruct.push_msg("No Scheduled Jobs").send_msg().await?;
            return Ok("COMPLETED.")
        }
        res.sort_by( |a,b| a.get_i64("time").unwrap().cmp(&b.get_i64("time").unwrap()) );
        let buff =
            "Scheduled Jobs:\n".to_string() +
            &res.iter()
            .map( |row| {
                let time = row.get_i64("time").unwrap();
                format!("`{}Z {}` `{}` `{}`",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time),
                    row.get_string("days").unwrap(),
                    row.get_string("name").unwrap(),
                    row.get_string("cmd").unwrap())
            } )
            .collect::<Vec<String>>()
            .join("\n");
        cmdstruct.push_msg(&buff).send_msg().await?;
        return Ok("COMPLETED.")
    }

    let msg = {
        let id = cmdstruct.id;
        let at = cmdstruct.at;
        let topic = &cmdstruct.topic;
        let now = cmdstruct.now;
        let envstruct = &getenvstruct!(cmdstruct)?;
        let dbconn = &envstruct.dbconn;

        let duration_caps =
            regex_to_vec(r"(?xi) (-)?  (?:(\d+)h)?  (?:(\d+)m)?  (\d*)", caps.as_str(4).unwrap_or(""))?;
        let neg = IF!(duration_caps.as_str(1).is_ok(),-1,1);
        let hours = duration_caps.as_i64(2).unwrap_or(0);
        let mins = duration_caps.as_i64(3).unwrap_or(0);
        let secs = duration_caps.as_i64(4).unwrap_or(0);

        let mut time =
            if let Ok(datestr) = caps.as_str(2) {
                LocalDateTime
                    ::from_str( &format!("{}{}", datestr, caps.as_str(3)?).to_uppercase() )?
                    .to_instant()
                    .seconds()
            } else if let Ok(timestr) = caps.as_str(3) {
                LocalTime
                    ::from_str( timestr )?
                    .to_seconds()
            } else {
                // Always assume UTC time with user input.  Default
                // time will be an hour behind in DST so add an hour
                // 4pmPDT is really 3pmPST == 11pmUTC but save datetime
                // as 0AM UTC
                let dstsecs = 60 * 60 * envstruct.dst_hours_adjust as i64;
                now + dstsecs
            }
            + neg*(hours*60*60 + mins*60 + secs);

        let daily = caps.as_str(5);
        if daily.is_ok() { time = time.rem_euclid(86400)}

        let mut days =
            match daily {
                Err(_) | Ok("*") => "",
                Ok(days) => days
            };
        if 7 == days.find("m").map_or(0, |_| 1) + days.find("t").map_or(0, |_| 1)
            + days.find("w").map_or(0, |_| 1) + days.find("h").map_or(0, |_| 1)
            + days.find("f").map_or(0, |_| 1) + days.find("s").map_or(0, |_| 1)
            + days.find("u").map_or(0, |_| 1)
        { days = ""; }
&
        // Save job
        if let Ok(command) = caps.as_str(6) {
                info!("now:{} [id:{} at:{} topic:{} time:{} days:{} cmd:{:?}]",
                    now, id, at, topic, time, days, command);
                glog!(getsql!(dbconn, "INSERT INTO schedules VALUES (?, ?, ?, ?, ?, ?)",
                    id, at, topic.parse::<i64>().unwrap_or(0), time, days, command));
                format!("Scheduled: `{}Z {}` `{}`",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time),
                    days,
                    command)
        } else { // Delete job
            if getsql!(dbconn, "SELECT * FROM schedules WHERE time=? AND days=?", time, days)?.len()
                != getsql!(dbconn, "DELETE FROM schedules WHERE time=? AND days=?", time, days)?.len()
            {
                format!("`{}Z` `{}` removed",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time),
                    days)
            }  else {
                format!("`{}Z` `{}` not found",
                    if time < 86400 { time2timestr } else { time2datetimestr }(time),
                    days)
            }
        }
    };

    cmdstruct.push_msg(&msg).send_msg().await?;

    Ok("COMPLETED.")
}

async fn do_rpn (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let expr = must_regex_to_vec(r"^(={1,3}) *(((-?[0-9]*[.][0-9]+)|(-?[0-9]+[.]?)|[^ ] | )*[^ ]?)$", &cmdstruct.message)?;

    let mut stack = Vec::new();
    for toks in Regex::new(r" *((-?[0-9]*[.][0-9]+)|(-?[0-9]+[.]?))|([+*/-])|([^ ])")?.captures_iter(&expr.as_string(2)?) {
        if let Some(num) = toks.get(1) { // Push a number
            stack.push(num.as_str().parse::<f64>()?)
        } else if let Some(op) = toks.get(4) { // Compute a mathematical operation on top most numbers in stack
            cmdstruct
                .set_msg(&(
                    stack.iter().map( |f| f.to_string() ).collect::<Vec<String>>().join(" ")
                    + " "
                    + op.as_str()))
                .edit_msg().await?;
            if stack.len() < 2 { cmdstruct.push_msg(" stack is lacking").edit_msg().await? }
            let b = stack.pop().ok_or("stack empty")?;
            let a = stack.pop().ok_or("stack empty")?;
            match op.as_str() {
                "+" => stack.push(a+b),
                "-" => stack.push(a-b),
                "*" => stack.push(a*b),
                "/" => stack.push(a/b),
                _ => ()
            }
        } else if let Some(op) = toks.get(5) {
            stack.push(
                op.as_str().chars().next().unwrap_or('\0')
                as u32 as f64);
        }
        cmdstruct.set_msg( &stack.iter().map( |f| f.to_string() ).collect::<Vec<String>>().join(" ") ).edit_msg().await?;
    }
    if expr.as_string(1)? == "==" {
        let v = stack[0] as u32;
        cmdstruct.push_msg(
            &format!(r#" "{}" 0x{:x} {:x?}"#,
                std::char::from_u32(v).unwrap_or('?'),
                v,
                std::char::from_u32(v).unwrap_or('?').to_string().as_bytes())
        ).edit_msg().await?;
    }

    Ok("COMPLETED.")
}

async fn do_alias (cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let caps = must_regex_to_vec(r"(?s)^/alias\s+/?([^\s]+)\s?(.*)$", &cmdstruct.message)?;
    let alias = caps.as_str(1)?;
    let mut cmd =  caps.as_string(2)?;
    let action = {
        let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
        if "" == cmd {
            let rows = getsql!(dbconn, "SELECT cmd FROM aliases WHERE alias=?", alias)?;
            cmd = IF!(rows.is_empty(), "", rows[0].get_str("cmd")?).to_string();
            ""
        } else {
            getsql!(dbconn, "INSERT OR REPLACE INTO aliases VALUES(?, ?)", alias, &cmd[..])?;
            "Set "
        }
    };
    if "" == cmd {
        cmdstruct
            .push_msg(&format!("Unbound Alias: {}", alias))
            .send_msg_id().await?
    } else {
        cmdstruct
            .push_msg(&format!("{}Alias: {}\n{}", action, alias, cmd))
            .send_msg().await?
    }

    Ok("COMPLETED.")
}

fn wordle_score_normalize (score: &str) -> usize {
    match score {
        "1" => 1,
        "2" => 2,
        "3" => 3,
        "4" => 4,
        "5" => 5,
        "6" => 6,
        "X" => 7,
          _ => 0
    }
}

async fn do_wordle(cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let expr = must_regex_to_vec(r"^Wordle \d+ ([123456X])/6(\*?)", &cmdstruct.message)?; // "Wordle 767 4/6\n\n..."

    let resultext = httpsbody("world.dv8.org:4441/jsondb/v1/wordle",
        format!("'{} {} 1 :wordle",
            getenvstruct!(cmdstruct)?.entity_id2name(cmdstruct.id)?,
            wordle_score_normalize(expr.as_str(1)?))
    ).await?;

    cmdstruct.push_msg(&resultext).send_msg().await?;

    Ok("COMPLETED.")
}

async fn do_aliasRun(mut cmdstruct: &mut CmdStruct) -> Bresult<&str> {
    let expr = must_regex_to_vec(r"(?sx) ^ /([^\s]+) \s? (.*) $ ", &cmdstruct.message)?;

    let cmd = expr.as_str(1)?;

    let cmd = cmd.rsplit_once('@').map_or(cmd, |(cmd, topic)| {
        cmdstruct.topic = topic.to_string(); // /aliascmd@topic force topic
        cmd
    });

    let rawcmd = {
        let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
        let aliasCmd = getsql!(dbconn, "SELECT cmd FROM aliases WHERE alias=?", cmd)?;
        if aliasCmd.is_empty() { Err("")? }
        aliasCmd[0]["cmd"].try_into::<&str>().or(Err("bad alias cmd"))?.to_string()
    };

    let args = expr.as_str(2)?;
    let parameters = args.split_ascii_whitespace().collect::<Vec<&str>>();
    let parametersCount = parameters.len().to_string();

    let mut usedParameters = false;
    let mut parameterError = false;

    let me = getenvstruct!(cmdstruct)?
        .entity_id2name(cmdstruct.id)
        .unwrap_or_else(|_e| {
            parameterError = true;
            &"?"
        })
        .to_string();
    let cmd = Regex::new(r#"(?x)
            \{ (
                (\d+) (:[a-z]+)?  #2 {num}  #3:transform
                | ( [a-z*\#]+ )   #4 {str}
            ) \}
            | ( \{ [}{] )         #5 {{ {}
            | ( . [^{]* )         #6 text
        "#)?
        .captures_iter(&rawcmd)
        .into_iter()
        .map(|c| {
            // Normal text
            if let Some(m) = c.get(6) {
                m.as_str().to_string()
            } else {
                usedParameters = true;
                // Parameter replacement {1} with optional {1:transform}
                if let Some(m) = c.get(2) {
                    let s =
                        parameters.get(m.as_str().parse::<usize>().unwrap_or(usize::MAX))
                        .unwrap_or_else(|| {
                            parameterError = true;
                            &"?"
                        });
                    IF!(Some(":urlencode") == c.get(3).map(|m| m.as_str()),
                        utf8_percent_encode(s, NON_ALPHANUMERIC).to_string(),
                        s.to_string())
                // Special parameter replacement {me} {*} {#}
                } else if let Some(s) = c.get(4) {
                    match s.as_str() {
                        "me" => &me,
                        "#" => &parametersCount,
                        "*" => args,
                        _ => "?",
                    }.to_string()
                } else if let Some(m) = c.get(5) {
                    // Escaped brace {{ or null parameter {}
                    if "{" == &m.as_str()[1..2] { "{" } else { "" }.to_string()
                } else {
                    // This should never happen
                    parameterError = true;
                    "?".to_string()
                }
            }
        })
        .collect::<Vec<String>>()
        .join("");

    if parameterError {
        cmdstruct
            .push_msg(&format!("alias error: {}", cmd))
            .send_msg_id().await?;
    } else {
        cmdstruct.message = if usedParameters {
            format!("{}", cmd)
        } else {
            format!("{} {}", rawcmd, args)
        }
        .trim_end()
        .into();
        do_most(&mut cmdstruct).await; // Attempt to run aliased command
    }
    Ok("COMPLETED.")
}

////////////////////////////////////////////////////////////////////////////////

#[macro_export()]
macro_rules! dolog {
    ($arg:expr) => {
        match &$arg.await {
            Err(e) => {let s=e.to_string(); if s != "" { error!("{} => {:?}", stringify!($arg), e) } }
            Ok(o)  => info!("{} => {:?}", stringify!($arg), o)
        }
    }
}
async fn do_most (cmdstruct: &mut CmdStruct) {
    dolog!(do_echo_lvl(cmdstruct));
    dolog!(do_help(cmdstruct));
    dolog!(do_curse(cmdstruct));
    dolog!(do_say(cmdstruct));
    dolog!(do_like(cmdstruct));
    dolog!(do_like_info(cmdstruct));
    dolog!(do_def(cmdstruct));
    dolog!(do_httpsget(cmdstruct));
    dolog!(do_httpsbody(cmdstruct));
    dolog!(do_httpsjson(cmdstruct));
    dolog!(do_json(cmdstruct));
    dolog!(do_sql(cmdstruct));
    dolog!(do_quotes(cmdstruct));
    dolog!(do_yolo(cmdstruct));
    dolog!(do_portfolio(cmdstruct));
    dolog!(do_trade_buy(cmdstruct));
    dolog!(do_trade_sell(cmdstruct));
    dolog!(do_exchange_bidask(cmdstruct));
    dolog!(do_orders(cmdstruct));
    dolog!(do_fmt(cmdstruct));
    dolog!(do_rebalance(cmdstruct));
    dolog!(do_rpn(cmdstruct));
    dolog!(do_alias(cmdstruct));
    dolog!(do_wordle(cmdstruct));
}

async fn do_all(cmdstruct: &mut CmdStruct) -> Bresult<()> {
    match do_schedule(cmdstruct).await {
        Err(e) => {
            if e.to_string() != "" {
                cmdstruct.push_msg(&format!("{BLD}Scheduler {}", e)).send_msg_id().await?;
                error!("{BLD}do_schedule() => {:?}", e)
            }
        }
        Ok(r) =>  {
            // Stop evaluating if this is a successfull scheduled job so as not to potentially trigger other do handlers.
            info!("{BLD}do_schedule() =>{:?}", r);
            if r == "COMPLETED." { return Ok(()) }
        }
    }
    do_most(cmdstruct).await;
    match do_aliasRun(cmdstruct).await { // Alias only happens once to avoid loops
        Err(e) => if e.to_string() != "" { error!("{BLD}do_aliasrun() -> {:?}", e) }
        Ok(r) => info!("{BLD}do_aliasrun() -> {:?}", r),
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// Scheduled Messages

async fn do_scheduled_job(
    env: Env, job: HashMap<String, sqlite::Value>, now: i64
) -> Bresult<()> {
    let at = job.get_i64("at")?;
    let mut cmdstruct =
        CmdStruct::new(
            env.clone(), now,
            job.get_i64("id")?, at, at, job.to_string("topic")?,
            job.get_string("cmd")?)?;
    let days = job.get_str("days")?;
    let day_now = match LocalDateTime::at(now).weekday() {
        Monday => "m", Tuesday => "t", Wednesday => "w", Thursday => "h",
        Friday => "f" , Saturday => "s", Sunday => "u" };
    if days.is_empty() || days.find(day_now).is_some() {
        glogd!("do_all()", do_all(&mut cmdstruct).await)
    }
    let time = job.get_i64("time")?;
    if 86400 <= time { // Delete the non-daily job
        let dbconn = &getenvstruct!(cmdstruct)?.dbconn;
        getsql!(
            dbconn,
            "DELETE FROM schedules WHERE id=? AND at=? AND topic=? AND time=? AND days='' AND cmd=?",
            cmdstruct.id, cmdstruct.at, cmdstruct.topic.parse::<i64>().unwrap_or(0), time, &cmdstruct.message[..])?;
    }
    Ok(())
}

pub fn start_scheduler (env: Env) -> Bresult<&'static str> {
    thread::spawn(move || loop {
        sleep_secs(10.0);
        let now = Instant::now().seconds() + 60*60*env.lock().unwrap().dst_hours_adjust as i64;

        let jobs = {
            let envstruct = env.lock().unwrap();
            let timeFrom = envstruct.time_scheduler % 86400;
            let mut timeTo = now % 86400;
            if timeTo < timeFrom { timeTo += 86400 } // might wrap to 0, make 'from' < 'to'
            match getsqlquiet!(&envstruct.dbconn,
                "SELECT id, at, topic, time, days, cmd FROM schedules WHERE (?<=time AND time<?) or (?<=time AND time<?) ORDER BY time",
                envstruct.time_scheduler, now, /*one-time jobs*/
                timeFrom, timeTo /*daily jobs*/)
            {
                Ok(jobs) => jobs,
                err => { glog!(err); continue }
            }
        };
        if 0 < jobs.len() {
            let env2 = env.clone();
            rt::System::new().block_on(async move {
                for job in jobs {
                    println!("");
                    info!("{BLD}::SCHEDULER~{RST} {:?}", &job);
                    glogd!("{BLD}--SCHEDULER~{RST}", do_scheduled_job(env2.clone(), job, now).await);
                }
            });
        }
        env.lock().unwrap().time_scheduler = now;
    });
    Ok("10s delay")
}

////////////////////////////////////////

fn header_tmbot() {
    println!();
    info!("{RST}{RED}{B_BLK} _____ __  __ ____        _   ");
    info!("{RST}{YEL}{B_BLK}|_   _|  \\/  | __ )  ___ | |_™");
    info!("{RST}{GRN}{B_BLK}  | | | |\\/| |  _ \\ / _ \\| __|");
    info!("{RST}{BLU}{B_BLK}  | | | |  | | |_) | (_) | |_ ");
    info!("{RST}{MAG}{B_BLK}  |_| |_|  |_|____/ \\___/ \\__|");
}

async fn tmbot(req: HttpRequest, body: web::Bytes) -> Bresult<()> {
    let env = req.app_data::<web::Data<Env>>()
        .ok_or("tmbot app_data")?
        .get_ref()
        .clone();
    let mut cmd = bytes2json(&body)
        .and_then(|json| CmdStruct::from_json(env, &json))?;
    do_all(&mut cmd).await
}

async fn handler_tmbot(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    header_tmbot();
    info!("{}", httpReqPretty(&req, &body));
    info!("::tmbot => {:?}",
        rt::spawn(async move {
            glogd!("--tmbot", tmbot(req, body).await) }));
    httpResponseOk!()
}

pub fn start_tmbot(env: Env) -> Bresult<()> {
    info!("::TMBOT");
    let ssl_acceptor_builder = {
        let envstruct = env.lock().unwrap();
        comm::new_ssl_acceptor_builder(&envstruct.telegram_key, &envstruct.telegram_cert)?
    };
    glogd!("--TMBOT", rt::System::new().block_on(
        HttpServer::new(move || App::new()
                .app_data(web::Data::new(env.clone()))
                .route("{tail:.*}", web::to(handler_tmbot)))
            .workers(2)
            .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
            .run()
        ) // SIGINT causes return
    );
    Ok(())
}

////////////////////////////////////////

fn _envstruct_get_entity<'a>(envstruct: &'a EnvStruct, name: &str) -> Bresult<&'a Entity> {
    envstruct.entitys
    .iter()
    .find_map( |(_id, entity)|
        if name == &entity.name { Some(entity) }
        else { None } )
    .ok_or("missing user".into())
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Line {
    line: String,
}

async fn _web_login (env: Env, name: &str) -> Bresult<i64> {
    let id = {
        let env = env.clone();
        let envstruct = env.lock().unwrap();
        _envstruct_get_entity(&envstruct, name)?._id
    };
    warn!("web_login id = {:?}", id);
    //thread::Builder::new().name("webLogin".into()).spawn( move || {
        let res =
                match CmdStruct::new(env, 0, id, id, id, "".into(), "".into()) {
                    Ok(mut cmdstruct) => {
                        let pw = ::rand::random::<usize>();
                        match cmdstruct.env.try_lock() {
                            Ok(mut envstruct) => glog!(envstruct._entity_uuid_set(id, pw)),
                            Err(e) => error!("websocketlogin {:?}", e)
                        };
                        let res = cmdstruct.push_msg(&pw.to_string()).send_msg().await;
                        info!("ws login msg {} to {} => {:?}", pw, id, res);
                        pw.to_string()
                    },
                    Err(e) => {
                        error!("CmdStruct::new => {:?}", e);
                        "who?".to_string()
                    }
                };
        info!("ws login res => {}", res);
    //})?;
    Ok(id)
}

fn _web_yolo(envstruct: &mut EnvStruct) -> Bresult<String> {
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

    let mut yololians =
      getsql!(&envstruct.dbconn, &sql)?
            .iter()
            .map( |row| (
                row.get_string("name").unwrap_or("?".to_string()),
                row.get_f64("yolo").unwrap_or(0.0) ) )
            .collect::<HashMap<String, f64>>();

    yololians.insert("hitcounter".to_string(), envstruct.entity_likes_inc(1544486685, 1)? as f64);

    let json = serde_json::to_string(&yololians)?;
    info!("created json: {:?}", &json);
    Ok(json)
}

async fn _web_stonks (cmdstruct: &mut CmdStruct) -> Bresult<String> {
    let positions = Position::get_users_positions(cmdstruct)?;
    let mut quotes :Vec<Vec<String>> = Vec::new();
    let mut long = 0.0;
    let mut short = 0.0;
    for mut pos in positions {
        if !is_self_stonk(&pos.ticker) {
            pos.update_quote(&cmdstruct).await?;
            info!("{} position {:?}", cmdstruct.id, &pos);
            let quote = pos.quote.as_ref().ok_or("quote not acquired")?;
            quotes.push(
                vec!(
                    pos.ticker,
                    pos.qty.to_string(),
                    pos.price.to_string(),
                    quote.price.to_string() ) );
            if pos.qty < 0.0 {
                short += pos.qty * quote.price;
            } else {
                long += pos.qty * quote.price;
            }
        }
    }

    let cash = roundcents(getenvstruct!(cmdstruct)?.entity_balance(cmdstruct.id)?);
    let bp = roundcents(long + short*3.0 + cash*2.0);
    let yolo = roundcents(long+short+cash);
    quotes.push( vec!(cash.to_string(), bp.to_string(), yolo.to_string()) );

    let json = serde_json::to_string(&quotes)?;
    info!("created json: {:?}", &json);
    Ok(json)
}

impl Handler<Line> for CmdStruct {
    type Result = ();
    fn handle(&mut self, line: Line, ctx: &mut Self::Context) {
        ctx.text(line.line);
    }
}

impl Actor for CmdStruct {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for CmdStruct {
    fn handle(
        &mut self, // CmdStruct
        msg: Result<ws::Message, ws::ProtocolError>,
        ctx: &mut Self::Context
    ) {
        warn!("WS handle() <= {:?}", msg);
        match msg {
            Ok(ws::Message::Text(text)) => {
                info!("WS handle() words = {:?}", text);
                self.message.clear();
                self.message.push_str(&text);

                let recipient = ctx.address().recipient();

                let fut = async move { recipient.do_send(Line{line:r#"["hello"]"#.into()}) };
                let fut = actix::fut::wrap_future::<_, Self>(fut);
                ctx.spawn(fut);

    //let env2 = self.env.clone();
    //std::thread::spawn( move || {
    //    let res =
    //        rt::System::new(/*"websocketlogin"*/)
    //        .block_on(async move {
    //            match CmdStruct::new(env2, 0, -572225120,-572225120,-572225120, "") {
    //                Ok(mut cmdstruct) => {
    //                    let res = cmdstruct.push_msg(&"Someone is playing Wordle!".to_string()).send_msg().await;
    //                    info!("wordle msg {:?}", res);
    //                },
    //                Err(e) => {
    //                    error!("new => {:?}", e);
    //                    "who?".to_string();
    //                }
    //            }
    //        });
    //    info!("wordle msg => {:?}", res);
    //});

            },
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Binary(_bin)) => ctx.binary(""),
            _ => { },
        }
        warn!("WS handle() returning");
    }
}

async fn _ws_handle_request (cmdstruct: &mut CmdStruct) -> String {
    let words = cmdstruct.message.split(" ").collect::<Vec<&str>>();
    if words.len() < 1 || 3 < words.len() { return "".to_string() }
    match words[0] {
        "stats" => {
            let envstruct = cmdstruct.env.lock().unwrap();
            let mut hm = HashMap::new();
            match envstruct.entitys.get(&cmdstruct.id) {
                Some(entity) => {
                    hm.insert("name", entity.name.to_string());
                    hm.insert("balance", roundcents(entity.balance).to_string());
                    hm.insert("likes", entity.likes.to_string());
                    serde_json::to_string(&hm).unwrap_or("{}".to_string())
                },
                None => {
                    error!("stats bad id {}", cmdstruct.id);
                    hm.insert("name", "nobody".to_string());
                    serde_json::to_string(&hm).unwrap_or("{}".to_string())
                }
            }
        },
        "yolo" => {
            let mut envstruct = cmdstruct.env.lock().unwrap();
            _web_yolo(&mut envstruct).unwrap_or_else( |e| { error!("{:?}", e); "error".into() } )
        },
        "stonks" => {
            _web_stonks(cmdstruct).await.unwrap_or_else( |e| { error!("{:?}", e); "error".into() } )
        },
        "login" => {
            if 2 == words.len() { // Message user privately their login code
                _web_login(cmdstruct.env.clone(), words[1]).await
                    .unwrap_or_else( |e| { error!("web_login => {:?}", e); 0 } );
                ""
            } else if 3 == words.len() { // Accept or reject passocode
                let envstruct = cmdstruct.env.lock().unwrap();
                let entity = _envstruct_get_entity(&envstruct, words[1]);
                match entity {
                    Ok(entity) => {
                        cmdstruct.id = entity._id;
                        IF!(words[2]!="" && entity._uuid!="" && words[2] == entity._uuid, words[1], "")
                    },
                    Err(e) => { error!("code result {:?}", e); "" }
                }
            } else {
                ""
            }
        }.to_string(),
        _ => format!("69.42@{}", cmdstruct.message)
    }
}


async fn _ws_handler(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, actix_web::Error> {
    warn!("_ws_handler req={:?}", req);
    let env :Env = req.app_data::<web::Data<Env>>().unwrap().get_ref().clone();
        

    let cmdstruct = match CmdStruct::new(env, 0, 0, 0, 0, "".into(), "".into()) {
        Ok(cmdstruct) => cmdstruct,
        e => { glog!(e); return Err(actix_web::error::ErrorNotFound("")) } // TODO: Return an error so the websocket connection closes?
    };

    let resp = ws::start(cmdstruct, &req, stream); // calls StreamHandler.handle()
    glogd!("_ws_handler ", resp);
    resp
}

pub fn _main_websocket(env: Env) -> Bresult<()> {
    thread::spawn( move || {
        let srv =
            match HttpServer::new(move ||
                    App::new()
                        .app_data(web::Data::new(env.clone()))
                        .route("*", web::get().to(_ws_handler)))
                .bind("0.0.0.0:7190")
            {
                Ok(srv) => srv,
                Err(err) => {
                    error!("HttpServer => {:?}", err);
                    return;
                }
            };
        let res = rt::System::new().block_on(async move {
            warn!("launch() => {:?}", srv.run().await)
        }); // This should never return
        error!("_main_websocket => {:?}", res);
    });
    Ok(())
}

pub fn _main_websocket_ssl(env: Env) -> Bresult<()> {
    let ssl_acceptor_builder = {
        let envstruct = env.lock().unwrap();
        comm::new_ssl_acceptor_builder(&envstruct.tmbot_key, &envstruct.tmbot_cert)?
    };
    thread::spawn( move || {
        let srv =
            match
                HttpServer::new(move || App::new().app_data(web::Data::new(env.clone())).route("*", web::get().to(_ws_handler)))
                    .bind_openssl("0.0.0.0:7189", ssl_acceptor_builder)
            {
                Ok(srv) => srv,
                Err(err) => {
                    error!("HttpServer -> {:?}", err);
                    return;
                }
            };
        let res = rt::System::new().block_on(async move {
            println!("launch() -> {:?}", srv.run().await)
        }); // This should never return
        error!("_main_websocket_ssl -> {:?}", res);
    });
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////

fn usage() {
    println!(r#"
USAGE:
    tmbot {{DST 0|1}}
ENVIRONMENT:
    TELEGRAM_API_TOKEN=0000000000:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    TELEGRAM_CERT=telegram_cert.pem
    TELEGRAM_KEY=telegram_key.pem
    TMBOT_CERT=tmbot_cert.pem
    TMBOT_KEY=tmbot_key.pem
    TMBOT_DB=tmbot.sqlite
"#);
}

pub fn main_launch() -> Bresult<()> {
    let argv = env::args();
    if 2 != argv.len() {
        usage();
        Err("invalid args")?
    }
    if !true { glogd!("create_schema", create_schema()) } // Create DB
    if true {
        let env = EnvStruct::new(argv)?;
        //glogd!("websocket()", _main_websocket(env.clone()));
        //glogd!("websocketssl()", _main_websocket_ssl(env.clone()));
        glogd!("start_scheduler()", start_scheduler(env.clone()));
        glogd!("start_tmlog()", crate::tmlog::start_tmlog(env.clone()));
        glogd!("start_tmbot()", start_tmbot(env));
    } else { // Playground
       info!("{:?}", argv);
    }
    Ok(())
}
