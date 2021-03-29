pub mod util;
pub use crate::util::*;

use ::std::{
    mem::transmute,
    time::{Duration},
    collections::{HashMap, HashSet},
    str::{from_utf8},
    fs::{read_to_string, write},
    env::{args},
    sync::{Mutex, mpsc::{channel, Sender} },
};
use ::actix_web::{
    web, App, HttpRequest, HttpServer, HttpResponse, Route,
    client::{Client, Connector}};
use ::openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use ::regex::{Regex};
use ::datetime::{
    Instant, LocalDate, LocalTime, LocalDateTime, DatePiece,
    Weekday::{Sunday, Friday, Saturday},
    ISO}; // ISO
use ::log::*;

////////////////////////////////////////////////////////////////////////////////

const QUOTE_DELAY_MINUTES :i64 = 2;

////////////////////////////////////////////////////////////////////////////////

// Return time (seconds midnight UTC) for the next trading day
fn next_trading_day (day :LocalDate) -> i64 {
    let skip_days =
        match day.weekday() {
            Friday => 3,
            Saturday => 2,
            _ => 1
        };
    LocalDateTime::new(day, LocalTime::midnight())
        .add_seconds( skip_days * 24 * 60 * 60 )
        .to_instant()
        .seconds()
}


/// Decide if a ticker's price should be refreshed given its last lookup time.
///    Market   PreMarket   Regular  AfterHours       Closed
///       PST   0900Zpre..  1430Z..  2100Z..0100Zaft  0100Z..9000Z
///       PDT   0800Z..     1330Z..  2000Z..2400Z     0000Z..0800Z
///  Duration   5.5h        6.5h     4h               8h
fn update_ticker_p (db :&DB, time :i64, now :i64) -> bool {

    // Since UTC time is used and the pre/regular/after hours are from 0900Z to
    // 0100Z the next morning, subtract a number of seconds so that the
    // "current day" is the same for the entire trading hours range.
    let day_normalized =
        LocalDateTime::from_instant(Instant::at(time - 90*60 )) // 90 minutes
        .date();

    let is_market_day =
        match day_normalized.weekday() {
            Saturday|Sunday => false,
            _ => true
        };

    let time_open = LocalDateTime::new(day_normalized, LocalTime::hms(9-db.dst_hours_adjust, 00, 0).unwrap()).to_instant().seconds();
    let time_close = time_open + 60*60*16 + 60*30; // add an extra half hour to after market closing for slow trades.

    return
        if is_market_day  &&  time < time_close {
            time_open <= now  &&  (time+(db.quote_delay_minutes*60) < now  ||  time_close <= now) // X minutes delay/throttle
        } else {
            let day = LocalDateTime::from_instant(Instant::at(time)).date();
            update_ticker_p(db, next_trading_day(day)+90*60, now)
        }
}

/// Round a float at the specified decimal offset
///  println!("{:?}", num::Float::integer_decode(n) );
fn round (num:f64, dec:i32) -> f64 {
    let fac = 10f64.powi(dec);
    let num_incremented = unsafe { transmute::<u64, f64>(transmute::<f64, u64>(num) + 1) };
    (num_incremented * fac).round() / fac
}

fn roundqty (num:f64) -> f64 { round(num, 4) }
fn roundcents (num:f64) -> f64 { round(num, 2) }

// Trim trailing . and 0
fn num_simp (num:&str) -> String {
    num
    .trim_end_matches("0")
    .trim_end_matches(".")
    .into()
}

// Try to squish number to 3 digits: "100%"  "99.9%"  "10.0%"  "9.99%"  "0.99%""
fn percent_squish (num:f64) -> String {
    if 100.0 <= num {
        format!("{:.0}", num)
    } else if 10.0 <= num {
        format!("{:.1}", num)
    } else if 1.0 <= num {
        format!("{:.2}", num)
    } else if num < 0.001 {
        "0.0".into()
    } else {
        format!("{:.3}", num).trim_start_matches('0').into()
    }
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
                        .get(i) // Get match via index
                        .map(|capmatch| (i.to_string(), capmatch.as_str().into())), // Match is None or a string
                    |capname| {
                        captures
                            .name(capname) // Get match via capture name
                            .map(|capmatch| (capname.into(), capmatch.as_str().into())) // Match is None or a string
                    },
                )
            })
            .collect()
    })
}

fn is_self_stonk (s:&str) -> bool {
    Regex::new(r"^[0-9]+$").unwrap().captures(s).is_some()
}

////////////////////////////////////////////////////////////////////////////////

async fn send_msg (db :&DB, chat_id :i64, text: &str) -> Bresult<i64> {
    info!("Telegram <= \x1b[36m{}", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    match Client::builder()
    .connector( Connector::new()
                .ssl( builder.build() )
                .timeout(Duration::new(30,0))
                .finish() )
    .finish()
    .get( db.url_api.clone() + "/sendmessage")
    .header("User-Agent", "Actix-web")
    .timeout(Duration::new(30,0))
    .query(&[["chat_id", &chat_id.to_string()],
             ["text", &text],
             ["disable_notification", "true"]]).unwrap()
    .send()
    .await?
    { mut result => {
        ginfod!("Telegram => \x1b[35m", &result);
        let body = result.body().await;
        ginfod!("Telegram => \x1b[1;35m", body);
        Ok( getin_i64(
                &bytes2json(&body?)?,
                &["result", "message_id"]
            )?
        )
    } }
}


async fn _send_edit_msg (db :&DB, chat_id :i64, message_id: i64, text: &str) -> Bresult<()> {
    info!("Telegram <= \x1b[36m{}", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    match
        Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(30,0))
                    .finish() )
        .finish()
        .get( db.url_api.clone() + "/editmessagetext")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["message_id", &message_id.to_string()],
                 ["text", &text],
                 ["disable_notification", "true"]]).unwrap()
        .send().await
    {
        Err(e) => {
            error!("Telegram \x1b[31m => {:?}", e);
        },
        Ok(mut result) => {
            ginfod!("Telegram => \x1b[35m", &result);
            ginfod!("Telegram => \x1b[1;35m", result.body().await);
        }
    }
    Ok(())
}


async fn send_msg_markdown (db :&DB, chat_id :i64, text: &str) -> Bresult<()> {
    let text = text // Poor person's uni/url decode
    .replacen("%20", " ", 10000)
    .replacen("%28", "(", 10000)
    .replacen("%29", ")", 10000)
    .replacen("%3D", "=", 10000)
    .replacen("%2C", ",", 10000)
    .replacen("%26%238217%3B", "'", 10000)
    // Telegram required markdown escapes
    .replacen(".", "\\.", 10000)
    .replacen("(", "\\(", 10000)
    .replacen(")", "\\)", 10000)
    .replacen("{", "\\{", 10000)
    .replacen("}", "\\}", 10000)
    .replacen("-", "\\-", 10000)
    .replacen("+", "\\+", 10000)
    .replacen("=", "\\=", 10000)
    .replacen("#", "\\#", 10000)
    .replacen("'", "\\'", 10000);

    info!("Telegram <= \x1b[33m{}\x1b[0m", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    let response =
        Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(30,0))
                    .finish() )
        .finish() // -> Client
        .get( db.url_api.clone() + "/sendmessage")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["text", &text],
                 ["parse_mode", "MarkdownV2"],
                 ["disable_notification", "true"]]).unwrap()
        .send()
        .await;

    match response {
        Err(e) => error!("\x1b[31m-> {:?}", e),
        Ok(mut r) => {
            ginfod!("Telegram => \x1b[35m", &r);
            ginfod!("Telegram => \x1b[1;35m", r.body().await);
        }
    }
    Ok(())
}

async fn send_edit_msg_markdown (db :&DB, chat_id :i64, message_id :i64, text: &str) -> Bresult<()> {
    let text = text // Poor person's uni/url decode
    .replacen("%20", " ", 10000)
    .replacen("%28", "(", 10000)
    .replacen("%29", ")", 10000)
    .replacen("%3D", "=", 10000)
    .replacen("%2C", ",", 10000)
    .replacen("%26%238217%3B", "'", 10000)
    // Telegram required markdown escapes
    .replacen(".", "\\.", 10000)
    .replacen("(", "\\(", 10000)
    .replacen(")", "\\)", 10000)
    .replacen("{", "\\{", 10000)
    .replacen("}", "\\}", 10000)
    .replacen("-", "\\-", 10000)
    .replacen("+", "\\+", 10000)
    .replacen("=", "\\=", 10000)
    .replacen("#", "\\#", 10000)
    .replacen("'", "\\'", 10000);

    info!("Telegram <= \x1b[33m{}\x1b[0m", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    match Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(30,0))
                    .finish() )
        .finish() // -> Client
        .get( db.url_api.clone() + "/editmessagetext")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["message_id", &message_id.to_string()],
                 ["text", &text],
                 ["parse_mode", "MarkdownV2"],
                 ["disable_notification", "true"]]).unwrap()
        .send()
        .await
    { response => {
        ginfod!("Telegram => \x1b[1;35m", response?.body().await);
        Ok(())
    } }
}

////////////////////////////////////////////////////////////////////////////////

async fn get_definition (word: &str) -> Bresult<Vec<String>> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://www.onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["q",word]])?
        .send()
        .await?
        .body().limit(1_000_000).await
        .or_else( |e| {
            error!(r#"get_definition http body {:?} for {:?}"#, e, word);
            Err(e)
        } )?;

    let domstr = from_utf8(&body)
        .or_else( |e| {
            error!(r#"get_definition http body2str {:?} for {:?}"#, e, word);
            Err(e)
        } )?;

    // The optional US definitions

    let usdef =
        Regex::new(r"var mm_US_def = '[^']+").unwrap()
        .find(&domstr)
        .map_or("", |r| r.as_str() );

    let mut lst = Regex::new(r"%3Cdiv%20class%3D%22def%22%3E([^/]+)%3C/div%3E").unwrap()
        .captures_iter(&usdef)
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>();

    // WordNet definitions

    Regex::new(r#"easel_def_+[0-9]+">([^<]+)"#).unwrap()
        .captures_iter(&domstr)
        .for_each( |cap| lst.push( cap[1].to_string() ) );

    Ok(lst)
}


async fn get_syns (word: &str) -> Bresult<Vec<String>> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls()).unwrap().build() )
                    .timeout( Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["clue", word]]).unwrap()
        .send()
        .await.unwrap()
        .body().limit(1_000_000).await;

    if body.is_err() {
         error!(r#"get_syns http body {:?} for {:?}"#, body, word);
         Err("get_syns http error")?;
    }

    let body = body.unwrap();
    let domstr = from_utf8(&body);
    if domstr.is_err() {
         error!(r#"get_syns http body2str {:?} for {:?}"#, domstr, word);
         Err("get_body2str error")?;
    }

    Ok( Regex::new("w=([^:&\"<>]+)").unwrap()
        .captures_iter(&domstr.unwrap())
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>() )
} // get_syns

async fn get_ticker_quote (db :&DB, ticker: &str) -> Bresult<Ticker> {
    info!("get_ticker_quote <- {}", ticker);
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://finance.yahoo.com/chart/".to_string() + ticker)
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .send()
        .await?
        .body().limit(1_000_000).await;

    let body = body.or_else( |r| Err(format!("get_ticker_quote for {:?}  {:?}", ticker, r)) )?;
    let domstr = from_utf8(&body).or_else( |r| Err(format!(r#"get_ticker_quote http body2str {:?} {:?}"#, ticker, r)) )?;

    let json = {
        let cap = Regex::new("(?m)^root.App.main = (.*);$")?.captures(&domstr);
        if cap.is_none() || 2 != cap.as_ref().unwrap().len() {
            Err("Unable to find json string in HTML.")?
        }
        bytes2json(&cap.unwrap()[1].as_bytes())?
    };

    let details = getin(&json, &["context", "dispatcher", "stores", "QuoteSummaryStore", "price"]);

    if details.is_null() {
         Err("Unable to find quote data in json key 'QuoteSummaryStore'")?
    }

    info!("{}", details);

    let title = getin_str(&details, &["longName"])?.trim_end_matches(|e|e=='.').to_string();
    let exchange = getin_str(&details, &["exchange"])?;

    let mut details = [
        (getin_str(&details, &["preMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["preMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["preMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["preMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["preMarketTime"]).unwrap_or(0),
         'p'),
        (getin_str(&details, &["regularMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["regularMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["regularMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["regularMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["regularMarketTime"]).unwrap_or(0),
         'r'),
        (getin_str(&details, &["postMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["postMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["postMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["postMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["postMarketTime"]).unwrap_or(0),
         'a')];

    // TODO: for now log all prices to me privately for debugging/verification
    glogd!("send_msg => \x1b[35m",
        send_msg(db, 308188500,
            &format!("{} \"{}\" ({})\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%",
                ticker, title, exchange,
                LocalDateTime::from_instant(Instant::at(details[0].4)).iso(), details[0].1, details[0].2, details[0].3,
                LocalDateTime::from_instant(Instant::at(details[1].4)).iso(), details[1].1, details[1].2, details[1].3,
                LocalDateTime::from_instant(Instant::at(details[2].4)).iso(), details[2].1, details[2].2, details[2].3)
        ).await);

    details.sort_by( |a,b| b.4.cmp(&a.4) ); // Find latest quote details

    Ok(Ticker{
        price       :details[0].1,
        amount      :roundqty(details[0].2), // Round for cases: -0.00999999 -1.3400116
        percent     :details[0].3,
        title       :title,
        hours       :details[0].5,
        exchange    :exchange})
} // get_ticker_quote

////////////////////////////////////////////////////////////////////////////////

fn sql_results_handler_ (
    snd :Sender<HashMap<String, String>>,
    res :&[(&str, Option<&str>)] // [ (column, value) ]
) -> bool {
    let mut v = HashMap::new();
    for r in res {
        trace!("sql_results_handler vec <- {:?}", r);
        v.insert( r.0.to_string(), r.1.unwrap_or("NULL").to_string() );
    }
    let res = snd.send(v);
    trace!("sql_results_handler snd <- {:?}", res);
    true
}

fn get_sql_ ( cmd :&str ) -> Bresult<Vec<HashMap<String, String>>> {
    info!("SQLite <= \x1b[1;36m{}", cmd);
    let sql = ::sqlite::open( "tmbot.sqlite" )?;
    let (snd, rcv) = channel::<HashMap<String, String>>();
    sql.iterate(cmd, move |r| sql_results_handler_(snd.clone(), r) )?;
    Ok(rcv.iter().collect::<Vec<HashMap<String,String>>>())
}

fn get_sql ( cmd :&str ) -> Bresult<Vec<HashMap<String, String>>> {
    let sql = get_sql_(cmd);
    info!("SQLite => \x1b[36m{:?}", sql);
    sql
}

fn sql_table_order_insert (id:i64, ticker:&str, qty:f64, price:f64, time:i64) -> Bresult<Vec<HashMap<String, String>>> {
    get_sql(&format!("INSERT INTO orders VALUES ({}, '{}', {}, {}, {} )", id, ticker, qty, price, time))
}

////////////////////////////////////////////////////////////////////////////////

fn make_pretty_quote (ticker:&str, quote:&Ticker) -> Bresult<String> {
    let gain_glyphs = if 0.0 < quote.amount {
            (from_utf8(b"\xF0\x9F\x9F\xA2")?, from_utf8(b"\xE2\x86\x91")?) // Green circle, Arrow up
        } else if 0.0 == quote.amount {
            (from_utf8(b"\xF0\x9F\x94\xB7")?, "") // Blue diamond, nothing
        } else {
            (from_utf8(b"\xF0\x9F\x9F\xA5")?, from_utf8(b"\xE2\x86\x93")?) // Red square, Arrow down
        };

    Ok(format!("{}{}@{}{} {}{} {}% {} {}",
        gain_glyphs.0,
        ticker,
        quote.price,
        match quote.hours { 'r'=>"", 'p'=>"p", 'a'=>"a", _=>"?"},
        gain_glyphs.1,
        quote.amount.abs(),
        percent_squish(quote.percent.abs()),
        quote.title,
        quote.exchange
    ))
}

async fn get_stonk (db :&DB, ticker :&str) -> Bresult<HashMap<String, String>> {
    // Expected symbols:  GME  308188500   Illegal: @shrewm
    if &ticker[0..1] == "@" { Err("Illegal ticker")? }

    let nowsecs :i64 = Instant::now().seconds();
    let is_self_stonk = is_self_stonk(ticker);

    let res = get_sql(&format!("SELECT * FROM stonks WHERE ticker='{}'", ticker))?;
    let is_in_table =
        if !res.is_empty() { // Stonks table contains this ticker
            let hm = &res[0];
            let timesecs = hm.get("time").ok_or("table missing 'time'")?.parse::<i64>()?;
            if !update_ticker_p(db, timesecs, nowsecs) || is_self_stonk {
                return Ok(hm.clone())
            }
            true
        } else { false };

    // Either not in table or need to update table

    let quote = if is_self_stonk { // Need to create the self-stonk ticker in the stonks table
        Ticker {
            price:0.0,
            amount:0.0,
            percent:0.0,
            title:"Self Stonk".to_string(),
            hours:'r',
            exchange:"™BOT".to_string() }
    } else {
        get_ticker_quote(db, ticker).await?
    };

    let pretty = make_pretty_quote(ticker, &quote)?;

    let sql = if is_in_table {
        format!("UPDATE stonks SET price={},time={},pretty='{}'WHERE ticker='{}'", quote.price, nowsecs, pretty, ticker)
    } else {
        format!("INSERT INTO stonks VALUES('{}',{},{},'{}')", ticker, quote.price, nowsecs, pretty)
    };
    get_sql(&sql)?;

    let mut hm :HashMap::<String, String> = HashMap::new();
    hm.insert("updated".into(), "".into());
    hm.insert("ticker".into(), ticker.to_string());
    hm.insert("price".into(),  quote.price.to_string());
    hm.insert("time".into(),   nowsecs.to_string());
    hm.insert("pretty".into(), pretty);
    return Ok(hm);
} // get_stonk

fn get_bank_balance (id :i64) -> Bresult<f64> {
    let res = get_sql(&format!("SELECT * FROM accounts WHERE id={}", id))?;
    Ok(
        if res.is_empty() {
            let sql = get_sql(&format!("INSERT INTO accounts VALUES ({}, {})", id, 1000.0))?;
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

////////////////////////////////////////////////////////////////////////////////
/// Blobs

#[derive(Clone, Debug)]
pub struct DB {
    url_api: String,
    chat_id_default: i64,
    quote_delay_minutes: i64,
    dst_hours_adjust: i8,
}

type MDB = Mutex<DB>;

////////////////////////////////////////

#[derive(Debug)]
struct Ticker {
    price: f64,
    amount: f64,
    percent: f64,
    title: String,
    hours: char,
    exchange: String
}

////////////////////////////////////////

#[derive(Debug)]
struct Cmd {
    id: i64,
    at: i64,
    to: i64,
    msg: String
}

impl Cmd {
    /// Creates a Cmd object from the useful details of a Telegram message.
    fn parse_cmd(body: &web::Bytes) -> Bresult<Self> {

        let json: Value = bytes2json(&body)?;

        let inline_query = &json["inline_query"];
        if inline_query.is_object() {
            let id = getin_i64(inline_query, &["from", "id"])?;
            let msg = getin_str(inline_query, &["query"])?.to_string();
            return Ok(Cmd { id, at:id, to:id, msg }); // Inline query message
        }

        let message = &json["message"];
        if message.is_object() {
            let id = getin_i64(message, &["from", "id"])?;
            let at = getin_i64(message, &["chat", "id"])?;
            let msg = getin_str(message, &["text"])?.to_string();

            return Ok(
                if let Ok(to) = getin_i64(&message, &["reply_to_message", "from", "id"]) {
                    Self { id, at, to, msg } // Reply message
                } else {
                    Self { id, at, to:id, msg } // Normal message
                }
            );
        }

        Err("Nothing to do.")?
    }
}

////////////////////////////////////////

#[derive(Debug)]
struct Position {
    id:i64,
    ticker:String,
    qty:f64,
    price:f64
}

impl Position {
    fn query (id:i64, ticker:&str) -> Bresult<Vec<Position>> {
        Ok(
            get_sql(&format!("SELECT qty, price FROM positions WHERE id={} AND ticker='{}'", id, ticker))?
            .iter()
            .map( |row|
                Self {
                    id,
                    ticker: ticker.into(),
                    qty: row.get("qty").unwrap().parse::<f64>().unwrap(),
                    price: row.get("price").unwrap().parse::<f64>().unwrap() } )
            .collect::<Vec<_>>() )
    }
}

////////////////////////////////////////

#[derive(Debug)]
struct Trade {
    id: i64,
    ticker: String,
    action: char,
    is_dollars: bool,
    amt: Option<f64>
}

impl Trade {
    fn new (id:i64, msg:&str) -> Bresult<Option<Self>> {
        let caps = //                  _____ticker____  _+-_  _$_   ____________amt_______________
            match regex_to_hashmap(r"^([A-Za-z0-9^.-]+)([+-])([$])?([0-9]+\.?|([0-9]*\.[0-9]{1,4}))?$", msg) {
                Some(caps) => caps,
                None => return Ok(None)
            };
        Ok(Some(Self {
            id,
            ticker: caps.get("1").unwrap().to_uppercase(),
            action: caps.get("2").unwrap().chars().nth(0).unwrap(),
            is_dollars: caps.get("3").is_some(),
            amt: caps.get("4").map(|m| m.parse::<f64>().unwrap())
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Bot's Do Handler Helpers
////////////////////////////////////////////////////////////////////////////////


// Return set of tickers that need attention
pub fn extract_tickers (txt :&str) -> HashSet<String> {
    let mut tickers = HashSet::new();
    let re = Regex::new(r"^[A-Za-z@._^]?[A-Za-z0-9^._=-]+$").unwrap(); // BRK.A ^GSPC BTC-USD don't end in - so a bad-$ trade doesn't trigger this
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
}

async fn get_quote_pretty (db :&DB, ticker :&str) -> Bresult<String> {
// Expects:  GME 308188500 @shrewm.   Includes local level 2 quotes as well.  Used by do_quotes only
    let ticker = resolve_ref_ticker(ticker).unwrap_or(ticker.to_string());
    let isselfstonk = is_self_stonk(&ticker);
    let bidask =
        if isselfstonk {
            let mut asks = "*Asks*".to_string();
            for ask in get_sql(&format!("SELECT -qty AS qty, price FROM exchange WHERE qty<0 AND ticker='{}' order by price;", ticker))? {
                asks.push_str(&format!(" `{}@{}`",
                    num_simp(ask.get("qty").unwrap()),
                    ask.get("price").unwrap().parse::<f64>()?,
                ));
            }
            let mut bids = "*Bids*".to_string();
            for bid in get_sql(&format!("SELECT qty, price FROM exchange WHERE 0<qty AND ticker='{}' order by price desc;", ticker))? {
                bids.push_str(&format!(" `{}@{}`",
                    num_simp(bid.get("qty").unwrap()),
                    bid.get("price").unwrap().parse::<f64>()?,
                ));
            }
            format!("\n{}\n{}", asks, bids).to_string()
        } else {
            "".to_string()
        };

    let stonk = get_stonk(db, &ticker).await?;
    let updated_indicator = if stonk.contains_key("updated") { "·" } else { "" };

    Ok( format!("{}{}{}",
            &stonk.get("pretty").unwrap().replace("_", "\\_"),
            updated_indicator,
            bidask) )
} // get_quote_pretty

fn format_position (ticker:&str, qty:f64, cost:f64, price:f64) -> Bresult<String> {
    let basis = qty*cost;
    let value = qty*price;
    let gain = format!("{:.2}", value - basis);
    let gain_percent = percent_squish((100.0 * (price-cost)/cost).abs());
    //let updown = if basis <= value { from_utf8(b"\xE2\x86\x91")? } else { from_utf8(b"\xE2\x86\x93")? }; // up down arrows
    let greenred =
        if basis < value {
            from_utf8(b"\xF0\x9F\x9F\xA2")? // green circle
        } else if basis == value {
            from_utf8(b"\xF0\x9F\x94\xB7")? // blue diamond
        } else {
            from_utf8(b"\xF0\x9F\x9F\xA5")? // red block
        };
    Ok(format!("\n`{:>7.2}``{:>8} {:>4}% {}``{} @{:.2}` *{}*_@{:.2}_",
        roundcents(value),
        gain, gain_percent, greenred,
        ticker, roundcents(price),
        qty, roundcents(cost)))
}

////////////////////////////////////////////////////////////////////////////////
/// Bot's Do Handlers -- The Meat And Potatos.  The Bread N Butter.  The Works.
////////////////////////////////////////////////////////////////////////////////

async fn do_help (db :&DB, cmd:&Cmd) -> Bresult<&'static str> {

    if Regex::new(r"/help").unwrap().captures(&cmd.msg).is_none() {
        return Ok("SKIP")
    }

    send_msg_markdown(db, cmd.at, &format!(
"`/yolo  ` `Stonks leaderboard`
`/stonks` `Your Stonkfolio`
`gme$   ` `Quote ({}min delay)`
`gme+   ` `Buy all`
`gme-   ` `Sell all`
`gme+2  ` `Buy 2 shares (min qty 0.0001)`
`gme-2  ` `Sell 2 shares`
`gme+$2 ` `Buy $2 worth (min amt $0.01`
`gme-$2 ` `Sell $2 worth`
`word:  ` `Definition`
`word;  ` `Synonyms`
`+?     ` `Likes leaderboard`", db.quote_delay_minutes)).await?;
    Ok("COMPLETED.")
}

async fn _do_curse (db :&DB, cmd:&Cmd) -> Bresult<&'static str> {

    if Regex::new(r"/curse").unwrap().captures(&cmd.msg).is_none() { return Ok("SKIP") }
    send_msg_markdown(db, cmd.at, 
        ["shit", "piss", "fuck", "cunt", "cocksucker", "motherfucker", "tits"][::rand::random::<usize>()%7]
    ).await?;
    Ok("COMPLETED.")
}

async fn do_like (db :&DB, cmd:&Cmd) -> Bresult<String> {

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
    send_msg(db, cmd.at, &text).await?;

    Ok("COMPLETED.".into())
}

async fn do_like_info (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {

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
    //info!("HEARTS -> msg tmbot {:?}", send_msg(db, chat_id, &(-6..=14).map( |n| num2heart(n) ).collect::<Vec<&str>>().join("")).await);
    send_msg(db, cmd.at, &text).await?;
    Ok("Ok do_like_info")
}

async fn do_syn (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {

    let cap = Regex::new(r"^([a-z]+);$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let word = &cap.unwrap()[1];

    info!("looking up {:?}", word);

    let mut defs = get_syns(word).await?;

    if 0 == defs.len() {
        send_msg_markdown(db, cmd.id, &format!("*{}* synonyms is empty", word)).await?;
        return Ok("do_syn empty synonyms");
    }

    let mut msg = String::new() + "*\"" + word + "\"* ";
    defs.truncate(10);
    msg.push_str( &defs.join(", ") );
    send_msg_markdown(db, cmd.at, &msg).await?;

    Ok("Ok do_syn")
}

async fn do_def (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {

    let cap = Regex::new(r"^([a-z]+):$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let word = &cap.unwrap()[1];

    let defs = get_definition(word).await?;

    if defs.is_empty() {
        send_msg_markdown(db, cmd.id, &format!("*{}* def is empty", word)).await?;
        return Ok("do_def def is empty");
    }

    let mut msg = String::new() + "*" + word;

    if 1 == defs.len() {
        msg.push_str( &format!(":* {}", defs[0].to_string()));
    } else {
        msg.push_str( &format!(" ({})* {}", 1, defs[0].to_string()));
        for i in 1..std::cmp::min(4, defs.len()) {
            msg.push_str( &format!(" *({})* {}", i+1, defs[i].to_string()));
        }
    }

    send_msg_markdown(db, cmd.at, &msg).await?;

    Ok("Ok do_def")
}

async fn do_sql (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {

    if cmd.id != 308188500 { return Ok("do_sql invalid user"); }

    let cap = Regex::new(r"^(.*)ß$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let expr = &cap.unwrap()[1];

    let result = get_sql(expr);
    if let Err(e) = result {
        send_msg_markdown(db, cmd.id, &format!("{:?}", e)).await?;
        return Err(e);
    }
    let results = result.unwrap();

    if results.is_empty() {
        send_msg(db, cmd.id, &format!("\"{}\" results is empty", expr)).await?;
        return Ok("do_sql def is empty");
    }

    for res in results {
        let mut buff = String::new();
        res.iter().for_each( |(k,v)| buff.push_str(&format!("{}:{} ", k, v)) );
        let res = format!("{}\n", buff);
        send_msg(db, cmd.at, &res).await?;
    }

    Ok("Ok do_sql")
}

async fn do_quotes (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {

    let tickers = extract_tickers(&cmd.msg);
    if tickers.is_empty() { return Ok("SKIP") }

    for ticker in &tickers {
        // Catch error and continue looking up tickers
        match get_quote_pretty(db, ticker).await {
            Ok(res) => { send_msg_markdown(db, cmd.at, &res).await?; }
            e => { glogd!("get_quote_pretty => ", e); }
        }
    }

    Ok("COMPLETED.")
}

async fn do_portfolio (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {
    if Regex::new(r"STONKS[!?]|[!?/]STONKS").unwrap().find(&cmd.msg.to_uppercase()).is_none() { return Ok("SKIP"); }

    let mut total = 0.0;
    let mut msg = String::new();

    let positions = get_sql(&format!("SELECT * FROM positions WHERE id={}", cmd.id))?;
    for pos in positions {
        info!("{} position {:?}", cmd.id, pos);
        let ticker = pos.get("ticker").unwrap();
        let ticker_refed = { // Unref self-stonk back to username
            if is_self_stonk(ticker) {
                ("@".to_string() + get_sql(&format!("SELECT name FROM entitys WHERE id={}", &ticker))?[0].get("name").unwrap()).to_string()
            } else {
                ticker.to_string()
            }
        };
        let qty = pos.get("qty").unwrap().parse::<f64>().unwrap();
        let cost = pos.get("price").unwrap().parse::<f64>().unwrap();
        let price = get_stonk(db, &ticker).await?.get("price").unwrap().parse::<f64>().unwrap();
        let value = qty * price;
        total += value;
        msg.push_str(&format_position(&ticker_refed, qty, cost, price)?);
    }

    let cash = get_bank_balance(cmd.id)?;
    msg.push_str(&format!("\n`{:7.2}``Cash`    `YOLO``{:.2}`", roundcents(cash), roundcents(total+cash)));

    send_msg_markdown(db, cmd.at, &msg).await?;
    Ok("COMPLETED.")
}

async fn do_yolo (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {

    // Handle: !yolo ?yolo yolo! yolo?
    if Regex::new(r"YOLO[!?]|[!?/]YOLO").unwrap().find(&cmd.msg.to_uppercase()).is_none() { return Ok("SKIP"); }

    let working_message = "working...".to_string();
    let message_id = send_msg(db, cmd.at, &working_message).await?;

    // Update all user-positioned tickers
    for row in get_sql("\
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
        //send_edit_msg(db, cmd.at, message_id, &working_message).await?;
        info!("Stonk \x1b[33m{:?}", get_stonk(db, ticker).await?);
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
    let results = get_sql(&sql)?;

    // Build and send response string

    let mut msg = "*YOLOlians*".to_string();
    for row in results {
        msg.push_str( &format!(" `{:.2}@{}`",
            row.get("yolo").unwrap().parse::<f64>()?,
            row.get("name").unwrap()) );
    }

    send_edit_msg_markdown(db, cmd.at, message_id, &msg).await?;

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
struct TradeBuy { qty:f64, price:f64, bank_balance: f64, trade: Trade }

impl TradeBuy {
    async fn new (trade:Trade, db:&DB) -> Bresult<Self> {
        let price =
            get_stonk(db, &trade.ticker).await?
            .get("price").ok_or("price missing from stonk")?
            .parse::<f64>()?;
        let bank_balance = get_bank_balance(trade.id)?;
        let qty = match trade.amt {
            Some(amt) => if trade.is_dollars { amt/price } else { amt },
            None => roundqty(bank_balance / price)
        };
        Ok( Self{qty, price, bank_balance, trade} )
    }
}

#[derive(Debug)]
struct TradeBuyCalc { qty:f64, new_balance:f64, new_qty:f64, new_cost:f64, positions:Vec<Position>, tradebuy:TradeBuy }

impl TradeBuyCalc {
    async fn compute_position (obj:TradeBuy, db:&DB) -> Bresult<Self> {
        let (qty, new_balance) =
            match
                verify_qty(obj.qty, obj.price, obj.bank_balance)
                .or_else( |_e| verify_qty(obj.qty-0.0001, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0002, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0003, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0004, obj.price, obj.bank_balance) )
                .or_else( |_e| verify_qty(obj.qty-0.0005, obj.price, obj.bank_balance) ) {
                Err(e) => {  // Message user problem and log
                    send_msg(db, obj.trade.id, &e).await?;
                    return Err(e.into())
                },
                Ok(r) => r
            };
        let positions = Position::query(obj.trade.id, &obj.trade.ticker)?;
        let (new_qty, new_cost) = match positions.len() {
            0 => (qty, obj.price),
            1 => {
                let qty_old = positions[0].qty;
                let price_old = positions[0].price;
                let new_cost = (qty * obj.price + qty_old * price_old) / (qty + qty_old);
                let new_qty = roundqty(qty + qty_old);
                (new_qty, new_cost) },
            _ => return Err(format!("Ticker {} has {} positions, expect 0 or 1", &obj.trade.ticker, positions.len()).into())
        };
        Ok(Self{qty, new_balance, new_qty, new_cost, positions, tradebuy:obj})
    }
}

#[derive(Debug)]
struct ExecuteBuy { msg:String, tradebuycalc:TradeBuyCalc }

impl ExecuteBuy {
    async fn execute (obj:TradeBuyCalc) -> Bresult<Self> {
        let id = obj.tradebuy.trade.id;
        let ticker = &obj.tradebuy.trade.ticker;
        let price = obj.tradebuy.price;

        sql_table_order_insert(id, ticker, obj.qty, price, Instant::now().seconds())?;
        let mut msg = format!("*Bought:*");

        if obj.positions.is_empty() {
            get_sql(&format!("INSERT INTO positions VALUES ({}, '{}', {}, {})", id, ticker, obj.new_qty, obj.new_cost))?;
        } else {
            msg += &format!("  `{:.2}``{}` *{}*_@{}_", obj.qty*price, ticker, obj.qty, price);
            info!("\x1b[1madd to existing position:  {} @ {}  ->  {} @ {}", obj.positions[0].qty, obj.positions[0].price, obj.new_qty, obj.new_cost);
            get_sql(&format!("UPDATE positions SET qty={}, price={} WHERE id='{}' AND ticker='{}'", obj.new_qty, obj.new_cost, id, ticker))?;
        }
        msg += &format_position(ticker, obj.new_qty, obj.new_cost, price)?;

        get_sql(&format!("UPDATE accounts SET balance={} WHERE id={}", obj.new_balance, id))?;

        Ok(Self{msg, tradebuycalc:obj})
    }
}

async fn do_trade_buy (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {
    let trade = Trade::new(cmd.id, &cmd.msg)?;
    if trade.as_ref().map_or(true, |trade| trade.action != '+') { return Ok("SKIP") }

    let res =
        TradeBuy::new(trade.unwrap(), db).await
        .map(|tradebuy| TradeBuyCalc::compute_position(tradebuy, db))?.await
        .map(ExecuteBuy::execute)?.await
        .map(|res| { info!("\x1b[1;31mResult {:#?}", &res); res })?;

    send_msg_markdown(db, cmd.at, &res.msg).await?; // Report to user
    Ok("COMPLETED.")
}

////////////////////////////////////////////////////////////////////////////////
/// Stonk Sell

#[derive(Debug)]
struct TradeSell {
    positions: Vec<Position>,
    qty: f64,
    price: f64,
    bank_balance: f64,
    new_balance: f64,
    new_qty: f64,
    trade: Trade
}

impl TradeSell {
    async fn new (trade:Trade, db:&DB) -> Bresult<Self> {
        let id = trade.id;
        let ticker = &trade.ticker;

        let positions = Position::query(id, ticker)?;
        if 1 != positions.len() {
            send_msg(db, id, "You lack a valid position.").await?;
             Err("expect 1 position in table")?
        }
        let pos_qty = positions[0].qty;

        let price = get_stonk(db, ticker).await?.get("price").ok_or("price missing from stonk")?.parse::<f64>().unwrap();

        let mut qty =
            roundqty(match trade.amt {
                Some(amt) => if trade.is_dollars { amt / price } else { amt }, // Convert dollars to shares maybe
                None => pos_qty // no amount set, so set to entire qty
            });
        if qty <= 0.0 {
            send_msg(db, id, "Quantity too low.").await?;
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
            send_msg(db, id, "You can't sell more than you own.").await?;
            return Err("not enough shares to sell".into());
        }

        let new_qty = roundqty(pos_qty-qty);

        Ok( Self{positions, qty, price, bank_balance, new_balance, new_qty, trade} )
    }
}

#[derive(Debug)]
struct ExecuteSell {
    msg: String,
    tradesell: TradeSell
}

impl ExecuteSell {
    async fn execute (obj:TradeSell) -> Bresult<Self> {
        let id = obj.trade.id;
        let ticker = &obj.trade.ticker;
        let position = &obj.positions[0];
        let qty = obj.qty;
        let price = obj.price;
        let new_qty = obj.new_qty;
        sql_table_order_insert(id, ticker, -qty, price, Instant::now().seconds())?;
        let mut msg = format!("*Sold:*");
        if new_qty == 0.0 {
            get_sql(&format!("DELETE FROM positions WHERE id={} AND ticker='{}'", id, ticker))?;
            msg += &format_position(ticker, qty, position.price, price)?;
        } else {
            msg += &format!("  `{:.2}``{}` *{}*_@{}_{}",
                qty*price, ticker, qty, price,
                &format_position(ticker, new_qty, position.price, price)?);
            get_sql(&format!("UPDATE positions SET qty={} WHERE id='{}' AND ticker='{}'", new_qty, id, ticker))?;
        }
        get_sql(&format!("UPDATE accounts SET balance={} WHERE id={}", obj.new_balance, id))?;
        Ok(Self{msg, tradesell:obj})
    }
}

async fn do_trade_sell (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {
    let trade = Trade::new(cmd.id, &cmd.msg)?;
    if trade.as_ref().map_or(true, |trade| trade.action != '-') { return Ok("SKIP") }

    let res =
        TradeSell::new(trade.unwrap(), db).await
        .map(ExecuteSell::execute)?.await?;

    info!("\x1b[1;31mResult {:#?}", res);
    send_msg_markdown(db, cmd.at, &res.msg).await?; // Report to user
    Ok("COMPLETED.")
}


////////////////////////////////////////////////////////////////////////////////
/// General Exchange Market Place
/*
    Create an ask quote (+price) on the exchange table, lower is better.
            [[ 0.01  0.30  0.50  0.99   1.00  1.55  2.00  9.00 ]]
    Best BID price (next buyer)--^      ^--Best ASK price (next seller)
    Positive quantity                      Negative quantity

   let trade: id bid/ask qty price
   let current_ticker_qty  // how many ID owns
   let current_ask_qty:    // sum of asks quantities for this ticker
*/ 

#[derive(Debug)]
struct Quote {
    id: i64,
    thing: String,
    qty: f64,
    price: f64,
    ticker: String,
    now: i64
}

fn resolve_ref_ticker (s :&str) -> Option<String> {
    // Expects:  @shrewm   Returns: Some(308188500)
    if &s[0..1] == "@" {
        // Ticker is whomever this nick matches
        get_sql(&format!("SELECT id FROM entitys WHERE name='{}'", &s[1..]))
            .ok()
            .filter( |v| 1 == v.len() )
            .map( |v| v[0].get("id").unwrap().to_string() )
    } else { None }
}

impl Quote {
    fn scan (id:i64, msg:&str) -> Bresult<Option<Self>> {
        let caps = //                  ____ticker_____  _____________qty____________________  @  ___________price______________
            match regex_to_hashmap(r"^(@[A-Za-z^.-_]+)([+-]([0-9]+[.]?|[0-9]*[.][0-9]{1,4}))[$@]([0-9]+[.]?|[0-9]*[.][0-9]{1,2})$", msg) {
                Some(caps) => caps,
                None => return Ok(None)
            };
        let thing = caps.get("1").unwrap().to_string();

        Ok(resolve_ref_ticker(&thing).map(
            |ticker|
            Self {
                id,
                thing,
                qty: roundqty(caps.get("2").unwrap().parse::<f64>().unwrap()),
                price: caps.get("4").unwrap().parse::<f64>().unwrap(),
                ticker,
                now: Instant::now().seconds() }))
    }
}

#[derive(Debug)]
struct QuoteCancelMine {
    qty: f64,
    myasks: Vec<HashMap<String,String>>,
    myasksqty: f64,
    mybids: Vec<HashMap<String,String>>,
    quote: Quote,
}
impl QuoteCancelMine {
    fn doit (quote :Quote) -> Bresult<Self> {
        let id = quote.id;
        let ticker = &quote.ticker;
        let mut qty = quote.qty;
        let price = quote.price;
        //let now = quote.now;

        let mut myasks = get_sql( &format!("SELECT * FROM exchange WHERE id={} AND ticker='{}' AND qty<0.0 ORDER BY price", id, ticker) )?;
        let myasksqty = -roundqty(myasks.iter().map( |ask| ask.get("qty").unwrap().parse::<f64>().unwrap() ).sum::<f64>());
        let mut mybids = get_sql( &format!("SELECT * FROM exchange WHERE id={} AND ticker='{}' AND 0.0<=qty ORDER BY price DESC", id, ticker) )?;

        if qty < 0.0 { // This is an ask quote
            // Remove/decrement a matching bid in the current settled market
            if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get("price").unwrap().parse::<f64>().unwrap() ) {
                let bidqty = roundqty(mybid.get("qty").unwrap().parse::<f64>()?);
                if bidqty <= -qty {
                    get_sql( &format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}", id, ticker, bidqty, price) )?;
                    mybid.insert("*UPDATED*".into(), "*REMOVED*".into());
                    qty = roundqty(qty + bidqty);
                } else {
                    let newbidqty = roundqty(bidqty+qty);
                    get_sql( &format!("UPDATE exchange SET qty={} WHERE id={} AND ticker='{}' AND price={}", newbidqty, id, ticker, price) )?;
                    mybid.insert("*UPDATED*".into(), format!("*QTY={:.}*", newbidqty));
                    qty = 0.0;
                }
            }
        } else if 0.0 < qty { // This is a bid quote
            // Remove/decrement a matching ask in the current settled market
            if let Some(myask) = myasks.iter_mut().find( |b| price == b.get("price").unwrap().parse::<f64>().unwrap() ) {
                let askqty = roundqty(myask.get("qty").unwrap().parse::<f64>()?);
                if -askqty <= qty {
                    get_sql( &format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}", id, ticker, askqty, price) )?;
                    myask.insert("*UPDATED*".into(), "*REMOVED*".into());
                    qty = roundqty(qty + askqty);
                } else {
                    let newaskqty = roundqty(askqty+qty);
                    get_sql( &format!("UPDATE exchange SET qty={} WHERE id={} AND ticker='{}' AND price={}", newaskqty, id, ticker, price) )?;
                    myask.insert("*UPDATED*".into(), format!("*QTY={:.}*", newaskqty));
                    qty = 0.0;
                }
            }

        }

        Ok(Self{qty, myasks, myasksqty, mybids, quote})
    }
}

#[derive(Debug)]
struct QuoteExecute {
    qty: f64,
    bids: Vec<HashMap<String,String>>,
    quotecancelmine: QuoteCancelMine,
}

impl QuoteExecute {
    async fn doit(mut obj :QuoteCancelMine, db:&DB) -> Bresult<Self> {
        let id = obj.quote.id;
        let ticker = &obj.quote.ticker;
        let price = obj.quote.price;
        let mut qty = obj.qty;
        let now = obj.quote.now;
        let myasks = &mut obj.myasks;
        let mybids = &mut obj.mybids;
        let asksqty = obj.myasksqty;

        // Other bids / asks (not mine) global since it's being updated and returned for logging
        let mut bids = get_sql( &format!("SELECT * FROM exchange WHERE id!={} AND ticker='{}' AND 0.0<qty ORDER BY price DESC", id, ticker) )?;
        let mut asks = get_sql( &format!("SELECT * FROM exchange WHERE id!={} AND ticker='{}' AND qty<0.0 ORDER BY price", id, ticker) )?;

        let position = Position::query(id, ticker)?;
        let (posqty, posprice) = if 1==position.len() { (position[0].qty, position[0].price) } else { (0.0, 0.0) };

        if qty < 0.0 { // This is an ask quote
            if posqty < -qty + asksqty { // does it exceed my current ask qty?
                send_msg(db, id, "You lack that available quantity to sell.").await?;
            } else {
                for abid in bids.iter_mut().filter( |b| price <= b.get("price").unwrap().parse::<f64>().unwrap() ) {
                    //error!("{:?}", mybid);
                    let aid = abid.get("id").unwrap().parse::<i64>()?;
                    let aqty = roundqty(abid.get("qty").unwrap().parse::<f64>()?);
                    let aprice = abid.get("price").unwrap().parse::<f64>()?;
                    //error!("my ask qty {}  asksqty sum {}  bid qty {}", qty, asksqty, aqty);

                    let xqty = aqty.min(-qty); // quantity exchanged is the smaller of the two (could be equal)

                    if xqty == aqty { // bid to be entirely settled
                        get_sql( &format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}", aid, ticker, aqty, aprice) )?;
                        abid.insert("*UPDATED*".into(), "*REMOVED*".into());
                    } else {
                        get_sql( &format!("UPDATE exchange set qty={} WHERE id={} AND ticker='{}' AND qty={} AND price={}", aqty-xqty, aid, ticker, aqty, aprice) )?;
                        abid.insert("*UPDATED*".into(), format!("*qty={}*", aqty-xqty));
                    }

                    //error!("new order {} {} {} {} {}", id, ticker, aqty, price, now);
                    // Update order table with the purchase and buy info
                    sql_table_order_insert(id, ticker, -xqty, aprice, now)?;
                    sql_table_order_insert(aid, ticker, xqty, aprice, now)?;

                    // Update each user's account
                    let value = xqty * aprice;
                    get_sql( &format!("UPDATE accounts SET balance=balance+{} WHERE id={}",  value, id) )?;
                    get_sql( &format!("UPDATE accounts SET balance=balance+{} WHERE id={}", -value, aid) )?;

                    // Update my position
                    let newposqty = roundqty(posqty - xqty);
                    if 0.0 == newposqty {
                        get_sql( &format!("DELETE FROM positions WHERE id={} AND ticker='{}' AND qty={} AND price={}", id, ticker, posqty, posprice) )?;
                    } else {
                        get_sql( &format!("UPDATE positions SET qty={} WHERE id={} AND ticker='{}' AND price={}", newposqty, id, ticker, posprice) )?;
                    }

                    // Update buyer's position
                    let aposition = Position::query(aid, ticker)?;
                    let (aposqty, aposprice) = if 1==aposition.len() { (aposition[0].qty, aposition[0].price) } else { (0.0, 0.0) };
                    if 0.0 == aposqty {
                        get_sql( &format!("INSERT INTO positions values({}, '{}', {}, {})", aid, ticker, xqty, value) )?;
                    } else {
                        let newaposqty = roundqty(aposqty+aqty);
                        let newaposcost = (aposprice + value) / (aposqty + xqty);
                        get_sql( &format!("UPDATE positions SET qty={}, price={} WHERE id={} AND ticker='{}' AND price={}", newaposqty, newaposcost, aid, ticker, aposprice) )?;
                    }

                    // Update stonk quote value
                    let price =
                        get_stonk(db, ticker).await?
                        .get("price").ok_or("price missing from stonk")?
                        .parse::<f64>()?;
                    let amt = aprice-price;
                    let per = amt/price;
                    let pretty = make_pretty_quote(
                        &("@".to_string() + get_sql(&format!("SELECT name FROM entitys WHERE id={}", &ticker))?[0].get("name").unwrap()),
                        &Ticker{
                            price:aprice, amount:amt, percent:per,
                            title:"Self Stonk".into(), hours:'r', exchange:"™BOT".to_string() })?;

                    get_sql( &format!("UPDATE stonks SET price={}, pretty='{}' WHERE ticker={}", aprice, pretty, ticker) )?;

                    qty = roundqty(qty+xqty);
                    if 0.0 <= qty { break }
                }

                // create or increment in exchange table my ask.  This could also be the case if no bids were executed.
                if qty < 0.0 {
                    if let Some(myask) = myasks.iter_mut().find( |a| price == a.get("price").unwrap().parse::<f64>().unwrap() ) {
                        let newqty = roundqty(myask.get("qty").unwrap().parse::<f64>().unwrap() - qty);
                        let mytime = myask.get("time").unwrap().parse::<i64>().unwrap();
                        get_sql(&format!("UPDATE exchange set qty={} WHERE id={} AND ticker='{}' AND price={} AND time={}", newqty, id, ticker, price, mytime))?;
                        myask.insert("*UPDATED*".into(), format!("qty={}", newqty));
                    } else {
                        get_sql(&format!("INSERT INTO exchange VALUES ({}, '{}', {:.4}, {:.4}, {})", id, ticker, qty, price, now))?;
                        // Add a fake entry to local myasks vector for logging's sake
                        let mut hm = HashMap::<String,String>::new();
                        hm.insert("*UPDATED*".to_string(), format!("*INSERTED* {} {} {} {} {}", id, ticker, qty, price, now));
                        myasks.push(hm);
                    }
                }
            }
        } else if 0.0 < qty { // This is a bid quote (want to buy someone)
            // Limited by available cash but that's up to the current best ask price and sum of ask costs.
            let basis = qty * price;
            let bank_balance = get_bank_balance(id)?;
            let mybidsprice =
                mybids.iter().map( |bid|
                    bid.get("qty").unwrap().parse::<f64>().unwrap()
                    * bid.get("price").unwrap().parse::<f64>().unwrap() )
                .sum::<f64>();

            if bank_balance < mybidsprice + basis { // Verify not over spending as this order could become a bid
                send_msg(db, id, "Available cash lacking for this bid.").await?;
            } else {
                for aask in asks.iter_mut().filter( |a| a.get("price").unwrap().parse::<f64>().unwrap() <= price ) {
                    //error!("{:?}", mybid);
                    let aid = aask.get("id").unwrap().parse::<i64>()?;
                    let aqty = roundqty(aask.get("qty").unwrap().parse::<f64>()?); // This is a negative value
                    let aprice = aask.get("price").unwrap().parse::<f64>()?;
                    //error!("my ask qty {}  asksqty sum {}  bid qty {}", qty, asksqty, aqty);

                    let xqty = roundqty(qty.min(-aqty)); // quantity exchanged is the smaller of the two (could be equal)

                    if xqty == -aqty { // ask to be entirely settled
                        get_sql( &format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}", aid, ticker, aqty, aprice) )?;
                        aask.insert("*UPDATED*".into(), "*REMOVED*".into());
                    } else {
                        let newqty = aqty+xqty;
                        get_sql( &format!("UPDATE exchange set qty={} WHERE id={} AND ticker='{}' AND qty={} AND price={}", newqty, aid, ticker, aqty, aprice) )?;
                        aask.insert("*UPDATED*".into(), format!("*qty={}*", newqty));
                    }

                    //error!("new order {} {} {} {} {}", id, ticker, aqty, price, now);
                    // Update order table with the purchase and buy info
                    sql_table_order_insert(id, ticker, xqty, aprice, now)?;
                    sql_table_order_insert(aid, ticker, -xqty, aprice, now)?;

                    // Update each user's account
                    let value = xqty * aprice;
                    get_sql( &format!("UPDATE accounts SET balance=balance+{} WHERE id={}", -value, id) )?;
                    get_sql( &format!("UPDATE accounts SET balance=balance+{} WHERE id={}",  value, aid) )?;

                    // Update my position
                    if 0.0 == posqty {
                        get_sql( &format!("INSERT INTO positions values({}, '{}', {}, {})", id, ticker, xqty, value) )?;
                    } else {
                        let newposqty = roundqty(posqty+xqty);
                        let newposcost = (posprice + value) / (posqty + xqty);
                        get_sql( &format!("UPDATE positions SET qty={}, price={} WHERE id={} AND ticker='{}' AND price={}", newposqty, newposcost, id, ticker, posprice) )?;
                    }

                    // Update their position
                    let aposition = Position::query(aid, ticker)?;
                    let (aposqty, aposprice) = if 1==aposition.len() { (aposition[0].qty, aposition[0].price) } else { (0.0, 0.0) };
                    let newaposqty = roundqty(aposqty - xqty);
                    if 0.0 == newaposqty {
                        get_sql( &format!("DELETE FROM positions WHERE id={} AND ticker='{}' AND qty={} AND price={}", aid, ticker, aposqty, aposprice) )?;
                    } else {
                        get_sql( &format!("UPDATE positions SET qty={} WHERE id={} AND ticker='{}' AND price={}", newaposqty, aid, ticker, aposprice) )?;
                    }

                    // Update stonk quote value
                    let price =
                        get_stonk(db, ticker).await?
                        .get("price").ok_or("price missing from stonk")?
                        .parse::<f64>()?;
                    let amt = aprice-price;
                    let per = amt/price;
                    let pretty = make_pretty_quote(
                        &("@".to_string() + get_sql(&format!("SELECT name FROM entitys WHERE id={}", &ticker))?[0].get("name").unwrap()),
                        &Ticker{
                            price:aprice, amount:amt, percent:per,
                            title:"Self Stonk".into(), hours:'r', exchange:"™BOT".to_string() })?;

                    get_sql( &format!("UPDATE stonks SET price={}, pretty='{}' WHERE ticker={}", aprice, pretty, ticker) )?;

                    qty = roundqty(qty-xqty);
                    if qty <= 0.0 { break }
                }

                // create or increment in exchange table my bid.  This could also be the case if no asks were executed.
                if 0.0 < qty{
                    if let Some(mybid) = mybids.iter_mut().find( |b| price == b.get("price").unwrap().parse::<f64>().unwrap() ) {
                        let newqty = roundqty(mybid.get("qty").unwrap().parse::<f64>().unwrap() + qty);
                        let mytime = mybid.get("time").unwrap().parse::<i64>().unwrap();
                        get_sql(&format!("UPDATE exchange set qty={} WHERE id={} AND ticker='{}' AND price={} AND time={}", newqty, id, ticker, price, mytime))?;
                        mybid.insert("*UPDATED*".into(), format!("qty={}", newqty));
                    } else {
                        get_sql(&format!("INSERT INTO exchange VALUES ({}, '{}', {:.4}, {:.4}, {})", id, ticker, qty, price, now))?;
                        // Add a fake entry to local mybids vector for logging's sake
                        let mut hm = HashMap::<String,String>::new();
                        hm.insert("*UPDATED*".to_string(), format!("*INSERTED* {} {} {} {} {}", id, ticker, qty, price, now));
                        mybids.push(hm);
                    }
                }
            }
        }
        Ok(Self{qty, bids, quotecancelmine: obj})
    }
}

async fn do_exchange_bidask (db :&DB, cmd :&Cmd) -> Bresult<&'static str> {

    let quote = Quote::scan(cmd.id, &cmd.msg)?;
    let quote = if quote.is_none() { return Ok("SKIP") } else { quote.unwrap() };

    let ret =
        QuoteCancelMine::doit(quote)
        .map(|obj| QuoteExecute::doit(obj, db) )?.await?;
    info!("\x1b[1;31mResult {:#?}", ret);
    //send_msg_markdown(db, cmd.at, &obj.msg).await?; // Report to user?
    return Ok("testing");

    /*
    if obj.quot.qty < 0.0 { // A seller has appeared:  ask quote (-qty, price)
        // (1a) Acquire bids on my ticker.
        // (1) Consider my bids and Decrement or delete existing matching bids (benign)

        // (2a) Acquire my asks on my ticker.  Subtract count from my qty.  Verify still positive.
        // (2) Add to andy of my matching ask quotes, guaranteed to not match any open bids since
        //     the exchange is always in a resolved.

        // (3) qty left can be matched with any bids above or at ask price) and money/security
        //     exchanged.

        // (4) remaining qty becomes a new ask quote

        // Check seller has the qty first
        let seller_position = get_sql(
            &format!("SELECT * \
                      FROM ( \
                        SELECT SUM(qty) as qty \
                        FROM ( \
                                SELECT qty FROM positions WHERE id={} AND ticker='{}' \
                            UNION ALL \
                                SELECT qty FROM exchange WHERE id={} AND ticker='{}' AND qty<0 ) ) \
                      WHERE {} <= qty",
            cmd.id, ticker, cmd.id, ticker, -quote_qty))?;
        if 1 != seller_position.len() { return Ok("Seller lacks the securities.") }

        // Any bidders willing to buy/pay?
        let bidder = get_sql(
            &format!("SELECT * FROM exchange  WHERE ticker='{}' AND 0<qty AND {}<=price  ORDER BY price DESC LIMIT 1",
                ticker, quote_price))?;

        if 1 <= bidder.len() {
            quote_qty *= -1.0;
            let bidder_id = bidder[0].get("id").unwrap().parse::<i64>()?;
            let qty =   bidder[0].get("qty").unwrap().parse::<f64>()?;
            let price = bidder[0].get("price").unwrap().parse::<f64>()?;

            let best_qty = quote_qty.min(qty);
            let best_price = quote_price.max(price);

            warn!("new ask/seller {}@{}  +  existing bi/buyer {}@{}  =>  best order {}@{}",
                quote_qty, quote_price,   qty, price,   best_qty, best_price);

            // Update The Exchange

            if qty == best_qty {
                // Delete the market quote
                get_sql(&format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}", bidder_id, ticker, qty, price))?;
            } else {
                // Update the market quote
                get_sql(&format!("UPDATE exchange SET qty={} WHERE id={} AND ticker='{}' AND price={}", qty-best_qty, bidder_id, ticker, price))?;
            }

            // Create the new orders

            if cmd.id != bidder_id {
                sql_table_order_insert(cmd.id, &ticker, -best_qty, best_price, now)?;
                sql_table_order_insert(
                    bidder_id,
                    &ticker, best_qty, best_price, now)?;
            }

            quote_qty = best_qty - quote_qty; // Adjust quote_qty and continue to creating a possible market quote
        }

        // Update exchange: qty -= amt  OR  remove if qty == amt
        // Positions
        //   Seller: qty-=amt, price = new basis using amt*price  OR  remove if qty == amt
        //   Buyer:  qty+=amt, price = new basis using amt*price  OR create
        // Update stonks with best_price

    } else { // A buyer has appeared:  bid quote (+price)
        let asker = get_sql(&format!("SELECT * FROM exchange WHERE ticker='{}' AND qty<0 AND price<={} ORDER BY price LIMIT 1", ticker, quote_price))?;

        if 1 <= asker.len() {
            // TODO wtf min maxing twice?
            let asker_id = asker[0].get("id").unwrap().parse::<i64>()?;
            let qty =     -asker[0].get("qty").unwrap().parse::<f64>()?;
            let price =    asker[0].get("price").unwrap().parse::<f64>()?;

            let best_qty = quote_qty.min(qty);
            let best_price = quote_price.min(price);

            warn!("new seller {}@{}  existing buyer {}@{} => {}@{}",
                quote_qty, quote_price,   qty, price,   best_qty, best_price);

            // Update The Exchange

            if qty == best_qty {
                // Delete the market quote
                get_sql(&format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}", asker_id, ticker, -qty, price))?;
            } else {
                // Update the market quote
                get_sql(&format!("UPDATE exchange SET qty={} WHERE id={} AND ticker='{}' AND price={}", best_qty-qty, asker_id, ticker, price))?;
            }

            // Create the new orders

            if cmd.id != asker_id {
                sql_table_order_insert(
                    asker_id,
                    &ticker, -best_qty, best_price, now)?;
                sql_table_order_insert(cmd.id, &ticker, best_qty, best_price, now)?;
            }

            quote_qty = quote_qty - best_qty; // Adjust quote_qty and continue to creating a possible market quote
        }
    }

    // Add quote to exchange:  This could either create a new quote or update an existing one with the id, ticker, price matches

    if 0.0 == quote_qty { return Ok("COMPLETED.") }

    let quote =
        if quote_qty < 0.0 {
            get_sql(&format!("SELECT * FROM exchange WHERE id={} AND ticker='{}' AND qty<0 AND price={}", cmd.id, ticker, quote_price))?
        } else {
            get_sql(&format!("SELECT * FROM exchange WHERE id={} AND ticker='{}' AND 0<qty AND price={}", cmd.id, ticker, quote_price))?
        };

    if quote.is_empty() {
        get_sql(&format!("INSERT INTO exchange VALUES ({}, '{}', {:.4}, {:.4}, {})", cmd.id, ticker, quote_qty, quote_price, now))?;
    } else {
        get_sql(&format!(
            "UPDATE exchange set qty={}, time={}  WHERE id={} AND ticker='{}' AND price={}",
            quote_qty + quote[0].get("qty").unwrap().parse::<f64>()?, now,
            cmd.id, ticker, quote_price))?;
    }

    Ok("COMPLETED.")
        */
}

/********************
 *  TODO:
 * 
 *  @worldtmbot-1  sell 1 market order (best ask price) will update stonk price
 *  @worldtmbot+1  buy 1 market order (best ask price) will update stonk price
 * 
 */

////////////////////////////////////////////////////////////////////////////////

fn do_schema() -> Bresult<()> {
    let sql = ::sqlite::open("tmbot.sqlite")?;

    sql.execute("
        CREATE TABLE entitys (
            id INTEGER  NOT NULL UNIQUE,
            name  TEXT  NOT NULL);
    ").map_or_else(gwarn, ginfo);

    sql.execute("
        CREATE TABLE accounts (
            id    INTEGER  NOT NULL UNIQUE,
            balance FLOAT  NOT NULL);
    ").map_or_else(gwarn, ginfo);

    for l in read_to_string("tmbot/users.txt").unwrap().lines() {
        let mut v = l.split(" ");
        let id = v.next().ok_or("User DB malformed.")?;
        let name = v.next().ok_or("User DB malformed.")?.to_string();
        sql.execute(
            format!("INSERT INTO entitys VALUES ( {}, '{}' )", id, name)
        ).map_or_else(gwarn, ginfo);
    }

    sql.execute("
        --DROP TABLE stonks;
        CREATE TABLE stonks (
            ticker  TEXT  NOT NULL UNIQUE,
            price  FLOAT  NOT NULL,
            time INTEGER  NOT NULL,
            pretty  TEXT  NOT NULL);
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


async fn do_all(db: &DB, body: &web::Bytes) -> Bresult<()> {
    let cmd = Cmd::parse_cmd(&body)?;
    info!("\x1b[33m{:?}", &cmd);

    glogd!("do_help =>",      do_help(&db, &cmd).await);
    glogd!("do_like =>",      do_like(&db, &cmd).await);
    glogd!("do_like_info =>", do_like_info(&db, &cmd).await);
    glogd!("do_syn =>",       do_syn(&db, &cmd).await);
    glogd!("do_def =>",       do_def(&db, &cmd).await);
    glogd!("do_sql =>",       do_sql(&db, &cmd).await);
    glogd!("do_quotes => ",   do_quotes(&db, &cmd).await);
    glogd!("do_portfolio =>", do_portfolio(&db, &cmd).await);
    glogd!("do_yolo =>",      do_yolo(&db, &cmd).await);
    glogd!("do_trade_buy =>", do_trade_buy(&db, &cmd).await);
    glogd!("do_trade_sell =>",do_trade_sell(&db, &cmd).await);
    glogd!("do_exchange_bidask =>", do_exchange_bidask(&db, &cmd).await);
    Ok(())
}

fn log_header () {
    info!("\x1b[0;31;40m _____ __  __ ____        _   ");
    info!("\x1b[0;33;40m|_   _|  \\/  | __ )  ___ | |_™ ");
    info!("\x1b[0;32;40m  | | | |\\/| |  _ \\ / _ \\| __|");
    info!("\x1b[0;34;40m  | | | |  | | |_) | (_) | |_ ");
    info!("\x1b[0;35;40m  |_| |_|  |_|____/ \\___/ \\__|");
}

async fn dispatch (req: HttpRequest, body: web::Bytes) -> HttpResponse {
    log_header();

    let db = req.app_data::<web::Data<MDB>>().unwrap().lock().unwrap();
    info!("\x1b[1m{:?}", &db);
    info!("\x1b[35m{}", format!("{:?}", &req.connection_info()).replace(": ", ":").replace("\"", "").replace(",", ""));
    info!("\x1b[35m{}", format!("{:?}", &req).replace("\n", "").replace(r#"": ""#, ":").replace("\"", "").replace("   ", ""));
    info!("\x1b[35m{}", from_utf8(&body).unwrap_or(&format!("{:?}", body)));

    do_all(&db, &body)
    .await
    .unwrap_or_else(|r| error!("{:?}", r));

    info!("End.");

    HttpResponse::from("")
}

////////////////////////////////////////////////////////////////////////////////

pub async fn mainstart() -> Bresult<()> {
    let mut ssl_acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ssl_acceptor_builder .set_private_key_file("key.pem", SslFiletype::PEM)?;
    ssl_acceptor_builder.set_certificate_chain_file("cert.pem")?;

    let botkey           = args().nth(1).ok_or("bad index")?;
    let chat_id_default  = args().nth(2).ok_or("bad index")?.parse::<i64>()?;
    let dst_hours_adjust = args().nth(3).ok_or("bad index")?.parse::<i8>()?;

    if !true { do_schema()?; }

    Ok(HttpServer::new(
        move ||
            App::new()
            .data(
                Mutex::new(
                    DB {
                        url_api:             "https://api.telegram.org/bot".to_string() + &botkey,
                        chat_id_default:     chat_id_default,
                        dst_hours_adjust:    dst_hours_adjust,
                        quote_delay_minutes: QUOTE_DELAY_MINUTES} ) )
            .service(
                web::resource("*")
                .route(
                    Route::new().to(dispatch) ) ) )
    .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
    .run().await?)
}

/*
DROP   TABLE users
CREATE TABLE users (name TEXT, age INTEGER)
INSERT INTO  users VALUES ('Alice', 42)
DELETE FROM  users WHERE age=42 AND name="Oskafunoioi"
UPDATE       users SET a=1, b=2  WHERE c=3
*/