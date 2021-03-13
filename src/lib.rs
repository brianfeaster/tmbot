mod serror;
mod log;
mod json;
mod util;

pub use crate::serror::*;
pub use crate::log::*;
pub use crate::json::*;
pub use crate::util::*;

use ::std::{
    time::{Duration},
    collections::{HashMap, HashSet},
    str::{from_utf8},
    fs::{read_to_string, write},
    env::{args},
    sync::{Mutex, mpsc::{channel, Sender} },
};
use ::log::*;
use ::actix_web::{
    web, App, HttpRequest, HttpServer, HttpResponse, Route,
    client::{Client, Connector}};
use ::openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use ::regex::{Regex};
use ::datetime::{
    Instant, LocalDate, LocalTime, LocalDateTime, DatePiece, Weekday::*,
    ISO}; // ISO


const QUOTE_DELAY :i64 = 2;

////////////////////////////////////////////////////////////////////////////////
/// Datetime details:
/// * Time is seconds since epoch, UTC
/// * Trading hours is 6.5 hours long from 1430-2100 , 1330-2000 if US in DST)
/// ? trading time is pegged to max("closing bell", "after opening bell")
/// ? cache ticker values:  Update only if cached time is before trading time

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
/// Market Hours PST:  pre 0900Z  reg 1430Z   post 2100Z..0100Z
///              PDT:      0800       1330         2000 ..2400
///                    5.5h       6.5h        4h                  [8h closed]

fn update_ticker_p (db :&DB, time :i64, now :i64) -> bool {
    let day =
        LocalDateTime::from_instant(Instant::at(time))
        .date();

    let is_market_day =
        match day.weekday() {
            Saturday | Sunday => false,
            _ => true
        };

    // TODO: Check if time is DST and adjust hours below by -1

    let time_open = LocalDateTime::new(day, LocalTime::hms(9, 00, 0).unwrap()).to_instant().seconds();
    let time_close = time_open + 60 * 60 * 16;

    return
        if is_market_day  &&  time < time_close {
            time_open <= now  &&  (time+(db.quote_delay*60) < now  ||  time_close <= now) // Five minute delay/throttle
        } else {
            update_ticker_p(db, next_trading_day(day), now)
        }
}

fn round (num:f64, pow:i32) -> f64 {
    let fac = 10f64.powi(pow);
    (num * fac).floor() / fac
}

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
        format!("{:4.0}%", num)
    } else if 10.0 <= num {
        format!("{:4.1}%", num)
    } else {
        format!("{:4.2}%", num)
    }
}

// (5,"a","bb") => "a..bb"
fn _pad_between(width: usize, a:&str, b:&str) -> String {
    let lenb = b.len();
    format!("{: <pad$}{}", a, b, pad=width-lenb)
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct DB {
    url_bot: String,
    chat_id_default: i64,
    quote_delay: i64
}

type MDB = Mutex<DB>;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct Cmd {
    from :i64,
    at   :i64,
    to   :i64,
    msg  :String
}

/// Creates a Cmd object from the useful details of a Telegram message.
fn parse_cmd(body: &web::Bytes) -> Result<Cmd, Serror> {

    let json: Value = bytes2json(&body)?;

    let inline_query = &json["inline_query"];
    if inline_query.is_object() {
        let from = getin_i64(inline_query, &["from", "id"])?;
        let msg = getin_str(inline_query, &["query"])?.to_string();
        return Ok(Cmd { from:from, at:from, to:from, msg:msg });
    }

    let message = &json["message"];
    if message.is_object() {
        let from = getin_i64(message, &["from", "id"])?;
        let at = getin_i64(message, &["chat", "id"])?;
        let msg = getin_str(message, &["text"])?.to_string();

        return Ok(
            if let Ok(to) = getin_i64(&message, &["reply_to_message", "from", "id"]) {
                Cmd { from:from, at:at, to:to, msg:msg }
            } else {
                Cmd { from:from, at:at, to:from, msg:msg }
            }
        );
    }

    Err("Nothing to do.")?
}

////////////////////////////////////////////////////////////////////////////////

async fn send_msg (db :&DB, chat_id :i64, text: &str) -> Result<i64, Serror> {
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
    .get( db.url_bot.clone() + "/sendmessage")
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


async fn _send_edit_msg (db :&DB, chat_id :i64, message_id: i64, text: &str) -> Result<(), Serror> {
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
        .get( db.url_bot.clone() + "/editmessagetext")
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


async fn send_msg_markdown (db :&DB, chat_id :i64, text: &str) -> Result<(), Serror> {
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
        .get( db.url_bot.clone() + "/sendmessage")
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

async fn send_edit_msg_markdown (db :&DB, chat_id :i64, message_id :i64, text: &str) -> Result<(), Serror> {
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
        .get( db.url_bot.clone() + "/editmessagetext")
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

async fn get_definition (word: &str) -> Result<Vec<String>, Serror> {
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


async fn get_syns (word: &str) -> Result<Vec<String>, Serror> {
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


async fn get_ticker_details (db :&DB, ticker: &str) -> Result<(String, f64, f64, f64, i64, String, String), Serror> {
    info!("get_ticker_details <- {}", ticker);
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

    let body = body.or_else( |r| Err(format!("get_ticker_details for {:?}  {:?}", ticker, r)) )?;
    let domstr = from_utf8(&body).or_else( |r| Err(format!(r#"get_ticker_details http body2str {:?} {:?}"#, ticker, r)) )?;

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

    println!("{:#}", details);

    let title = getin_str(&details, &["longName"])?.trim_end_matches(|e|e=='.').to_string();
    let exchange = getin_str(&details, &["exchange"])?;

    let timepre  = getin_i64(&details, &["preMarketTime"]).unwrap_or(0);
    let timereg  = getin_i64(&details, &["regularMarketTime"]).unwrap_or(0);
    let timepost = getin_i64(&details, &["postMarketTime"]).unwrap_or(0);

    let details_pre =
        (getin_str(&details, &["preMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["preMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["preMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["preMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         timepre, title.to_string(), exchange.to_string());
    let details_reg =
        (getin_str(&details, &["regularMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["regularMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["regularMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["regularMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         timereg, title.to_string(), exchange.to_string());
    let details_post =
        (getin_str(&details, &["postMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["postMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["postMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["postMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         timepost, title.to_string(), exchange.to_string());

    // TODO: for now log all prices to me privately for debugging/verification
    glogd!("send_msg => \x1b[35m",
        send_msg(db, 308188500,
            &format!("{} \"{}\" ({})\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%",
                ticker, title, exchange,
                LocalDateTime::from_instant(Instant::at(details_pre.4)).iso(), details_pre.1, details_pre.2, details_pre.3,
                LocalDateTime::from_instant(Instant::at(details_reg.4)).iso(), details_reg.1, details_reg.2, details_reg.3,
                LocalDateTime::from_instant(Instant::at(details_post.4)).iso(), details_post.1, details_post.2, details_post.3)
        ).await);

    // Consider most up to date ticker details
    let mut times = [timepre, timereg, timepost];
    times.sort_by( |a,b| b.cmp(a) );

    Ok(if      timepre == times[0] { details_pre }
       else if timereg == times[0] { details_reg }
       else                        { details_post } )

} // get_ticker_details 

////////////////////////////////////////////////////////////////////////////////

fn sql_results_handler (
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

fn get_sql ( cmd :&str ) -> Result<Vec<HashMap<String, String>>, Serror> {
    info!("SQLite <= \x1b[36m{}", cmd);
    let sql = ::sqlite::open( "tmbot.sqlite" )?;
    let (snd, rcv) = channel::<HashMap<String, String>>();
    sql.iterate(cmd, move |r| sql_results_handler(snd.clone(), r) )?;
    Ok(rcv.iter().collect::<Vec<HashMap<String,String>>>())
}


fn sql_table_order_insert (id:i64, ticker:&str, qty:f64, price:f64, time:i64) {
    let sql = format!("INSERT INTO orders VALUES ({}, '{}', {:.4}, {:.8}, {} )", id, ticker, qty, price, time);
    info!("SQLite => {:?}", get_sql(&sql));
}

////////////////////////////////////////////////////////////////////////////////

async fn get_stonk (db :&DB, ticker :&str) -> Result<HashMap<String, String>, Serror> {

    let nowsecs :i64 = Instant::now().seconds();
    let is_self_stonk = &ticker[0..1] == "@";

    let res = get_sql(&format!("SELECT * FROM stonks WHERE ticker='{}'", ticker))?;
    let is_in_table =
        if !res.is_empty() { // Stonks table contains this ticker
            let hm = &res[0];
            let timesecs = hm.get("time").unwrap().parse::<i64>().unwrap();
            if !update_ticker_p(db, timesecs, nowsecs)
                || is_self_stonk {
                return Ok(hm.clone())
            }
            true
        } else { false };

    // Either not in table or need to update table

    let quote = if is_self_stonk {
        // TODO are self stonks implemented and need this?
        Some((from_utf8(b"\xF0\x9F\x94\xB7").unwrap().to_string() + ticker + "@0.00", "0.00".into()))
    } else {
         // prettyPrice:String price:f64 amt:f64 %:f64 time:i64 title:String xchg:String
        let details = get_ticker_details(db, &ticker).await?;

        Some(// Tuple   ("red/green emoji" "pretty price change name" price)
            ( // delta as a colored emoji
                if 0.0 < details.2
                { from_utf8(b"\xF0\x9F\x9F\xA2").unwrap() } // Green circle
                else { from_utf8(b"\xF0\x9F\x9F\xA5").unwrap() } // Red square
                .to_string()
                // ticker@
                + ticker + "@"
                // the numbers price "3,4544.22 
                + &format!("{} ", details.0)
                // {up or down symbol}611.22
                + if 0.0 < details.2
                { from_utf8(b"\xE2\x86\x91").unwrap() } // Arrow up
                else { from_utf8(b"\xE2\x86\x93").unwrap() } // Arrow down
                + &format!("{:.2} ", details.2.abs())
                // 2.3% Famous Company
                + &format!("{:.2}% {}", details.3, details.5)
            , // price full includes commas, decimal zeros
                details.0
            )
        )
    };

    if let Some(quote) = quote { // quote(pretty, fair-market-price-could-be-pre/in/post-market)
        let pretty = quote.0;
        let sql = if is_in_table {
            format!("UPDATE stonks SET price={},time={},pretty='{}'WHERE ticker='{}'", quote.1, nowsecs, pretty, ticker)
        } else {
            format!("INSERT INTO stonks VALUES('{}',{},{},'{}')", ticker, quote.1, nowsecs, pretty)
        };
        info!("SQLite => {:?}", get_sql(&sql)?);
        let mut hm :HashMap::<String, String> = HashMap::new();
        hm.insert("updated".into(), "".into());
        hm.insert("ticker".into(), ticker.to_string());
        hm.insert("price".into(),  quote.1);
        hm.insert("time".into(),   nowsecs.to_string());
        hm.insert("pretty".into(), pretty);
        return Ok(hm);
    }

    Err("ticker not found")?
}

fn get_bank_balance (id :i64) -> Result<f64, Serror> {
    let sql = format!("SELECT * FROM accounts WHERE id={}", id);
    let res = get_sql(&sql)?;
    info!("=> {:?}", res);
    Ok(
        if res.is_empty() {
            info!("{:?}", get_sql(&format!("INSERT INTO accounts VALUES ({}, {})", id, 1000.0))?);
            1000.0
        } else {
            res[0].get("balance".into()).ok_or("balance missing from accounts table")?.parse::<f64>().or(Err("can't parse balance field from accounts table"))?
        }
    )
}

////////////////////////////////////////////////////////////////////////////////

// Return set of tickers that need attention
pub fn extract_tickers (txt :&str) -> HashSet<String> {
    let mut tickers = HashSet::new();
    let re = Regex::new(r"^@?[A-Za-z0-9^.=_-]*[A-Za-z0-9^._]+$").unwrap(); // BRK.A ^GSPC BTC-USD don't end in - so a bad-$ trade doesn't trigger this
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

async fn get_quote_pretty (db :&DB, ticker :&str) -> Result<String, Serror> {
    // Dump Level 2 quotes as well
    let bidask =
        if &ticker[0..1] == "@" { 
            let mut asks = "*Asks*".to_string();
            for ask in get_sql(&format!("SELECT -qty AS qty, price FROM exchange WHERE qty<0 AND ticker='{}' order by price;", ticker))? {
                asks.push_str(&format!(" `{:.2}/{}`",
                    ask.get("price").unwrap().parse::<f64>()?,
                    num_simp(ask.get("qty").unwrap()),
                ));
            }
            let mut bids = "*Bids*".to_string();
            for bid in get_sql(&format!("SELECT qty, price FROM exchange WHERE 0<qty AND ticker='{}' order by price desc;", ticker))? {
                bids.push_str(&format!(" `{:.2}/{}`",
                    bid.get("price").unwrap().parse::<f64>()?,
                    num_simp(bid.get("qty").unwrap()),
                ));
            }
            format!("\n{}\n{}", asks, bids).to_string()
        } else {
            "".to_string()
        };
    let stonk = get_stonk(db, ticker).await?;

    Ok( if stonk.contains_key("updated") {
        format!("{}·{}", stonk.get("pretty").unwrap().replace("_", "\\_"), bidask)
    } else {
        format!("{}{}", &stonk.get("pretty").unwrap().replace("_", "\\_"), bidask)
    })
}

fn format_position (ticker:&str, qty:f64, cost:f64, price:f64) -> Result<String, Serror> {
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
    Ok(format!("\n`{:>7.2}``{:>8} {} {}``{} @{:.2}` *{}*_@{:.2}_",
        value,
        gain, gain_percent, greenred,
        ticker, price,
        qty, cost))
}

////////////////////////////////////////////////////////////////////////////////
async fn do_help (db :&DB, cmd:&Cmd) -> Result<&'static str, Serror> {

    if Regex::new(r"/help").unwrap().captures(&cmd.msg).is_none() {
        return Ok("SKIP")
    }

    send_msg_markdown(db, cmd.at, &format!(
"`/yolo  ` `The leaderboard`
`/stonks` `Your portfolio`
`gme$   ` `Quote ({}min delay)`
`gme-   ` `Sell entire position`
`gme+   ` `Buy with all cash`
`gme+2  ` `Buy 2 shares`
`gme+$9 ` `Buy $9 worth`", db.quote_delay)).await?;
    Ok("COMPLETED.")
}

async fn do_like (db :&DB, cmd:&Cmd) -> Result<String, Serror> {

    let amt :i32 = match cmd.msg.as_ref() { "+1" => 1, "-1" => -1, _=>0 };
    if amt == 0 { return Ok("SKIP".into()); }

    if cmd.from == cmd.to { return Ok( format!("do_like SKIP self plussed {}", cmd.from)); }

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

    info!("update likes in filesystem {:?} {:?} {:?}",
        cmd.to, likes,
        write( format!("tmbot/{}", cmd.to), likes.to_string()));

    let sfrom = cmd.from.to_string();
    let sto = cmd.to.to_string();
    let fromname = people.get(&cmd.from).unwrap_or(&sfrom);
    let toname   = people.get(&cmd.to).unwrap_or(&sto);
    let text = format!("{}{}{}", fromname, num2heart(likes), toname);
    send_msg(db, cmd.at, &text).await?;

    Ok("Ok do_like".into())
}

async fn do_like_info (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

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

async fn do_syn (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let cap = Regex::new(r"^([a-z]+);$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let word = &cap.unwrap()[1];

    info!("looking up {:?}", word);

    let mut defs = get_syns(word).await?;

    if 0 == defs.len() {
        send_msg_markdown(db, cmd.from, &format!("*{}* synonyms is empty", word)).await?;
        return Ok("do_syn empty synonyms");
    }

    let mut msg = String::new() + "*\"" + word + "\"* ";
    defs.truncate(10);
    msg.push_str( &defs.join(", ") );
    send_msg_markdown(db, cmd.at, &msg).await?;

    Ok("Ok do_syn")
}

async fn do_def (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let cap = Regex::new(r"^([a-z]+):$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let word = &cap.unwrap()[1];

    let defs = get_definition(word).await?;

    if defs.is_empty() {
        send_msg_markdown(db, cmd.from, &format!("*{}* def is empty", word)).await?;
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

async fn do_sql (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    if cmd.from != 308188500 {
        return Ok("do_sql invalid user");
    }

    let cap = Regex::new(r"^(.*)ß$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let expr = &cap.unwrap()[1];

    let result = get_sql(expr);
    if let Err(e) = result {
        send_msg_markdown(db, cmd.from, &format!("{:?}", e)).await?;
        return Err(e);
    }
    let results = result.unwrap();

    if results.is_empty() {
        send_msg(db, cmd.from, &format!("\"{}\" results is empty", expr)).await?;
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

/// TODO if a ticker fails, keep looking up remaining
async fn do_quotes (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let tickers = extract_tickers(&cmd.msg);
    if tickers.is_empty() { return Ok("SKIP") }

    for ticker in &tickers {
        match get_quote_pretty(db, ticker).await {
            Ok(res) => { send_msg_markdown(db, cmd.at, &res).await?; }
            e => { glogd!("get_quote_pretty => ", e); }
        }
    }

    Ok("COMPLETED.")
}

async fn do_portfolio (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    if Regex::new(r"STONKS[!?]|[!?/]STONKS").unwrap().find(&cmd.msg.to_uppercase()).is_none() { return Ok("SKIP"); }

    let cash = get_bank_balance(cmd.from)?;

    let positions = get_sql(&format!("SELECT * FROM positions WHERE id={}", cmd.from))?;
    warn!("=> {:?}", positions);

    let mut msg = String::new();
    let mut total = 0.0;

    for pos in positions {
        let ticker = pos.get("ticker").unwrap();
        let qty = pos.get("qty").unwrap().parse::<f64>().unwrap();
        let cost = pos.get("price").unwrap().parse::<f64>().unwrap();
        //let basis = qty * cost;
        let price = get_stonk(db, ticker).await?.get("price").unwrap().parse::<f64>().unwrap();
        let value = qty * price;
        //let gain = value-basis;
        total += value;
        msg.push_str(&format_position(ticker, qty, cost, price)?);
    }
    msg.push_str(&format!("\n`{:7.2}``Cash`    `YOLO``{:.2}`", cash, total+cash));
    send_msg_markdown(db, cmd.at, &msg).await?;

    Ok("COMPLETED.")
}

async fn do_yolo (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    // Handle: !yolo ?yolo yolo! yolo?
    if Regex::new(r"YOLO[!?]|[!?/]YOLO").unwrap().find(&cmd.msg.to_uppercase()).is_none() { return Ok("SKIP"); }

    let working_message = "working...".to_string();
    let message_id = send_msg(db, cmd.at, &working_message).await?;

    // Update all user-positioned tickers
    for row in get_sql("SELECT ticker FROM positions GROUP BY ticker")? {
        let ticker = row.get("ticker").unwrap();
        //working_message.push_str(ticker);
        //working_message.push_str("...");
        //send_edit_msg(db, cmd.at, message_id, &working_message).await?;
        info!("Stonk \x1b[33m{:?}", get_stonk(db, ticker).await?);
    }

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
    ORDER BY yolo DESC";
    let results = get_sql(&sql)?;
    warn!("SQLite => \x1b[33m{:?}\x1b[0m", results);

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

async fn do_trade_buy (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let cap = Regex::new(r"^([A-Za-z0-9^.-]+)\+(\$?)([0-9]+\.?|([0-9]*\.[0-9]{1,4}))?$").unwrap().captures(&cmd.msg);

    if cap.is_none() { return Ok("SKIP"); }
    let trade = &cap.unwrap();

    let ticker = trade[1].to_uppercase();
    let mut is_dollars = !trade[2].is_empty();

    let bank_balance = get_bank_balance(cmd.from)?;

    let qty_or_dollars =
        if trade.get(3).is_none() {
            is_dollars=true;
            bank_balance
        } else {
            trade[3].parse::<f64>()?
        };

    let stonk = get_stonk(db, &ticker).await?;
    let mut price = stonk.get("price").ok_or("price missing from stonk")?.parse::<f64>()?;

    // Convert dollars to shares maybe.  Round to 4 decimal places
    let mut qty = round(qty_or_dollars / if is_dollars { price } else { 1.0 }, 4);

    if qty <= 0.0 {
        send_msg(db, cmd.from, "You can't buy non-positive shares.").await?;
        return Ok("OK do_trade_buy non positive share count.");
    }

    let basis = qty * price;
    let new_balance = bank_balance - basis;

    if new_balance < 0.0 {
        send_msg(db, cmd.from, "You need more $$$ to YOLO like that.").await?;
        return Ok("OK do_trade_buy not enough cash");
    }

    info!("trading_buy {} qty_or_dollars:{}  shares:{} price:{} basis:{}  bank_balance:{}=>{}", ticker, qty_or_dollars, qty, price, basis, bank_balance, new_balance);

    sql_table_order_insert(cmd.from, &ticker, qty, price, Instant::now().seconds());

     // Maybe add to existing position

    let sql = format!("SELECT * FROM positions WHERE id={} AND ticker='{}'", cmd.from, ticker);
    let positions = get_sql(&sql)?;
    warn!("=> {:?}", positions);

    match positions.len() {
        0 => {
            let sql = format!("INSERT INTO positions VALUES ({}, '{}', {:.4}, {:.8})", cmd.from, ticker, qty, price);
            info!("Trade add position result {:?}", get_sql(&sql));
        },
        1 => {
            let qty_old = positions[0].get("qty").unwrap().parse::<f64>().unwrap();
            let price_old = positions[0].get("price").unwrap().parse::<f64>().unwrap();
            price = (qty*price + qty_old*price_old) / (qty+qty_old);
            qty += qty_old;
            info!("trading_buy adding to existing position:  new_qty:{}  new_price:{}", qty, price);
            let sql = format!("UPDATE positions SET qty={:.4}, price={:.8} WHERE id='{}' AND ticker='{}'", qty, price, cmd.from, ticker);
            info!("Trade update position result {:?}", get_sql(&sql));
        },
        _ => return Err(Serror::Message(format!("Ticker {} has {} positions, expect 0 or 1", ticker, positions.len())))
    };


    let sql = format!("UPDATE accounts set balance={:.2} where id={}", new_balance, cmd.from);
    info!("Update bank balance result {:?}", get_sql(&sql));

    // Send current portfolio
    let cmd2 = Cmd { from:cmd.from, at:cmd.at, to:cmd.to, msg:format!("STONKS?")};
    info!("via do_trade_buy {:?}", do_portfolio(db, &cmd2).await);

    Ok("COMPLETED.")
}

async fn do_trade_sell (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {
    let cap = Regex::new(r"^([A-Za-z^.-]+)([-])$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let trade = &cap.unwrap();

    let ticker = trade[1].to_uppercase();

    let sql = format!("SELECT * FROM positions WHERE id={} AND ticker='{}'", cmd.from, ticker);
    let positions = get_sql(&sql)?;
    warn!("=> {:?}", positions);

    if 1 != positions.len() { return Err(Serror::Message(format!("Ticker {} has {} positions", ticker, positions.len()))); }

    let qty = positions[0].get("qty").unwrap().parse::<f64>().unwrap();
    let cost = positions[0].get("price").unwrap().parse::<f64>().unwrap();
    let price = get_stonk(db, &ticker).await?.get("price").unwrap().parse::<f64>().unwrap();
    let value = qty * price;

    let bank_balance = get_bank_balance(cmd.from)?;
    let new_bank_balance = bank_balance + value;

    sql_table_order_insert(cmd.from, &ticker, -qty, price, Instant::now().seconds());

    let sql = format!("UPDATE accounts SET balance={:.2} WHERE id={}", new_bank_balance, cmd.from);
    info!("Update bank balance {} => {} SQLite => {:?}", bank_balance, new_bank_balance, get_sql(&sql));

    let sql = format!("DELETE FROM positions WHERE id={} AND ticker='{}'", cmd.from, ticker);
    info!("SQLite => {:?}", get_sql(&sql));

    // Send final gain realized
    let msg = format!("Closing:{}", &format_position(&ticker, qty, cost, price)?);
    send_msg_markdown(db, cmd.at, &msg).await?;

    // Send current portfolio
    //let cmd2 = Cmd { from:cmd.from, at:cmd.at, to:cmd.to, msg:format!("STONKS?")};
    //info!("do_trade_sell => do_portfolio => {:?}", do_portfolio(db, &cmd2).await);

    Ok("COMPLETED.")
}

//           qtys are positive         v--Best ASK price (next to sell)
//       [[ 0.01  0.30  0.50  0.99     1.00  1.55  2.00  9.00 ]]
//  Best BID price (next buyer)--^        qtys are negative
//          

// Create an ask quote (+price) on the exchange table, lower is better.
async fn do_exchange_bidask (_db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {
    //                       ____ticker_____  _____________qty__________________  $  ___________price______________
    let cap = Regex::new(r"^(@?[A-Za-z^.-_]+)([+-]([0-9]+[.]?|[0-9]*[.][0-9]{1,4}))[$]([0-9]+[.]?|[0-9]*[.][0-9]{1,2})$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("SKIP"); }
    let quote = &cap.unwrap();

    let ticker = &quote[1];
    let mut quote_qty = quote[2].parse::<f64>()?;
    let quote_price = quote[4].parse::<f64>()?;

    let now = Instant::now().seconds();

    if quote_qty < 0.0 { // A seller has appeared:  ask quote (-qty, price)

        // Check seller has the qty first
        let seller_position = get_sql(
            &format!("SELECT * FROM ( \
                        SELECT SUM(qty) as qty FROM ( \
                            SELECT qty FROM positions WHERE id={} AND ticker='{}' \
                        UNION ALL \
                            SELECT qty FROM exchange WHERE id={} AND ticker='{}' AND qty<0 ) ) \
                      WHERE {}<=qty",
            cmd.from, ticker, cmd.from, ticker, -quote_qty))?;
        if 1 != seller_position.len() { return Ok("Seller lacks the securities.") }

        // Any bidders willing to buy/pay?
        let bidder = get_sql(
            &format!("SELECT * FROM exchange WHERE ticker='{}' AND 0<qty AND {}<=price ORDER BY price DESC LIMIT 1",
                ticker, quote_price))?;
        warn!("bidder = {:?}", bidder);

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
                warn!("SQLite => {:?}",
                    get_sql(&format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}",
                        bidder_id, ticker, qty, price)));
            } else {
                // Update the market quote
                warn!("SQLite => {:?}",
                    get_sql(&format!("UPDATE exchange SET qty={} WHERE id={} AND ticker='{}' AND price={}",
                        qty-best_qty, bidder_id, ticker, price)));
            }

            // Create the new orders

            if cmd.from != bidder_id {
                sql_table_order_insert(cmd.from, &ticker, -best_qty, best_price, now);
                sql_table_order_insert(
                    bidder_id,
                    &ticker, best_qty, best_price, now);
            }

            quote_qty = best_qty - quote_qty; // Adjust quote_qty and continue to creating a possible market quote
        }

        // Update exchange: qty -= amt  OR  remove if qty == amt
        // Positions
        //   Seller: qty-=amt, price = new basis using amt*price  OR  remove if qty == amt
        //   Buyer:  qty+=amt, price = new basis using amt*price  OR create
        // Update stonks with best_price

    } else { // A buyer has appeared:  bid quote (+price)
        let asker = get_sql(
            &format!("SELECT * FROM exchange WHERE ticker='{}' AND qty<0 AND price<={} ORDER BY price LIMIT 1",
                ticker, quote_price))?;
        warn!("asker => {:?}", asker);

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
                warn!("SQLite => {:?}",
                    get_sql(&format!("DELETE FROM exchange WHERE id={} AND ticker='{}' AND qty={} AND price={}",
                        asker_id, ticker, -qty, price)));
            } else {
                // Update the market quote
                warn!("SQLite => {:?}",
                    get_sql(&format!("UPDATE exchange SET qty={} WHERE id={} AND ticker='{}' AND price={}",
                        best_qty-qty, asker_id, ticker, price)));
            }

            // Create the new orders

            if cmd.from != asker_id {
                sql_table_order_insert(
                    asker_id,
                    &ticker, -best_qty, best_price, now);
                sql_table_order_insert(cmd.from, &ticker, best_qty, best_price, now);
            }

            quote_qty = quote_qty - best_qty; // Adjust quote_qty and continue to creating a possible market quote
        }
    }

    // Add quote to exchange:  This could either create a new quote or update an existing one with the id, ticker, price matches

    if 0.0 == quote_qty { return Ok("COMPLETED.") }
    
    let quote =
        if quote_qty < 0.0 {
            get_sql(&format!(
                "SELECT * FROM exchange WHERE id={} AND ticker='{}' AND qty<0 AND price={}",
                cmd.from, ticker, quote_price))
        } else {
            get_sql(&format!(
                "SELECT * FROM exchange WHERE id={} AND ticker='{}' AND 0<qty AND price={}",
                cmd.from, ticker, quote_price))
        }?;
    
    if quote.is_empty() {
        info!("SQLite => {:?}", get_sql(&format!(
            "INSERT INTO exchange VALUES ({}, '{}', {:.4}, {:.4}, {})",
            cmd.from, ticker, quote_qty, quote_price, now))?)
    } else {
        info!("SQLite => {:?}",  get_sql(&format!(
            "UPDATE exchange set qty={}, time={}  WHERE id={} AND ticker='{}' AND price={}",
            quote_qty + quote[0].get("qty").unwrap().parse::<f64>()?, now,
            cmd.from, ticker, quote_price))?)
    }

    Ok("COMPLETED.")
}

////////////////////////////////////////////////////////////////////////////////

fn do_schema() -> Result<(), Serror> {
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


async fn do_all(db: &DB, body: &web::Bytes) -> Result<(), Serror> {
    let cmd = parse_cmd(&body)?;
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
    info!("\x1b[0;33;40m|_   _|  \\/  | __ )  ___ | |_™");
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

pub async fn mainstart() -> Result<(), Serror> {
    let mut ssl_acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ssl_acceptor_builder .set_private_key_file("key.pem", SslFiletype::PEM)?;
    ssl_acceptor_builder.set_certificate_chain_file("cert.pem")?;

    let botkey          = args().nth(1).unwrap();
    let chat_id_default = args().nth(2).unwrap().parse::<i64>()?;

    if !true { do_schema()?; }

    Ok(HttpServer::new( move || App::new()
        .data( Mutex::new( DB {
            url_bot: String::from("https://api.telegram.org/bot") + &botkey,
            chat_id_default: chat_id_default,
            quote_delay: QUOTE_DELAY
        } ) )
        .service( web::resource("*")
                    .route( Route::new().to(dispatch) ) ) )
    .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
    .workers(1)
    .run().await?)
}

/*
DROP   TABLE users
CREATE TABLE users (name TEXT, age INTEGER)
INSERT INTO  users VALUES ('Alice', 42)
DELETE FROM  users WHERE age=42 AND name="Oskafunoioi"
UPDATE       users SET a=1, b=2  WHERE c=3
*/