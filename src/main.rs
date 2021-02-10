use std::{
    time::{Duration},
    collections::{HashMap, HashSet},
    str::{from_utf8, Utf8Error},
    fs::{read_to_string, write},
    env::{args},
    sync::{Mutex},
};
use log::*;
use actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use actix_web::client::{Client, Connector};
use openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use regex::{Regex};
use json::{JsonValue};

////////////////////////////////////////////////////////////////////////////////

/*
use ::serde::{Serialize, Deserialize};
use ::serde_json::{Value, from_str, to_string_pretty};
const  OFF :&str = "\x1b[0m";
const  MAG :&str = "\x1b[36m";
const  GRN :&str = "\x1b[32m";
const BGRN :&str = "\x1b[1;32m";
*/
fn ginfo<T: std::fmt::Debug>(e: T) { info!("{:?}", e); }
fn gerror<T: std::fmt::Debug>(e: T) { error!("{:?}", e); }

fn ginfod<T: std::fmt::Debug>(h:&str, e: T) { info!("{} {}", h, format!("{:?}", e).replace("\n","").replace("\\\\", "\\").replace("\\\"", "\"")); }
fn gerrord<T: std::fmt::Debug>(h:&str, e: T) { error!("{} {}", h, format!("{:?}", e).replace("\n","").replace("\\\\", "\\").replace("\\\"", "\"")); }

fn glogd<
    R: std::fmt::Debug,
    T: std::fmt::Debug
> (
    h:&str,
    e: Result<R, T>
) {
    match e {
        Ok(r) => ginfod(h, r),
       Err(r) => gerrord(h, r)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum Serror {
   StdIoError(std::io::Error),
   Error(json::Error),
   Utf8Error(Utf8Error),
   SetLoggerError(log::SetLoggerError),
   SslErrorStack(openssl::error::ErrorStack),
   Msg(&'static str),
   Err(&'static str)
}

impl From<std::io::Error> for Serror {
    fn from(e: std::io::Error) -> Self { Serror::StdIoError(e) }
}
impl From<Utf8Error> for Serror {
    fn from(e: Utf8Error) -> Self { Serror::Utf8Error(e) }
}
impl From<json::Error> for Serror {
    fn from(e: json::Error) -> Self { Serror::Error(e) }
}
impl From<&'static str> for Serror {
    fn from(s: &'static str) -> Self { Serror::Msg(s) }
}
impl From<openssl::error::ErrorStack> for Serror {
    fn from(e: openssl::error::ErrorStack) -> Self { Serror::SslErrorStack(e) }
}
impl From<log::SetLoggerError> for Serror {
    fn from(e: log::SetLoggerError) -> Self { Serror::SetLoggerError(e) }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
struct DB {
    url_bot: String,
    chat_id_default: i64
}

type MDB = Mutex<DB>;

////////////////////////////////////////////////////////////////////////////////

async fn sendmsg (db :&mut DB, chat_id :i64, text: &str) {
    info!("\x1b[33m<- {}", text);
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file("cert.pem").unwrap();

    let response =
        Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(10,0))
                    .finish() )
        .finish() // -> Client
        .get( db.url_bot.clone() + "/sendmessage")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["text", text],
                 //"&parse_mode=HTML" +
                 ["disable_notification", "true"]]).unwrap()
        .send()
        .await;

    match response {
        Err(e) => error!("\x1b[31m-> {:?}", e),
        Ok(mut r) => {
            ginfod("\x1b[32m->", &r); 
            ginfod("\x1b[1;32m->", r.body().await);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

async fn get_ticker_quote (ticker: &str) -> Option<(String, String)> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls()).unwrap().build() )
                    .timeout( Duration::new(10,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://finance.yahoo.com/quote/".to_string() + ticker + "/")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .send()
        .await.unwrap()
        .body().limit(1_000_000).await;

    if body.is_err() {
         error!(r#"http body {:?} for {:?}"#, body, ticker);
         return None;
    }

    let body = body.unwrap();
    let domstr = from_utf8(&body);
    if domstr.is_err() {
         error!(r#"http body2str {:?} for {:?}"#, domstr, ticker);
         return None;
    }

    let re = Regex::new(r#"<title>([^(<]+)"#).unwrap();
    let title =
        match re.captures(domstr.unwrap()) {
            Some(cap) => if 2==cap.len() { cap[1].to_string() } else { "stonk".to_string()  }
            _ => "sonk".to_string()
        };

    let re = Regex::new(r#"data-reactid="[0-9]+">([0-9,]+\.[0-9]+)"#).unwrap();
    let caps = re
        .captures_iter(domstr.unwrap())
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>();

    info!(r#"http dom prices {:?} {:?}"#, ticker, caps);

    if caps.len() < 4 {
         error!(r#"http dom regex matched too few prices"#);
         return None;
    }

    let price = caps[3].to_string();

    let re = Regex::new(r#"data-reactid="[0-9]+">([-+][0-9]+\.[0-9]+) \(([-+][0-9]+\.[0-9]+%)\)<"#).unwrap();
    let caps_percentages = re
        .captures_iter(domstr.unwrap())
        .map( |cap| {
            let amt = &cap[1];
            let per = &cap[2];
            ( // delta as a colored emoji
                if amt.chars().next().unwrap() == '+' { from_utf8(b"\xF0\x9F\x9F\xA2").unwrap() } else { from_utf8(b"\xF0\x9F\x9F\xA5").unwrap() }
                .to_string()
            , // the delta string
                if amt.chars().next().unwrap() == '+' { from_utf8(b"\xE2\x86\x91").unwrap() } else { from_utf8(b"\xE2\x86\x93").unwrap() }
                .to_string()
                + &cap[1][1..]
                + " "
                + &per[1..]
            )
        } )
        .collect::<Vec<(String, String)>>();
    info!(r#"http dom percentages {:?} {:?}"#, ticker, caps_percentages);

    if caps_percentages.is_empty() {
        return Some( ("".to_string(), price + &title) );
    } else {
        let percentage = &caps_percentages[0];
        return Some( (percentage.0.to_string(), price + " " + &percentage.1 + " " + &title) );
    }
}

fn text_parse_for_tickers (txt :&str) -> HashSet<String> {
    let mut tickers = HashSet::new();
    let re = Regex::new(r"[^A-Za-z^.-]").unwrap();
    for s in txt.split(" ") {
        let w = s.split("$").collect::<Vec<&str>>();
        if 2 == w.len() {
            let mut idx = 42;
            if w[0]!=""  &&  w[1]=="" { idx = 0; } // "$" is to the right of ticker symbol
            if w[0]==""  &&  w[1]!="" { idx = 1; } // "$" is to the left of ticker symbol
            if 42!=idx && re.captures(w[idx]).is_none() { // ticker characters only
               tickers.insert(w[idx].to_string());
            }
        }
    }
    tickers
}

////////////////////////////////////////////////////////////////////////////////

fn bytes2json(body: &web::Bytes) -> Result<JsonValue, Serror> {
    let json = json::parse( from_utf8(&body)? )?;
    info!("json = \x1b[1;35m{}\x1b[0m", json);
    Ok(json)
}

fn json_getin_i64 (json :&JsonValue, keys :&[&str]) -> Option<i64> {
    let mut j = json;
    for k in keys { j = &j[*k] }
    j.as_i64()
}
fn json_getin_str <'t> (json :&'t JsonValue, keys :&[&str]) -> Option<&'t str> {
    let mut j = json;
    for k in keys { j = &j[*k] }
    j.as_str()
}

fn json_message_chat_id (db :&DB, json :&JsonValue) -> i64 {
    json_getin_i64(json, &["message", "chat", "id"])
    .unwrap_or(db.chat_id_default)
}

////////////////////////////////////////////////////////////////////////////////

async fn do_ticker (db :&mut DB, json :&JsonValue) -> Result<&'static str, Serror> {

    // It's either a @bot query or text message
    let txt =
        json_getin_str(json, &["message", "text"])
        .or_else( || json_getin_str(json, &["inline_query", "query"]))
        .ok_or(Serror::Err("ticker text/query field not found"))?;

    // Who gets response?
    let chat_id :i64 =
        json_getin_i64(json, &["inline_query", "from", "id"])
        .unwrap_or_else( || json_message_chat_id(db, json));

    let tickers = text_parse_for_tickers(&txt);

    if tickers.is_empty() {
        return Ok("do_ticker SKIP no tickers");
    }

    info!("tickers {:?}", tickers);

    for ticker in tickers {
        if let Some(price) = get_ticker_quote(&ticker).await {
            let quote = price.0 + &ticker + "@" + &price.1;
            sendmsg(db, chat_id, &quote).await;
        }
    }

    Ok("Ok do_ticker")
}

async fn do_plussy_all (db :&mut DB, json :&JsonValue) -> Result<&'static str, Serror> {

    let textfield = json_getin_str(json, &["message", "text"]);
    if textfield.is_none() || textfield.unwrap() != "+?" {
        return Ok("do_plussy_all SKIP");
    }

    let chat_id = json_message_chat_id(db, json);

    let mut likes = Vec::new();
    // Over each user in file
    for l in read_to_string("telegram/users.txt").unwrap().lines() {
        let v = l.split(" ").collect::<Vec<&str>>();
        let id = v[0];
        let nom = v[1].to_string();

        // Read the count file
        let count = read_to_string( "telegram/".to_string() + &id )
            .unwrap_or("0".to_string()).trim().parse::<i32>().unwrap();

        likes.push((count, nom));
    }

    let mut text = String::new();
    likes.sort_by(|a,b| b.0.cmp(&a.0) );
    // %3c %2f b %3e
    for (likes,nom) in likes {
        text.push_str(&format!(" {}{}", nom, num2heart(likes)));
    }
    //info!("HEARTS -> msg telegram {:?}", sendmsg(db, chat_id, &(-6..=14).map( |n| num2heart(n) ).collect::<Vec<&str>>().join("")).await);
    sendmsg(db, chat_id, &text[1..]).await;
    Ok("Ok do_plussy_all")
}

fn num2heart (n :i32) -> &'static str {
    if n < -4 { return from_utf8(b"\xCE\xBB").unwrap(); } // lambda
    if n <= -1{ return from_utf8(b"\xF0\x9F\x96\xA4").unwrap(); } // black heart
    if n == 0 { return from_utf8(b"\xF0\x9F\x92\x94").unwrap(); } // red broken heart
    if n == 1 { return from_utf8(b"\xE2\x9D\xA4\xEF\xB8\x8F").unwrap(); } // red heart
    if n == 2 { return from_utf8(b"\xF0\x9F\xA7\xA1").unwrap(); } // orange heart
    if n == 3 { return from_utf8(b"\xF0\x9F\x92\x9B").unwrap(); } // yellow heart
    if n == 4 { return from_utf8(b"\xF0\x9F\x92\x9A").unwrap(); } // green heart
    if n == 5 { return from_utf8(b"\xF0\x9F\x92\x99").unwrap(); } // blue heart
    if n == 6 { return from_utf8(b"\xF0\x9F\x92\x9C").unwrap(); } // violet heart
    if n == 7 { return from_utf8(b"\xF0\x9F\x92\x97").unwrap(); } // pink growing heart
    if n == 8 { return from_utf8(b"\xF0\x9F\x92\x96").unwrap(); } // pink sparkling heart
    if n == 9 { return from_utf8(b"\xF0\x9F\x92\x93").unwrap(); } // beating heart
    if n == 10 { return from_utf8(b"\xF0\x9F\x92\x98").unwrap(); } // heart with arrow
    if n == 11 { return from_utf8(b"\xF0\x9F\x92\x9D").unwrap(); } // heart with ribbon
    if n == 12 { return from_utf8(b"\xF0\x9F\x92\x9E").unwrap(); } // revolving hearts
    return from_utf8(b"\xF0\x9F\x8D\x86").unwrap(); // egg plant
}

async fn do_plussy (db :&mut DB, json :&JsonValue) -> Result<String, Serror> {

    let textfield = json_getin_str(json, &["message", "text"]).unwrap_or("");
    let amt :i32 = match textfield { "+1"=>1, "-1"=>-1, _=>0 };

    if amt == 0 { return Ok("do_plussy SKIP".into()); }

    let chat_id = json_message_chat_id(db, json);
    let from = json_getin_i64(json, &["message", "from", "id"]).ok_or("wat")?;
    let to = json_getin_i64(json, &["message", "reply_to_message", "from", "id"]).ok_or("oh no")?;

    if from == to { return Ok( format!("do_plussy SKIP self plussed {}", from)); }


    // Load database of peoplekkkkkkk

    let mut people :HashMap<String, String> = HashMap::new();
    for l in read_to_string("telegram/users.txt").unwrap().lines() {
        let v = l.split(" ").collect::<Vec<&str>>();
        people.insert(v[0].to_string(), v[1].to_string());
    }
    info!("{:?}", people);

    let from = from.to_string();
    let to = to.to_string();

    let froms = people.get(&from).unwrap_or(&from);
    let tos   = people.get(&to).unwrap_or(&to);

    // Load/update/save likes

    let tlikes = read_to_string( "telegram/".to_string() + &to )
        .unwrap_or("0\n".to_string())
        .lines()
        .nth(0).unwrap()
        .parse::<i32>()
        .unwrap() + amt;

    info!("update likes in filesystem {:?} {:?} {:?}",
        to, tlikes,
        write("telegram/".to_string() + &to, tlikes.to_string()));

    let text = format!("{}{}{}", froms, num2heart(tlikes), tos);
    sendmsg(db, chat_id, &text).await;

    Ok("Ok do_plussy".into())
}


async fn do_all(mdb: &web::Data<MDB>, body: &web::Bytes) -> Result<(), Serror> {
    let mut db = mdb.lock().unwrap();

    let json = bytes2json(&body)?;
    glogd("do_ticker", do_ticker(&mut db, &json).await);
    glogd("do_plussy", do_plussy(&mut db, &json).await);
    glogd("do_plussy_all", do_plussy_all(&mut db, &json).await);
    Ok(())
}

async fn handle_silently(mdb: web::Data<MDB>, req: HttpRequest, body: web::Bytes) -> HttpResponse {
    ginfod("\x1b[35mdb:", &mdb.lock().unwrap());
    ginfod("\x1b[35mreq:", format!("{:?}", &req).replace("\n", "").replace("  ", " "));
    do_all(&mdb, &body)
    .await
    .map_or_else(
        |r| {
            gerrord("\x1b[31mbody:", &body);
            gerror(r)
        },
        |r| ginfo(r)
    );
    HttpResponse::from("")
}

#[actix_web::main]
async fn main() -> std::io::Result<()>{
    let mut ssl_acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ssl_acceptor_builder .set_private_key_file("key.pem", SslFiletype::PEM)?;
    ssl_acceptor_builder.set_certificate_chain_file("cert.pem")?;

    let botkey = args().nth(1).unwrap();
    let chat_id_default = args().nth(2).unwrap().parse::<i64>().unwrap();

    let srv =
    HttpServer::new( move || App::new()
        .data( Mutex::new( DB{
                url_bot: String::from("https://api.telegram.org/bot") + &botkey,
                chat_id_default: chat_id_default
            } ) )
        .service( web::resource("*")
                    .route( Route::new().to(handle_silently) ) ) )
    .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
    .workers(1)
    .run();

    ::pretty_env_logger::try_init().unwrap();
    info!("{}:{} ::{}::async-main()", std::file!(), core::line!(), core::module_path!());
    //info!("{:?}", args().collect::<Vec<String>>());

    srv.await
}