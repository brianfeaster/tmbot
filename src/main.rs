use log::*;
use std::time::{Duration};
use std::collections::{HashMap, HashSet};
use std::str::{from_utf8, Utf8Error};
use actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use actix_web::client::{Client, Connector};
use openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use regex::{Regex};
use json::{JsonValue};
use std::fs::{read_to_string, write};

/*
const  OFF :&str = "\x1b[0m";
const  MAG :&str = "\x1b[36m";
const  GRN :&str = "\x1b[32m";
const BGRN :&str = "\x1b[1;32m";
*/

//use ::serde::{Serialize, Deserialize};
//use ::serde_json::{Value, from_str, to_string_pretty};

#[derive(Debug)]
enum Serror {
   StdIoError(std::io::Error),
   Error(json::Error),
   Utf8Error(Utf8Error),
   SetLoggerError(log::SetLoggerError),
   SslErrorStack(openssl::error::ErrorStack)
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
impl From<&str> for Serror {
    fn from(s: &str) -> Self { Serror::Error(json::Error::WrongType(s.to_string())) }
}
impl From<openssl::error::ErrorStack> for Serror {
    fn from(e: openssl::error::ErrorStack) -> Self { Serror::SslErrorStack(e) }
}
impl From<log::SetLoggerError> for Serror {
    fn from(e: log::SetLoggerError) -> Self { Serror::SetLoggerError(e) }
}

async fn sendmsg (botkey :&str, chat_id: &str, text: &String) {
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
        .get( String::new() +
                "https://api.telegram.org/bot" + botkey +
                "/sendmessage" +
                "?chat_id=" + chat_id +
                "&text=" + text +
                "&disable_notification=true" )
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .send()
        .await;

    if response.is_err() {
        error!("telegram sendmsg response {:?}", response);
    }
    //error!("telegram sendmsg response body {:?}", response.unwrap().body().limit(1_000_000).await.unwrap());
}

async fn get_ticker_quote(ticker: &str) -> Option<String> {
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
    let titlere = re.captures(domstr.unwrap()).unwrap();
    info!("title = {:?}", titlere);
    let title = if 2 == titlere.len() {
        &titlere[1]
    } else {
        "stonk"
    };

    let re = Regex::new(r#"data-reactid="[0-9]+">([0-9,]+\.[0-9]+)"#).unwrap();
    let caps = re
        .captures_iter(domstr.unwrap())
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>();

    if caps.is_empty() {
         error!(r#"http dom prices regex empty for {:?}"#, ticker);
         return None;
    }

    info!(r#"http dom possible prices for {:?} {:?}"#, ticker, caps);

    if caps.len() < 4 {
         error!(r#"http dom regex matched too few prices"#);
         return None;
    }
    let price = caps[3].to_string();

    return Some( str::replace( &(price+" "+title), " ", "+") );
}

/// Incomming POST handler that extracts the ".message.text" field from JSON
fn body2json(body: &web::Bytes) -> core::result::Result<JsonValue, Serror> {
    let json = json::parse( from_utf8(&body)? )?;
    info!("json = \x1b[1;35m{}\x1b[0m", json);
    Ok(json)
}

fn parse_tickers (txt :&str) -> HashSet<String> {
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

async fn do_ticker (botkey :&String, chat_id :&str, json :&JsonValue) {

    let txt = &json["message"]["text"].as_str(); // might return null
    if txt.is_none() { error!("ticker string .message.text = {:?}", txt); return; }

    let tickers = parse_tickers(&txt.unwrap());
    info!("tickers {:?}", tickers);
    if tickers.is_empty() { return }

    for ticker in tickers {
        match get_ticker_quote(&ticker).await {
            Some(price) => {
                let quote = String::new() + &ticker + "@" + &price;
                info!("{:?} -> msg telegram {:?}", quote, sendmsg(botkey, chat_id, &quote).await);
            }, _ => ()
        }
    }
}

async fn do_plussy_all (botkey :&String, chat_id :&str, json :&JsonValue) {
    let textfield = &json["message"]["text"];
    if textfield != "+?" {
        error!("plussy_all  txt {:?}", textfield);
        return;
    }

    for l in read_to_string("telegram/users.txt").unwrap().lines() {
        let v = l.split(" ").collect::<Vec<&str>>();
        let id = v[0];
        let nom = v[1];

        let likes = read_to_string( "telegram/".to_string() + &id )
            .unwrap_or("0".to_string());

        let text = format!("{}({})", nom, likes);
        info!("{} {} -> msg telegram {:?}", id, text, sendmsg(botkey, chat_id, &text).await);
    }
}

async fn do_plussy (botkey :&String, chat_id :&str, json :&JsonValue) {
    let textfield = &json["message"]["text"];
    let from = &json["message"]["from"]["id"].as_i64();
    let to = &json["message"]["reply_to_message"]["from"]["id"].as_i64();

    if textfield != "+1" || from.is_none() || to.is_none() {
        error!("plussy  txt {:?}  from {:?}  to {:?}", textfield, from, to);
        return;
    }

    // Load database of people

    let mut people :HashMap<String, String> = HashMap::new();
    for l in read_to_string("telegram/users.txt").unwrap().lines() {
        let v = l.split(" ").collect::<Vec<&str>>();
        people.insert(v[0].to_string(), v[1].to_string());
    }
    info!("{:?}", people);

    let from = from.unwrap().to_string();
    let to = to.unwrap().to_string();

    let froms = people.get(&from).unwrap_or(&from);
    let tos   = people.get(&to).unwrap_or(&to);

    // Load/update/save likes

    let flikes = read_to_string( "telegram/".to_string() + &from )
        .unwrap_or("0".to_string())
        .parse::<i64>()
        .unwrap();

    let tlikes = read_to_string( "telegram/".to_string() + &to )
        .unwrap_or("0\n".to_string())
        .lines()
        .nth(0).unwrap()
        .parse::<i64>()
        .unwrap() + 1;

    info!("update likes in filesystem {:?} {:?} {:?}",
        to, tlikes,
        write("telegram/".to_string() + &to, tlikes.to_string()));

    let text = format!("{}({})+liked+{}({})", froms, flikes, tos, tlikes);
    info!("{:?} -> msg telegram {:?}", text, sendmsg(botkey, chat_id, &text).await);
}

async fn do_all(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    info!("args {:?}", std::env::args());
    let botkey = &std::env::args().nth(1).unwrap();
    let chat_id = &std::env::args().nth(2).unwrap();
    let json = match body2json(&body) {
        Ok(json) => json,
        Err(e) => {
            error!("{:?}\x1b[1;36m{:?}\x1b[0;36m{:?}\x1b[0m", e, req, body);
            return HttpResponse::from("");
        }
    };

    do_ticker(botkey, chat_id, &json).await;
    do_plussy(botkey, chat_id, &json).await;
    do_plussy_all(botkey, chat_id, &json).await;

    HttpResponse::from("")
}

fn ginfo<T: std::fmt::Debug>(e: T) { info!("{:?}", e); }
fn gerror<T: std::fmt::Debug>(e: T) { error!("{:?}", e); }

#[actix_web::main]
async fn main() -> std::io::Result<()>{
    ::pretty_env_logger::try_init();

    println!("{:?}", std::env::args());
    info!("{}:{} ::{}::async-main()", std::file!(), core::line!(), core::module_path!());

    let mut ssl_acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ssl_acceptor_builder .set_private_key_file("key.pem", SslFiletype::PEM)?;
    ssl_acceptor_builder.set_certificate_chain_file("cert.pem")?;

    HttpServer::new( || App::new()
            .service(
                web::resource("*")
                .route( Route::new().to(do_all) ) ) )
    .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
    .run()
    .await
}