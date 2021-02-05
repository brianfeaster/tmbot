use log::*;
use std::time::{Duration};
use std::collections::{HashSet};
use std::str::{from_utf8, Utf8Error};
use actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use actix_web::client::{Client, Connector};
use openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use regex::{Regex};
use json::{JsonValue};


//use ::serde::{Serialize, Deserialize};
//use ::serde_json::{Value, from_str, to_string_pretty};

#[derive(Debug)]
enum Serror {
   Error(json::Error),
   Utf8Error(Utf8Error)
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

async fn sendmsg(botkey :&str, tickers: &String) {
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
                "/sendmessage?chat_id=-1001082930701&text=" + tickers +
                "&disable_notification=true" )
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .send()
        .await;

    info!("client {:?}", response);
}

async fn getticker(ticker: &str) -> Option<String> {

    let body =
        Client::builder()
        .connector(
            Connector::new()
            .ssl( SslConnector::builder(SslMethod::tls())
                .unwrap()
                .build() )
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

    let re = Regex::new(r#"data-reactid="[0-9]+">([0-9,]+\.[0-9]+)"#).unwrap();
    let caps = re
        .captures_iter(domstr.unwrap())
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>();

    if 0 == caps.len() {
         error!(r#"http dom regex matched nothing for {:?}"#, ticker);
         return None;
    }

    info!(r#"http dom possible prices for {:?} {:?}"#, ticker, caps);

    if caps.len() < 4 {
         error!(r#"http dom regex matched too few prices"#);
         return None;
    }
    let price = caps[3].to_string();

    return Some(price);
}

/// Incomming POST handler that extracts the ".message.text" field from JSON
fn body2json(body: &web::Bytes) -> Result<JsonValue, Serror> {
    let json = json::parse( from_utf8(&body)? )?;
    info!("json = \x1b[1;35m{}\x1b[0m", json);
    Ok(json)
}

fn text_field(json: &JsonValue) -> Result<String, Serror> {
    let textfield = &json["message"]["text"]; // might return null

    info!("\x1b[35m.message.text = {}\x1b[0m", textfield);

    Ok( textfield.as_str()
        .ok_or( json::Error::WrongType( "not a string".to_string() ) )?
        .to_string()
    )
}

async fn do_ticker_things (botkey :&String, txt :&String) {
    let re = Regex::new(r"[^A-Za-z^.]").unwrap();
    let mut tickers = HashSet::new();
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

    if 0 == tickers.len() { return }

    info!("set of tickers {:?}", tickers);
    for ticker in tickers {
        match getticker(&ticker).await {
            Some(price) => {
                let tickerstr = String::new() + &ticker + "@" + &price;
                //info!("{:?} -> msg telegram", tickerstr);
                info!("{:?} -> msg telegram {:?}", tickerstr, sendmsg(botkey, &tickerstr).await);
            }, _ => ()
        }
    }
}

async fn do_all(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    info!("args {:?}", std::env::args());
    let botkey = &std::env::args().nth(1).unwrap();
    let json = match body2json(&body) {
        Ok(json) => json,
        Err(e) => {
            error!("{:?}\x1b[1;36m{:?}\x1b[0;36m{:?}\x1b[0m", e, req, body);
            return HttpResponse::from("");
        }
    };

    let txt = text_field(&json);
    if txt.is_err() {
        error!("{:?}\x1b[1;36m{:?}\x1b[0;36m{:?}\x1b[0m", txt, req, body);
        return HttpResponse::from("");
    }

    let txt = txt.unwrap(); // Consider message string

    do_ticker_things(botkey, &txt).await;

    HttpResponse::from("")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("\x1b[35m{:?} {:?}\x1b[0m", ::pretty_env_logger::try_init(), std::env::args());
    info!("{}:{} ::{}::async-main()", std::file!(), core::line!(), core::module_path!());
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    builder .set_private_key_file("key.pem", SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;
    HttpServer::new( || App::new()
            .service(
                web::resource("*")
                .route( Route::new().to(do_all) ) ) )
    .bind_openssl("0.0.0.0:8443", builder)?
    .run()
    .await
}