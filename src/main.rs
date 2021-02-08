use log::*;
use std::time::{Duration};
use std::collections::{HashMap, HashSet};
use std::str::{from_utf8, Utf8Error};
use std::env::{args, Args};
use actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use actix_web::client::{Client, Connector};
use openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use regex::{Regex};
use json::{JsonValue};
use std::fs::{read_to_string, write};

/*
use ::serde::{Serialize, Deserialize};
use ::serde_json::{Value, from_str, to_string_pretty};
const  OFF :&str = "\x1b[0m";
const  MAG :&str = "\x1b[36m";
const  GRN :&str = "\x1b[32m";
const BGRN :&str = "\x1b[1;32m";
fn ginfo<T: std::fmt::Debug>(e: T) { info!("{:?}", e); }
fn gerror<T: std::fmt::Debug>(e: T) { error!("{:?}", e); }
*/

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

async fn sendmsg (botkey :&str, chat_id: &str, text: &str) {
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
                //"&parse_mode=HTML" +
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

    return Some( str::replace( &(price+" "+&title), " ", "+") );
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


fn json_message_chat_id (json :&JsonValue, default_chat_id :&str) -> String {
    match json["message"]["chat"]["id"].as_number() {
        Some(num) => {
            let ns = num.to_string();
            info!("ticker chat_id = {:?}", ns);
            ns
        },
        _ => default_chat_id.to_string()
    }
}

async fn do_ticker (botkey :&String, chat_id_default :&str, json :&JsonValue) {

    // Who gets response?
    let chat_id = json_message_chat_id(json, chat_id_default);

    let mut txt = &json["message"]["text"].as_str(); // might return null
    let qry = &json["inline_query"]["query"].as_str(); // might return null
    if txt.is_none() && qry.is_none() {
        error!("ticker string .message.text = {:?}", txt);
        return;
    }
    if txt.is_none() { txt = qry; }

    let tickers = parse_tickers(&txt.unwrap());
    info!("tickers {:?}", tickers);
    if tickers.is_empty() { return }

    for ticker in tickers {
        match get_ticker_quote(&ticker).await {
            Some(price) => {
                let quote = String::new() + &ticker + "@" + &price;
                info!("{:?} -> msg telegram {:?}", quote, sendmsg(botkey, &chat_id, &quote).await);
            }, _ => ()
        }
    }
}

async fn do_plussy_all (botkey :&String, chat_id_default :&str, json :&JsonValue) {
    let chat_id = json_message_chat_id(json, chat_id_default);
    let textfield = &json["message"]["text"];
    if textfield != "+?" {
        error!("plussy_all  txt {:?}", textfield);
        return;
    }

    let mut likes = Vec::new();
    // Over each user in file
    for l in read_to_string("telegram/users.txt").unwrap().lines() {
        let v = l.split(" ").collect::<Vec<&str>>();
        let id = v[0];
        let nom = v[1].to_string();

        // Read the count file
        let count = read_to_string( "telegram/".to_string() + &id )
            .unwrap_or("0".to_string()).parse::<i32>().unwrap();

        likes.push((count, nom));
    }


    let mut text = String::new();
    likes.sort_by(|a,b| b.0.cmp(&a.0) );
    // %3c %2f b %3e
    for (likes,nom) in likes {
        text.push_str(&format!("+{}{}", nom, num2heart(likes)));
    }
    //info!("HEARTS -> msg telegram {:?}", sendmsg(botkey, &chat_id, &(-11..=14).map( |n| num2heart(n) ).collect::<Vec<&str>>().join("")).await);
    info!("{:?} -> msg telegram {:?}", text, sendmsg(botkey, &chat_id, &text[1..]).await);
}

fn num2heart (n :i32) -> &'static str {
    if n < -4  { return "%CE%BB"; } // lambda
    if n < 0  { return "%F0%9F%96%A4"; } // black heart
    if n == 0 { return "%F0%9F%92%94"; } // red broken heart
    if n == 1 { return "%E2%9D%A4%EF%B8%8F"; } // red heart
    if n == 2 { return "%F0%9F%A7%A1"; } // orange heart
    if n == 3 { return "%F0%9F%92%9B"; } // yellow heart
    if n == 4 { return "%F0%9F%92%9A"; } // green heart
    if n == 5 { return "%F0%9F%92%99"; } // blue heart
    if n == 6 { return "%F0%9F%92%9C"; } // violet heart
    if n == 7 { return "%F0%9F%92%97"; } // pink growing heart
    if n == 8 { return "%F0%9F%92%96"; } // pink sparkling heart
    if n == 9 { return "%F0%9F%92%93"; } // beating heart
    if n == 10 { return "%F0%9F%92%98"; } // heart with arrow
    if n == 11 { return "%F0%9F%92%9D"; } // heart with ribbon
    return "%F0%9F%92%9E" // revolving hearts
}

async fn do_plussy (botkey :&String, chat_id_default :&str, json :&JsonValue) {
    let chat_id = json_message_chat_id(json, chat_id_default);
    let textfield = &json["message"]["text"].as_str().unwrap_or("");
    let from = &json["message"]["from"]["id"].as_i64();
    let to = &json["message"]["reply_to_message"]["from"]["id"].as_i64();

    let amt :i32 = match textfield {
        &"+1" => 1,
        &"-1" => -1,
        _ => 0
    };

    if amt == 0 || from.is_none() || to.is_none() || from==to {
        error!("plussy txt {:?}  amt {:?}  from {:?}  to {:?}", textfield, amt, from, to);
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
        .parse::<i32>()
        .unwrap();

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
    info!("{:?} -> msg telegram {:?}", text, sendmsg(botkey, &chat_id, &text).await);
}

async fn do_all(args: web::Data<Args>, req: HttpRequest, body: web::Bytes) -> HttpResponse {
    info!("args {:?}", args);
    let botkey = &args().nth(1).unwrap();
    let chat_id = &args().nth(2).unwrap();
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


#[actix_web::main]
async fn main() -> std::io::Result<()>{
    ::pretty_env_logger::try_init().unwrap();
    info!("{}:{} ::{}::async-main() {:?}", std::file!(), core::line!(), core::module_path!(), std::env::args());

    let mut ssl_acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ssl_acceptor_builder .set_private_key_file("key.pem", SslFiletype::PEM)?;
    ssl_acceptor_builder.set_certificate_chain_file("cert.pem")?;

    HttpServer::new(
        || App::new()
        .data(std::env::args())
        .service(
            web::resource("*")
            .route( Route::new().to(do_all) ) ) )
    .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
    .run()
    .await
}