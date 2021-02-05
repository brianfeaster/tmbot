use log::*;
use std::time::{Duration};
use actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use actix_web::client::{Client, Connector};
use openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use regex::{Regex};
use std::collections::{HashSet};


//use ::serde::{Serialize, Deserialize};
//use ::serde_json::{Value, from_str, to_string_pretty};

#[derive(Debug)]
enum Serror {
   Error(json::Error),
   Utf8Error(std::str::Utf8Error)
}

impl From<std::str::Utf8Error> for Serror {
    fn from(e: std::str::Utf8Error) -> Self { Serror::Utf8Error(e) }
}
impl From<json::Error> for Serror {
    fn from(e: json::Error) -> Self { Serror::Error(e) }
}
impl From<&str> for Serror {
    fn from(s: &str) -> Self { Serror::Error(json::Error::WrongType(s.to_string())) }
}


//let mut rt = tokio::runtime::Runtime::new().unwrap();
//info!("client {:?}", rt.block_on(sendmsg()));

async fn sendmsg(tickers: &String) {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file("cert.pem").unwrap();

    let client = Client::builder()
        .connector(
            Connector::new()
                .ssl( builder.build() )
                .timeout(Duration::new(10,0))
                .finish() )
        .finish();

    let mut url = "https://api.telegram.org/bot1511069753:AAEFHXhfCVfo-aAETh450c4aARp37u_QIrg/sendmessage?chat_id=-1001082930701&text=".to_string();
    url.push_str(tickers);

    // Create request builder, configure request and send
    let response = client
        //.get("http://localhost:8888/sendmessage?chat_id=-1001082930701&text=hello+stonks!")
        .get(url)
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .send()
        .await;
    info!("client {:?}", response);
}

async fn getticker(ticker: &str) -> Option<String> {

    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();

    let client = Client::builder()
        .connector(
            Connector::new()
                .ssl( builder.build() )
                .timeout(Duration::new(10,0))
                .finish() )
        .finish();

    let mut url = "https://finance.yahoo.com/quote/".to_string();
    url.push_str(ticker);
    url.push_str("/");
    //error!("yahoo url = {:?}", url);

    // Create request builder, configure request and send
    let mut response = client
        .get(url)
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .send()
        .await.unwrap();

    //error!("yahoo! response = {:?}", response);

    let body = response.body().limit(1_000_000).await;
    if body.is_err() {
         error!("yahoo http reuest body {:?}", body);
         return None;
    }
    let body = body.unwrap();

    let domstr = std::str::from_utf8(&body);
    if domstr.is_err() {
         error!("yahoo dom parse {:?}", domstr);
         return None;
    }

    let cap = Regex::new("data-reactid=\"50\">([0-9]+.[0-9]+)")
        .unwrap()
        .captures(domstr.unwrap());
    if cap.is_none() {
         error!("yahoo regex captures {:?}", cap);
        return None;
    }

    let cap = cap.unwrap();
    if 2 != cap.len() {
        error!("yahoo regex cap len = {} != 2", cap.len());
        return None;
    }

    let price = cap[1].to_string();
    
    info!("yahoo ticker {}  price {}", ticker, price);
    return Some(price);
}

fn _do_all(req: HttpRequest, body: web::Bytes) -> Result<String, Serror> {
    debug!("\x1b[0;1;36m{:?}\x1b[0;36m{:?}\x1b[30m", req, body);
    let s = std::str::from_utf8(&body)?;
    let j = json::parse(s)?;
    let v = &j["message"]["text"]; // might return null
    info!("\x1b[35m .message.text = {}\x1b[0m", v);
    //Ok(HttpResponse::from(v.as_str().ok_or(json::Error::WrongType("not a string value".to_string()))?))
    //Ok(HttpResponse::from(v.as_str().ok_or("not a string")?.to_string()))
    Ok(String::from(v.as_str().ok_or(json::Error::WrongType("not a string".to_string()))?))
}

async fn do_all(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    let msg = _do_all(req, body);
    if msg.is_err() { return HttpResponse::from(""); }
    let msg = msg.unwrap(); // Consider message string
    let mut tickers = HashSet::new();

    let re = Regex::new(r"[^A-Za-z]").unwrap();

    for s in msg.split(" ") {
        let w = s.split("$").collect::<Vec<&str>>();
        if 2 == w.len()
            && w[0] != ""
            && w[1] == ""
            && re.captures(w[0]).is_none() {
            tickers.insert(w[0].to_string());
        }
    }

    info!("message tickers set {:?}", tickers);

    for ticker in tickers {
        match getticker(&ticker).await {
            Some(price) => {
                let mut tickerstr = String::new();
                tickerstr.push_str(&ticker);
                tickerstr.push_str("@");
                tickerstr.push_str(&price);
                //error!("ticker querystr {:?}", tickerstr);
                info!("client {:?}", sendmsg(&tickerstr).await);
            },
            _ => {
                error!("ticker lookup failed for {}", ticker);
            }
        }
    }

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