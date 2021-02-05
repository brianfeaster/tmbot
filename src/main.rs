use log::*;
use std::time::{Duration};
use std::collections::{HashSet};
use std::str::{from_utf8, Utf8Error};
use actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use actix_web::client::{Client, Connector};
use openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use regex::{Regex};


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


//let mut rt = tokio::runtime::Runtime::new().unwrap();
//info!("client {:?}", rt.block_on(sendmsg()));

async fn sendmsg(botkey :&str, tickers: &String) {
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

    let mut url = "https://api.telegram.org/bot".to_string();
    url.push_str(botkey);
    url.push_str("/sendmessage?chat_id=-1001082930701&text=");
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

    let builder = SslConnector::builder(SslMethod::tls()).unwrap();

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
    //error!("http url = {:?}", url);

    // Create request builder, configure request and send
    let mut response = client
        .get(url)
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .send()
        .await.unwrap();

    //error!("http response = {:?}", response);

    let body = response.body().limit(1_000_000).await;
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
fn http_body_json_field(req: HttpRequest, body: web::Bytes) -> Result<String, Serror> {
    debug!("\x1b[0;1;36m{:?}\x1b[0;36m{:?}\x1b[30m", req, body);

    let bodystr = json::parse(from_utf8(&body)?)?;
    info!("\x1b[1;35m{}\x1b[0m", bodystr);

    let textfield = &bodystr["message"]["text"]; // might return null
    info!("\x1b[35m.message.text = {}\x1b[0m", textfield);

    Ok( textfield.as_str()
        .ok_or( json::Error::WrongType( "not a string".to_string() ) )?
        .to_string()
    )
}

async fn do_all(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    info!("args {:?}", std::env::args());
    let botkey = &std::env::args().nth(1).unwrap();

    let msg = http_body_json_field(req, body);
    if msg.is_err() {
        error!("{:?}", msg);
        return HttpResponse::from("");
    }

    let msg = msg.unwrap(); // Consider message string
    let re = Regex::new(r"[^A-Za-z^.]").unwrap();
    let mut tickers = HashSet::new();

    for s in msg.split(" ") {
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

    if 0 == tickers.len() { return HttpResponse::from(""); }

    info!("set of tickers {:?}", tickers);
    for ticker in tickers {
        match getticker(&ticker).await {
            Some(price) => {
                let mut tickerstr = String::new();
                tickerstr.push_str(&ticker);
                tickerstr.push_str("@");
                tickerstr.push_str(&price);
                //info!("{:?} -> msg telegram", tickerstr);
                info!("{:?} -> msg telegram {:?}", tickerstr, sendmsg(botkey, &tickerstr).await);
            }, _ => ()
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