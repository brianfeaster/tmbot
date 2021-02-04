use log::*;
use std::time::{Duration};
use actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use actix_web::client::{Client, Connector};
use openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};


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
    let mut tickers = String::new();

    for s in msg.split(" ") {
        let mut w = s.split("$").collect::<Vec<&str>>();
        if 2 == w.len() && w[0] != "" && w[1] == "" {
            tickers.push_str(w[0]);
            tickers.push_str("@");
            tickers.push_str(&format!("{:.2}", utils::rf32(100.0)));
            tickers.push_str("+");
        }
    }
    info!("message tickers message {:?}", tickers);

    if 0 < tickers.len() {
        info!("client {:?}", sendmsg(&tickers).await);
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