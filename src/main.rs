use ::std::{
    time::{Duration},
    collections::{HashMap, HashSet},
    str::{from_utf8},
    fs::{read_to_string, write},
    env::{args},
    sync::{Mutex},
};
use ::log::*;
use ::actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use ::actix_web::client::{Client, Connector};
use ::openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use ::regex::{Regex};
use ::tmbot::*;

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
struct DB {
    url_bot: String,
    chat_id_default: i64
}

type MDB = Mutex<DB>;

////////////////////////////////////////////////////////////////////////////////

async fn sendmsg (db :&DB, chat_id :i64, text: &str) {
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

async fn sendmsgmd (db :&DB, chat_id :i64, text: &str) {
    let text = text.replace(".", "\\.").replace("(", "\\(").replace(")", "\\)");
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
                 ["text", &text],
                 ["parse_mode", "MarkdownV2"],
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

    let re = Regex::new(r#"data-reactid="[0-9]+">([-+][0-9,]+\.[0-9]+) \(([-+][0-9,]+\.[0-9]+%)\)<"#).unwrap();
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

async fn get_definition (word: &str) -> Result<Vec<String>, Serror> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls()).unwrap().build() )
                    .timeout( Duration::new(10,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://www.onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .query(&[["q", word]])?
        .send()
        .await?
        .body().limit(1_000_000).await;

    if body.is_err() {
         error!(r#"http body {:?} for {:?}"#, body, word);
         Err("get_definition http error")?;
    }

    let body = body.unwrap();
    let domstr = from_utf8(&body);
    if domstr.is_err() {
         error!(r#"http body2str {:?} for {:?}"#, domstr, word);
         Err("get_body2str error")?;
    }

    Ok( Regex::new(r"%3Cdiv%20class%3D%22def%22%3E([^/]+)%3C/div%3E").unwrap()
        .captures_iter(&domstr.unwrap())
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>() )
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

async fn do_ticker (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let txt = &cmd.msg;
    let chat_id = cmd.at; // Message destination

    let tickers = text_parse_for_tickers(&txt);

    if tickers.is_empty() { return Ok("do_ticker SKIP no tickers"); }

    info!("tickers {:?}", tickers);

    for ticker in tickers {
        if let Some(price) = get_ticker_quote(&ticker).await {
            let quote = price.0 + &ticker + "@" + &price.1;
            sendmsg(db, chat_id, &quote).await;
        }
    }

    Ok("Ok do_ticker")
}

async fn do_def (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let word =
     &Regex::new(r"^([a-z]+):$").unwrap()
     .captures(&cmd.msg)
     .ok_or("do_def SKIP")?[1];

    let defs = get_definition(word).await?;

    if defs.is_empty() {
        sendmsgmd(db, cmd.from, &format!("*{}* def is empty", word)).await;
        return Ok("do_def def is empty");
    }

    let mut msg = String::new() + "*" + word + ":*";

    if 1 == defs.len() {
        msg.push_str( &format!(" {}", defs[0].to_string()));
    } else {
        for i in 0..defs.len() {
            msg.push_str( &format!(" *({})* {}", i+1, defs[i].to_string()));
        }
    }

    let msg = msg // Poor person's uni/url decode
        .replace("%20", " ")
        .replace("%2C", ",")
        .replace("-", "\\-")
        .replace("%26%238217%3B", "'");

    sendmsgmd(db, cmd.at, &msg).await;

    Ok("Ok do_def")
}

async fn do_like_info (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    if cmd.msg != "+?" { return Ok("do_like_info SKIP"); }

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
    //info!("HEARTS -> msg tmbot {:?}", sendmsg(db, chat_id, &(-6..=14).map( |n| num2heart(n) ).collect::<Vec<&str>>().join("")).await);
    sendmsg(db, cmd.at, &text).await;
    Ok("Ok do_like_info")
}

async fn do_like (db :&DB, cmd:&Cmd) -> Result<String, Serror> {

    let amt :i32 = match cmd.msg.as_ref() { "+1" => 1, "-1" => -1, _=>0 };
    if amt == 0 { return Ok("do_like SKIP".into()); }

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
    sendmsg(db, cmd.at, &text).await;

    Ok("Ok do_like".into())
}

#[derive(Debug)]
struct Cmd {
    from :i64,
    at   :i64,
    to   :i64,
    msg  :String
}

fn parse_cmd(body: &web::Bytes) -> Result<Cmd, Serror> {

    let json: JsonValue = bytes2json(&body)?;

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

async fn do_all(db: &DB, body: &web::Bytes) -> Result<(), Serror> {
    let cmd = parse_cmd(&body)?;
    warn!("\x1b[33m{:?}", &cmd);
    info!("{:?}", do_like(&db, &cmd).await?);
    info!("{:?}", do_like_info(&db, &cmd).await?);
    info!("{:?}", do_ticker(&db, &cmd).await?);
    info!("{:?}", do_def(&db, &cmd).await?);
    Ok(())
}

async fn dispatch (req: HttpRequest, body: web::Bytes) -> HttpResponse {
    info!("\x1b[1;34m ™™™ ™™ ™™ |    ___   _   _     ");
    info!("\x1b[1;34m  ™  ™ ™ ™ |\\ /\\ |   | | | |   |");
    info!("\x1b[1;34m  ™  ™   ™ |/ \\/ |   |_|.|_|.  |");

    let db = req.app_data::<web::Data<MDB>>().unwrap().lock().unwrap();
    info!("\x1b[1m{:?}", &db);
    info!("\x1b[35m{:?}", &req.connection_info());
    info!("\x1b[35m{}", format!("{:?}", &req).replace("\n", "").replace("  ", " "));

    do_all(&db, &body)
    .await
    .map_or_else(
        |r| {
            error!("\x1b[31mbody {:?}", &body);
            error!("End. {:?}", r)
        },
        |r| info!("End. {:?}", r)
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
                    .route( Route::new().to(dispatch) ) ) )
    .bind_openssl("0.0.0.0:8443", ssl_acceptor_builder)?
    .workers(1)
    .run();

    ::pretty_env_logger::try_init().unwrap();
    info!("{}:{} ::{}::async-main()", std::file!(), core::line!(), core::module_path!());
    //info!("{:?}", args().collect::<Vec<String>>());

    srv.await
}