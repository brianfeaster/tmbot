use ::std::{
    time::{Duration},
    collections::{HashMap, HashSet},
    str::{from_utf8},
    fs::{read_to_string, write},
    env::{args},
    sync::{Mutex, mpsc::{channel, Sender} },
};
use ::log::*;
use ::actix_web::{web, App, HttpRequest, HttpServer, HttpResponse, Route};
use ::actix_web::client::{Client, Connector};
use ::openssl::ssl::{SslConnector, SslAcceptor, SslFiletype, SslMethod};
use ::regex::{Regex};
use ::datetime::{Instant, LocalDate, LocalTime, LocalDateTime, DatePiece, Weekday, Month};

use ::tmbot::*;

////////////////////////////////////////////////////////////////////////////////
/// Datetime details:
/// * Time is seconds since epoch, UTC
/// * Trading hours is 6.5 hours long from 1430-2100 , 1330-2000 if US in DST)
/// ? trading time is pegged to max("closing bell", "after opening bell")
/// ? cache ticker values:  Update only if cached time is before trading time

/// For now simple M-F check.  TODO: check for holidays, half days
fn trading_day_p (date :LocalDate) -> bool {
    match date.weekday() {
        Weekday::Saturday|Weekday::Sunday => false,
        _ => true
    }
}

fn trading_time_next (date :LocalDate) -> i64 {
}

fn days_diff (a :LocalDate, b: LocalDate) -> i64 {
    (LocalDateTime::new(b, LocalTime::midnight()).to_instant().seconds()
     -
     LocalDateTime::new(a, LocalTime::midnight()).to_instant().seconds()
    ) / 86400
}


/// Decide if a ticker should be updated given its last update time.
///
fn update_ticker_p (cached :i64, now :i64) -> bool {
    // Consider cached now times as dates (floored to midnight UTC)
    let cachedday = LocalDateTime::from_instant(Instant::at(cached)).date();
    let cached_is_tradingday = trading_day_p(cachedday);
    let cacheopen = LocalDateTime::new(cachedday, LocalTime::hms(14, 30, 0).unwrap()).to_instant().seconds();
    let cacheclose = LocalDateTime::new(cachedday, LocalTime::hms(21, 0, 0).unwrap()).to_instant().seconds();

    let nowday = LocalDateTime::from_instant(Instant::at(now)).date();
    let now_is_tradingday = trading_day_p(nowday);
    let nowopen = LocalDateTime::new(nowday, LocalTime::hms(14, 30, 0).unwrap()).to_instant().seconds();
    let nowclose = LocalDateTime::new(nowday, LocalTime::hms(21, 0, 0).unwrap()).to_instant().seconds();

    let days_diff = days_diff(cachedday, nowday);
    // Guaranteed two days difference will include a weekday

    //                               cached on a weekend          now another weekend
    // |_________*********______|____________C___________| ... |____________N___________|________**********______|
    //     c1       c1      c3             c3                          c3                   c2      c2       c2
    // c1 cached on incomplete trading day

    // * now will always be equal or past cache
    // * If now is past nowOpen,

    // Clear your mind II !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if "cached.isTradingDay()" && "cached.beforeClosing()" {
        if "not-throttled" || "cached.closing <= now "{  Yahoo!() }
    } else { // cached on nontrading-day || cached after closing
        goal = cached.next_open_day_time()
        if "goal.afterOpenHour() <= now" {
            if "not-throttled" || "goal.closing <= now "{  Yahoo!() }
        }
    }



        if  "not throttled" || "now past cachedClosing" { // Doesn't matter when now is, as long as it's after cached closing
            // Yahoo!
        }
    } else { // cached on non-trading day

    }

    // Clear your mind!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if "cached on a trading day && cached before closing" {
        if  "not throttled" || "now past cachedClosing" { // Doesn't matter when now is, as long as it's after cached closing
            // Yahoo!
        }
    } else if "now on a trading day" {  // cached after closing || cached on a no-market day
        // Yahoo!
    } else {  // (cached after closing || cached on a no-market day) && "now not a trading day"
        // If there has been a trading day between cached and now... (the secret sauce)
    }

    if 3 <= days_diff {
        return true;

    } else if 0 == days_diff { // Only one day is an option

        // Do we need to catch-up to same-day market hours?  Might cache on any future day.
        if cached_is_tradingday                        && gwarn("update_ticker_p A Is a trading day...")
            && cached < cacheclose                     && gwarn("update_ticker_p A cached before closing...")
            && cacheopen <= now                        && gwarn("update_ticker_p A now after cached open...")
            && (cacheclose <= now || cached+60 <= now) && gwarn("update_ticker_p A not throttled...")
        {
            gwarn("update_ticker_p A -> \x1b[32mTRUE\x1b[0m");
            return true;
        }
    } 1 == days_diff { // two possible days four combinations [[ __  _*  **  *_ ]]
        if !cached_is_trading_day && !now_is_trading_day { return false; }
    }

    if cached_is_tradingday && cached < cachedclose {
        // maybe cachedday?

    } else { // cached is on a weekend
    }

    if cached <= nowclose && nowopen <= now && (nowclose <= now || cached+60 <= now) {
        warn!("update_ticker_p -> YES ticker needs to ");
        return true
    }
    false
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
struct DB {
    url_bot: String,
    chat_id_default: i64
}

type MDB = Mutex<DB>;

////////////////////////////////////////////////////////////////////////////////

async fn send_msg (db :&DB, chat_id :i64, text: &str) {
    info!("\x1b[33m<- {}", text);
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file("cert.pem").unwrap();

    match
        Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(10,0))
                    .finish() )
        .finish()
        .get( db.url_bot.clone() + "/sendmessage")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["text", &text],
                 ["disable_notification", "true"]]).unwrap()
        .send().await
    {
        Err(e) => error!("\x1b[31m-> {:?}", e),
        Ok(mut result) => {
            ginfod("\x1b[32m->", &result);
            ginfod("\x1b[1;32m->", result.body().await);
        }
    }
}

async fn send_msg_markdown (db :&DB, chat_id :i64, text: &str) {
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
    .replacen("`", "\\`", 10000)
    .replacen("=", "\\=", 10000)
    .replacen("'", "\\'", 10000);

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

async fn get_ticker_quote (ticker: &str) -> Option<(String, String, String)> {
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
            Some(cap) =>
                if 2==cap.len() {
                    cap[1].trim()
                    .trim_end_matches(|c|c=='.')
                    .replace("&amp;", "&")
                    .to_string()
                } else { "stonk".to_string() },
            _ =>
                "stonk".to_string()
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
    let price_bare = price.replacen(",", "", 1000);

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
        return Some( ( "".to_string(),
                       price + &title,
                       price_bare ) );
    } else {
        let percentage = &caps_percentages[0];
        return Some( ( percentage.0.to_string(),
                       price + " " + &percentage.1 + " " + &title,
                       price_bare ) );
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

    // The optional US definitions

    let usdef =
        Regex::new(r"var mm_US_def = '[^']+").unwrap()
        .find(&domstr.unwrap())
        .map_or("", |r| r.as_str() );

    let mut lst = Regex::new(r"%3Cdiv%20class%3D%22def%22%3E([^/]+)%3C/div%3E").unwrap()
        .captures_iter(&usdef)
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>();

    // WordNet definitions

    Regex::new(r#"easel_def_+[0-9]+">([^<]+)"#).unwrap()
        .captures_iter(&domstr.unwrap())
        .for_each( |cap| lst.push( cap[1].to_string() ) );

    Ok(lst)
}

fn text_parse_for_tickers (txt :&str) -> Option<HashSet<String>> {
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
    if tickers.is_empty() { None } else { Some(tickers) }
}

////////////////////////////////////////////////////////////////////////////////

async fn do_ticker (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let tickers = text_parse_for_tickers(&cmd.msg).ok_or("do_ticker SKIP no tickers")?;

    for ticker in &tickers {
        let ticker = ticker.to_uppercase();
        let sql = format!("SELECT * FROM stonk WHERE ticker='{}'", ticker);
        warn!("sql = {}", &sql);
        let res = get_sql(&sql).unwrap();
        let nowsecs :i64 = Instant::now().seconds();
        let mut is_cached = false;
        if !res.is_empty() {
            let hm = &res[0];
            let timesecs = hm.get("time").unwrap().parse::<i64>().unwrap();
            let time = LocalDateTime::from_instant(Instant::at(timesecs));
            let should_update = update_ticker_p(timesecs, nowsecs);
            info!("{}@{} {} {:?} {} {}",
                hm.get("ticker").unwrap(),
                hm.get("price").unwrap(),
                timesecs, time, should_update,
                hm.get("pretty").unwrap());
            if !should_update {
                 send_msg(db, cmd.at, &format!("{}", hm.get("pretty").unwrap())).await;
                 continue;
            }
            is_cached = true;
        }

        // Update
        if let Some(price) = get_ticker_quote(&ticker).await {
            let pretty = format!("{}{}@{}", price.0, ticker, price.1);
            send_msg(db, cmd.at, &(pretty.to_string() + "·")).await;
            let sql = if is_cached {
                format!("UPDATE stonk SET price={}, time={}, pretty='{}' WHERE ticker='{}'", price.2, nowsecs, pretty, ticker)
            } else {
                format!("INSERT INTO stonk VALUES ('{}', {}, {}, '{}')", ticker, price.2, nowsecs, pretty)
            };
            warn!("sql = {}", &sql);
            info!("Stonks update row results {:?}", get_sql(&sql));
        }
    }

    Ok("Ok do_ticker")
}

async fn get_syns (word: &str) -> Result<Vec<String>, Serror> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls()).unwrap().build() )
                    .timeout( Duration::new(10,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(10,0))
        .query(&[["clue", word]]).unwrap()
        .send()
        .await.unwrap()
        .body().limit(1_000_000).await;

    if body.is_err() {
         error!(r#"http body {:?} for {:?}"#, body, word);
         Err("get_syns http error")?;
    }

    let body = body.unwrap();
    let domstr = from_utf8(&body);
    if domstr.is_err() {
         error!(r#"http body2str {:?} for {:?}"#, domstr, word);
         Err("get_body2str error")?;
    }

    Ok( Regex::new("w=([^:&\"<>]+)").unwrap()
        .captures_iter(&domstr.unwrap())
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>() )
}

/*
fn str_after_str<'t> (heystack :&'t str, needle :&str) -> &'t str {
    &heystack[(heystack.find(needle).map_or(-(needle.len() as i32), |n| n as i32) + needle.len() as i32) as usize ..]
}
*/

async fn do_syn (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let cap = Regex::new(r"^([a-z]+);$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("do_syn SKIP"); }
    let word = &cap.unwrap()[1];

    info!("looking up {:?}", word);

    let mut defs = get_syns(word).await?;

    if 0 == defs.len() {
        send_msg_markdown(db, cmd.from, &format!("*{}* synonyms is empty", word)).await;
        return Ok("do_syn empty synonyms");
    }

    let mut msg = String::new() + "*\"" + word + "\"* ";
    defs.truncate(10);
    msg.push_str( &defs.join(", ") );
    send_msg_markdown(db, cmd.at, &msg).await;

    Ok("Ok do_syn")
}

async fn do_def (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    let cap = Regex::new(r"^([a-z]+):$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("do_def SKIP"); }
    let word = &cap.unwrap()[1];

    let defs = get_definition(word).await?;

    if defs.is_empty() {
        send_msg_markdown(db, cmd.from, &format!("*{}* def is empty", word)).await;
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

    send_msg_markdown(db, cmd.at, &msg).await;

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
    //info!("HEARTS -> msg tmbot {:?}", send_msg(db, chat_id, &(-6..=14).map( |n| num2heart(n) ).collect::<Vec<&str>>().join("")).await);
    send_msg(db, cmd.at, &text).await;
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
    send_msg(db, cmd.at, &text).await;

    Ok("Ok do_like".into())
}

async fn do_sql (db :&DB, cmd :&Cmd) -> Result<&'static str, Serror> {

    if cmd.from != 308188500 {
        return Ok("do_sql invalid user");
    }

    let cap = Regex::new(r"^(.*)ß$").unwrap().captures(&cmd.msg);
    if cap.is_none() { return Ok("do_sql SKIP"); }
    let expr = &cap.unwrap()[1];

    let result = get_sql(expr);
    if let Err(e) = result {
        send_msg_markdown(db, cmd.from, &format!("{:?}", e)).await;
        return Err(e);
    }
    let results = result.unwrap();

    if results.is_empty() {
        send_msg(db, cmd.from, &format!("\"{}\" results is empty", expr)).await;
        return Ok("do_sql def is empty");
    }

    for res in results {
        let mut buff = String::new();
        res.iter().for_each( |(k,v)| buff.push_str(&format!("{}:{} ", k, v)) );
        let res = format!("{}\n", buff);
        send_msg(db, cmd.at, &res).await;
    }

    Ok("Ok do_sql")
}

fn snarf (
    snd :Sender<HashMap<String, String>>,
    res :&[(&str, Option<&str>)] // [ (column, value) ]
) -> bool {
    let mut v = HashMap::new();
    for r in res {
        trace!("snarf vec <- {:?}", r);
        v.insert( r.0.to_string(), r.1.unwrap_or("NULL").to_string() );
    }
    let res = snd.send(v);
    trace!("snarf snd <- {:?}", res);
    true
}

fn get_sql ( cmd :&str ) -> Result<Vec<HashMap<String, String>>, Serror> {
    let sql = ::sqlite::open( "tmbot.sqlite" )?;
    let (snd, rcv) = channel::<HashMap<String, String>>();
    sql.iterate(cmd, move |r| snarf(snd.clone(), r) )?;
    Ok(rcv.iter().collect::<Vec<HashMap<String,String>>>())
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
    info!("{:?}", do_like(&db, &cmd).await);
    info!("{:?}", do_like_info(&db, &cmd).await);
    info!("{:?}", do_ticker(&db, &cmd).await);
    info!("{:?}", do_def(&db, &cmd).await);
    info!("{:?}", do_syn(&db, &cmd).await);
    info!("{:?}", do_sql(&db, &cmd).await);
    //info!("{:?}", do_stonks(&db, &cmd).await);
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

fn do_schema() -> Result<(), Serror> {
    let sql = ::sqlite::open("tmbot.sqlite")?;

    sql.execute("
        CREATE TABLE entity (
            id INTEGER  NOT NULL UNIQUE,
            name  TEXT  NOT NULL);
    ").map_or_else(gwarn, ginfo);

    sql.execute("
        CREATE TABLE bank (
            id    INTEGER  NOT NULL UNIQUE,
            amount  FLOAT  NOT NULL);
    ").map_or_else(gwarn, ginfo);

    /*
    for l in read_to_string("tmbot/users.txt").unwrap().lines() {
        let mut v = l.split(" ");
        let id = v.next().ok_or("User DB malformed.")?;
        let name = v.next().ok_or("User DB malformed.")?.to_string();
        sql.execute(
            format!("INSERT INTO entity VALUES ( {}, '{}' )", id, name)
        ).map_or_else(gwarn, ginfo);
    }
    */

    sql.execute("
        --DROP TABLE stonk;
        CREATE TABLE stonk (
            ticker  TEXT  NOT NULL UNIQUE,
            price  FLOAT  NOT NULL,
            time INTEGER  NOT NULL,
            pretty  TEXT  NOT NULL);
    ").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO stonk VALUES ( 'TWNK', 14.97, 1613630678, '14.97')").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO stonk VALUES ( 'GOOG', 2128.31, 1613630678, '2,128.31')").map_or_else(gwarn, ginfo);
    //sql.execute("UPDATE stonk SET time=1613630678 WHERE ticker='TWNK'").map_or_else(gwarn, ginfo);
    //sql.execute("UPDATE stonk SET time=1613630678 WHERE ticker='GOOG'").map_or_else(gwarn, ginfo);

    sql.execute("
        --DROP TABLE orders;
        CREATE TABLE orders (
            entity INTEGER  NOT NULL,
            ticker    TEXT  NOT NULL,
            amount   FLOAT  NOT NULL,
            cost     FLOAT  NOT NULL,
            time   INTEGER  NOT NULL);
    ").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO orders VALUES ( 241726795, 'TWNK', 500, 14.95, 1613544000 )").map_or_else(gwarn, ginfo);
    //sql.execute("INSERT INTO orders VALUES ( 241726795, 'GOOG', 0.25, 2121.90, 1613544278 )").map_or_else(gwarn, ginfo);

    Ok(())
}


#[actix_web::main]
async fn main() -> Result<(), Serror> {
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

    do_schema()?;

    Ok(srv.await?)
}
/*
    sql.execute("
        DROP TABLE users;
        CREATE TABLE users (name TEXT, age INTEGER);
        INSERT INTO users VALUES ('Alice', 42);
    ")?;
*/