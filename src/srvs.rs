use crate::*;

pub async fn get_definition (word: &str) -> Bresult<Vec<String>> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( std::time::Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://www.onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(std::time::Duration::new(30,0))
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


pub async fn get_syns (word: &str) -> Bresult<Vec<String>> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls()).unwrap().build() )
                    .timeout( std::time::Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(std::time::Duration::new(30,0))
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

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Ticker {
    pub price: f64,
    pub hours: i64,
    pub amount: f64,
    pub percent: f64,
    pub title: String,
    pub market: char,
    pub exchange: String
}

pub async fn get_ticker_quote (_cmd:&Cmd, ticker: &str) -> Bresult<Ticker> {
    info!("get_ticker_quote <- {}", ticker);
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( std::time::Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://finance.yahoo.com/chart/".to_string() + ticker)
        .header("User-Agent", "Actix-web")
        .timeout(std::time::Duration::new(30,0))
        .send()
        .await?
        .body().limit(1_000_000).await;

    let body = body.or_else( |r| Err(format!("get_ticker_quote for {:?}  {:?}", ticker, r)) )?;
    let domstr = from_utf8(&body).or_else( |r| Err(format!(r#"get_ticker_quote http body2str {:?} {:?}"#, ticker, r)) )?;

    let json = {
        let cap = Regex::new("(?m)^root.App.main = (.*);$")?.captures(&domstr);
        if cap.is_none() || 2 != cap.as_ref().unwrap().len() {
            Err("Unable to find json string in HTML.")?
        }
        bytes2json(&cap.unwrap()[1].as_bytes())?
    };

    let details = getin(&json, &["context", "dispatcher", "stores", "QuoteSummaryStore", "price"]);
    if details.is_null() { Err("Unable to find quote data in json key 'QuoteSummaryStore'")? }
    info!("{}", details);

    let title_raw :&str =
        &getin_str(&details, &["longName"])
        .or_else(|_e|getin_str(&details, &["shortName"]))?;
    let mut title = title_raw;
    let title = loop { // Repeatedly strip title of useless things
        let title_new = title
            .trim_end_matches(".")
            .trim_end_matches(" ")
            .trim_end_matches(",")
            .trim_end_matches("Inc")
            .trim_end_matches("Corp")
            .trim_end_matches("Corporation")
            .trim_end_matches("Holdings")
            .trim_end_matches("Brands")
            .trim_end_matches("Company")
            .trim_end_matches("USD");
        if title == title_new { break title_new.to_string() }
        title = title_new;
    };
    info!("title pretty {} => {}", title_raw, &title);

    let exchange = getin_str(&details, &["exchange"])?;

    let hours = getin(&details, &["volume24Hr"]);
    let hours :i64 = if hours.is_object() && 0 != hours.as_object().unwrap().keys().len() { 24 } else { 16 };

    let previous_close = getin_f64(&details, &["regularMarketPreviousClose", "raw"]).unwrap_or(0.0);
    let pre_market_price = getin_f64(&details, &["preMarketPrice", "raw"]).unwrap_or(0.0);
    let reg_market_price = getin_f64(&details, &["regularMarketPrice", "raw"]).unwrap_or(0.0);
    let pst_market_price = getin_f64(&details, &["postMarketPrice", "raw"]).unwrap_or(0.0);

    let mut details = [
        (pre_market_price,
         pre_market_price - reg_market_price, //getin_f64(&details, &["preMarketChange", "raw"]).unwrap_or(0.0),
         (pre_market_price - reg_market_price) / reg_market_price, // getin_f64(&details, &["preMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["preMarketTime"]).unwrap_or(0),
         'p'),
        (reg_market_price,
         reg_market_price - previous_close, // getin_f64(&details, &["regularMarketChange", "raw"]).unwrap_or(0.0),
         (reg_market_price - previous_close) / previous_close, // getin_f64(&details, &["regularMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["regularMarketTime"]).unwrap_or(0),
         'r'),
        (pst_market_price,
         pst_market_price - previous_close, // getin_f64(&details, &["postMarketChange", "raw"]).unwrap_or(0.0),
         (pst_market_price - previous_close) / previous_close, //getin_f64(&details, &["postMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["postMarketTime"]).unwrap_or(0),
         'a')];

    /* // Log all prices for sysadmin requires "use ::datetime::ISO"
    glogd!("get_ticker_quote  send_msg_id => ", 
        send_msg_id(
            cmd.level(3), // An insignificant level that no one can legally set theirs at so msg sent to sysadmin.
            &format!("{} \"{}\" ({}) {}hrs\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%",
                ticker, title, exchange, hours,
                LocalDateTime::from_instant(Instant::at(details[0].3)).iso(), details[0].0, details[0].1, details[0].2,
                LocalDateTime::from_instant(Instant::at(details[1].3)).iso(), details[1].0, details[1].1, details[1].2,
                LocalDateTime::from_instant(Instant::at(details[2].3)).iso(), details[2].0, details[2].1, details[2].2)).await); */

    details.sort_by( |a,b| b.3.cmp(&a.3) ); // Find latest quote details

    Ok(Ticker{
        price:   details[0].0,
        hours,
        amount:  roundqty(details[0].1), // Round for cases: -0.00999999 -1.3400116
        percent: details[0].2,
        title,
        market:  details[0].4,
        exchange})
} // get_ticker_quote