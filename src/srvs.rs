use crate::*;

pub async fn get_definition (word: &str) -> Bresult<Vec<String>> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://www.onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
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
                    .timeout( Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
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
    pub amount: f64,
    pub percent: f64,
    pub title: String,
    pub hours: char,
    pub exchange: String
}

pub async fn get_ticker_quote (cmd:&Cmd, ticker: &str) -> Bresult<Ticker> {
    info!("get_ticker_quote <- {}", ticker);
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( Duration::new(30,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://finance.yahoo.com/chart/".to_string() + ticker)
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
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

    if details.is_null() {
         Err("Unable to find quote data in json key 'QuoteSummaryStore'")?
    }

    info!("{}", details);

    let title = getin_str(&details, &["longName"])?.trim_end_matches(|e|e=='.').to_string();
    let exchange = getin_str(&details, &["exchange"])?;

    let mut details = [
        (getin_str(&details, &["preMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["preMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["preMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["preMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["preMarketTime"]).unwrap_or(0),
         'p'),
        (getin_str(&details, &["regularMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["regularMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["regularMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["regularMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["regularMarketTime"]).unwrap_or(0),
         'r'),
        (getin_str(&details, &["postMarketPrice", "fmt"]).unwrap_or("?".into()),
         getin_f64(&details, &["postMarketPrice", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["postMarketChange", "raw"]).unwrap_or(0.0),
         getin_f64(&details, &["postMarketChangePercent", "raw"]).unwrap_or(0.0) * 100.0,
         getin_i64(&details, &["postMarketTime"]).unwrap_or(0),
         'a')];

    // TODO: for now log all prices to me privately for debugging/verification
    glogd!("send_msg => \x1b[35m",
        send_msg_id(cmd.to(308188500_i64),
            &format!("{} \"{}\" ({})\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%\n{} {:.2} {:.2} {:.2}%",
                ticker, title, exchange,
                LocalDateTime::from_instant(Instant::at(details[0].4)).iso(), details[0].1, details[0].2, details[0].3,
                LocalDateTime::from_instant(Instant::at(details[1].4)).iso(), details[1].1, details[1].2, details[1].3,
                LocalDateTime::from_instant(Instant::at(details[2].4)).iso(), details[2].1, details[2].2, details[2].3)
        ).await);

    details.sort_by( |a,b| b.4.cmp(&a.4) ); // Find latest quote details

    Ok(Ticker{
        price       :details[0].1,
        amount      :roundqty(details[0].2), // Round for cases: -0.00999999 -1.3400116
        percent     :details[0].3,
        title       :title,
        hours       :details[0].5,
        exchange    :exchange})
} // get_ticker_quote