use crate::*;

pub async fn get_definition (word: &str) -> Bresult<Vec<String>> {
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( std::time::Duration::new(90,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://www.onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(std::time::Duration::new(90,0))
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
                    .timeout( std::time::Duration::new(90,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://onelook.com/")
        .header("User-Agent", "Actix-web")
        .timeout(std::time::Duration::new(90,0))
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

pub async fn _get_ticker_raw_1 (ticker: &str) -> Bresult<Value> {
    info!("_get_ticker_quote_1 <- {}", ticker);
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( std::time::Duration::new(90,0) )
                    .finish() )
        .finish() // -> Client
        .get( &format!("https://finance.yahoo.com/chart/{}?p={}", ticker, ticker) )
        .header("User-Agent", "Actix-web")
        .timeout(std::time::Duration::new(90,0))
        .send()
        .await?
        .body().limit(1_000_000).await;

    let body = body.or_else( |r| Err(format!("get_ticker_quote for {:?}  {:?}", ticker, r)) )?;
    let domstr = from_utf8(&body).or_else( |r| Err(format!(r#"get_ticker_quote http body2str {:?} {:?}"#, ticker, r)) )?;

    let cap = Regex::new("(?m)^root.App.main = (.*);$")?.captures(&domstr);
    if cap.is_none() || 2 != cap.as_ref().unwrap().len() {
        Err("Unable to find json string in HTML.")?
    }
    bytes2json(&cap.unwrap()[1].as_bytes())
} // _get_ticker_raw_1

pub async fn _get_ticker_raw_2 (ticker: &str) -> Bresult<Value> {
    info!("get_ticker_quote <- {}", ticker);
    let body =
        Client::builder()
        .connector( Connector::new()
                    .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                    .timeout( std::time::Duration::new(90,0) )
                    .finish() )
        .finish() // -> Client
        .get("https://query1.finance.yahoo.com/v7/finance/quote".to_string())
        .header("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36")
        .header("Referer", "https://finance.yahoo.com/__finStreamer-worker.js")
        .timeout(std::time::Duration::new(90,0))
        .query(&[["symbols", ticker],["events","split"]]).unwrap()
        .send()
        .await?
        .body().limit(1_000_000).await;

    let body = body.or_else( |r| Err(format!("_get_ticker_raw_2 for {:?}  {:?}", ticker, r)) )?;
    let jsonstr = from_utf8(&body).or_else( |r| Err(format!(r#"_get_ticker_raw_2 http body2str {:?} {:?}"#, ticker, r)) )?;

    bytes2json(jsonstr.as_bytes())
} // _get_ticker_raw_2

pub fn get_ticker_raw (ticker: &str) -> Bresult<Value> {
    info!("get_ticker_quote <- {}", ticker);
    let sslConnector = SslConnector::builder(SslMethod::tls())?.build();
    let ticker2 = ticker.to_string();
    let mut sendClientRequest = rt::System::new("tmbot").block_on( async move {
        Client::builder()
        .connector( Connector::new()
                    .ssl( sslConnector )
                    .timeout( std::time::Duration::new(90,0) )
                    .finish() )
        .finish() // -> Client
        .get( format!("https://query1.finance.yahoo.com/v8/finance/chart/{}", ticker2) )
        .header("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36")
        //.header("Referer", "https://finance.yahoo.com/__finStreamer-worker.js")
        .timeout(std::time::Duration::new(90,0))
        //.query(&[["symbols", ticker],["events","split"]]).unwrap()
        .send()
        .await
    })?;

    let body = rt::System::new("tmbot")
        .block_on(async move { sendClientRequest.body().limit(1_000_000).await });

    let body = body.or_else( |r| Err(format!("get_ticker_raw for {:?}  {:?}", ticker, r)) )?;
    let jsonstr = from_utf8(&body).or_else( |r| Err(format!(r#"get_ticker_raw http body2str {:?} {:?}"#, ticker, r)) )?;

    bytes2json(jsonstr.as_bytes())
} // get_ticker_raw

pub async fn get_https_raw(url: &str) -> Bresult<String> {
    let url = format!("https://{}", url);
    let client =
        Client::builder()
        .connector( Connector::new()
                    .ssl(SslConnector::builder(SslMethod::tls())?.build())
                    .timeout(std::time::Duration::new(90, 0))
                    .finish() )
        .finish() // -> Client
        .get(url.to_string()) // -> ClientRequest
        .timeout(std::time::Duration::new(90, 0));

    info!(
        "<= \x1b[34m{:?} {} {} {}",
        client.get_version(),
        client.get_method(),
        client.get_uri(),
        headersPretty(&client.headers())
    );

    let mut resp = client
        .send() // -> SendClientRequest
        .await?;

    let body = resp.body().limit(1_000_000).await;

    let body = body.or_else(|r| Err(format!("get_https_raw {:?}  {:?}", url, r)))?;
    let body = from_utf8(&body)
        .or_else(|r| Err(format!(r#"get_https_raw from_utf8 {:?} {:?}"#, body, r)))?;

    info!(
        "=> \x1b[34m{:?} {} \x1b[33;100m{} {}",
        resp.version(),
        resp.status(),
        body.replace("\n", " \x1b7\x08\x1b[1m|\x1b8"),
        headersPretty(&resp.headers())
    );

    Ok(body.into())
} // get_https_raw

pub fn post_https_text(url: &str, text: String) -> Bresult<String> {
    let url = format!("https://{}", url);
    let clientRequest = Client::builder()
        .connector(
            Connector::new()
                .ssl( SslConnector::builder(SslMethod::tls())?.build() )
                .timeout( std::time::Duration::new(90,0) )
                .finish(),
        )
        .header(actix_web::http::header::USER_AGENT, "TMBot")
        .timeout(std::time::Duration::new(90,0)) // ClientBuilder
        .finish() // -> Client
        .post(url.to_string()); // -> ClientRequest

    info!("<= \x1b[34m{:?} {} {} \x1b[33;100m{} {}",
        clientRequest.get_version(),
        clientRequest.get_method(),
        clientRequest.get_uri(),
        text.replace("\n", " \x1b7\x08\x1b[1m|\x1b8"),
        headersPretty(&clientRequest.headers())
    );

    let body = rt::System::new("tmbot").block_on(async move {
        match clientRequest.send_body(text).await {
            Ok(mut clientResponse) => {
                let body = clientResponse
                    .body()
                    .limit(1_000_000)
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(|b| {
                        from_utf8(&b)
                            .map(|s| s.to_string())
                            .map_err(|e| e.to_string())
                    })
                    .unwrap();

                info!(
                    "=> \x1b[34m{:?} {} \x1b[33;100m{} {}",
                    clientResponse.version(),
                    clientResponse.status(),
                    body.replace("\n", " \x1b7\x08\x1b[1m|\x1b8"),
                    headersPretty(&clientResponse.headers())
                );
                Ok(body)
            }
            Err(e) => Err(e.to_string()),
        }
    })?;

    Ok(body.into())
} // post_https_text

pub fn post_https_json(url: &str, jsons: &str) -> Bresult<String> {
    let url = format!("https://{}", url);
    let clientRequest = Client::builder() // ClientBuilder
        .connector(
            Connector::new()
                .ssl(SslConnector::builder(SslMethod::tls())?.build())
                .timeout(std::time::Duration::new(90, 0))
                .finish(),
        )
        .header(actix_web::http::header::USER_AGENT, "TMBot")
        .finish() // -> Client
        .post(url.to_string()) // -> ClientRequest
        .timeout(std::time::Duration::new(90, 0));

    let js = bytes2json(jsons.as_bytes())?;
    let jsonsIsEmpty = jsons == "";

    let mut clientResponse = rt::System::new("tmbot").block_on(async move {
        if jsonsIsEmpty {
            clientRequest
                .header("Content-Type", "application/json")
                .send() // SendClientRequest
                .await // ClientResponse
        } else {
            clientRequest
                .send_json(&js) // SendClientRequest
                .await // ClientResponse
        }
    })?;

    let body = rt::System::new("tmbot").block_on(async move { clientResponse.body().await });

    let body = body.or_else(|r| Err(format!("post_https_json {:?}  {:?}", url, r)))?;
    let body = from_utf8(&body)
        .or_else(|r| Err(format!(r#"post_https_json from_utf8 {:?} {:?}"#, body, r)))?;
    Ok(body.into())
} // post_https_json
