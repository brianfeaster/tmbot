use crate::*;

pub async fn get_definition (word: &str) -> Bresult<Vec<String>> {

    let word2 = word.to_string();
    let body = newHttpsClient()?
        .get("https://www.onelook.com/")
        .query(&[["q",&word2]])?
        .send().await?
        .body().await
        .map_err(|e| format!(r#"get_definition http body {:?} for {:?}"#, e, word2))?;

    let domstr = from_utf8(&body)
        .map_err(|e| format!(r#"get_definition http from_utf8 {:?} for {:?}"#, e, word))?;

    // The optional US definitions
    let usdef =
        Regex::new(r"var mm_US_def = '[^']+")?
        .find(&domstr)
        .map_or("", |r| r.as_str() );

    let mut lst = Regex::new(r"%3Cdiv%20class%3D%22def%22%3E([^/]+)%3C/div%3E")?
        .captures_iter(&usdef)
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>();

    // WordNet definitions
    Regex::new(r#"easel_def_+[0-9]+">([^<]+)"#)?
        .captures_iter(&domstr)
        .for_each( |cap| lst.push( cap[1].to_string() ) );

    Ok(lst)
}

pub async fn get_syns (word: &str) -> Bresult<Vec<String>> {

    let word2 = word.to_string();
    let body = newHttpsClient()?
        .get("https://api.onelook.com/words")
        .query(&[["max", "10"],["ml", &word2]])?
        .send().await?
        .body().await
        .map_err(|e| format!(r#"get_syns http body {:?} for {:?}"#, e, word2))?;


    let domstr = from_utf8(&body)
        .map_err(|e|format!(r#"get_syns http body2str {:?} for {:?}"#, e, word))?;

    Ok( Regex::new(r#""word":"(([^\\"]|\\\\|\\")+)""#)?
        .captures_iter(&domstr)
        .map( |cap| cap[1].to_string() )
        .collect::<Vec<String>>() )
} // get_syns

////////////////////////////////////////////////////////////////////////////////

/*
pub async fn _get_ticker_raw_1 (ticker: &str) -> Bresult<Value> {
    info!("_get_ticker_quote_1 <- {}", ticker);
    let body = newHttpsClient()?
        .get( &format!("https://finance.yahoo.com/chart/{}?p={}", ticker, ticker) )
        .send().await?
        .body().await;

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
    let body = newHttpsClient()?
        .get("https://query1.finance.yahoo.com/v7/finance/quote".to_string())
        .insert_header(("Referer", "https://finance.yahoo.com/__finStreamer-worker.js"))
        .query(&[["symbols", ticker],["events","split"]]).unwrap()
        .send().await?
        .body().await;

    let body = body.or_else( |r| Err(format!("_get_ticker_raw_2 for {:?}  {:?}", ticker, r)) )?;
    let jsonstr = from_utf8(&body).or_else( |r| Err(format!(r#"_get_ticker_raw_2 http body2str {:?} {:?}"#, ticker, r)) )?;

    bytes2json(jsonstr.as_bytes())
}
*/

pub async fn get_ticker_raw(ticker: &str) -> Bresult<Value> {
    info!("get_ticker_quote <- {}", ticker);
    let client = newHttpsClient()?;
    let ticker2 = ticker.to_string();
    let rbody =
        match client
            .get( format!("https://query1.finance.yahoo.com/v8/finance/chart/{}", ticker2) )
            .insert_header(("User-Agent", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"))
            .send().await
        {
            Ok(mut clientResponse) => match clientResponse.body().await {
                Ok(resp) => Ok(resp),
                err => Err(format!("{:?}", err))
            }
            err => Err(format!("{:?}", err))
        };

    let body = rbody.or_else( |r| Err(format!("get_ticker_raw for {:?}  {:?}", ticker, r)) )?;
    let jsonstr = from_utf8(&body).or_else( |r| Err(format!(r#"get_ticker_raw http body2str {:?} {:?}"#, ticker, r)) )?;
    bytes2json(jsonstr.as_bytes())
}

////////////////////////////////////////////////////////////////////////////////

fn normalizeUrl (url: &str) -> String {
    must_re_to_vec(regex!("(?:https?://)?(.*)"), url)
    .and_then(|caps|
        caps.as_str(1)
        .map(|s| format!("https://{}", s)))
    .unwrap_or(url.into())
}

pub async fn httpget(url: &str) -> Bresult<String> {
    let clientRequest = newHttpsClient()?.get(url);
    info!("{}", reqPretty(&clientRequest, ""));
    let mut clientResponse = clientRequest.send().await?;
    let body = from_utf8(&clientResponse.body().await?)?.to_string();
    info!("{}", resPretty(&clientResponse, &body));
    Ok(body)
}

pub async fn httpsget(url: &str) -> Bresult<String> {
    let url = normalizeUrl(url);
    let clientRequest = newHttpsClient()?.get(&url);
    info!("{}", reqPretty(&clientRequest, ""));
    let mut clientResponse = clientRequest.send().await?;
    let body = from_utf8(&clientResponse.body().await?)?.to_string();
    info!("{}", resPretty(&clientResponse, &body));
    Ok(body)
}

pub async fn httpsbody(url: &str, txt: String) -> Bresult<String> {
    let url = normalizeUrl(url);
    let clientRequest = newHttpsClient()?.post(&url);
    info!("{}", reqPretty(&clientRequest, &txt));
    let mut clientResponse = clientRequest.send_body(txt).await?;
    let body = from_utf8(&clientResponse.body().await?)?.to_string();
    info!("{}", resPretty(&clientResponse, &body));
    Ok(body)
}

pub async fn httpsjson (url: &str, jsontxt: String) -> Bresult<String> {
    let url = normalizeUrl(url);
    let clientRequest = newHttpsClient()?
        .post(url)
        .insert_header((CONTENT_TYPE, "application/json"));
    info!("{}", reqPretty(&clientRequest, &jsontxt));
    let mut clientResponse = crate::IF!(jsontxt == "",
        clientRequest.send(),
        clientRequest.send_json(&bytes2json(jsontxt.as_ref())?)
    ).await?;
    let body = from_utf8(&clientResponse.body().await?)?.to_string();
    info!("{}", resPretty(&clientResponse, &body));
    Ok(body)
}
