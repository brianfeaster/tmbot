//! # Telegram Communication

use crate::*;
use ::std::{fmt, time::{Duration} };
use ::openssl::ssl::{SslRef, SslAlert, SniError, NameType, SslConnector, SslAcceptor, SslMethod, SslConnectorBuilder, SslAcceptorBuilder};
use ::actix_web::client::{Client, Connector};

pub fn verifyServerName (cert_pem: &str)
    -> Bresult<
        impl Fn(&mut SslRef, &mut SslAlert)
        -> Result<(), SniError> >
{
    let mut names = Vec::new();
    let mut ab = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    ab.set_certificate_chain_file(cert_pem)?;
    // Make list of domain names from certificate
    ab.build().context().certificate().map(|cert| {
        // SAN
        cert.subject_alt_names()
        .map( |sans|
            sans.iter()
            .for_each( |gn| {
                gn.dnsname()
                .map( |n|
                    names.push(n.to_string())); } ) );
        // CN
        cert.subject_name().entries()
        .for_each( |xne| {
            from_utf8(xne.data().as_slice())
            .map( |n| names.push(n.to_string()))
            .ok(); } );
    } );
    Ok(move |sr: &mut SslRef, _: &mut SslAlert| {
        let sni = sr.servername(NameType::HOST_NAME).unwrap_or("");
        if !names.iter().any(|n| sni==n) {
            warn!("Rejected SNI '{}'", sni);
            Err(SniError::ALERT_FATAL)
        } else {
            Ok(())
        }
    })
}

// WEB_KEY_PEM, WEB_CERT_PEM
pub fn new_ssl_acceptor_builder(key_pem: &str, cert_pem: &str) -> Bresult<SslAcceptorBuilder> {
    let mut acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    acceptor_builder.set_private_key_file(key_pem, SslFiletype::PEM)?;
    acceptor_builder.set_certificate_chain_file(cert_pem)?;
    acceptor_builder.set_servername_callback(verifyServerName(cert_pem)?);
    Ok(acceptor_builder)
}

// TELEGRAM_KEY_PEM, TELEGRAM_CERT_PEM
pub fn new_ssl_connector_builder(key_pem: &str, cert_pem: &str) -> Bresult<SslConnectorBuilder> {
    let mut ssl_connector_builder = SslConnector::builder(SslMethod::tls())?;
    ssl_connector_builder.set_private_key_file(key_pem, openssl::ssl::SslFiletype::PEM)?;
    ssl_connector_builder.set_certificate_chain_file(cert_pem)?;
    Ok(ssl_connector_builder)
}

pub trait MsgDetails {
    fn at (&self) -> i64;
    fn topic (&self) -> Option<i64>;
    fn markdown (&self) -> bool;
    fn msg_id (&self) -> Option<i64>;
    fn msg (&self) -> &str;
}

#[derive(Debug)]
pub struct MsgCmd<'a> {
    pub at: i64,
    pub topic: Option<i64>,
    pub markdown: bool,
    pub msg_id: Option<i64>, // message_id to update instead of sending a new message, set to the message_id actually written
    pub msg: &'a str
}

////////////////////////////////////////

pub struct Telegram {
    client: Client,
    url_api: String
}

impl Telegram {
    pub fn new (
        url_api: &str,
        telegram_key: &str,
        telegram_cert: &str
    ) -> Bresult<Self> {
        let ssl_connector_builder = new_ssl_connector_builder(telegram_key, telegram_cert)?;
        let client =
            Client::builder() // ClientBuilder
                .connector(
                    Connector::new()
                        .ssl( ssl_connector_builder.build() )
                        .timeout(Duration::new(90,0))
                        .finish())
                .finish(); // Client
        Ok(Telegram { client, url_api:url_api.into() })
    }
}

impl Telegram {
    pub async fn send_msg<'a> (&self, obj: &'a impl MsgDetails) -> Bresult<MsgCmd<'a>> {
        let mut mc = MsgCmd {
            at:       obj.at(),
            topic:    obj.topic(),
            markdown: obj.markdown(),
            msg_id:   obj.msg_id(),
            msg:      obj.msg()
        };

        info!("\x1b[36m{:?}\x1b[0m", mc);

        let chat_id = mc.at.to_string();
        let text = if mc.markdown {
            mc.msg // Quick and dirty uni/url decode
            .replacen("%20", " ", 10000)
            .replacen("%27", "'", 10000)
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
            .replacen("=", "\\=", 10000)
            .replacen("#", "\\#", 10000)
            .replacen("'", "\\'", 10000)
            .replacen("!", "\\!", 10000)
            .replacen("|", "\\|", 10000)
        } else { mc.msg.to_string() };

        let mut query = vec![
            ["chat_id", &chat_id],
            ["text", &text],
            ["disable_notification", "true"],
            ["disable_web_page_preview", "true"],
        ];

        let mut edit_msg_id_str = String::new(); // Str must exist as long as query has ref
        if let Some(edit_msg_id) = mc.msg_id {
            edit_msg_id_str.push_str(&edit_msg_id.to_string());
            query.push( ["message_id", &edit_msg_id_str] )
        }

        let mut topic_str = String::new(); // Str must exist as long as query has ref
        if let Some(id) = mc.topic {
           topic_str.push_str(&id.to_string());
           query.push(["message_thread_id", &topic_str])
        }

        if mc.markdown { query.push(["parse_mode", "MarkdownV2"]) }

        let theurl =
            format!("{}/{}",
                self.url_api,
                if mc.msg_id.is_some() { "editmessagetext" } else { "sendmessage"} );

        info!("{} {}", theurl,
            query.iter()
            .map(|[k,v]| format!("{}=\x1b[1;30m{}\x1b[0m",k,v.replace("\n", "⬅")))
            .collect::<Vec<String>>()
            .join(" "));

        let clientRequest =
            self.client
                .get(theurl) // ClientRequest
                .header("User-Agent", "TMBot")
                .timeout(Duration::new(90,0))
                .query(&query)
                .unwrap();

        info!("<= \x1b[34m{:?} {} {}\x1b[0m  {}",
            clientRequest.get_version(),
            clientRequest.get_method(),
            clientRequest.get_uri(),
            headersPretty(&clientRequest.headers(), "  ")
        );

        let mut clientResponse = clientRequest.send().await?;

        let body = clientResponse.body().await;

        info!("=> \x1b[34m{:?} {} \x1b[33m{}\x1b[0m {}",
            clientResponse.version(),
            clientResponse.status(),
            body.as_ref().map(|b|from_utf8(b).unwrap_or("?")).unwrap_or("?"),
            headersPretty(&clientResponse.headers(), "  "));


        // Return the new message's id
        mc.msg_id = Some( getin_i64(
            &bytes2json(&body?)?,
            &["result", "message_id"])? );

        info!("\x1b[36m{:?}\x1b[0m", mc);
        Ok(mc)
    }
}

impl fmt::Debug for Telegram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
    {
        f.debug_struct("Telegram")
         .field("client", &format!("{{Client Object}}"))
         .field("url_api", &self.url_api)
         .finish()
    }
}
