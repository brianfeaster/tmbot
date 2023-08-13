//! # Telegram Communication

use crate::util::*;
use actix_web::rt;
use openssl::ssl::{
    NameType, SniError, SslAcceptor, SslAcceptorBuilder, SslAlert, SslFiletype, SslMethod, SslRef
};
use std::fmt::{self, Formatter};

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
            warn!("Rejected SNI '{}'  {:?}", sni, sr.state_string_long());
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

//// TELEGRAM_KEY_PEM, TELEGRAM_CERT_PEM
//pub fn new_ssl_connector_builder(key_pem: &str, cert_pem: &str) -> Bresult<SslConnectorBuilder> {
//    let mut ssl_connector_builder = SslConnector::builder(SslMethod::tls())?;
//    ssl_connector_builder.set_private_key_file(key_pem, openssl::ssl::SslFiletype::PEM)?;
//    ssl_connector_builder.set_certificate_chain_file(cert_pem)?;
//    Ok(ssl_connector_builder)
//}

////////////////////////////////////////

pub trait MsgDetails {
    fn telegram (&self) -> &Telegram;
    fn at (&self) -> i64;
    fn topic (&self) -> &str;
    fn markdown (&self) -> bool;
    fn msg_id (&self) -> Option<i64>;
    fn set_msg_id (&mut self, i: i64);
    fn msg (&self) -> &str;
}

pub struct Telegram {
    client: Client,
    url_api: String
}
impl fmt::Debug for Telegram {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Telegram")
         .field("client", &format!("{{Client Object}}"))
         .field("url_api", &self.url_api)
         .finish()
    }
}

impl Telegram {
    pub fn new(url_api: &str) -> Bresult<Self> {
        let client = rt::System::new("Telegram::new").block_on(async move {
            newHttpsClient()
        })?;
        Ok(Telegram {client, url_api:url_api.to_string()})
    }
    pub fn send_msg(obj: &mut impl MsgDetails) -> Bresult<()> {
        let chat_id = obj.at().to_string();
        let text = if obj.markdown() {
            obj.msg() // Quick and dirty uni/url decode
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
        } else { obj.msg().to_string() };

        let mut query = vec![
            ["chat_id", &chat_id],
            ["text", &text],
            ["disable_notification", "true"],
            ["disable_web_page_preview", "true"],
        ];

        let mut edit_msg_id_str = String::new(); // Str must exist as long as query has ref
        if let Some(edit_msg_id) = obj.msg_id() {
            edit_msg_id_str.push_str(&edit_msg_id.to_string());
            query.push( ["message_id", &edit_msg_id_str] )
        }

        if "" != obj.topic() {
           query.push(["message_thread_id", obj.topic()])
        }

        if obj.markdown() { query.push(["parse_mode", "MarkdownV2"]) }

        let theurl =
            format!("{}/{}",
                obj.telegram().url_api,
                crate::IF!(obj.msg_id().is_some(), "editmessagetext", "sendmessage"));

        let clientRequest =
            obj.telegram().client
                .get(theurl)
                .query(&query)?;

        info!("{}", reqPretty(&clientRequest, &""));

        let body = rt::System::new("sendmsg").block_on(async move {
            Bresult::Ok({
                let mut resp = clientRequest.send().await?;
                let body = from_utf8(&resp.body().await?)?.to_string();
                info!("{}", resPretty(&resp, &body));
                body
            })
        })?;

        // Record new message id
        obj.set_msg_id(getin_i64(&serde_json::from_str(&body)?, "/result/message_id")?);
        Ok(())
    }
}
