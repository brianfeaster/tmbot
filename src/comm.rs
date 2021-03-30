//! # Communication with external services

use crate::*;

use ::std::{
    time::{Duration},
};
use ::actix_web::{
    client::{Client, Connector}};
use ::openssl::ssl::{SslConnector, SslMethod};
use ::log::*;

/// Send plain-text message to chat.
pub async fn send_msg (db :&DB, chat_id :i64, text: &str) -> Bresult<i64> {
    info!("Telegram <= \x1b[36m{}", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    match Client::builder()
    .connector( Connector::new()
                .ssl( builder.build() )
                .timeout(Duration::new(30,0))
                .finish() )
    .finish()
    .get( db.url_api.clone() + "/sendmessage")
    .header("User-Agent", "Actix-web")
    .timeout(Duration::new(30,0))
    .query(&[["chat_id", &chat_id.to_string()],
             ["text", &text],
             ["disable_notification", "true"]]).unwrap()
    .send()
    .await?
    { mut result => {
        ginfod!("Telegram => \x1b[35m", &result);
        let body = result.body().await;
        ginfod!("Telegram => \x1b[1;35m", body);
        Ok( getin_i64(
                &bytes2json(&body?)?,
                &["result", "message_id"]
            )?
        )
    } }
}


/// Send plain-text edit message to chat.
async fn _send_edit_msg (db :&DB, chat_id :i64, message_id: i64, text: &str) -> Bresult<()> {
    info!("Telegram <= \x1b[36m{}", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    match
        Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(30,0))
                    .finish() )
        .finish()
        .get( db.url_api.clone() + "/editmessagetext")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["message_id", &message_id.to_string()],
                 ["text", &text],
                 ["disable_notification", "true"]]).unwrap()
        .send().await
    {
        Err(e) => {
            error!("Telegram \x1b[31m => {:?}", e);
        },
        Ok(mut result) => {
            ginfod!("Telegram => \x1b[35m", &result);
            ginfod!("Telegram => \x1b[1;35m", result.body().await);
        }
    }
    Ok(())
}


/// Send markdown-rich message to chat.
pub async fn send_msg_markdown (db :&DB, chat_id :i64, text: &str) -> Bresult<()> {
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
    .replacen("=", "\\=", 10000)
    .replacen("#", "\\#", 10000)
    .replacen("'", "\\'", 10000);

    info!("Telegram <= \x1b[33m{}\x1b[0m", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    let response =
        Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(30,0))
                    .finish() )
        .finish() // -> Client
        .get( db.url_api.clone() + "/sendmessage")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["text", &text],
                 ["parse_mode", "MarkdownV2"],
                 ["disable_notification", "true"]]).unwrap()
        .send()
        .await;

    match response {
        Err(e) => error!("\x1b[31m-> {:?}", e),
        Ok(mut r) => {
            ginfod!("Telegram => \x1b[35m", &r);
            ginfod!("Telegram => \x1b[1;35m", r.body().await);
        }
    }
    Ok(())
}

/// Send markdown-rich edit message to chat.
pub async fn send_edit_msg_markdown (db :&DB, chat_id :i64, message_id :i64, text: &str) -> Bresult<()> {
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
    .replacen("=", "\\=", 10000)
    .replacen("#", "\\#", 10000)
    .replacen("'", "\\'", 10000);

    info!("Telegram <= \x1b[33m{}\x1b[0m", text);
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    match Client::builder()
        .connector( Connector::new()
                    .ssl( builder.build() )
                    .timeout(Duration::new(30,0))
                    .finish() )
        .finish() // -> Client
        .get( db.url_api.clone() + "/editmessagetext")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::new(30,0))
        .query(&[["chat_id", &chat_id.to_string()],
                 ["message_id", &message_id.to_string()],
                 ["text", &text],
                 ["parse_mode", "MarkdownV2"],
                 ["disable_notification", "true"]]).unwrap()
        .send()
        .await
    { response => {
        ginfod!("Telegram => \x1b[1;35m", response?.body().await);
        Ok(())
    } }
}
