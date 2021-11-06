//! # Telegram Communication
use crate::*;
//use util::*;
use ::std::{fmt, time::{Duration} };
use ::openssl::ssl::{SslConnector, SslMethod};
use ::actix_web::{ client::{Client, Connector} };


pub trait MsgDetails {
    fn at (&self) -> i64;
    fn markdown (&self) -> bool;
    fn msg_id (&self) -> Option<i64>;
    fn msg (&self) -> &str;
}

#[derive(Debug)]
pub struct MsgCmd<'a> {
    pub at: i64,
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
    pub fn new (url_api: String) -> Bresult<Self> {
        let mut ssl_connector_builder = SslConnector::builder(SslMethod::tls())?;
        ssl_connector_builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
        ssl_connector_builder.set_certificate_chain_file("cert.pem")?;
        let client =
            Client::builder()
                .connector(
                    Connector::new()
                        .ssl( ssl_connector_builder.build() )
                        .timeout(Duration::new(90,0))
                        .finish())
                .finish();
        Ok(Telegram { client, url_api })
    }
}

impl Telegram {
    pub async fn send_msg<'a> (&self, obj: &'a impl MsgDetails) -> Bresult<MsgCmd<'a>> {
        let mut mc = MsgCmd {
            at:       obj.at(),
            markdown: obj.markdown(),
            msg_id:   obj.msg_id(),
            msg:      obj.msg()
        };
        info!("Telegram {:?}", mc);
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
        } else { mc.msg.to_string() };

        let mut query = vec![
            ["chat_id", &chat_id],
            ["text", &text],
            ["disable_notification", "true"],
        ];

        let mut edit_msg_id_str = String::new(); // Str must exist as long as query has ref
        if let Some(edit_msg_id) = mc.msg_id {
            edit_msg_id_str.push_str(&edit_msg_id.to_string());
            query.push( ["message_id", &edit_msg_id_str] )
        }

        if mc.markdown { query.push(["parse_mode", "MarkdownV2"]) }

        let theurl =
            format!("{}/{}",
                self.url_api,
                if mc.msg_id.is_some() { "editmessagetext" } else { "sendmessage"} );

        info!("Telegram <= \x1b[1;36m{:?} {:?}", theurl, query);

        let mut send_client_request =
            self.client
                .get(theurl)
                .header("User-Agent", "Actix-web TMBot/0.1.0")
                .timeout(Duration::new(90,0))
                .query(&query)
                .unwrap()
                .send().await?;

        ginfod!("Telegram => ", &send_client_request);
        let body = send_client_request.body().await;
        ginfod!("Telegram => \x1b[36m", body);

        // Return the new message's id
        mc.msg_id = Some( getin_i64(
            &bytes2json(&body?)?,
            &["result", "message_id"])? );
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