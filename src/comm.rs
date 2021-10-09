//! # Telegram Communication
use crate::*;
//use util::*;
use ::std::{fmt, time::{Duration} };
use ::openssl::ssl::{SslConnector, SslMethod};
use ::actix_web::{ client::{Client, Connector} };


#[derive(Debug)]
pub struct MsgCmd<'a> {
    pub id: i64,
    pub at: i64,
    pub id_level: i64,
    pub at_level: i64,
    pub dm_id: Option<i64>, // Unoverideable un-overridable destination chat_id
    pub markdown: bool,
    pub msg_id: Option<i64>, // message_id to send as, set on return to message_id written
    pub msg: &'a str
}

impl<'a> MsgCmd<'a> {
    pub fn markdown(mut self) -> Self {
        self.markdown = true;
        self
    }
    pub fn dm(mut self, id:i64) -> Self {
        self.dm_id = Some(id);
        self
    }
    pub fn _message_id(mut self, id:i64) -> Self {
        self.msg_id = Some(id);
        self
    }
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
                        .timeout(Duration::new(30,0))
                        .finish())
                .finish();
        Ok(Telegram { client, url_api })
    }
}

impl Telegram {
    pub async fn send_msg<'a> (&self, mut mc: MsgCmd<'a>) -> Bresult<i64> {
        info!("{:?} {:?}", self, mc);
        let chat_id =
            if let Some(dm_id) = mc.dm_id {
                dm_id // Forced message to id
            } else if mc.id_level <= mc.at_level {
                mc.at // Message the channel if sender has better/equal privs than channel
            } else {
                mc.id // Message the "id" channel
            }.to_string();
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

        info!("Telegram <= \x1b[1;36m{:?}", query);

        let theurl =
            format!("{}/{}",
                self.url_api,
                if mc.msg_id.is_some() { "editmessagetext" } else { "sendmessage"} );

        let mut send_client_request =
            self.client
                .get(theurl)
                .header("User-Agent", "Actix-web")
                .timeout(Duration::new(30,0))
                .query(&query)
                .unwrap()
                .send().await?;

        ginfod!("Telegram => ", &send_client_request);
        let body = send_client_request.body().await;
        ginfod!("Telegram => \x1b[36m", body);

        // Return the message id so it may be edited/updated
        let message_id = getin_i64(&bytes2json(&body?)?, &["result", "message_id"])?;
        mc.msg_id = Some(message_id);
        Ok(message_id)
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