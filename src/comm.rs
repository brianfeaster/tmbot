//! # Telegram Communication
use crate::*;
//use util::*;
use ::std::{fmt, time::{Duration} };
use ::openssl::ssl::{SslConnector, SslMethod};
use ::actix_web::{ client::{Client, Connector} };


#[derive(Debug)]
pub struct MsgCmd {
    pub id: i64,
    pub at: i64,
    pub id_level: i64,
    pub at_level: i64,
    pub url_api: String,
    pub chat_id: Option<i64>, // Unoverideable un-overridable destination chat_id
    pub level: i64,
}

impl MsgCmd {
    // force a message to this id/channel
    pub fn _to (mut self, to:i64) -> Self {
         self.chat_id = Some(to);
         self
    }
    pub fn _level (mut self, level:i64) -> Self {
         self.level = level;
         self
    }
}

////////////////////////////////////////

pub struct Telegram {
    client: Client
}

impl Telegram {
    pub fn new () -> Bresult<Self> {
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
        Ok(Telegram { client })
    }
    pub async fn send_msg (&self, mc:MsgCmd, text:&str) -> Bresult<i64> {
        send_msg(&self.client, mc, 1, false, false, None, text).await
    }

    pub async fn send_msg_markdown (&self, mc:MsgCmd, text:&str) -> Bresult<i64> {
        send_msg(&self.client, mc, 1, false, true, None, text).await
    }

    pub async fn send_edit_msg (&self, mc:MsgCmd, edit_msg_id:i64, text:&str) -> Bresult<i64> {
        send_msg(&self.client, mc, 1, true, false, Some(edit_msg_id), text).await
    }

    pub async fn send_edit_msg_markdown (&self, mc:MsgCmd, edit_msg_id:i64, text:&str) -> Bresult<i64> {
        send_msg(&self.client, mc, 1, true, true, Some(edit_msg_id), text).await
    }

    pub async fn send_msg_id (&self, mc:MsgCmd, text:&str) -> Bresult<i64> {
        send_msg(&self.client, mc, 0, false, false, None, text).await
    }

    pub async fn send_msg_markdown_id (&self, mc:MsgCmd, text:&str) -> Bresult<i64> {
        send_msg(&self.client, mc, 0, false, true,  None, text).await
    }
} // impl Telegram

impl fmt::Debug for Telegram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
    {
        f.debug_struct("Telegram")
         .field("client", &format!("Client Object"))
         .finish()
    }
}

async fn send_msg (
    client: &Client,
    mc: MsgCmd,
    target: i64, // 0->cmd.id   1->cmd.at
    is_edit_message: bool,
    is_markdown: bool,
    edit_msg_id: Option<i64>,
    text: &str
) -> Bresult<i64> {
    info!("Telegram \x1b[1;33m{:?}  target:{}  edit_message?:{}  markdown?:{}  edit_msg_id:{:?}  text:{:?}",
        mc, target, is_edit_message, is_markdown, edit_msg_id, text);

    // Message either: 
    let chat_id =
        if let Some(chat_id) = mc.chat_id {
            chat_id // Forced message to id
        } else if 1 == target && mc.level <= mc.at_level {
            mc.at // Message the channel if sender has better/equal privs than channel
        } else {
            mc.id // Message the "id" channel
        }.to_string();
    let text = if is_markdown {
        text // Quick and dirty uni/url decode
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
    } else { text.to_string() };

    let theurl =
        format!("{}/{}",
            mc.url_api,
            if is_edit_message { "editmessagetext" } else { "sendmessage"} );

    let mut query = vec![
        ["chat_id", &chat_id],
        ["text", &text],
        ["disable_notification", "true"],
    ];

    let mut edit_msg_id_str = String::new(); // Str must exist as long as query has ref
    if let Some(edit_msg_id) = edit_msg_id {
        edit_msg_id_str.push_str(&edit_msg_id.to_string());
        query.push( ["message_id", &edit_msg_id_str] )
    }

    if is_markdown { query.push(["parse_mode", "MarkdownV2"]) }

    info!("Telegram <= \x1b[1;36m{:?}", query);

    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;

    let mut send_client_request =
        client
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
    Ok( getin_i64(
            &bytes2json(&body?)?,
            &["result", "message_id"]
        )?
    )
}