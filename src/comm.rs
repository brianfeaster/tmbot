//! # Communication with external services

use crate::*;

use ::std::{
    time::{Duration},
};
use ::actix_web::{
    client::{Client, Connector}};
use ::openssl::ssl::{SslConnector, SslMethod};
use ::log::*;


#[derive(Debug)]
pub struct MsgCmd {
    cmd: Cmd,
    chat_id: Option<i64>, // Unoverideable un-overridable destination chat_id
    level: i64,
}

impl From<&Cmd> for MsgCmd {
    fn from (cmd :&Cmd) -> Self {
        MsgCmd{cmd:cmd.copy(), chat_id:None, level:2}
    }
}

impl MsgCmd {
    pub fn to (mut self, to:i64) -> Self {
         self.chat_id = Some(to);
         self
    }
    pub fn level (mut self, level:i64) -> Self {
         self.level = level;
         self
    }
}

pub async fn send_msg(mc:MsgCmd, text:&str) -> Bresult<i64> {
    _send_msg(mc, 1, false, false, None, text).await
}

pub async fn send_msg_markdown(mc:MsgCmd, text:&str) -> Bresult<i64> {
    _send_msg(mc, 1, false, true, None, text).await
}

pub async fn _send_edit_msg(mc:MsgCmd, edit_msg_id:i64, text:&str) -> Bresult<i64> {
    _send_msg(mc, 1, true, false, Some(edit_msg_id), text).await
}

pub async fn send_edit_msg_markdown(mc:MsgCmd, edit_msg_id:i64, text:&str) -> Bresult<i64> {
    _send_msg(mc, 1, true, true, Some(edit_msg_id), text).await
}

pub async fn send_msg_id(mc:MsgCmd, text:&str) -> Bresult<i64> {
    _send_msg(mc, 0, false, false, None, text).await
}

pub async fn send_msg_markdown_id(mc:MsgCmd, text:&str) -> Bresult<i64> {
    _send_msg(mc, 0, false, true,  None, text).await
}

async fn _send_msg (
    mc: MsgCmd,
    target: i64, // 0->cmd.id   1->cmd.at
    is_edit_message: bool,
    is_markdown: bool,
    edit_msg_id: Option<i64>,
    text: &str
) -> Bresult<i64> {
    info!("Telegram \x1b[1;33m{:?}", mc);
    info!("Telegram \x1b[1;33mtarget:{} edit_message?:{} markdown?:{} edit_msg_id:{:?}", target, is_edit_message, is_markdown, edit_msg_id);
    info!("Telegram <= \x1b[36m{}", text);

    let cmd = &mc.cmd;
    let level = mc.level;
    let chat_id = if let Some(chat_id) = mc.chat_id {
        info!("Telegram chat_id {} forced", chat_id);
        chat_id // Forced msg to this ID
    } else if 1 == target && level <= cmd.at_level {
        info!("Telegram chat_id {} at group", cmd.at);
        cmd.at
    } else if level <= cmd.id_level {
        info!("Telegram chat_id {} id user", cmd.id);
        cmd.id
    } else {
        info!("Telegram chat_id {} default", cmd.env.chat_id_default);
        cmd.env.chat_id_default
    };
    let text = if is_markdown {
        text // Poor person's uni/url decode
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
        .replacen("'", "\\'", 10000)
    } else { text.to_string() };

    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file("key.pem", openssl::ssl::SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;
    let theurl =
        format!("{}/{}",
            cmd.env.url_api,
            if is_edit_message { "editmessagetext" } else { "sendmessage"} );
    let chat_id = chat_id.to_string();
    let mut query = vec![
        ["chat_id", &chat_id],
        ["text", &text],
        ["disable_notification", "true"],
    ];

    let mut edit_msg_id_str = String::new();
    if let Some(edit_msg_id) = edit_msg_id {
        edit_msg_id_str.push_str(&edit_msg_id.to_string());
        query.push( ["message_id", &edit_msg_id_str] )
    }

    if is_markdown {
        query.push(["parse_mode", "MarkdownV2"])
    }

    match Client::builder()
    .connector( Connector::new()
                .ssl( builder.build() )
                .timeout(Duration::new(30,0))
                .finish() )
    .finish()
    .get(theurl)
    .header("User-Agent", "Actix-web")
    .timeout(Duration::new(30,0))
    .query(&query)
    .unwrap()
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