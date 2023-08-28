//! # Telegram Communication

use crate::util::*;
use actix_web::{rt};
use openssl::ssl::{
    NameType, SniError, SslAcceptor, SslAcceptorBuilder, SslAlert, SslFiletype, SslMethod, SslRef
};
use std::{
    sync::mpsc::{Receiver, Sender, channel}
};

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

#[derive(Debug)]
struct Msg {
    pub at: i64,
    pub topic: String,
    pub markdown: bool,
    pub msg_id: Option<i64>,
    pub msg: String
}

pub trait MsgDetails {
    fn at (&self) -> i64;
    fn topic (&self) -> String;
    fn markdown (&self) -> bool;
    fn msg_id (&self) -> Option<i64>;
    fn set_msg_id (&mut self, i: Option<i64>);
    fn msg (&self) -> String;
}

impl From<&dyn MsgDetails> for Msg {
    fn from(o: &dyn MsgDetails) -> Msg {
        Msg{at:o.at(), topic:o.topic(), markdown:o.markdown(), msg_id:o.msg_id(), msg:o.msg()}
    }
}

#[derive(Debug)]
pub struct Messenger {
    sender: Sender<Msg>,
    recver: Receiver<Msg>
}

impl Messenger {
    pub fn new(url: String) -> Bresult<Messenger> {
        let (sendMesg, recvMesg) = channel::<Msg>();
        let (sendResp, recvResp) = channel::<Msg>();
        thread::spawn(move || crate::glog!(messageLoop(url, recvMesg, sendResp)));
        Ok(Messenger{sender: sendMesg, recver: recvResp})
    }
    pub fn send(&self, msg: &mut dyn MsgDetails) -> Bresult<()> {
        self.sender.send((&*msg).into())?;
        msg.set_msg_id(self.recver.recv()?.msg_id);
        Ok(())
    }
}

fn messageLoop(url: String, recvMesg: Receiver<Msg>, sendResp: Sender<Msg>) -> Bresult<()> {
    let client = newHttpsClient()?;
    rt::System::new().block_on(async { loop {
        let mut msg = recvMesg.recv()?;
        info!("{BLD}::MESSENGER~{RST} {:?}", msg);
        let res =
            if msg.msg.is_empty() && msg.msg_id.is_some() {
                del_msg(&client, &url, &mut msg).await
            } else {
                send_msg(&client, &url, &mut msg).await
            };
        info!("{BLD}--MESSENGER~{RST} -> {:?}", res);
        sendResp.send(msg).map_err(|e| error!("{:?}", e)).ok();
    }})
}

async fn send_msg(client: &Client, url: &str, msg: &mut Msg) -> Bresult<()> {
    let chat_id = msg.at.to_string();
    let text = if msg.markdown {
        msg.msg // Quick and dirty uni/url decode
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
    } else { msg.msg.to_string() };

    let mut query = vec![
        ["chat_id", &chat_id],
        ["text", &text],
        ["disable_notification", "true"],
        ["disable_web_page_preview", "true"],
    ];

    let mut edit_msg_id_str = String::new(); // Str must exist as long as query has ref
    if let Some(edit_msg_id) = msg.msg_id {
        edit_msg_id_str.push_str(&edit_msg_id.to_string());
        query.push( ["message_id", &edit_msg_id_str] )
    }

    if "" != msg.topic {
       query.push(["message_thread_id", &msg.topic])
    }

    if msg.markdown { query.push(["parse_mode", "MarkdownV2"]) }

    let theurl =
        format!("{}/{}",
            url,
            crate::IF!(msg.msg_id.is_some(), "editmessagetext", "sendmessage"));

    let clientRequest =
        client
            .get(theurl)
            .query(&query)?;

    info!("{}", reqPretty(&clientRequest, &""));

    let mut resp = clientRequest.send().await?;
    let body = from_utf8(&resp.body().await?)?.to_string();
    info!("{}", resPretty(&resp, &body));

    // Record new message id
    getin_i64(&serde_json::from_str(&body)?, "/result/message_id")
        .map(|i| msg.msg_id = Some(i))
        .ok();
    Ok(())
}

async fn del_msg(client: &Client, url: &str, msg: &mut Msg) -> Bresult<()> {
    let chat_id = msg.at.to_string();
    let edit_msg_id_str = msg.msg_id.ok_or("missing msg_id")?.to_string();

    let clientRequest = client
        .get(format!("{}/deleteMessage", url))
        .query(&vec![
            ["chat_id", &chat_id],
            ["message_id", &edit_msg_id_str]])?;

    info!("{}", reqPretty(&clientRequest, &""));

    let mut resp = clientRequest.send().await?;
    let body = from_utf8(&resp.body().await?)?.to_string();
    info!("{}", resPretty(&resp, &body));

    Ok(())
}
