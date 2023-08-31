use crate::*;

fn header() {
    println!();
    info!("{RST}{RED}{B_BLK} _____ __  __ _          __ _™");
    info!("{RST}{YEL}{B_BLK}|_   _|  \\/  | |    ___ / _` |");
    info!("{RST}{GRN}{B_BLK}  | | | |\\/| | |   / _ \\|(_| |");
    info!("{RST}{BLU}{B_BLK}  | | | |  | | |__| (_) \\__, /");
    info!("{RST}{MAG}{B_BLK}  |_| |_|  |_|_____\\___/|___/ ");
}

fn tmlog(req: HttpRequest, body: web::Bytes) -> Bresult<()> {
    header();
    info!("{}", httpReqPretty(&req, &body));

    let capv = must_re_to_vec(regex!(r#"(?x) ^ /(-?\d+) (/(-?\d+))? (/(.*))? $ "#), &req.path())?;

    let at    = capv.as_i64(1)?;
    let topic = capv.as_string_or("", 3);

    let urltostr = capv.as_str_or("", 5).split('/').collect::<Vec<_>>().join(" ").replace("+", " ");

    let urlPathAndBodyMessage =
       percent_decode_str(&urltostr).decode_utf8()?
        + trimmedQuotes(from_utf8(&body)?);

    Env::new(req.app_data::<WebData>().ok_or("tmlog app_data")?.clone(),
            Instant::now().seconds(),
            0, at, 0, topic, "".into(), false)?
        .push_msg(&urlPathAndBodyMessage)
        .send_msg()
}

async fn handler_tmlog(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    let res = tmlog(req, body);
    glogd!("::tmlog", res);
    IF!(res.is_ok(), httpResponseOk!(), httpResponseNotFound!())
}

pub fn start(wd: WebData) -> Bresult<()> {
    let ssl_acceptor_builder = {
        let tge = tgelock!(wd)?;
        comm::new_ssl_acceptor_builder(&tge.tmbot_key, &tge.tmbot_cert)?
    };
    info!("::TMLOG");
    thread::Builder::new().name("httptmlog".into()).spawn(move ||
        glogd!("--TMLOG",
            match HttpServer::new(move ||
                App::new()
                    .app_data(wd.clone())
                    .route("{tail:.*}", web::to(handler_tmlog)))
            .workers(2)
            .bind_openssl("0.0.0.0:7065", ssl_acceptor_builder)
        {
            Ok(httpserver) => rt::System::new().block_on(httpserver.run()),
            Err(err) => Err(err.into())
        })
    )?; // Thread returns after SIGINT

    Ok(())
}
