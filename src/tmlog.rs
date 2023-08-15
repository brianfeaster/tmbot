use crate::{
    comm, glogd, httpResponseNotFound, httpResponseOk, rt, util::*, App, CmdStruct, Env,
    HttpResponse, HttpServer, Instant, IF,
};

fn header_tmlog() {
    println!();
    info!("{RST}{RED}{B_BLK} _____ __  __ _          __ _™");
    info!("{RST}{YEL}{B_BLK}|_   _|  \\/  | |    ___ / _` |");
    info!("{RST}{GRN}{B_BLK}  | | | |\\/| | |   / _ \\|(_| |");
    info!("{RST}{BLU}{B_BLK}  | | | |  | | |__| (_) \\__, /");
    info!("{RST}{MAG}{B_BLK}  |_| |_|  |_|_____\\___/|___/ ");
}

fn tmlog(req: HttpRequest, body: web::Bytes) -> Bresult<()> {
    header_tmlog();
    info!("{}", httpReqPretty(&req, &body));
    let capv = must_regex_to_vec(r#"(?x) ^ /(-?\d+) (/(-?\d+))? (/(.*))? $ "#, &req.path())?;
    let env = req.app_data::<web::Data<Env>>().ok_or("tmlog app_data")?.get_ref().clone();
    let at    = capv.as_i64(1)?;
    let topic = capv.as_str(3).unwrap_or("").to_string();
    let message =
        capv.as_str(5).unwrap_or("").split('/').collect::<Vec<_>>().join(" ")
        + from_utf8(&body)?;
    let mut cmdstruct = CmdStruct::new(env, Instant::now().seconds(), 0, at, 0, topic, "".into())?;
    rt::spawn( async move {
        glogd!("--tmlog", cmdstruct.push_msg(&message).send_msg().await)
    });
    Ok(())
}

async fn handler_tmlog(req: HttpRequest, body: web::Bytes) -> HttpResponse {
    let res = tmlog(req, body);
    glogd!("::tmlog", res);
    IF!(res.is_ok(), httpResponseOk!(), httpResponseNotFound!())
}

pub fn start_tmlog(env: Env) -> Bresult<()> {
    let ssl_acceptor_builder = {
        let envstruct = env.lock().unwrap();
        comm::new_ssl_acceptor_builder(&envstruct.tmbot_key, &envstruct.tmbot_cert)?
    };
    info!("::TMLOG");
    thread::Builder::new().name("httptmlog".into()).spawn(move ||
        glogd!("--TMLOG",
            match HttpServer::new(move ||
                App::new().app_data(web::Data::new(env.clone())).route("{tail:.*}", web::to(handler_tmlog)))
            .workers(2)
            .bind_openssl("0.0.0.0:7065", ssl_acceptor_builder)
        {
            Ok(httpserver) => rt::System::new().block_on(async move { httpserver.run().await }),
            Err(err) => Err(err.into())
        })
    )?; // Thread returns after SIGINT

    Ok(())
}

