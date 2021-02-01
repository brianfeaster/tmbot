use ::log::*;
use actix_web::{web, App, HttpRequest, HttpServer, Responder, Route};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

async fn do_all(req: HttpRequest, body: web::Bytes) -> impl Responder { // -> Result<HttpResponse, Error> {
    info!("\x1b[0;1;36m{:?}\x1b[0;36m{:?}\x1b[30m", req, body);
    "thank you drive through"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("\x1b[35m{:?} {:?}\x1b[0m", ::pretty_env_logger::try_init(), std::env::args());
    info!("{}:{} ::{}::async-main()", std::file!(), core::line!(), core::module_path!());
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
    builder .set_private_key_file("key.pem", SslFiletype::PEM)?;
    builder.set_certificate_chain_file("cert.pem")?;
    HttpServer::new( || App::new()
            .service(
                web::resource("*")
                .route( (Route::new()).to(do_all) ) ) )
    .bind_openssl("127.0.0.1:8443", builder)?
    .run()
    .await
}