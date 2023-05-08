use std::ops::Range;

use actix_web::{web, Responder, get, HttpServer, App, post, HttpRequest};
use config::get_config;
use scylla::Session;

mod db;
mod config;
mod notify;


#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = &get_config().server.api_addr;
    unsafe { 
        SESSION = Some(db::init()
            .await
            .expect("could not connect to cluster")); 
    }
    HttpServer::new(|| {
            App::new()
            .service(fetch)
            .service(add)
            .service(rem)
        })
        .bind(addr)?
        .run()
        .await
}

static mut SESSION: Option<Session> = None; 

#[get("/hello/{name}")]
async fn greet(name: web::Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[get("/fetch/{email_type}/{range}")]
async fn fetch(path: web::Path<(Option<String>, Option<Range<usize>>)>) -> impl Responder {
    let conf = get_config().db.default_batch_size.unwrap();
    let (email_type, range) = path.into_inner();
    let email_type = email_type.unwrap_or("email".to_string());
    let range = range.unwrap_or(0..conf);
    let session = unsafe {
        SESSION.as_ref().expect("uninitialized session")
    };

    let db_resp = db::fetch(&session, email_type, range).await;
    db::to_json(db_resp)
}

#[post("/add/{email_type}")]
async fn add(path: web::Path<Option<String>>, body: web::Json<Vec<db::Combo>>, req: HttpRequest) -> impl Responder {
    let email_type = path.into_inner().unwrap_or("email".to_string());
    let session = unsafe {
        SESSION.as_ref().expect("uninitialized session")
    };
    let params = req.headers()
        .iter()
        .filter(|header| header.0.as_str().starts_with("p-"))
        .map(|header| {
            format!("{}|{}\n", &header.0.as_str()[2..], header.1.to_str().unwrap())
        })
        .reduce(|a, b| a + &b)
        .unwrap_or_default()
        .to_lowercase();
    let db_resp = db::add(session, email_type, body.0, params).await;
    db::to_json(db_resp)
}

#[post("/rem/{email_type}")]
async fn rem(path: web::Path<Option<String>>, body: web::Json<Vec<String>>) -> impl Responder {
    let email_type = path.into_inner().unwrap_or("email".to_string());
    let session = unsafe {
        SESSION.as_ref().expect("uninitialized session")
    };

    let db_resp = db::invalidate(session, email_type, body.0).await;
    db::to_json(db_resp)
}