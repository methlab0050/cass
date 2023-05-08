use std::ops::Range;

use actix_web::{web, Responder, get, HttpServer, App, post, HttpRequest};
use scylla::Session;

mod db;
mod config;
mod notify;


#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new().service(greet)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
//REVIEW - server address

static SESSION: Option<Session> = None; 

#[get("/hello/{name}")]
async fn greet(name: web::Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[get("/fetch/{email_type}/{range}")]
async fn fetch(path: web::Path<(Option<String>, Option<Range<usize>>)>) -> impl Responder {
    let (email_type, range) = path.into_inner();
    let (email_type, range, session) = (email_type.unwrap_or("email".to_string()), range.unwrap_or(0..10), SESSION.as_ref().unwrap());
    db::fetch(&session, email_type, range).await.unwrap();
    ""
}

#[post("/add/{email_type}")]
async fn add(path: web::Path<Option<String>>, body: web::Json<Vec<db::Combo>>, req: HttpRequest) -> impl Responder {
    let email_type = path.into_inner().unwrap_or("email".to_string());
    let session = SESSION.as_ref().unwrap();
    let params = req.headers()
    .iter()
    .filter(|header| header.0.as_str().starts_with("p-"))
    .map(|header| {
        format!("{}|{}\n", &header.0.as_str()[2..], header.1.to_str().unwrap())
    })
    .reduce(|a, b| a + &b)
    .unwrap_or_default()
    .to_lowercase();
    db::add(session, email_type, body.0, params).await.unwrap();
    
    ""
}

#[post("/rem/{email_type}")]
async fn rem(path: web::Path<Option<String>>, body: web::Json<Vec<String>>) -> impl Responder {
    let email_type = path.into_inner().unwrap_or("email".to_string());
    let session = SESSION.as_ref().unwrap();

    db::invalidate(session, email_type, body.0).await.unwrap();
    ""
}

