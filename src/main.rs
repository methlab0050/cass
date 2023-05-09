use std::ops::Range;

use actix_web::{web::{Path, Json}, Responder, get, HttpServer, App, post, HttpRequest};
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

fn auth(req: &HttpRequest) -> bool {
    let Some(auth_header) = req.headers().get("auth") else { return false; };
    let Ok(auth_header) = auth_header.to_str() else { return false; };
    get_config().auth.api_keys.contains(&auth_header.to_owned())
}

static mut SESSION: Option<Session> = None; 

#[get("/hello/{name}")]
async fn greet(name: Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[get("/fetch/{email_type}/{range}")]
async fn fetch(path: Path<(Option<String>, Option<Range<usize>>)>, req: HttpRequest) -> impl Responder {
    if auth(&req) == false {
        return "\"not authenticated\"".to_owned();
    }
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
async fn add(path: Path<Option<String>>, body: Json<Vec<db::Combo>>, req: HttpRequest) -> impl Responder {
    if auth(&req) == false {
        return "\"not authenticated\"".to_owned();
    }
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
async fn rem(path: Path<Option<String>>, body: Json<Vec<String>>, req: HttpRequest) -> impl Responder {
    if auth(&req) == false {
        return "\"not authenticated\"".to_owned();
    }
    let email_type = path.into_inner().unwrap_or("email".to_string());
    let session = unsafe {
        SESSION.as_ref().expect("uninitialized session")
    };

    let db_resp = db::invalidate(session, email_type, body.0).await;
    db::to_json(db_resp)
}