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
    let headers = req.headers();
    let get_header = |header_key| headers.get(header_key);
    let Some(auth_header) = get_header("auth")
        .or(get_header("authentication")) 
        else { return false; };
    dbg!(auth_header);
    let auth_header = String::from_utf8_lossy(auth_header.as_bytes()).into_owned();
    dbg!(&auth_header);
    get_config().auth.api_keys.contains(&auth_header)
}

static mut SESSION: Option<Session> = None; 

#[get("/hello/{name}")]
async fn greet(name: Path<String>) -> impl Responder {
    format!("Hello {name}!")
}

#[get("/fetch/{email_type}")]
async fn fetch(path: Path<String>, req: HttpRequest) -> impl Responder {
    if auth(&req) == false {
        return "\"not authenticated\"".to_owned();
    }
    let conf = get_config().db.default_batch_size.unwrap();
    let email_type = path.into_inner();
    let range = 0..conf;
    let session = unsafe {
        SESSION.as_ref().expect("uninitialized session")
    };

    let db_resp = db::fetch(&session, email_type, range).await;
    db::to_json(db_resp)
}

#[post("/add/{email_type}")]
async fn add(path: Path<String>, body: Json<Vec<String>>, req: HttpRequest) -> impl Responder {
    if auth(&req) == false {
        return "\"not authenticated\"".to_owned();
    }
    let email_type = path.into_inner();
    let session = unsafe {
        SESSION.as_ref().expect("uninitialized session")
    };
    let body = body.0
        .into_iter()
        .filter_map(|email_n_passw| {
            let email_n_passw = email_n_passw.split(":")
                .collect::<Vec<&str>>();
            let email = match email_n_passw.get(0) {
                Some(str) => (*str).to_owned(),
                None => return None,
            };
            let password = match email_n_passw.get(1) {
                Some(str) => (*str).to_owned(),
                None => return None,
            };
            Some(db::Combo{ email, password })
        })
        .collect();
    let params = req.headers()
        .iter()
        .filter(|header| header.0.as_str().starts_with("p-"))
        .map(|header| {
            format!("{}|{}\n", &header.0.as_str()[2..], header.1.to_str().unwrap())
        })
        .reduce(|a, b| a + &b)
        .unwrap_or_default()
        .to_lowercase();
    let db_resp = db::add(session, email_type, body, params).await;
    match db_resp {
        Ok(_) => "\"success\"".to_string(),
        Err(err) => err,
    }
}

#[post("/rem/{email_type}")]
async fn rem(path: Path<String>, body: Json<Vec<String>>, req: HttpRequest) -> impl Responder {
    if auth(&req) == false {
        return "\"not authenticated\"".to_owned();
    }
    let email_type = path.into_inner();
    let session = unsafe {
        SESSION.as_ref().expect("uninitialized session")
    };

    let db_resp = db::invalidate(session, email_type, body.0).await;
    db::to_json(db_resp)
}