use std::{net::SocketAddr, time::Duration};

use axum::{Router, routing::{get, post}, extract::Path};
use tokio::time::sleep;

use crate::db::{Keyspaces, graceful_shutdown};

mod db;
mod config;
mod notify;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/fetch", get(fetch))
        .route("/fetch/:keyspace", get(fetch_from_keyspace))
        .route("/fetch/:keyspace/:num", get(fetch_n_from_keyspace))
        .route("/add", post(add))
        .route("/add/:keyspace", post(add_to_keyspace))
        .route("/rem", post(invalidate))
        .route("/rem/:keyspace", post(invalidate_at_keyspace));


    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    
    println!("Starting server");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(async {
            sleep(Duration::from_secs(10)).await;
        })
        .await
        .unwrap();

    println!("Gracefully shutting down");
    // graceful_shutdown().await;
}

// static SESSION = 

fn get_keyspace(keyspace: Option<&&str>) -> Keyspaces {
    match keyspace {
        Some(&"discord") => Keyspaces::Discord,
        Some(&"valid") => Keyspaces::Valid,
        _ => Keyspaces::Email,
    }
}

async fn fetch() {}

async fn fetch_from_keyspace(Path(keyspace): Path<String>) {}

async fn fetch_n_from_keyspace(Path((keyspace, num)): Path<(String, u16)>) {}

async fn add() {}

async fn add_to_keyspace(Path(keyspace): Path<String>,) {}

async fn invalidate() {}

async fn invalidate_at_keyspace(Path(keyspace): Path<String>,) {}
