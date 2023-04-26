use std::{fmt::Display, sync::atomic::{AtomicUsize, Ordering}};

use async_std::stream::StreamExt;
use axum::http::status::StatusCode;
use scylla::{Session, SessionConfig, transport::errors::NewSessionError, batch::Batch, query::Query};
use serde_json::json;

pub enum Keyspaces {
    Valid,
    Discord,
    Email,
}

impl Display for Keyspaces {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Keyspaces::Valid => f.write_str("valid"),
            Keyspaces::Discord => f.write_str("discord"),
            Keyspaces::Email => f.write_str("email"),
        }.unwrap()
        ;todo!()
    }
}

pub struct Combo {
    email: String,
    password: String,
}

pub async fn init() -> Result<Session, NewSessionError> {
    let mut config = SessionConfig::new();

    let nodes = [""];

    config.add_known_nodes(&nodes);
    // config.ssl_context = 
    // TODO

    Session::connect(config).await
}

pub async fn graceful_shutdown(session: Session) {
    // TODO - find wut to shutdown
}

fn get_table(keyspace: &String, index: &mut AtomicUsize) -> (String, usize) {
    let new_index = if index.get_mut() >= &mut 10 {
        // TODO -                                ^^&mut CONFIG.cassandra.total_tables.clone()
        0
    } else {
        index.get_mut().clone() + 1
    };

    index.swap(new_index, Ordering::Relaxed);

    (format!("{}.t{}", keyspace, new_index), new_index)
}

pub async fn fetch(session: Session, keyspace: String, num: u16, index: &mut AtomicUsize) {
    let (table, _) = get_table(&keyspace, index);
        
    let statement = format!(
        "SELECT * FROM {table}
    LIMIT {}
    ALLOW FILTERING
    ", 0
    // CONFIG.settings.batch_size
    );
    let mut statement = Query::new(statement);
    statement.set_page_size(12);
    //CONFIG.settings.batch_size

    let mut emails = match session.query_iter(statement, ()).await {
        Ok(val) => val,
        Err(err) => {
            return ();
            (StatusCode::INTERNAL_SERVER_ERROR, json!({
                "errors": err.to_string()
            }));
        },
    };
    let column_names = emails.get_column_specs()
        .iter()
        .map(|x| x.name.clone());
    // let emails = emails.iter().collect::<Vec<_>>();

//     let mut data = Vec::new();
//     let mut errors = Vec::new();

    // CONFIG.settings.batch_size
    for i in 0..60 {
        let Some(row) = emails.next().await else { break; };
        let Ok(row) = row else { continue; };

//         macro_rules! get {
//             ($name:literal) => {
//                 match row.get_column_by_name($name) {
//                     Ok(val) => val.to_string(),
//                     Err(err) => {
//                         errors.push(format!("{}", err));
//                         continue;
//                     },
//                 }
//             };
//         }

//         let email = get!("email");
//         let password = get!("passw");
//         let params = get!("p");
//         let id = get!("id");

//         let combo = FullCombo { email, password, params, id: id.to_owned() };
//         data.push(combo);
//         self.invalidate_combo(id);
    }
    
//     match (data.is_empty(), errors.is_empty()) {
//         (false, false) => {
//             (Status::Ok, json!({
//                 "errors": errors,
//                 "data": data,
//             }))
//         }
//         (false, true) => {
//             (Status::Ok, json!({
//                 "data": data,
//             }))
//         },
//         (true, false) => {
//             (Status::InternalServerError, json!({
//                 "errors": errors
//             }))
//         }
//         (true, true) => {
//             (Status::ImATeapot, json!("There're no errors... but no data either.... What???? This error should be infalible, but just to be sure, I'm putting a very serious 418 status code on it"))
//         }
//     }
}

fn generate_id(i: usize) -> String {
    format!("{}-{}", i, &uuid::Uuid::new_v4().to_string().replace("-", "")[..8])
}

pub async fn add(session: Session, keyspace: String, combos: Vec<Combo>, index: &mut AtomicUsize) -> serde_json::Value {
    let mut batch = Batch::default();
    let (table, i) = get_table(&keyspace, index);
    let query = format!("INSERT INTO {table} (email, passw, lastcheck, p, id) VALUES (?, ?, 0, ?, ?)");
    let query = session
        .prepare(query)
        .await
        .unwrap();

    let mut added = 0;

    let mut batch_values = Vec::new();

    for combo in combos {
        added += 1;
        let params = String::new();
        batch.append_statement(query.clone());
        batch_values.push((sanitize(combo.email), sanitize(combo.password), sanitize(params), generate_id(i)));
    }

    match session.batch(&batch, batch_values).await {
        Ok(_) => json!({
            "success": format!("added {} combos to the database", added)
        }),
        Err(err) => json!({
            "failure": "failed to add combos to the database",
            "error": err.to_string()
        }),
    }
}

fn get_table_by_uuid(keyspace: String, id: String) -> String {
    format!("{}.t{}", keyspace, id.split("-").nth(0).unwrap())
}

fn sanitize<S: AsRef<str>>(input: S) -> String {
    input.as_ref().replace("'", "''")
}

pub async fn invalidate(session: Session, keyspace: String, id: String) {
    let table = get_table_by_uuid(keyspace, id.clone());
    // let statement = format!("DELETE FROM {table} WHERE id = '{}'", sanitize(id));
    // let statement = stmt!(&statement);
    // session.execute(&statement).await.err()
}

// pub async fn table_init(&self) {
//     let statement = stmt!(&format!(
//         "CREATE KEYSPACE IF NOT EXISTS {}
//     WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}",
//         self.keyspace
//     ));

//     match self.session.execute(&statement).await {
//         Err(err) => {
//             eprintln!("Error: failed to create keyspace {:?} {}", self.keyspace, err);
//             return;
//         }
//         Ok(_) => println!("Created keyspace {}", self.keyspace),
//     }

//     for i in 0..CONFIG.cassandra.total_tables {
//         let table_name = format!("{}.t{}", self.keyspace, i);
//         println!("Creating table {table_name}...");
//         let statement = stmt!(&format!("CREATE TABLE IF NOT EXISTS {table_name} (id text PRIMARY KEY, email text, passw text, lastcheck timestamp, p text)"));
//         match self.session.execute(&statement).await {
//             Ok(_) => println!("Created table {}", table_name),
//             Err(err) => eprintln!("Failed to create table {table_name}: {err}"),
//         }
//     }

//     println!("Created all tables")
// }