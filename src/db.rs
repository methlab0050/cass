use std::ops::Range;

use scylla::{Session, transport::errors::{NewSessionError, QueryError, BadQuery}, batch::Batch, query::Query, prepared_statement::PreparedStatement, SessionBuilder, _macro_internal::CqlValue::{Timestamp, Text}};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};
use uuid::Uuid;

use crate::config::get_config;

#[derive(Deserialize)]
pub struct Combo {
    pub email: String,
    pub password: String,
}

type QueryResult<T> = Result<T, QueryError>;

pub async fn init() -> Result<Session, NewSessionError> {
    let config = get_config();
    let session = SessionBuilder::new()
        .known_nodes(&config.server.node_addrs)
        .user(&config.auth.username, &config.auth.password)
        .build()
        .await?;

    init_keyspace(&session, "emails").await
        .expect("Could not initalize default keyspace, \"emails\"");

    for keyspace in &config.db.keyspaces {
        match init_keyspace(&session, keyspace).await {
            Ok(_) => {
                println!("Keyspace for {:?} initalized", keyspace)
            }
            Err(err) => {
                eprintln!("Could not initialize keyspace for {keyspace:?}\n    {err}")
            }
        };
    }

    Ok(session)
}

static mut TABLE_INIT_STMT: Option<(PreparedStatement, PreparedStatement)> = None;

pub async fn init_keyspace(session: &Session, email_type: &str) -> QueryResult<()> {
    let replication_factor = get_config().db.replication_factor.unwrap();
    let create_keyspace = Query::new(format!("CREATE  KEYSPACE IF NOT EXISTS {} 
    WITH REPLICATION = {{ 
        'class' : 'SimpleStrategy', 
        'replication_factor' : {}
    }}", email_type, replication_factor));
    session.query(create_keyspace, []).await?;
    session.use_keyspace(email_type, false).await?;
    match unsafe { &TABLE_INIT_STMT } {
        Some((main, used)) => {
            session.execute(main, []).await?;
            session.execute(used, []).await?;
        },
        None => {
            let query = |table| format!("CREATE TABLE IF NOT EXISTS {table} (
                id text PRIMARY KEY, 
                email text, 
                passw text, 
                lastcheck timestamp, 
                p text
            )");
            let used = session.prepare(query("used")).await?;
            let main = session.prepare(query("main")).await?;
            session.execute(&main, []).await?;
            session.execute(&used, []).await?;
            unsafe { TABLE_INIT_STMT = Some((main, used)); }
        },
    };
    Ok(())
}

static mut FETCH_STMT: Option<PreparedStatement> = None;

pub async fn fetch(session: &Session, email_type: String, range: Range<usize>) -> QueryResult<String> {
    let batch_size = get_config().db
        .max_batch_size
        .unwrap_or(1000)
        .min(range.end - range.start);

    session.use_keyspace(email_type.clone(), false).await?;
    
    if unsafe { FETCH_STMT.is_none() } {
        let stmt = session.prepare("SELECT * FROM main").await?;
        unsafe { FETCH_STMT = Some(stmt) };
    }

    let stmt = unsafe { 
        FETCH_STMT.as_ref().ok_or(
            QueryError::BadQuery(
                BadQuery::Other(
                    "Failed to cache prepared statement".to_owned()
                )
            )
        )? 
    };

    let rows = session.execute_iter(stmt.clone(), &[])
        .await?;
    let column_names = rows.get_column_specs()
        .iter()
        .map(|column_spec| column_spec.name.clone())
        .collect::<Vec<_>>();
    let mut rows = rows.skip(range.start)
        .take(batch_size)
        .collect::<Vec<_>>()
        .await
        .into_iter();

    let mut json_resp = Vec::new();

    let mut invalid_ids = Vec::new();

    while let Some(Ok(row)) = rows.next() {
        let mut columns = row.columns.into_iter().enumerate();

        let mut table = Map::new(); 

        
        while let Some((index, Some(value))) = columns.next() {
        
            let column_name = column_names[index].to_owned();

            let value = match value {
                Timestamp(lastcheck)=>lastcheck.to_string(),
                Text(text)=>text.to_string(),
                _ => panic!("Not expected in schema")
            };

            if column_name == "id".to_owned() {
                invalid_ids.push(value.clone());
            }

            table.insert(column_name, Value::String(value));
        }

        json_resp.push(table);
    }

    invalidate(session, email_type, invalid_ids).await.err();

    let resp = serde_json::to_string(&json_resp)
        .unwrap_or("\"could not return expected value\"".to_owned());

    Ok(resp)
}

static mut INSERT_STMT: Option<PreparedStatement> = None;

pub async fn add(session: &Session, email_type: String, emails: Vec<Combo>, params: String) -> QueryResult<()> {
    session.use_keyspace(email_type, false).await?;

    let mut batch = Batch::default();

    if unsafe { INSERT_STMT.is_none() } {
        let stmt = "INSERT INTO main (email, passw, lastcheck, p, id) VALUES (?, ?, 0, ?, ?)";
        let stmt = session.prepare(stmt).await?;
        unsafe { INSERT_STMT = Some(stmt) };
    }

    if let Some(stmt) = unsafe { &INSERT_STMT } {
        for _ in 0..emails.len() {
            batch.append_statement(stmt.clone());
        }
    
        session.batch(&batch, convert_to_tuple(emails, params)).await?;
    }

    Ok(())
}

fn convert_to_tuple(emails: Vec<Combo>, param: String) -> Vec<(String, String, String, String)> {
    emails.into_iter()
        .map(|combo| {
            (combo.email, combo.password, param.clone(), Uuid::new_v4().to_string())
        })
        .collect::<Vec<_>>()
}

pub async fn invalidate(session: &Session, email_type: String, combo_ids: Vec<String>) -> QueryResult<()> {
    session.use_keyspace(email_type, false).await?;
    let mut batch = Batch::default();
    let mut batch_values: Vec<()> = Vec::new();

    for id in combo_ids {
        let query = Query::new(format!("DELETE FROM main WHERE id = {id}"));
        batch.append_statement(query);
        batch_values.push(());
    }

    session.batch(&batch, batch_values).await?;

    Ok(())
}

pub fn to_json<'a, T: Deserialize<'a> + Serialize>(res: QueryResult<T>) -> std::string::String {
    match res {
        Ok(value) => serde_json::to_string(&value).unwrap_or("\"could not return expected value\"".to_owned()),
        Err(err) => format!("\"{err}\"")
    }
}
