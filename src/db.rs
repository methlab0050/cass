use std::{ops::Range, collections::HashMap};

use scylla::{Session, transport::errors::{NewSessionError, QueryError, BadQuery}, batch::Batch, query::Query, prepared_statement::PreparedStatement, SessionBuilder, _macro_internal::CqlValue};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

    init_keyspace(&session, "emails").await?;

    for keyspace in &config.db.keyspaces {
        init_keyspace(&session, keyspace).await?;
    }

    Ok(session)
}

static mut TABLE_INIT_STMT: Option<(PreparedStatement, PreparedStatement)> = None;

pub async fn init_keyspace(session: &Session, email_type: &str) -> QueryResult<()> {
    let replication_factor = get_config().db.replication_factor.unwrap();
    let create_keyspace = Query::new(format!("CREATE  KEYSPACE IF NOT EXISTS {email_type} 
    WITH REPLICATION = {{ 
        'class' : 'SimpleStrategy', 
        'replication_factor' : {replication_factor} 
    }}"));
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
            ) WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {replication_factor} }}");
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
// TODO convert back to tuple
pub async fn fetch(session: &Session, email_type: String, range: Range<usize>) -> QueryResult<String> {
    session.use_keyspace(email_type, false).await?;
    if unsafe { FETCH_STMT.is_none() } {
        let stmt = session.prepare("SELECT * FROM main").await?;
        unsafe { FETCH_STMT = Some(stmt) };
    }

    let resp = if let Some(stmt) = unsafe { &FETCH_STMT } {
        let rows = session.execute_iter(stmt.clone(), &[])
            .await?;
        let column_names = rows
            .get_column_specs()
            .iter()
            .enumerate()
            .map(|(index, column_spec)| (index, column_spec.name.clone()))
            .collect::<HashMap<_, _>>();
        let json_response = rows.skip(range.start)
            .take(range.end - range.start)
            .filter_map(|c|async{ c.ok() })
            .map(|row|
                row.columns.into_iter()
                    .filter_map(|c| c)
                    .enumerate()
                    .filter_map(|(index, value)| {
                        Some((column_names.get(&index)?.to_string(), Value::String( match value {
                            CqlValue::Timestamp(lastcheck)=>lastcheck.to_string(),
                            CqlValue::Text(text)=>text.to_string(),
                            _ => panic!("Not expected in schema")
                        })))
                    })
                    .collect::<serde_json::map::Map<_, _>>()
            )
            .collect::<Vec<_>>()
            .await;
        serde_json::to_string(&json_response).unwrap_or("\"could not return expected value\"".to_owned())
    } else {
        return Err(
            QueryError::BadQuery(
                BadQuery::Other(
                    "Failed to cache prepared statement".to_owned()
                )
            )
        )
    };

    Ok(resp)
}

static mut INSERT_STMT: Option<PreparedStatement> = None;

pub async fn add(session: &Session, email_type: String, emails: Vec<Combo>, params: String) -> QueryResult<()> {
    session.use_keyspace(email_type, false).await?;

    let mut batch = Batch::default();

    if unsafe { INSERT_STMT.is_none() } {
        let stmt = session.prepare("INSERT INTO main (email, passw, lastcheck, p, id) VALUES (?, ?, 0, ?, ?)").await.unwrap();
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


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn streams() {
        let r = futures::stream::iter(0..10)
            .skip(5)
            .take(2)
            .collect::<Vec<_>>()
            .await;
        dbg!(r);
    }
}