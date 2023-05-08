use std::ops::Range;

// use axum::http::status::StatusCode;
use scylla::{Session, SessionConfig, transport::errors::{NewSessionError, QueryError, BadQuery}, batch::Batch, query::Query, prepared_statement::PreparedStatement, SessionBuilder};
use futures::stream::StreamExt;
use serde::Deserialize;

//ANCHOR - Important
#[derive(Deserialize)]
pub struct Combo {
    pub email: String,
    pub password: String,
}

type QueryResult<T> = Result<T, QueryError>;

pub async fn init() -> Result<Session, NewSessionError> {
    let session = SessionBuilder::new()
        // .known_nodes(&[])
        // .user(username, passwd)
        // .pool_size(size)
        // .ssl_context(ssl_context)
        .build()
        .await?;

    for keyspace in ["".to_string()] {
        init_keyspace(&session, keyspace).await?;
    }

    Ok(session)
}
// REVIEW - config nodes and keyspaces

static mut TABLE_INIT_STMT: Option<(PreparedStatement, PreparedStatement)> = None;

pub async fn init_keyspace(session: &Session, email_type: String) -> QueryResult<()> {
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
            ) WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}");
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

pub async fn fetch(session: &Session, email_type: String, range: Range<usize>) -> Result<Vec<(String, String, String, String, String)>, QueryError> {
    session.use_keyspace(email_type, false).await?;
    if unsafe { FETCH_STMT.is_none() } {
        let stmt = session.prepare("SELECT * FROM main").await?;
        unsafe { FETCH_STMT = Some(stmt) };
    }

    let rows = if let Some(stmt) = unsafe { &FETCH_STMT } {
        session.execute_iter(stmt.clone(), &[])
            .await?
            .into_typed::<(String, String, String, String, String)>()
            .skip(range.start)
            .take(range.end - range.start)
            .filter_map(|x| async { x.ok() })
            .collect::<Vec<_>>()
            .await
    } else {
        return Err(
            QueryError::BadQuery(
                BadQuery::Other(
                    "Failed to cache prepared statement".to_owned()
                )
            )
        )
    };

    Ok(rows)
}

static mut INSERT_STMT: Option<PreparedStatement> = None;

pub async fn add(session: &Session, email_type: String, emails: Vec<Combo>, params: String) -> QueryResult<()> {
    session.use_keyspace(email_type, false).await?;

    let mut batch = Batch::default();

    if unsafe { INSERT_STMT.is_none() } {
        let stmt = session.prepare("INSERT INTO main (email, passw, lastcheck, p, id) VALUES (?, ?, 0, ?, ?)").await.unwrap();
        // TODO - test if cqlsh will auto produce ids
        unsafe { INSERT_STMT = Some(stmt) };
    }

    if let Some(stmt) = unsafe { &INSERT_STMT } {
        for _ in 0..emails.len() {
            batch.append_statement(stmt.clone());
        }
    
        session.batch(&batch, sanitize(emails)).await?;
    }

    Ok(())
}

fn sanitize(emails: Vec<Combo>) -> &'static [(String, &'static str, &'static str, &'static str)] {
    // (email, passw, lastcheck, p, id)
    todo!()
    // TODO
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

