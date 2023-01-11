use flate2::read::MultiGzDecoder;
use reqwest::blocking::Client;
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::io::prelude::*;
use std::io::{BufReader, Read};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{env, io};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use bo_cc::{analyse_warc, prepare_db, AnalysisResult};

async fn manager(
    mut rx: mpsc::Receiver<(i64, AnalysisResult)>,
    db: sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), io::Error> {
    use bo_cc::AnalysisResult::*;

    while let Some((warc_id, analysis)) = rx.recv().await {
        match analysis {
            FormsWithPatterns {
                url: _,
                with,
                nr_without,
            } => (),
            NoFormsWithPatterns { url: _, nr_forms } => (),
            NoForms => (),
            NotHTML | UnknownEncoding => (),
            WarcDone => {
                println!("Submitted all of {}!", warc_id)
            }
        }
    }

    println!("All channels closed!");

    Ok(())
}

async fn get_or_insert_warc(url: String, db: &sqlx::Pool<sqlx::Sqlite>) -> Option<(String, i64)> {
    let (id, done) = sqlx::query_as(include_str!("sql/get-or-update-warc.sql"))
        .bind(&url)
        .fetch_one(db)
        .await
        .unwrap();

    if done {
        None
    } else {
        Some((url, id))
    }
}

fn get_warcs(client: &Client, db: &sqlx::Pool<sqlx::Sqlite>) -> Result<Vec<String>, BoxDynError> {
    // FIXME this archive is hard-coded!
    let gz = BufReader::new(
        client
            .get(" https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-49/warc.paths.gz")
            .send()?,
    );

    Ok(BufReader::new(MultiGzDecoder::new(gz))
        .lines()
        .flatten()
        .map(|url| format!("https://data.commoncrawl.org/{}", url))
        .collect())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let (db_submit, db_receive) = mpsc::channel(32);

    let sqlite_options = SqliteConnectOptions::from_str("sqlite://form_validation.db")
        .unwrap()
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(30));

    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(sqlite_options)
        .await?;

    sqlx::query("pragma temp_store = memory;")
        .execute(&db)
        .await?;
    sqlx::query("pragma mmap_size = 30000000000;")
        .execute(&db)
        .await?;
    sqlx::query("pragma page_size = 4096;").execute(&db).await?;

    prepare_db(&db).await; // We must finish preparing the DB before allocating to it.

    let client = reqwest::blocking::Client::new();

    let urls = futures::future::join_all(
        get_warcs(&client, &db)?
            .into_iter()
            .map(|url| get_or_insert_warc(url, &db)),
    )
    .await
    .into_iter()
    .flatten();

    let (tx_path, mut rx_path) = mpsc::channel::<(String, i64)>(10);

    let process_paths = tokio::spawn(async move {
        let mut analysis_tasks = JoinSet::new();
        let client = Arc::new(client);
        while let Some((path, id)) = rx_path.recv().await {
            println!("Analysing {}", &path);
            analysis_tasks.spawn(analyse_warc(
                path,
                id,
                db_submit.clone(),
                Arc::clone(&client),
            ));

            if analysis_tasks.len() > 5 {
                if let Err(e) = analysis_tasks.join_next().await.unwrap() {
                    println!("Task error: {}", e);
                }
            }
        }

        // Drain the queue
        while let Some(outcome) = analysis_tasks.join_next().await {
            if let Err(e) = outcome {
                println!("Task error: {}", e);
            }
        }
    });

    let manager_future = tokio::spawn(manager(db_receive, db));

    for url_and_id in urls {
        tx_path.send(url_and_id).await?;
    }

    println!("All paths submitted!");

    drop(tx_path);

    process_paths.await?;
    manager_future.await??;
    Ok(())
}
