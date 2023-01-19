use flate2::read::MultiGzDecoder;
use futures::future::join_all;
use reqwest::blocking::Client;
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, Read};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use bo_cc::{analyse_warc, prepare_db, AnalysisResult};
const COMPRESSION_LEVEL: u32 = 6;

async fn mark_done(db: &sqlx::Pool<sqlx::Sqlite>, warc_id: i64) {
    sqlx::query("UPDATE archives SET all_records_submitted_for_analysis = TRUE WHERE id = ?;")
        .bind(warc_id)
        .execute(db)
        .await
        .unwrap();
}

async fn increment_urls(db: &sqlx::Pool<sqlx::Sqlite>, warc_id: i64) {
    sqlx::query("UPDATE archives SET nr_urls = nr_urls + 1 WHERE id = ?;")
        .bind(warc_id)
        .execute(db)
        .await
        .unwrap();
}

async fn increment_unknown_encoding(db: &sqlx::Pool<sqlx::Sqlite>, warc_id: i64) {
    sqlx::query(include_str!("sql/increment-unknown-encoding.sql"))
        .bind(warc_id)
        .execute(db)
        .await
        .unwrap();
}

async fn increment_urls_and_forms(db: &sqlx::Pool<sqlx::Sqlite>, warc_id: i64, forms_by: u32) {
    sqlx::query("UPDATE archives SET nr_urls = nr_urls + 1, nr_forms = nr_forms + ? WHERE id = ?;")
        .bind(forms_by)
        .bind(warc_id)
        .execute(db)
        .await
        .unwrap();
}

async fn insert_forms(
    db: &sqlx::Pool<sqlx::Sqlite>,
    warc_id: i64,
    url: String,
    forms_with: Vec<String>,
    nr_without: u32,
) {
    let nr_forms_total = u32::try_from(forms_with.len()).unwrap() + nr_without;
    let url_id = sqlx::query(
        "INSERT INTO urls(page_url, from_archive_id, nr_forms_total) VALUES (?, ?, ?);",
    )
    .bind(url)
    .bind(warc_id)
    .bind(nr_forms_total)
    .execute(db)
    .await
    .unwrap()
    .last_insert_rowid();

    use xz2::bufread::XzEncoder;

    let submit_all = forms_with.into_iter().map(|form| {
        let compressed_form: Vec<u8> = XzEncoder::new(form.as_bytes(), COMPRESSION_LEVEL)
            .bytes()
            .map(|x| x.unwrap())
            .collect();
        sqlx::query("INSERT INTO forms(form, from_url) VALUES (?, ?)")
            .bind(compressed_form)
            .bind(url_id)
            .execute(db)
    });
    join_all(submit_all).await;
}

async fn manager(
    mut rx: mpsc::Receiver<(i64, AnalysisResult)>,
    db: sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), io::Error> {
    use bo_cc::AnalysisResult::*;

    while let Some((warc_id, analysis)) = rx.recv().await {
        match analysis {
            FormsWithPatterns {
                url,
                with,
                nr_without,
            } => insert_forms(&db, warc_id, url, with, nr_without).await,
            NoFormsWithPatterns { nr_forms } => {
                increment_urls_and_forms(&db, warc_id, nr_forms).await
            }
            NoForms => increment_urls(&db, warc_id).await,
            UnknownEncoding => {
                increment_unknown_encoding(&db, warc_id).await;
            }
            NotHTML => (),
            WarcDone => mark_done(&db, warc_id).await,
        }
    }

    println!("All channels closed!");

    Ok(())
}

async fn get_or_insert_warc(url: String, db: &sqlx::Pool<sqlx::Sqlite>) -> Option<(String, i64)> {
    let (id, done) = sqlx::query_as(include_str!("sql/get-or-update-warc.sql"))
        .bind(&url)
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

fn get_warcs(client: &Client) -> Result<Vec<String>, BoxDynError> {
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
        .busy_timeout(Duration::MAX);

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

    let urls = get_warcs(&client)?
        .into_iter()
        .map(|url| get_or_insert_warc(url, &db));

    let (tx_path, mut rx_path) = mpsc::channel::<(String, i64)>(10);

    let process_paths = tokio::spawn(async move {
        let mut analysis_tasks = JoinSet::new();
        let client = Arc::new(client); // use clone!
        while let Some((path, id)) = rx_path.recv().await {
            println!("Analysing {}", &path);
            analysis_tasks.spawn(analyse_warc(
                path,
                id,
                db_submit.clone(),
                Arc::clone(&client),
            ));

            if analysis_tasks.len() > 2 {
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

    let manager_future = tokio::spawn(manager(db_receive, db.clone()));

    for url_data in urls {
        if let Some(url_and_id) = url_data.await {
            tx_path.send(url_and_id).await?;
        }
    }

    println!("All paths submitted!");

    drop(tx_path);

    process_paths.await?;
    manager_future.await??;
    Ok(())
}
