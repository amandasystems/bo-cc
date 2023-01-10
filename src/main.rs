use flate2::read::MultiGzDecoder;
use reqwest::blocking::Client;
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::io::prelude::*;
use std::io::{BufReader, Read};
use std::str::FromStr;
use std::sync::Arc;
use std::{env, io};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use bo_cc::{analyse_warc, prepare_db, AnalysisResult};

async fn manager(
    mut rx: mpsc::Receiver<AnalysisResult>,
    db: sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), io::Error> {
    use bo_cc::AnalysisResult::*;
    // let row: (i64,) = sqlx::query_as("SELECT $1")
    //     .bind(150_i64)
    //     .fetch_one(&db)
    //     .await
    //     .unwrap();

    // println!("{:?}", row);

    let mut nr_forms_seen = 0;
    let mut nr_no_forms = 0;
    let mut nr_forms_w_pattern = 0;
    while let Some(analysis) = rx.recv().await {
        match analysis {
            FormsWithPatterns {
                url: _,
                with,
                nr_without,
            } => {
                nr_forms_seen += with.len() + nr_without;
                nr_forms_w_pattern += with.len()
            }
            NoFormsWithPatterns { url: _, nr_forms } => nr_forms_seen += nr_forms,
            NoForms => nr_no_forms += 1,
            NotHTML | UnknownEncoding => (),
            WarcDone => {
                println!("Submitted an entire WARC!")
            }
        }
        // println!(
        //     "NO FORM: {}, FORMS: {}, FORMS WITH PATTERNS: {}",
        //     nr_no_forms, nr_forms_seen, nr_forms_w_pattern
        // );
    }

    println!("All channels closed!");

    Ok(())
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
        .create_if_missing(true);

    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(sqlite_options)
        .await?;

    prepare_db(&db).await; // We must finish preparing the DB before allocating to it.

    let client = reqwest::blocking::Client::new();

    let urls = get_warcs(&client)?; // TODO Filter by finished ones, according to DB

    let (tx_path, mut rx_path) = mpsc::channel(10);

    let process_paths = tokio::spawn(async move {
        let mut analysis_tasks = JoinSet::new();
        let client = Arc::new(client);
        while let Some(path) = rx_path.recv().await {
            println!("Analysing {}", &path);
            analysis_tasks.spawn(analyse_warc(path, db_submit.clone(), Arc::clone(&client)));

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

    let manager_future = tokio::spawn(manager(db_receive, db));

    for url in urls {
        tx_path.send(url).await?;
    }

    println!("All paths submitted!");

    drop(tx_path);

    process_paths.await?;
    manager_future.await??;
    Ok(())
}
