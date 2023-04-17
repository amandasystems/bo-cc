use flate2::read::MultiGzDecoder;
use futures::future::join_all;
use log::{error, info, warn};
use reqwest::blocking::Client;
use sqlx::error::BoxDynError;
use sqlx::postgres::PgPoolOptions;
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, Read};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use bo_cc::{analyse_warc, prepare_db, AnalysisResult};
const COMPRESSION_LEVEL: u32 = 6;

async fn mark_done(db: &sqlx::Pool<sqlx::Postgres>, warc_id: i64) {
    info!("Done with WARC ID {}", warc_id);
    if let Err(e) =
        sqlx::query("UPDATE archives SET all_records_submitted_for_analysis = TRUE WHERE id = $1;")
            .bind(warc_id)
            .execute(db)
            .await
    {
        error!("Error marking WARC ID {} as done: {}", warc_id, e);
    }
}

async fn increment_urls(db: &sqlx::Pool<sqlx::Postgres>, warc_id: i64) {
    if let Err(e) = sqlx::query("UPDATE archives SET nr_urls = nr_urls + 1 WHERE id = $1;")
        .bind(warc_id)
        .execute(db)
        .await
    {
        warn!("Error incrementing nr URLS for WARC id {}: {}", warc_id, e)
    }
}

async fn increment_unknown_encoding(db: &sqlx::Pool<sqlx::Postgres>, warc_id: i64) {
    if let Err(e) = sqlx::query(include_str!("sql/increment-unknown-encoding.sql"))
        .bind(warc_id)
        .execute(db)
        .await
    {
        warn!(
            "Error incrementing unknown encoding for WARC {}: {}",
            warc_id, e
        )
    }
}

async fn increment_urls_and_forms(db: &sqlx::Pool<sqlx::Postgres>, warc_id: i64, forms_by: u32) {
    if let Err(e) = sqlx::query(
        "UPDATE archives SET nr_urls = nr_urls + 1, nr_forms = nr_forms + $1 WHERE id = $2;",
    )
    .bind(forms_by as i64)
    .bind(warc_id)
    .execute(db)
    .await
    {
        warn!(
            "Error incrementing urls and forms for WARC id {}: {}",
            warc_id, e
        );
    }
}

async fn insert_forms(
    db: &sqlx::Pool<sqlx::Postgres>,
    warc_id: i64,
    url: String,
    forms_with: Vec<String>,
    nr_without: u32,
) {
    let nr_forms_total = u32::try_from(forms_with.len()).unwrap() + nr_without;
    use xz2::bufread::XzEncoder;

    let res = sqlx::query_as(
        "INSERT INTO urls(page_url, from_archive_id, nr_forms_total) VALUES ($1, $2, $3) RETURNING id;",
    )
    .bind(&url)
    .bind(warc_id)
    .bind(nr_forms_total as i32)
    .fetch_one(db)
    .await;

    if let Err(e) = res {
        warn!("Error inserting URL {}: {}", e, url);
        return;
    }

    let (url_id,): (i32,) = res.unwrap();

    let submit_all = forms_with.into_iter().map(|form| {
        let compressed_form: Vec<u8> = XzEncoder::new(form.as_bytes(), COMPRESSION_LEVEL)
            .bytes()
            .map(|x| x.unwrap())
            .collect();
        sqlx::query("INSERT INTO forms(form, from_url) VALUES ($1, $2)")
            .bind(compressed_form)
            .bind(url_id)
            .execute(db)
    });
    join_all(submit_all).await;
}

async fn manager(
    mut rx: mpsc::Receiver<(i64, AnalysisResult)>,
    db: sqlx::Pool<sqlx::Postgres>,
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

    info!("All channels closed!");

    Ok(())
}

async fn get_or_insert_warc(url: String, db: &sqlx::Pool<sqlx::Postgres>) -> Option<(String, i32)> {
    sqlx::query("INSERT INTO archives(record_url) VALUES($1) ON CONFLICT (record_url) DO NOTHING;")
        .bind(&url)
        .execute(db)
        .await
        .unwrap();

    let (id, done) = sqlx::query_as(
        "SELECT id, all_records_submitted_for_analysis FROM archives WHERE record_url = $1;",
    )
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
    env_logger::init();
    let (db_submit, db_receive) = mpsc::channel(10000);

    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://amanda@localhost/amanda")
        .await?;

    prepare_db(&db).await; // We must finish preparing the DB before allocating to it.

    let client = reqwest::blocking::Client::new();

    let urls = join_all(
        get_warcs(&client)?
            .into_iter()
            .map(|url| get_or_insert_warc(url, &db)),
    )
    .await
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    info!("Loaded {} WARC URLs!", urls.len());

    let (tx_path, mut rx_path) = mpsc::channel::<(String, i32)>(64);

    let process_paths = tokio::spawn(async move {
        let mut analysis_tasks = JoinSet::new();
        while let Some((path, id)) = rx_path.recv().await {
            info!("Analysing {}", &path);
            analysis_tasks.spawn(analyse_warc(
                path,
                id.into(),
                db_submit.clone(),
                client.clone(),
            ));

            if analysis_tasks.len() > 5 {
                info!("Waiting for a task to finish...");
                if let Err(e) = analysis_tasks.join_next().await.unwrap() {
                    warn!("Task error: {}", e);
                }
            }
        }

        // Drain the queue
        while let Some(outcome) = analysis_tasks.join_next().await {
            if let Err(e) = outcome {
                warn!("Task error: {}", e);
            }
        }
    });

    let manager_future = tokio::spawn(manager(db_receive, db));

    for url_and_id in urls.into_iter() {
        tx_path.send(url_and_id).await?;
    }

    info!("All paths submitted!");

    drop(tx_path);

    process_paths.await?;
    manager_future.await??;
    Ok(())
}
