use flate2::read::MultiGzDecoder;
use futures::stream::{self, StreamExt};
use log::{info, trace, warn};
use reqwest::blocking::Client;
use sqlx::error::BoxDynError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::io::prelude::*;
use std::io::BufReader;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use bo_cc::{prepare_db, process_warc, processed_warcs, AnalysisWriter, ArchiveSummary};

fn warc_present(url: &str) -> bool {
    let x = processed_warcs();
    x.contains(url)
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

    let sqlite_options = SqliteConnectOptions::from_str("sqlite://form_validation.db")
        .unwrap()
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(1200));

    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(sqlite_options)
        .await?;

    prepare_db(&db).await; // We must finish preparing the DB before allocating to it.

    let client = reqwest::blocking::Client::new();
    let mut urls = stream::iter(get_warcs(&client)?);
    let mut analysis_tasks = JoinSet::new();
    let (tx_summary, mut rx_summary) = mpsc::channel::<ArchiveSummary>(4);

    let mut writer = AnalysisWriter::new();

    while let Some(warc_url) = urls.next().await {
        if warc_present(&warc_url) {
            info!("Skipping already processed WARC {}", &warc_url);
            continue;
        }

        info!("Analysing {}", &warc_url);
        analysis_tasks.spawn(process_warc(warc_url, client.clone(), tx_summary.clone()));
        if analysis_tasks.len() > 2 {
            trace!("Waiting for a task to finish...");
            if let Err(e) = analysis_tasks.join_next().await.unwrap() {
                warn!("Task error: {}", e);
            }
        }
    }

    drop(tx_summary);

    while let Some(res) = rx_summary.recv().await {
        info!("Recieved WARC!");
        writer.write(res)?;
    }

    // Drain the queue
    while let Some(outcome) = analysis_tasks.join_next().await {
        if let Err(e) = outcome {
            warn!("Task error: {}", e);
        }
    }

    info!("All paths submitted!");

    Ok(())
}
