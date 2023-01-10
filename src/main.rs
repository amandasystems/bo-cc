use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;
use std::{env, error, io};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::sleep;

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
            WarcDone => {println!("Submitted an entire WARC!")},
        }
        // println!(
        //     "NO FORM: {}, FORMS: {}, FORMS WITH PATTERNS: {}",
        //     nr_no_forms, nr_forms_seen, nr_forms_w_pattern
        // );
    }

    println!("All channels closed!");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let (db_submit, db_receive) = mpsc::channel(32);

    let paths: Vec<_> = {
        let mut args = env::args();
        args.next();
        args.collect()
    };

    let sqlite_options = SqliteConnectOptions::from_str("sqlite://form_validation.db")
        .unwrap()
        .create_if_missing(true);

    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(sqlite_options)
        .await?;

    prepare_db(&db).await; // We must finish preparing the DB before allocating to it.

    let (tx_path, mut rx_path) = mpsc::channel(10);

    let process_paths = tokio::spawn(async move {
        let mut analysis_tasks = JoinSet::new();
        while let Some(path) = rx_path.recv().await {
            println!("Analysing {}", &path);
            analysis_tasks.spawn(analyse_warc(path, db_submit.clone()));

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

    for path in paths {
        tx_path.send(path).await?;
    }

    println!("All paths submitted!");

    drop(tx_path);

    process_paths.await?;
    manager_future.await??;
    Ok(())
}
