use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::str::FromStr;
use std::{env, error, io};
use tokio::sync::mpsc;

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
            NoForms { url: _ } => nr_no_forms += 1,
            NotHTML { bad_mimetype: _ } | UnknownEncoding => (),
        }
        println!(
            "NO FORM: {}, FORMS: {}, FORMS WITH PATTERNS: {}",
            nr_no_forms, nr_forms_seen, nr_forms_w_pattern
        );
    }

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

    let mut tasks = Vec::new();

    for path in paths {
        println!("Analysing {}", &path);
        tasks.push(analyse_warc(path, db_submit.clone()));
    }

    drop(db_submit); // No one else in this thread will submit

    let manager_future = tokio::spawn(manager(db_receive, db));

    for outcome in futures::future::join_all(tasks).await {
        if let Err(e) = outcome {
            println!("Task error: {}", e);
        }
    }
    manager_future.await??;

    Ok(())
}
