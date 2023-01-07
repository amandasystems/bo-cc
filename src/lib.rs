use std::{
    borrow::Cow,
    fs::File,
    io::{self, BufReader, ErrorKind},
};

use chardetng::EncodingDetector;
use flate2::read::MultiGzDecoder;
use rust_warc::WarcReader;
use tokio::{sync::mpsc, task::JoinSet};

#[derive(Debug)]
pub enum AnalysisResult {
    UnknownEncoding,
    NotHTML {
        bad_mimetype: String,
    },
    NoForms {
        url: String,
    },
    NoFormsWithPatterns {
        url: String,
        nr_forms: usize,
    },
    FormsWithPatterns {
        url: String,
        with: Vec<String>,
        nr_without: usize,
    },
}

fn decode_body(body: &[u8]) -> Result<Cow<str>, io::Error> {
    let mut detector = EncodingDetector::new();
    const DETECTOR_CHUNK_SIZE_BYTES: usize = 1024;

    // FIXME this may split in the middle of a character which is bad maybe
    // We should also maybe feed smaller cunks one at a time until we successfully detect the encoding.
    let (subslice, is_last) = if body.len() > DETECTOR_CHUNK_SIZE_BYTES {
        (&body[..DETECTOR_CHUNK_SIZE_BYTES], false)
    } else {
        (body, true)
    };

    detector.feed(subslice, is_last);

    let document_encoding = detector.guess(None, true);

    let (cow, decoder_used, had_errors) = document_encoding.decode(body);
    if had_errors {
        Err(io::Error::new(
            ErrorKind::Other,
            format!(
                "Error decoding body with detected encoding {}",
                decoder_used.name()
            ),
        ))
    } else {
        Ok(cow)
    }
}

// fn extract_tld(record: &Record<BufferedBody>) -> Option<TldResult> {
//     let extractor = TldExtractor::new(TldOption::default());
//     let record_uri = record.header(WarcHeader::TargetURI).unwrap();
//     extractor.extract(&record_uri).ok()
// }

fn analyse_record(record: rust_warc::WarcRecord) -> AnalysisResult {
    use crate::AnalysisResult::*;

    let content_type = record
        .header
        .get(&"warc-identified-payload-type".into())
        .unwrap();

    if !(content_type == "text/html" || content_type == "application/xhtml+xml") {
        return NotHTML {
            bad_mimetype: content_type.to_string(),
        };
    }

    let (nr_forms, with) = {
        if let Ok(res) = second_opinion(&record.content) {
            res
        } else {
            return UnknownEncoding;
        }
    };

    let url = record
        .header
        .get(&"warc-target-uri".into())
        .unwrap()
        .to_string();

    if nr_forms == 0 {
        return NoForms { url };
    }

    if with.is_empty() {
        return NoFormsWithPatterns { url, nr_forms };
    }

    FormsWithPatterns {
        url,
        nr_without: nr_forms - with.len(),
        with,
    }
}

fn second_opinion(content: &[u8]) -> Result<(usize, Vec<String>), std::io::Error> {
    let body = decode_body(content)?;
    let dom = tl::parse(&body, tl::ParserOptions::default()).unwrap();
    let parser = dom.parser();

    let mut nr_forms = 0;
    let mut interesting_forms: Vec<String> = Vec::new();
    let forms = dom
        .query_selector("form")
        .unwrap()
        .filter_map(|handle| handle.get(parser).and_then(|n| n.as_tag()));

    for form in forms {
        nr_forms += 1;

        if form
            .children()
            .all(parser)
            .iter()
            .filter_map(|e| e.as_tag())
            .any(|tag| {
                let attributes = tag.attributes();
                tag.name().as_bytes() == b"input"
                    && (attributes.contains("pattern")
                        || attributes.contains("data-val-regex-pattern")
                        || attributes.contains("ng-pattern"))
            })
        {
            if let Some(form_sourcecode) = form.raw().try_as_utf8_str().map(|s| s.to_string()) {
                interesting_forms.push(form_sourcecode)
            }
        }
    }
    Ok((nr_forms, interesting_forms))
}

pub async fn analyse_warc(
    path: String,
    out_pipe: mpsc::Sender<AnalysisResult>,
) -> Result<(), io::Error> {
    let f = File::open(path)?;

    let mut workers = tokio::task::spawn_blocking(move || {
        // This thread parses and filters the records
        let mut workers = JoinSet::new();
        let decoder = MultiGzDecoder::new(f);
        let reader = BufReader::new(decoder);
        let warc = WarcReader::new(reader);

        for record in warc
            .filter_map(|r| r.ok())
            .filter(|r| r.header.get(&"WARC-Type".into()) == Some(&"response".into()))
        {
            let pipe = out_pipe.clone();
            workers.spawn(async move {
                let analysis_result = tokio::task::spawn_blocking(move || analyse_record(record))
                    .await
                    .unwrap();
                pipe.send(analysis_result).await.unwrap();
            });
        }
        workers
    })
    .await?;

    while let Some(r) = workers.join_next().await {
        r? // Terminate if one of the subtasks failed
    }

    Ok(())
}

pub async fn prepare_db(db: &sqlx::Pool<sqlx::Sqlite>) {
    print!("Initialising database...");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS urls
                (id INTEGER PRIMARY KEY,
                page_url TEXT,
                from_archive_id INTEGER,
                nr_forms_total INTEGER,
                FOREIGN KEY(from_archive_id) REFERENCES archives(id))
                ",
    )
    .execute(db)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS forms
        (id INTEGER PRIMARY KEY,
        form BLOB,
        from_url INTEGER,
        has_pattern INTEGER,
        FOREIGN KEY(from_url) REFERENCES urls(id))",
    )
    .execute(db)
    .await
    .unwrap()
    .last_insert_rowid();

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS archives
                (id INTEGER PRIMARY KEY,
                 record_url TEXT,
                 nr_urls INTEGER,
                 nr_forms INTEGER,
                 nr_interesting_forms INTEGER)",
    )
    .execute(db)
    .await
    .unwrap();

    println!("done.")
}
