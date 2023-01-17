use std::{
    borrow::Cow,
    error::Error,
    io::{self, BufReader, ErrorKind},
    sync::Arc,
};

use chardetng::EncodingDetector;
use encoding_rs::Encoding;
use flate2::read::MultiGzDecoder;
use httparse::Header;
use rust_warc::WarcReader;
use sqlx::error::BoxDynError;
use tokio::{sync::mpsc, task::JoinSet};

#[derive(Debug)]
pub enum AnalysisResult {
    WarcDone,
    UnknownEncoding,
    NotHTML,
    NoForms,
    NoFormsWithPatterns {
        nr_forms: u32,
    },
    FormsWithPatterns {
        url: String,
        with: Vec<String>,
        nr_without: u32,
    },
}

fn get_encoding_by_header(headers: [Header; 64]) -> Option<&'static Encoding> {
    headers
        .into_iter()
        .take_while(|h| h != &httparse::EMPTY_HEADER)
        .find(|h| h.name == "Content-type")
        .map(|h| String::from_utf8_lossy(h.value))
        .and_then(|content_type| {
            let mut iter = content_type.split("; charset=");
            iter.next(); // Skip first half
            iter.next().map(|s| s.to_owned())
        })
        .and_then(|content_type| Encoding::for_label(content_type.as_bytes()))
}

fn decode_body(body: &[u8]) -> Result<Cow<str>, Box<dyn Error>> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut response = httparse::Response::new(&mut headers);

    let body = if let httparse::Status::Complete(body_offset) = response.parse(body)? {
        &body[body_offset..]
    } else {
        body // Fall back to using the entire response: this is wrong, but probably OK
    };

    let document_encoding = get_encoding_by_header(headers).unwrap_or_else(|| {
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
        detector.guess(None, true)
    });

    let (cow, decoder_used, had_errors) = document_encoding.decode(body);
    if had_errors {
        Err(Box::new(io::Error::new(
            ErrorKind::Other,
            format!(
                "Error decoding body with detected encoding {}",
                decoder_used.name()
            ),
        )))
    } else {
        Ok(cow)
    }
}

fn analyse_record(record: rust_warc::WarcRecord) -> AnalysisResult {
    use crate::AnalysisResult::*;

    let content_type = record
        .header
        .get(&"warc-identified-payload-type".into())
        .unwrap();

    if !(content_type == "text/html" || content_type == "application/xhtml+xml") {
        return NotHTML;
    }

    let (nr_forms, with) = {
        if let Ok(res) = second_opinion(&record.content) {
            res
        } else {
            return UnknownEncoding;
        }
    };

    if nr_forms == 0 {
        return NoForms;
    }

    if with.is_empty() {
        return NoFormsWithPatterns { nr_forms };
    }

    let nr_with: u32 = with.len().try_into().unwrap();
    let url = record
        .header
        .get(&"warc-target-uri".into())
        .unwrap()
        .to_string();

    FormsWithPatterns {
        url,
        nr_without: nr_forms - nr_with,
        with,
    }
}

fn second_opinion(content: &[u8]) -> Result<(u32, Vec<String>), Box<dyn Error>> {
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
            let (start, end) = form.boundaries(&parser);
            let tag_text = body[start..=end].to_owned();
            if !tag_text.contains("</form>") {
                // For some reason, we sometimes only get the opening tag.
                return Err("No closing tag in form: assuming broken HTML".into());
            }
            interesting_forms.push(tag_text);
        }
    }
    Ok((nr_forms, interesting_forms))
}

pub async fn analyse_warc(
    url: String,
    warc_id: i64,
    out_pipe: mpsc::Sender<(i64, AnalysisResult)>,
    client: Arc<reqwest::blocking::Client>,
) -> Result<(), BoxDynError> {
    let gz = BufReader::new(client.get(url).send()?);
    let mut workers = tokio::task::spawn_blocking(move || {
        // This thread parses and filters the records
        let mut workers = JoinSet::new();
        let decoder = MultiGzDecoder::new(gz);
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
                pipe.send((warc_id, analysis_result)).await.unwrap();
            });
        }
        workers.spawn(async move {
            out_pipe
                .send((warc_id, AnalysisResult::WarcDone))
                .await
                .unwrap()
        });
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
    sqlx::query(include_str!("sql/init-db.sql"))
        .execute(db)
        .await
        .unwrap();
    println!("done.")
}
