use std::{
    borrow::Cow,
    error::Error,
    io::{self, BufReader, ErrorKind},
};
const COMPRESSION_LEVEL: u32 = 6;

use chardetng::EncodingDetector;
use encoding_rs::Encoding;
use flate2::read::MultiGzDecoder;
use futures::future::join_all;
use httparse::Header;
use log::{info, trace, warn};
use rust_warc::{WarcReader, WarcRecord};
use sqlx::error::BoxDynError;
use tokio::{sync::mpsc, task::JoinSet};

#[derive(Debug)]
pub struct URLSummary {
    url: String,
    with_patterns: Vec<String>,
}

impl URLSummary {
    async fn write_out(
        &self,
        warc_id: i64,
        db: &sqlx::Pool<sqlx::Sqlite>,
    ) -> Result<(), BoxDynError> {
        let url_id = sqlx::query("INSERT INTO urls(page_url, from_archive_id) VALUES (?, ?);")
            .bind(&self.url)
            .bind(warc_id)
            .execute(db)
            .await?
            .last_insert_rowid();

        use std::io::prelude::*;
        use xz2::bufread::XzEncoder;

        let submit_all = self.with_patterns.iter().map(|form| {
            let compressed_form: Vec<u8> = XzEncoder::new(form.as_bytes(), COMPRESSION_LEVEL)
                .bytes()
                .map(|x| x.unwrap())
                .collect();
            sqlx::query("INSERT INTO forms(form, from_url) VALUES (?, ?)")
                .bind(compressed_form)
                .bind(url_id)
                .execute(db)
        });

        for r in join_all(submit_all).await {
            r?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ArchiveSummary {
    archive_url: String,
    nr_unknown_encoding: i64,
    nr_urls_without_patterns: i64,
    nr_forms_without_patterns: i64,
    urls_with_pattern_forms: Vec<URLSummary>,
}

impl ArchiveSummary {
    fn new(archive_url: String) -> ArchiveSummary {
        ArchiveSummary {
            archive_url,
            nr_unknown_encoding: 0,
            nr_urls_without_patterns: 0,
            nr_forms_without_patterns: 0,
            urls_with_pattern_forms: Vec::with_capacity(1024),
        }
    }

    fn add_result(&mut self, result: AnalysisResult) {
        use AnalysisResult::*;
        match result {
            NotHTML => (),
            UnknownEncoding => self.nr_unknown_encoding += 1,
            NoForms => self.nr_urls_without_patterns += 1,
            FormsWithPatterns {
                url,
                with,
                nr_without,
            } => {
                self.nr_forms_without_patterns += nr_without;
                self.urls_with_pattern_forms.push(URLSummary {
                    url,
                    with_patterns: with,
                })
            }
            NoFormsWithPatterns { nr_forms } => {
                self.nr_urls_without_patterns += 1;
                self.nr_forms_without_patterns += nr_forms;
            }
        }
    }

    pub async fn write_out(&self, db: &sqlx::Pool<sqlx::Sqlite>) -> Result<(), BoxDynError> {
        let nr_forms_with_pattern: i64 = self
            .urls_with_pattern_forms
            .iter()
            .map(|u| u.with_patterns.len() as i64)
            .sum();
        let warc_id = sqlx::query(
            "INSERT INTO archives(record_url, nr_urls, nr_unknown_encoding, nr_forms) VALUES(?, ?, ?, ?)",
        )
        .bind(&self.archive_url)
        .bind(self.nr_urls_without_patterns + self.urls_with_pattern_forms.len() as i64)
        .bind(self.nr_unknown_encoding)
        .bind(self.nr_forms_without_patterns + nr_forms_with_pattern)
        .execute(db)
        .await?
        .last_insert_rowid();

        let mut url_jobs = Vec::with_capacity(self.urls_with_pattern_forms.len());

        info!(
            "Found {} forms with patterns in WARC {}",
            self.urls_with_pattern_forms.len(),
            &self.archive_url
        );

        for summary in self.urls_with_pattern_forms.iter() {
            url_jobs.push(summary.write_out(warc_id, db));
        }

        for j in join_all(url_jobs).await {
            j?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum AnalysisResult {
    UnknownEncoding,
    NotHTML,
    NoForms,
    NoFormsWithPatterns {
        nr_forms: i64,
    },
    FormsWithPatterns {
        url: String,
        with: Vec<String>,
        nr_without: i64,
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
        warn!("Unable to parse headers, using entire request as body!");
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
        trace!("Ignoring unknown content type: {}", content_type);
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

    let nr_with = with.len() as i64;
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

fn second_opinion(content: &[u8]) -> Result<(i64, Vec<String>), Box<dyn Error>> {
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
            let (start, end) = form.boundaries(parser);
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

pub async fn get_records(
    warc_url: String,
    client: reqwest::blocking::Client,
    tx: mpsc::Sender<WarcRecord>,
) -> Result<(), BoxDynError> {
    let warc_reader = WarcReader::new(BufReader::new(MultiGzDecoder::new(BufReader::new(
        client.get(&warc_url).send()?.error_for_status()?,
    ))));

    for record in warc_reader
        .filter_map(|r| r.ok())
        .filter(|r| r.header.get(&"WARC-Type".into()) == Some(&"response".into()))
    {
        tx.send(record)
            .await
            .map_err(|e| format!("Error sending WARC {} for analysis: {}", e, warc_url))?;
    }
    Ok(())
}

pub async fn process_warc(
    url: String,
    client: reqwest::blocking::Client,
    db: sqlx::Pool<sqlx::Sqlite>,
) -> Result<(), BoxDynError> {
    let (tx_records, mut rx_records) = mpsc::channel::<WarcRecord>(10);
    // FIXME use lifetimes to get rid of this clone!
    let parse_warc = tokio::task::spawn(get_records(url.clone(), client, tx_records));
    let mut workers = JoinSet::new();

    while let Some(record) = rx_records.recv().await {
        workers.spawn(
            async move { tokio::task::spawn_blocking(move || analyse_record(record)).await },
        );
    }

    // FIXME this is not worth it for the logging!
    let mut summary = ArchiveSummary::new(url.clone());
    while let Some(r) = workers.join_next().await {
        /* If something went wrong with the pipes,
        the whole system is unsound and we cannot trust the result! */
        summary.add_result(r??);
    }

    info!("Done with WARC ID {}", &url);

    parse_warc.await??;
    summary.write_out(&db).await?;

    Ok(())
}

pub async fn prepare_db(db: &sqlx::Pool<sqlx::Sqlite>) {
    info!("Initialising database...");
    sqlx::query(include_str!("sql/init-db.sql"))
        .execute(db)
        .await
        .unwrap();
    info!("Done initialising database!");
}
