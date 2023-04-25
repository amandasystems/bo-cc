use std::{
    borrow::Cow,
    collections::HashSet,
    error::Error,
    io::{self, BufReader, ErrorKind},
    sync::mpsc::Receiver,
};
const COMPRESSION_LEVEL: u32 = 6;
use serde::{Deserialize, Serialize};

use chardetng::EncodingDetector;
use encoding_rs::Encoding;
use flate2::read::MultiGzDecoder;
use httparse::Header;
use log::{error, info, trace, warn};
use rust_warc::{WarcReader, WarcRecord};
use sqlx::error::BoxDynError;
use std::fs;
use std::io::prelude::*;
use std::io::BufWriter;
use tokio::{sync::mpsc, task::JoinSet};

const WRITE_BACKLOG: usize = 32;

pub fn processed_warcs() -> HashSet<String> {
    BufReader::new(fs::File::open("forms.d/index").expect("No index file!"))
        .lines()
        .flatten()
        .collect()
}

pub struct AnalysisWriter {
    inbox: std::sync::mpsc::SyncSender<ArchiveSummary>,
    thread: std::thread::JoinHandle<()>,
}

impl AnalysisWriter {
    fn process_inbox(incoming: Receiver<ArchiveSummary>) {
        info!("Writer thread started!");
        fs::create_dir_all("forms.d").expect("Unable to create forms.d directory!");
        let mut nr_seen = 0;
        let mut index_bw =
            BufWriter::new(fs::File::create("forms.d/index").expect("Unable to open index file"));
        // FIXME do compression too!
        while let Ok(summary) = incoming.recv() {
            let archive_fn = format!("forms.d/{}.cbor", nr_seen);
            let archive_writer = BufWriter::new(fs::File::create(&archive_fn).expect(&format!(
                "Unable to open archive dump file: {}",
                &archive_fn
            )));

            // Add compression!
            serde_cbor::to_writer(archive_writer, &summary)
                .expect("Error writing archive summary!");

            writeln!(index_bw, "{}", summary.archive_url)
                .expect("Unable to write WARC URL to index!");
            nr_seen += 1;
        }
    }
    pub fn close(self: Self) {
        drop(self.inbox);
        if let Err(e) = self.thread.join() {
            error!("Writer: Join error: {:#?}", e);
        }
    }
    pub fn write(&mut self, summary: ArchiveSummary) -> Result<(), BoxDynError> {
        self.inbox.send(summary)?;
        Ok(())
    }
    pub fn new() -> Self {
        let (send, recieve) = std::sync::mpsc::sync_channel(WRITE_BACKLOG);
        Self {
            inbox: send,
            thread: std::thread::spawn(move || Self::process_inbox(recieve)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct URLSummary {
    url: String,
    with_patterns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArchiveSummary {
    archive_url: String,
    nr_unknown_encoding: i64,
    nr_urls_without_patterns: i64,
    nr_forms_without_patterns: i64,
    urls_with_pattern_forms: Vec<URLSummary>,
}

impl ArchiveSummary {
    fn new(archive_url: String, initial: AnalysisResult) -> ArchiveSummary {
        use AnalysisResult::*;
        match initial {
            NotHTML => ArchiveSummary {
                archive_url,
                nr_unknown_encoding: 0,
                nr_urls_without_patterns: 0,
                nr_forms_without_patterns: 0,
                urls_with_pattern_forms: Vec::with_capacity(1024),
            },
            UnknownEncoding => ArchiveSummary {
                archive_url,
                nr_unknown_encoding: 1,
                nr_urls_without_patterns: 0,
                nr_forms_without_patterns: 0,
                urls_with_pattern_forms: Vec::with_capacity(1024),
            },
            NoForms => ArchiveSummary {
                archive_url,
                nr_unknown_encoding: 0,
                nr_urls_without_patterns: 1,
                nr_forms_without_patterns: 0,
                urls_with_pattern_forms: Vec::with_capacity(1024),
            },
            FormsWithPatterns {
                url,
                with,
                nr_without,
            } => ArchiveSummary {
                archive_url,
                nr_unknown_encoding: 0,
                nr_urls_without_patterns: 0,
                nr_forms_without_patterns: nr_without,
                urls_with_pattern_forms: vec![URLSummary {
                    url,
                    with_patterns: with,
                }],
            },
            NoFormsWithPatterns { nr_forms } => ArchiveSummary {
                archive_url,
                nr_unknown_encoding: 0,
                nr_urls_without_patterns: 1,
                nr_forms_without_patterns: nr_forms,
                urls_with_pattern_forms: Vec::with_capacity(1024),
            },
        }
    }

    fn merge(mut self, other: ArchiveSummary) -> ArchiveSummary {
        assert_eq!(self.archive_url, other.archive_url);
        let mut summarised_forms = self.urls_with_pattern_forms;
        summarised_forms.extend(other.urls_with_pattern_forms);
        ArchiveSummary {
            archive_url: self.archive_url,
            nr_unknown_encoding: self.nr_unknown_encoding + other.nr_unknown_encoding,
            nr_urls_without_patterns: self.nr_urls_without_patterns
                + other.nr_urls_without_patterns,
            nr_forms_without_patterns: self.nr_forms_without_patterns
                + other.nr_forms_without_patterns,
            urls_with_pattern_forms: summarised_forms,
        }
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
        client.get(&warc_url).send()?,
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
    outbox: tokio::sync::mpsc::Sender<ArchiveSummary>,
) -> Result<(), BoxDynError> {
    let (tx_records, mut rx_records) = mpsc::channel::<WarcRecord>(1);
    // FIXME use lifetimes to get rid of this clone!
    let parse_warc = tokio::task::spawn(get_records(url.clone(), client, tx_records));
    let mut workers = JoinSet::new();

    while let Some(record) = rx_records.recv().await {
        workers.spawn(
            async move { tokio::task::spawn_blocking(move || analyse_record(record)).await },
        );
    }

    let mut summary: Option<ArchiveSummary> = None;
    while let Some(r) = workers.join_next().await {
        let r = ArchiveSummary::new(url.clone(), r??);

        if let Some(inner) = summary {
            summary = Some(inner.merge(r));
        } else {
            summary = Some(r);
        }
    }
    parse_warc.await??;
    info!("Done with WARC ID {}", &url);

    outbox.send(summary.unwrap()).await?;

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
