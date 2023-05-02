use std::{
    borrow::Cow,
    error::Error,
    io::{self, BufReader, ErrorKind},
    sync::mpsc::{self, Receiver},
    thread,
};
const COMPRESSION_LEVEL: u32 = 6;
use serde::{Deserialize, Serialize};

use attohttpc;
use chardetng::EncodingDetector;
use encoding_rs::Encoding;
use flate2::read::MultiGzDecoder;
use httparse::Header;
use log::{info, trace, warn};
use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;
use rust_warc::{WarcReader, WarcRecord};
use std::fs;
use std::io::prelude::*;
use std::io::BufWriter;
use xz2::write::XzEncoder;

pub type BoxDynError = Box<dyn Error + 'static + Send + Sync>;

const WRITE_BACKLOG: usize = 32;

pub fn processed_warcs() -> Vec<String> {
    match fs::File::open("forms.d/index") {
        Ok(fp) => BufReader::new(fp).lines().flatten().collect(),
        Err(_) => {
            info!("No index file found, assuming no previous progress.");
            vec![]
        }
    }
}

pub struct AnalysisWriter {
    inbox: Option<mpsc::SyncSender<(String, ArchiveSummary)>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl AnalysisWriter {
    fn process_inbox(incoming: Receiver<(String, ArchiveSummary)>) {
        info!("Writer thread started!");
        fs::create_dir_all("forms.d").expect("Unable to create forms.d directory!");
        let seen = processed_warcs();
        let mut index_bw =
            BufWriter::new(fs::File::create("forms.d/index").expect("Unable to open index file"));
        for s in seen.into_iter() {
            writeln!(index_bw, "{}", s).expect("Unable to rewrite index!");
        }
        while let Ok((warc_url, summary)) = incoming.recv() {
            let archive_fn = format!("forms.d/{}.json.xz", warc_url.replace("/", "!"));
            let archive_writer = XzEncoder::new(
                BufWriter::new(fs::File::create(&archive_fn).unwrap_or_else(|_| {
                    panic!("Unable to open archive dump file: {}", &archive_fn)
                })),
                COMPRESSION_LEVEL,
            );

            serde_json::to_writer(archive_writer, &summary)
                .expect("Error writing archive summary!");

            writeln!(index_bw, "{}", warc_url).expect("Unable to write WARC URL to index!");
            index_bw.flush().expect("Unable to write to index!");
        }
    }
    pub fn write(&mut self, warc_url: String, summary: ArchiveSummary) -> Result<(), BoxDynError> {
        self.inbox.as_ref().unwrap().send((warc_url, summary))?;
        Ok(())
    }
    pub fn new() -> Self {
        let (send, recieve) = std::sync::mpsc::sync_channel(WRITE_BACKLOG);
        Self {
            inbox: Some(send),
            thread: Some(thread::spawn(move || Self::process_inbox(recieve))),
        }
    }
}

impl Drop for AnalysisWriter {
    fn drop(&mut self) {
        drop(self.inbox.take());
        if let Some(thread) = self.thread.take() {
            thread.join().expect("Worker error!");
        }
    }
}

impl Default for AnalysisWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct URLSummary {
    url: String,
    with_patterns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ArchiveSummary {
    nr_unknown_encoding: i64,
    nr_urls_without_patterns: i64,
    nr_forms_without_patterns: i64,
    urls_with_pattern_forms: Vec<URLSummary>,
}

impl ArchiveSummary {
    fn merge(self, other: ArchiveSummary) -> ArchiveSummary {
        let mut summarised_forms = self.urls_with_pattern_forms;
        summarised_forms.extend(other.urls_with_pattern_forms);
        ArchiveSummary {
            nr_unknown_encoding: self.nr_unknown_encoding + other.nr_unknown_encoding,
            nr_urls_without_patterns: self.nr_urls_without_patterns
                + other.nr_urls_without_patterns,
            nr_forms_without_patterns: self.nr_forms_without_patterns
                + other.nr_forms_without_patterns,
            urls_with_pattern_forms: summarised_forms,
        }
    }

    fn from_record(record: rust_warc::WarcRecord) -> Option<ArchiveSummary> {
        let content_type = record
            .header
            .get(&"warc-identified-payload-type".into())
            .unwrap();

        if !(content_type == "text/html" || content_type == "application/xhtml+xml") {
            trace!("Ignoring unknown content type: {}", content_type);
            return None;
        }

        let (nr_forms, with) = {
            if let Ok(res) = extract_forms(&record.content) {
                res
            } else {
                return Some(ArchiveSummary {
                    nr_unknown_encoding: 1,
                    ..Default::default()
                });
            }
        };

        if nr_forms == 0 || with.is_empty() {
            return Some(ArchiveSummary {
                nr_urls_without_patterns: 1,
                ..Default::default()
            });
        }

        let mut header = record.header;
        let url = header.remove(&"warc-target-uri".into()).unwrap();

        Some(ArchiveSummary {
            nr_forms_without_patterns: nr_forms - with.len() as i64,
            urls_with_pattern_forms: vec![URLSummary {
                url,
                with_patterns: with,
            }],
            ..Default::default()
        })
    }
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

fn extract_forms(content: &[u8]) -> Result<(i64, Vec<String>), Box<dyn Error>> {
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

pub fn get_records(
    warc_url: &str,
    client: &attohttpc::Session,
) -> Result<impl Iterator<Item = WarcRecord>, attohttpc::Error> {
    let warc_reader = WarcReader::new(BufReader::new(MultiGzDecoder::new(BufReader::new(
        client
            .get(format!("https://data.commoncrawl.org/{}", warc_url))
            .send()?
            .error_for_status()?,
    ))));

    Ok(warc_reader
        .filter_map(|r| r.ok())
        .filter(|r| r.header.get(&"WARC-Type".into()) == Some(&"response".into())))
}

pub fn process_warc(
    url: &str,
    client: &attohttpc::Session,
) -> Result<ArchiveSummary, attohttpc::Error> {
    let summary = get_records(url, client)?
        .into_iter()
        .par_bridge()
        .flat_map(ArchiveSummary::from_record)
        .reduce(ArchiveSummary::default, |a, b| a.merge(b));
    info!("Done with WARC ID {}", &url);
    Ok(summary)
}
