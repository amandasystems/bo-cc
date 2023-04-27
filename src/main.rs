use flate2::read::MultiGzDecoder;
use log::error;
use log::info;
use log::trace;
use reqwest::blocking::Client;
use std::collections::HashSet;
use std::io::prelude::*;
use std::io::BufReader;

use bo_cc::{process_warc, processed_warcs, AnalysisWriter, BoxDynError};

fn get_warcs(
    client: &Client,
    warcs_present: HashSet<String>,
) -> Result<impl Iterator<Item = String>, reqwest::Error> {
    // FIXME this archive is hard-coded!
    let gz = BufReader::new(
        client
            .get("https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-49/warc.paths.gz")
            .send()?
            .error_for_status()?,
    );

    Ok(BufReader::new(MultiGzDecoder::new(gz))
        .lines()
        .flatten()
        .filter(move |u| !warcs_present.contains(u)))
}

fn main() -> Result<(), BoxDynError> {
    env_logger::init();

    let client = reqwest::blocking::Client::new();
    let seen: HashSet<String> = processed_warcs().into_iter().collect();
    let warc_urls = get_warcs(&client, seen)?;

    let mut writer = AnalysisWriter::new();

    for warc_url in warc_urls {
        trace!("Analysing {}", &warc_url);
        match process_warc(&warc_url, &client) {
            Ok(summary) => writer.write(warc_url, summary)?,
            Err(e) => {
                let url = e.url().map(|u| u.to_string()).unwrap_or(warc_url);
                if let Some(code) = e.status() {
                    error!("Code {} fetchig {}, giving up.", code, url);
                } else {
                    error!("Unknown error fetching {}: {}, giving up.", url, e);
                }
                break;
            }
        }
    }
    info!("All WARCs processed!");

    Ok(())
}
