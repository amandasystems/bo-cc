use flate2::read::MultiGzDecoder;
use log::error;
use log::info;
use log::trace;
use reqwest::blocking::Client;
use std::io::prelude::*;
use std::io::BufReader;
use std::thread;
use std::time;

use bo_cc::{process_warc, processed_warcs, AnalysisWriter, BoxDynError};

fn warc_absent(url: &String) -> bool {
    let x = processed_warcs(); // FIXME: this is S L O W
    !x.contains(url)
}

fn get_warcs(client: &Client) -> Result<impl Iterator<Item = String>, BoxDynError> {
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
        .filter(warc_absent))
}

fn main() -> Result<(), BoxDynError> {
    env_logger::init();

    let client = reqwest::blocking::Client::new();
    let warc_urls = get_warcs(&client)?;

    let mut writer = AnalysisWriter::new();

    for warc_url in warc_urls {
        trace!("Analysing {}", &warc_url);
        match process_warc(&warc_url, &client) {
            Ok(summary) => writer.write(warc_url, summary)?,
            Err(e) => {
                error!(
                    "Error fetching {}: {}, nothing written for that WARC",
                    warc_url, e
                );
                thread::sleep(time::Duration::from_secs(60));
            }
        }
    }
    info!("All WARCs processed!");

    Ok(())
}
