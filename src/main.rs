use flate2::read::MultiGzDecoder;
use log::{info, trace, warn};
use reqwest::blocking::Client;
use std::io::prelude::*;
use std::io::BufReader;
use std::sync::mpsc;

use bo_cc::{process_warc, processed_warcs, AnalysisWriter, ArchiveSummary, BoxDynError};

fn warc_present(url: &String) -> bool {
    let x = processed_warcs();
    x.contains(url)
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
        .filter_map(|url| {
            let full_url = format!("https://data.commoncrawl.org/{}", url);
            if !warc_present(&full_url) {
                Some(full_url)
            } else {
                None
            }
        }))
}

fn main() -> Result<(), BoxDynError> {
    env_logger::init();

    let client = reqwest::blocking::Client::new();
    let warc_urls = get_warcs(&client)?;
    let (tx_summary, mut rx_summary) = mpsc::sync_channel::<ArchiveSummary>(4);

    let mut writer = AnalysisWriter::new();

    for warc_url in warc_urls {
        info!("Analysing {}", &warc_url);
        let summary = process_warc(warc_url, client.clone())?;
        println!("{:?}", summary);
    }
    info!("All WARCs processed!");

    Ok(())
}
