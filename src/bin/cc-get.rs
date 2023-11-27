use flate2::read::MultiGzDecoder;
use log::{error, info, trace};
use std::collections::HashSet;
use std::error::Error;
use std::io::prelude::*;
use std::io::BufReader;

use bo_cc::{process_warc, processed_warcs, AnalysisWriter, Client};

fn get_warcs(
    client: &mut Client,
    warcs_present: HashSet<String>,
    archive: &str,
) -> Result<impl Iterator<Item = String>, reqwest::Error> {
    let gz = BufReader::new(
        client
            .get(&format!("crawl-data/{}/warc.paths.gz", archive))?
            .error_for_status()?,
    );

    Ok(BufReader::new(MultiGzDecoder::new(gz))
        .lines()
        .flatten()
        .filter(move |u| !warcs_present.contains(u)))
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let archive = std::env::args()
        .nth(1)
        .ok_or("Usage: cc-get <archive, e.g. CC-MAIN-2023-40>")?;

    let mut client = bo_cc::Client::new();

    let seen: HashSet<String> = processed_warcs().into_iter().collect();
    let warc_urls = get_warcs(&mut client, seen, &archive)?;

    let mut writer = AnalysisWriter::new();

    for warc_url in warc_urls {
        trace!("Analysing {}", &warc_url);
        match process_warc(&warc_url, &client) {
            Ok(summary) => {
                writer.write(warc_url, summary)?;
            }
            Err(e) => {
                error!("Unknown error fetching {}: {}, giving up.", warc_url, e);
                break;
            }
        }
    }
    info!("Shutting down...");
    Ok(())
}
