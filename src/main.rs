use attohttpc;
use flate2::read::MultiGzDecoder;
use log::error;
use log::info;
use log::trace;
use std::collections::HashSet;
use std::io::prelude::*;
use std::io::BufReader;

use bo_cc::{process_warc, processed_warcs, AnalysisWriter, BoxDynError};

fn get_warcs(
    client: &attohttpc::Session,
    warcs_present: HashSet<String>,
) -> Result<impl Iterator<Item = String>, attohttpc::Error> {
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

    let client = attohttpc::Session::new();
    let seen: HashSet<String> = processed_warcs().into_iter().collect();
    let warc_urls = get_warcs(&client, seen)?;

    let mut writer = AnalysisWriter::new();

    for warc_url in warc_urls {
        trace!("Analysing {}", &warc_url);
        match process_warc(&warc_url, &client) {
            Ok(summary) => writer.write(warc_url, summary)?,
            Err(e) => {
                error!("Unknown error fetching {}: {}, giving up.", warc_url, e);
                break;
            }
        }
    }
    info!("All WARCs processed!");

    Ok(())
}
