use flate2::read::MultiGzDecoder;
use log::{error, info, trace};
use std::collections::HashSet;
use std::error::Error;
use std::io::prelude::*;
use std::io::BufReader;
use std::thread;
use std::time::Duration;

use bo_cc::{process_warc, processed_warcs, user_agent, AnalysisWriter};

fn get_warcs(
    client: &attohttpc::Session,
    warcs_present: HashSet<String>,
    archive: &str,
) -> Result<impl Iterator<Item = String>, attohttpc::Error> {
    // FIXME this archive is hard-coded!
    let gz = BufReader::new(
        client
            .get(format!(
                "http://data.commoncrawl.org/crawl-data/{}/warc.paths.gz",
                archive
            ))
            .header("User-agent", user_agent())
            .send()?
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

    let client = attohttpc::Session::new();
    let seen: HashSet<String> = processed_warcs().into_iter().collect();
    let warc_urls = get_warcs(&client, seen, &archive)?;
    println!("Waiting for cooldown...");
    thread::sleep(Duration::from_secs_f32(2.0));

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
