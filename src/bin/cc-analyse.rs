use std::error::Error;

use bo_cc::{patterns_in, processed_warcs, to_storage_fn, ArchiveSummary};
use rayon::prelude::*;

enum Cmd {
    Summary,
    Patterns,
    Forms,
}

fn cmd_summarise(warcs: Vec<String>) {
    let nr_warcs = warcs.len();

    let summary = warcs
        .par_iter()
        .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(&warc)))
        .reduce(ArchiveSummary::default, |a, b| a.merge(b));

    let nr_urls_with_patterns = summary.urls_with_pattern_forms.len() as f64;
    let successful_urls = nr_urls_with_patterns as f64 + summary.nr_urls_without_patterns as f64;
    let total_urls = successful_urls + summary.nr_unknown_encoding as f64;
    let nr_forms_with_patterns: usize = summary
        .urls_with_pattern_forms
        .iter()
        .map(|u| u.with_patterns.len())
        .sum();
    let total_forms = summary.nr_forms_without_patterns as f64 + nr_forms_with_patterns as f64;

    println!("Processed {nr_warcs} WARCs with {total_urls} URLs ({successful_urls} OK). Results: ");
    println!(
        "Nr URLs with unknown encoding, broken HTML, etc: {} ({:.4}%)",
        summary.nr_unknown_encoding,
        summary.nr_unknown_encoding as f64 / total_urls
    );
    println!(
        "URLs with pattern/s: {}, ({:.1}%)",
        nr_urls_with_patterns,
        100f64 * (nr_urls_with_patterns / total_urls)
    );
    println!(
        "Forms with patterns: {} ({:.1}%)",
        nr_forms_with_patterns,
        100f64 * (nr_forms_with_patterns as f64 / total_forms)
    );
}

fn cmd_forms_with(warcs: Vec<String>) {
    warcs
        .par_iter()
        .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(&warc)))
        .flat_map(|summary| summary.urls_with_pattern_forms)
        .flat_map(|form_summary| form_summary.with_patterns)
        .for_each(|form| {
            let stripped_form = form.replace("\n", "").replace("\r", "");
            println!("{stripped_form}");
        });
}

fn cmd_patterns(warcs: Vec<String>) {
    warcs
        .par_iter()
        .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(&warc)))
        .flat_map(|summary| summary.urls_with_pattern_forms)
        .flat_map(|form_summary| form_summary.with_patterns)
        .flat_map(|form| patterns_in(&form))
        .for_each(|pattern| {
            println!("{pattern}");
        });
}

fn main() -> Result<(), Box<dyn Error>> {
    let subcommand = std::env::args()
        .nth(1)
        .and_then(|arg| match arg.as_str() {
            "summary" => Some(Cmd::Summary),
            "patterns" => Some(Cmd::Patterns),
            "forms" => Some(Cmd::Forms),
            _ => None,
        })
        .ok_or("usage: cc-get summary | patterns | forms")?;

    let warcs: Vec<_> = processed_warcs();

    match subcommand {
        Cmd::Summary => cmd_summarise(warcs),
        Cmd::Patterns => cmd_patterns(warcs),
        Cmd::Forms => cmd_forms_with(warcs),
    }

    Ok(())
}
