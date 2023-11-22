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

    let (
        urls_with_pattern,
        total_urls,
        forms_w_pattern,
        total_forms,
        nr_unknown_encoding,
        successful_urls,
    ) = warcs
        .into_par_iter()
        .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(&warc)))
        .fold(
            || (0, 0, 0, 0, 0, 0),
            |(
                urls_with_pattern,
                total_urls,
                forms_w_pattern,
                total_forms,
                unknown_encoding,
                successful_urls,
            ),
             next| {
                let next_urls_w_pattern = next.urls_with_pattern_forms.len() as i64;
                let successful = next_urls_w_pattern + next.nr_urls_without_patterns;
                let next_forms_w_pattern: i64 = next
                    .urls_with_pattern_forms
                    .iter()
                    .map(|u| u.with_patterns.len() as i64)
                    .sum();

                (
                    urls_with_pattern + next_urls_w_pattern,
                    total_urls + successful + next.nr_unknown_encoding,
                    forms_w_pattern + next_forms_w_pattern,
                    total_forms + next_forms_w_pattern + next.nr_forms_without_patterns,
                    unknown_encoding + next.nr_unknown_encoding,
                    successful_urls + successful,
                )
            },
        )
        .reduce(
            || (0, 0, 0, 0, 0, 0),
            |l, r| {
                (
                    l.0 + r.0,
                    l.1 + r.1,
                    l.2 + r.2,
                    l.3 + r.3,
                    l.4 + r.4,
                    l.5 + r.5,
                )
            },
        );

    println!("Processed {nr_warcs} WARCs with {total_urls} URLs ({successful_urls} OK). Results: ");
    println!(
        "Nr URLs with unknown encoding, broken HTML, etc: {nr_unknown_encoding} ({:.4}%)",
        nr_unknown_encoding as f64 / total_urls as f64
    );
    println!(
        "URLs with pattern/s: {urls_with_pattern}, ({:.1}%)",
        100f64 * (urls_with_pattern as f64 / total_urls as f64)
    );
    println!(
        "Forms with patterns: {forms_w_pattern} ({:.1}%)",
        100f64 * (forms_w_pattern as f64 / total_forms as f64)
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
