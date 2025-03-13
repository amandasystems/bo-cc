use std::error::Error;
use std::io::{self, BufRead};

use bo_cc::{
    attributes_have_pattern, elements_matching_query, elements_with, interesting_patterns, patterns_in, processed_warcs, to_storage_fn, ArchiveSummary
};
use rayon::prelude::*;

enum Cmd {
    Summary,
    Patterns,
    Forms,
    FindPattern,
    FindInput,
    Help,
}

impl Cmd {
    fn run(&self, warcs: Vec<String>) {
        match self {
            Cmd::Summary => cmd_summarise(warcs),
            Cmd::Patterns => cmd_patterns(warcs),
            Cmd::Forms => cmd_forms_with(warcs),
            Cmd::FindPattern => cmd_find_pattern(warcs),
            Cmd::FindInput => Self::find_input(warcs),
            Cmd::Help => Self::help(),
        }
    }

    fn from_string(maybe_a_command: &str) -> Option<Self> {
        match maybe_a_command {
            "summary" => Some(Cmd::Summary),
            "patterns" => Some(Cmd::Patterns),
            "forms" => Some(Cmd::Forms),
            "find-pattern" => Some(Cmd::FindPattern),
            "find-input" => Some(Cmd::FindInput),
            _ => None,
        }
    }

    fn parse_args() -> Self {
        std::env::args()
            .nth(1)
            .and_then(|s| Cmd::from_string(&s))
            .unwrap_or(Cmd::Help)
    }

    fn find_input(warcs: Vec<String>) {
        let stdin = io::stdin();
        eprintln!("Element query on stdin...");
        let element_query = stdin.lock().lines().next().unwrap().unwrap();
        eprintln!("Looking for elements matching {element_query}...");
        warcs
            .par_iter()
            .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(warc)))
            .flat_map(|summary| summary.urls_with_pattern_forms)
            .filter_map(|url_summary| {
                let matching_elements: Vec<String> = url_summary
                    .with_patterns
                    .into_iter()
                    .flat_map(|form| {
                        elements_matching_query(
                            &form,
                            &element_query,
                            |tag| {
                                attributes_have_pattern(tag.attributes())
                            },
                            |input_tag, _| {
                                let attributes = input_tag.attributes();
                                interesting_patterns(attributes).map(|a| a.to_owned()).collect()
                            },
                        )
                    })
                    .collect();

                if matching_elements.is_empty() {
                    None
                } else {
                    Some((url_summary.url, matching_elements))
                }
            })
            .for_each(|(_, matching_elements)| {
                for element in matching_elements.into_iter() {
                    let element = element.trim();
                    if !(element.is_empty()){
                    println!("{element}");
}                }
            });
    }

    fn help() {
        println!("Usage: cc-get summary | patterns | forms | find-input");
    }
}

type Tally = (i64, i64, i64, i64, i64, i64);

fn identity_tally() -> Tally {
    (0, 0, 0, 0, 0, 0)
}

fn elementwise_sum(l: Tally, r: Tally) -> Tally {
    (
        l.0 + r.0,
        l.1 + r.1,
        l.2 + r.2,
        l.3 + r.3,
        l.4 + r.4,
        l.5 + r.5,
    )
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
        .flat_map(|warc| {
            let summary = ArchiveSummary::from_file(&to_storage_fn(&warc))?;
            let urls_w_pattern = summary.urls_with_pattern_forms.len() as i64;
            let successful = urls_w_pattern + summary.nr_urls_without_patterns;
            let forms_w_pattern: i64 = summary
                .urls_with_pattern_forms
                .iter()
                .map(|u| u.with_patterns.len() as i64)
                .sum();
            let total_urls = successful + summary.nr_unknown_encoding;
            let total_forms = forms_w_pattern + summary.nr_forms_without_patterns;

            Ok::<_, std::io::Error>((
                urls_w_pattern,
                total_urls,
                forms_w_pattern,
                total_forms,
                summary.nr_unknown_encoding,
                successful,
            ))
        })
        .reduce(identity_tally, elementwise_sum);

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
        .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(warc)))
        .flat_map(|summary| summary.urls_with_pattern_forms)
        .flat_map(|form_summary| form_summary.with_patterns)
        .for_each(|form| {
            let stripped_form = form.replace(['\n', '\r'], "");
            println!("{stripped_form}");
        });
}

fn cmd_patterns(warcs: Vec<String>) {
    warcs
        .par_iter()
        .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(warc)))
        .flat_map(|summary| summary.urls_with_pattern_forms)
        .flat_map(|url_summary| url_summary.with_patterns)
        .flat_map(|form| patterns_in(&form))
        .for_each(|pattern| {
            println!("{pattern}");
        });
}

fn cmd_find_pattern(warcs: Vec<String>) {
    let stdin = io::stdin();
    let pattern = stdin.lock().lines().next().unwrap().unwrap();
    println!("Searching for forms containing {pattern}...");
    warcs
        .par_iter()
        .flat_map(|warc| ArchiveSummary::from_file(&to_storage_fn(warc)))
        .flat_map(|summary| summary.urls_with_pattern_forms)
        .filter_map(|url_summary| {
            let matching_elements: Vec<String> = url_summary
                .with_patterns
                .into_iter()
                .flat_map(|form| elements_with(&form, &pattern))
                .collect();

            if matching_elements.is_empty() {
                None
            } else {
                Some((url_summary.url, matching_elements))
            }
        })
        .for_each(|(url, matching_elements)| {
            println!("URL: {url}");
            for element in matching_elements.into_iter() {
                println!("<!-- BEGIN FORM --!>");
                println!("{element}");
                println!("<!-- END FORM --!>")
            }
        });
}

fn main() -> Result<(), Box<dyn Error>> {
    Cmd::parse_args().run(processed_warcs());
    Ok(())
}
