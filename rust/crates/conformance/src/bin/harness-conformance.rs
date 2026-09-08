//! Validate a cross-language Harness report and emit its qualification receipt.

use acyclic_conformance::runner::validate_harness_report;
use std::io::{self, Read};

fn main() {
    if let Err(error) = run() {
        eprintln!("harness conformance: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os();
    let _binary = arguments.next();
    let input = match (arguments.next(), arguments.next()) {
        (None, None) => {
            let mut bytes = Vec::new();
            io::stdin().read_to_end(&mut bytes)?;
            bytes
        }
        (Some(path), None) => std::fs::read(path)?,
        _ => return Err("usage: harness-conformance [REPORT.json]".into()),
    };
    let receipt = validate_harness_report(&input)?;
    println!("{}", serde_json::to_string(&receipt)?);
    if !receipt.qualified {
        return Err("one or more canonical cases did not pass".into());
    }
    Ok(())
}
