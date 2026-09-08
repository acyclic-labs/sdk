//! Validate a cross-language Harness report and emit its qualification receipt.

use acyclic_conformance::runner::{harness_protocol_identity, validate_harness_report};
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
    let first = arguments.next();
    if first.as_deref() == Some(std::ffi::OsStr::new("identity")) {
        if arguments.next().is_some() {
            return Err("usage: harness-conformance identity".into());
        }
        println!("{}", serde_json::to_string(&harness_protocol_identity())?);
        return Ok(());
    }
    if first.as_deref() == Some(std::ffi::OsStr::new("digest")) {
        if arguments.next().is_some() {
            return Err("usage: harness-conformance digest < INPUT".into());
        }
        let mut bytes = Vec::new();
        io::stdin().read_to_end(&mut bytes)?;
        println!("blake3:{}", blake3::hash(&bytes).to_hex());
        return Ok(());
    }
    if first.as_deref() == Some(std::ffi::OsStr::new("bundle-digest")) {
        let mut paths = arguments.collect::<Vec<_>>();
        paths.sort();
        if paths.is_empty() {
            return Err("usage: harness-conformance bundle-digest ARCHIVE...".into());
        }
        let mut hasher = blake3::Hasher::new();
        for path in paths {
            let name = std::path::Path::new(&path)
                .file_name()
                .ok_or("artifact path has no file name")?
                .as_encoded_bytes();
            hasher.update(name);
            hasher.update(&[0]);
            hasher.update(&std::fs::read(path)?);
        }
        println!("blake3:{}", hasher.finalize().to_hex());
        return Ok(());
    }
    let first = match first.as_deref() {
        Some(value) if value == std::ffi::OsStr::new("validate") => arguments.next(),
        value => value.map(std::ffi::OsStr::to_os_string),
    };
    let input = match (first, arguments.next()) {
        (None, None) => {
            let mut bytes = Vec::new();
            io::stdin().read_to_end(&mut bytes)?;
            bytes
        }
        (Some(path), None) => std::fs::read(path)?,
        _ => return Err("usage: harness-conformance [validate] [REPORT.json] | identity".into()),
    };
    let receipt = validate_harness_report(&input)?;
    println!("{}", serde_json::to_string(&receipt)?);
    if !receipt.qualified {
        return Err("one or more canonical cases did not pass".into());
    }
    Ok(())
}
