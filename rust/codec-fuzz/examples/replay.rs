//! Replay captured inputs and check their recorded provenance.
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::Path,
};

fn external_expectations() -> Result<BTreeMap<String, (String, String)>, Box<dyn Error>> {
    let mut expectations = BTreeMap::new();
    for entry in std::fs::read_dir("captures")? {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
        for output in metadata
            .get("outputs")
            .and_then(serde_json::Value::as_array)
            .ok_or("capture metadata has no output expectations")?
        {
            let field = |name| {
                output
                    .get(name)
                    .and_then(serde_json::Value::as_str)
                    .ok_or("incomplete external capture expectation")
            };
            if expectations
                .insert(
                    field("path")?.to_owned(),
                    (field("codec")?.to_owned(), field("method")?.to_owned()),
                )
                .is_some()
            {
                return Err("duplicate external capture expectation".into());
            }
        }
    }
    Ok(expectations)
}

fn replay(selected: Option<&str>, entry: Option<&str>) -> Result<usize, Box<dyn Error>> {
    let mut expectations = external_expectations()?;
    let sources = [
        ("corpus", std::fs::read_to_string("corpus/SOURCES.tsv")?),
        ("external", std::fs::read_to_string("corpus/EXTERNAL.tsv")?),
    ];
    let mut names = BTreeSet::new();
    let mut count = 0;
    for (prefix, manifest) in &sources {
        for line in manifest
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let fields: Vec<_> = line.splitn(4, '\t').collect();
            let [name, length, digest, _producer] = fields.as_slice() else {
                return Err(format!("invalid provenance row: {line}").into());
            };
            if !names.insert(*name) {
                return Err(format!("duplicate captured name: {name}").into());
            }
            let (target, _) = name.split_once('/').ok_or("captured name has no target")?;
            if selected.is_some_and(|selected| selected != target) {
                continue;
            }
            let bytes = std::fs::read(Path::new("corpus").join(name))?;
            if bytes.len() != length.parse::<usize>()?
                || hex::encode(Sha256::digest(&bytes)) != *digest
            {
                return Err(format!("captured bytes differ from provenance: {name}").into());
            }
            let golden = acyclic_proto_build::golden::frame(&format!("{prefix}/{name}"))?;
            if bytes != golden {
                return Err(format!("captured bytes differ from shared golden: {name}").into());
            }
            let expectation = if *prefix == "external" {
                Some(
                    expectations
                        .remove(*name)
                        .ok_or("external seed has no admission expectation")?,
                )
            } else {
                None
            };
            let entry = entry.unwrap_or(target);
            std::panic::catch_unwind(|| {
                acyclic_codec_fuzz::exercise(entry, &bytes)?;
                if let Some((codec, method)) = &expectation {
                    acyclic_codec_fuzz::admit_external(codec, method, &bytes)
                        .map_err(|error| format!("positive external admission {name}: {error}"))?;
                }
                Ok::<(), String>(())
            })
            .unwrap_or_else(|_| Err("panicked".into()))
            .map_err(|error| format!(
                "decoder oracle {name} (entry {entry}) failed: {error}; recovery: cargo +nightly fuzz run --fuzz-dir . {entry} corpus/{name}"
            ))?;
            count += 1;
        }
    }
    if expectations.keys().any(|name| {
        selected.is_none_or(|target| name.split_once('/').is_some_and(|(own, _)| own == target))
    }) {
        return Err("external expectations have no captured input".into());
    }
    if count == 0 {
        return Err("captured corpus is empty".into());
    }
    Ok(count)
}

fn main() {
    let mut arguments = std::env::args().skip(1);
    let selected = arguments.next();
    let entry = arguments.next();
    let recovery = format!(
        "cargo run --locked --example replay -- {}",
        [selected.as_deref(), entry.as_deref()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ")
    );
    match std::panic::catch_unwind(|| replay(selected.as_deref(), entry.as_deref())) {
        Ok(Ok(count)) => println!("replayed {count} captured inputs"),
        Ok(Err(error)) => {
            eprintln!(
                "corpus replay failed: {error}; recovery after correcting the reported mismatch: {recovery}"
            );
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("corpus decoder validation panicked; recovery: {recovery}");
            std::process::exit(1);
        }
    }
}
