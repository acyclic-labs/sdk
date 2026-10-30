//! Replay captured inputs and check their recorded provenance.
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, error::Error, path::Path};

fn replay() -> Result<usize, Box<dyn Error>> {
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
            match std::panic::catch_unwind(|| acyclic_codec_fuzz::exercise(target, &bytes)) {
                Ok(result) => result?,
                Err(_) => {
                    return Err(format!(
                        "decoder oracle {name} panicked; recovery: cargo +nightly fuzz run {target} corpus/{name}"
                    ).into());
                }
            }
            count += 1;
        }
    }
    if count == 0 {
        return Err("captured corpus is empty".into());
    }
    Ok(count)
}

fn main() {
    match std::panic::catch_unwind(replay) {
        Ok(Ok(count)) => println!("replayed {count} captured inputs"),
        Ok(Err(error)) => {
            eprintln!(
                "corpus replay failed: {error}; recovery after correcting the reported mismatch: cargo run --example replay"
            );
            std::process::exit(1);
        }
        Err(_) => {
            eprintln!("corpus decoder validation panicked; recovery: cargo run --example replay");
            std::process::exit(1);
        }
    }
}
