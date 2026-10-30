//! Replay captured encoder output and check its recorded provenance.
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, error::Error, path::Path};

fn replay() -> Result<usize, Box<dyn Error>> {
    let manifest = std::fs::read_to_string("corpus/SOURCES.tsv")?;
    let mut names = BTreeSet::new();
    let mut count = 0;
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
            || format!("{:x}", Sha256::digest(&bytes)) != *digest
        {
            return Err(format!("captured bytes differ from provenance: {name}").into());
        }
        acyclic_codec_fuzz::exercise(target, &bytes)?;
        count += 1;
    }
    if count == 0 {
        return Err("captured corpus is empty".into());
    }
    Ok(count)
}

fn main() {
    match std::panic::catch_unwind(replay) {
        Ok(Ok(count)) => println!("replayed {count} captured encoder outputs"),
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
