//! One writer and provenance format for captures from the original SDK encoders.
use sha2::{Digest, Sha256};
use std::{error::Error, fmt::Write, fs, path::Path};

pub fn write(
    directory: &str,
    manifest_path: &str,
    records: impl IntoIterator<Item = (&'static str, Vec<u8>, String)>,
) -> Result<(), Box<dyn Error>> {
    let directory = Path::new(directory);
    fs::create_dir_all(directory)?;
    let mut manifest =
        String::from("# Original SDK writer output; public deterministic test keys\n");
    for (name, bytes, producer) in records {
        let digest = Sha256::digest(&bytes);
        fs::write(directory.join(name), &bytes)?;
        write!(manifest, "{name}\t{}\t", bytes.len())?;
        for byte in digest {
            write!(manifest, "{byte:02x}")?;
        }
        writeln!(manifest, "\t{producer}")?;
    }
    fs::write(manifest_path, manifest)?;
    Ok(())
}
