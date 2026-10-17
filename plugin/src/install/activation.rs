use acyclic_native_runtime::{RenameMode, durable_rename};
use std::{
    fs,
    io::{self, Read as _},
    path::Path,
};

pub(crate) fn activation(
    command: &str,
    arguments: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match (command, arguments) {
        ("__installer-rename", [from, to, mode]) => {
            let mode = match mode.as_str() {
                "replace" => RenameMode::Replace,
                "no-replace" => RenameMode::NoReplace,
                _ => return Err(io::Error::other("invalid installer rename mode").into()),
            };
            durable_rename(Path::new(from), Path::new(to), mode, None)?;
        }
        ("__verify-certification", [receipt]) => {
            let mut bytes = Vec::new();
            fs::File::open(receipt)?
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 1024 * 1024 {
                return Err(io::Error::other("certification receipt exceeds 1 MiB").into());
            }
            let receipt = serde_json::from_slice(&bytes)?;
            let mut hash = blake3::Hasher::new();
            hash.update_reader(fs::File::open(std::env::current_exe()?)?)?;
            if !super::certification::valid(
                &receipt,
                hash.finalize().to_hex().as_str(),
                std::env::consts::OS,
                std::env::consts::ARCH,
                env!("CARGO_PKG_VERSION"),
            ) {
                return Err(io::Error::other(
                    "certification receipt does not match this Acyclic executable",
                )
                .into());
            }
        }
        _ => return Err(io::Error::other("invalid installer activation arguments").into()),
    }
    Ok(())
}
