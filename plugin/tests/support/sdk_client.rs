//! The qualification fixtures use the same authenticated SDK providers as hooks.
use acyclic_stream::grpc::Client;
use serde_json::Value;
use std::path::Path;

pub async fn connect(root: &Path, scope: &str) -> Result<Client, Box<dyn std::error::Error>> {
    let address: Value = serde_json::from_slice(&std::fs::read(root.join("address.json"))?)?;
    let secret: [u8; 32] = std::fs::read(root.join("credential"))?
        .try_into()
        .map_err(|_| "invalid owner credential")?;
    let token = format!(
        "{scope}.{}",
        blake3::keyed_hash(&secret, scope.as_bytes()).to_hex()
    );
    Ok(Client::connect_eager_with_ca_certificate(
        address["endpoint"].as_str().ok_or("missing endpoint")?,
        &token,
        address["certificate"]
            .as_str()
            .ok_or("missing certificate")?
            .as_bytes(),
    )
    .await?)
}

pub async fn workspace(
    root: &Path,
    directory: &Path,
) -> Result<Client, Box<dyn std::error::Error>> {
    let canonical = std::fs::canonicalize(directory)?;
    let scope = blake3::hash(canonical.as_os_str().as_encoded_bytes()).to_hex();
    connect(root, scope.as_str()).await
}
