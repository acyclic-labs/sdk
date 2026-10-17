//! One per-user provider owner; hooks remain ordinary Streams clients.
use crate::{Error, LocalHarness, LocalHarnessConfig, Result};
use acyclic_stream::{LocalStream, StreamPath, grpc::Client};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

mod bootstrap;
mod credential;
mod provider;
mod registry;
mod request_feed;
mod response;
mod server;
mod workers;

pub(super) use bootstrap::{binding, existing_binding};
pub use server::{run_local_owner, stop_local_owner};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Address {
    endpoint: String,
    certificate: String,
    instance: uuid::Uuid,
}

impl Address {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let address: Self = super::volume::decode(bytes).map_err(storage)?;
        let uri: http::Uri = address.endpoint.parse().map_err(storage)?;
        if uri.scheme_str() != Some("https")
            || uri.host() != Some("127.0.0.1")
            || uri.port_u16().is_none_or(|port| port == 0)
            || address.certificate.is_empty()
        {
            return Err(Error::Unauthorized(
                "local owner requires pinned loopback TLS".into(),
            ));
        }
        Ok(address)
    }
}

#[cfg(feature = "test-support")]
pub(crate) fn fuzz_input(bytes: &[u8]) {
    super::handoff::fuzz(bytes);
    super::local_paths::fuzz(bytes);
    request_feed::fuzz_bytes(bytes);
    let _ = Address::from_bytes(bytes);
    let _ = super::volume::decode::<PathBuf>(bytes);
    if let Ok(text) = std::str::from_utf8(bytes) {
        let _ = credential::verify(&[0; 32], text);
    }
}

#[cfg(feature = "test-support")]
pub(crate) use request_feed::fuzz_request;

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn read(path: &Path) -> Result<Vec<u8>> {
    super::native::read_bounded(
        std::fs::File::open(path).map_err(storage)?,
        acyclic_stream::MAX_RECORD_BYTES,
    )
}

fn directory() -> Result<PathBuf> {
    let path = std::env::var_os("ACYCLIC_LOCAL_OWNER_DIR")
        .map(PathBuf::from)
        .or_else(|| dirs::data_local_dir().map(|path| path.join("acyclic/harness/owner")))
        .ok_or_else(|| Error::Unsupported("per-user local directory is unavailable".into()))?;
    crate::local::private_directory(&path)
}

async fn connect(address: &Address, credential: &str) -> Result<Client> {
    Client::connect_eager_with_ca_certificate(
        &address.endpoint,
        credential,
        address.certificate.as_bytes(),
    )
    .await
    .map_err(storage)
}
