//! Browser persistence providers for the same canonical Filesystem engine.
//!
//! These providers carry no browser binding facade and can be composed directly
//! with ordinary Rust consumers. Opening them grants no workspace authority.
//! Authority publication uses the canonical `Streams` journal and volume commit
//! path. `IndexedDB` supplies only the durable journal and immutable objects.
//!
//! Object payloads use `acyclic_stream::OwnedBytes`: ordinary WASM vectors move
//! into their carrier without a second Rust byte copy. Work counters bound SDK
//! buffers and retained Blob-handle vectors before materialization; opaque
//! browser/provider allocations are not native physical-quota receipts.
#[cfg(target_arch = "wasm32")]
#[path = "indexed_db/mod.rs"]
mod indexed_db;
#[cfg(target_arch = "wasm32")]
pub use indexed_db::{IndexedDbObjectStore, IndexedDbOpenError};

#[cfg(target_arch = "wasm32")]
pub(crate) fn authority_database_name(name: &str) -> String {
    format!("acyclic-fs-authority/{name}")
}

/// Opens browser authority through the same `Streams` commit path as native hosts.
#[cfg(target_arch = "wasm32")]
pub async fn open_authority(
    name: &str,
    limits: acyclic_stream::BrowserStreamLimits,
) -> Result<
    crate::StreamAuthorityStore<dyn acyclic_stream::StreamProvider>,
    acyclic_stream::StreamError,
> {
    if name.is_empty() {
        return Err(acyclic_stream::StreamError::InvalidArgument);
    }
    // Reject an incompatible durable object/authority schema before creating a
    // journal. Opening a fresh journal beside an older authority would silently
    // substitute an empty volume for persisted user state.
    let objects = indexed_db::open_database(name)
        .await
        .map_err(|_| acyclic_stream::StreamError::Unavailable)?;
    objects.close();
    let provider =
        acyclic_stream::BrowserStream::open(&authority_database_name(name), limits).await?;
    let provider: std::sync::Arc<dyn acyclic_stream::StreamProvider> =
        std::sync::Arc::new(provider);
    Ok(crate::StreamAuthorityStore::new(provider))
}
