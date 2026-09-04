//! Immutable, permanently versioned Objects contract and reference providers.

/// Generated public gRPC schema and client/server bindings.
#[allow(missing_docs, clippy::all)]
pub mod wire {
    include!("generated/acyclic.objects.v1.rs");
}

/// Canonical public descriptor set used by compatibility and conformance gates.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-objects-v1.bin");

/// Fixed public compatibility limits.
pub mod limits {
    /// Maximum UTF-8 object-key length.
    pub const KEY_BYTES: usize = 1_024;
    /// Maximum encoded user metadata per version.
    pub const USER_METADATA_BYTES: usize = 2 * 1_024;
    /// Maximum entries returned in one listing page.
    pub const LIST_PAGE_ENTRIES: u32 = 1_000;
    /// Maximum complete object size.
    pub const OBJECT_BYTES: u64 = 5 * 1_024 * 1_024 * 1_024 * 1_024;
    /// Maximum single-put size.
    pub const SINGLE_PUT_BYTES: u64 = 5 * 1_024 * 1_024 * 1_024;
    /// Maximum multipart part count.
    pub const MULTIPART_PARTS: u32 = 10_000;
    /// Minimum non-final multipart part size.
    pub const MIN_MULTIPART_PART_BYTES: u64 = 5 * 1_024 * 1_024;
    /// Maximum multipart part size.
    pub const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1_024 * 1_024 * 1_024;
    /// Retention of exact idempotency outcomes.
    pub const IDEMPOTENCY_RETENTION_SECONDS: u64 = 7 * 24 * 60 * 60;
    /// Lifetime of an ordinary listing view.
    pub const LISTING_VIEW_SECONDS: u64 = 24 * 60 * 60;
    /// Minimum grace before unreachable bytes become reclaimable.
    pub const RECLAMATION_GRACE_SECONDS: u64 = 7 * 24 * 60 * 60;
}

#[cfg(feature = "grpc")]
mod grpc;
#[cfg(feature = "grpc")]
pub use grpc::*;

mod provider;
pub use provider::*;
