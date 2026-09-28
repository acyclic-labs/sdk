//! Route-to-response contract for the hosted Objects HTTP adapter.
//!
//! This small platform-neutral module is shared by the WASM validator and the native contract
//! generator, so the native workspace build does not need to compile browser bindings.

/// Route-to-response contract consumed by the generated TypeScript HTTP decoder.
pub const HTTP_RESPONSE_CONTRACT: &[(&str, &str)] = &[
    ("buckets/create", "bucket"),
    ("buckets/head", "bucket"),
    ("buckets/delete", "existed"),
    ("objects/put", "version"),
    ("objects/head", "version"),
    ("objects/get", "stored"),
    ("objects/delete", "delete"),
    ("objects/list", "list"),
    ("snapshots/create", "snapshot"),
    ("snapshots/destroy", "existed"),
    ("snapshots/fork", "bucket"),
    ("multipart/create", "multipart"),
    ("multipart/upload-part", "part"),
    ("multipart/list-parts", "parts"),
    ("multipart/complete", "version"),
    ("multipart/abort", "existed"),
];
