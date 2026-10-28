//! Code-generation probe for downstream hosts sharing public provider owners.
//! Runtime observations are qualified by the unchanged scenario corpus.
use std::sync::Arc;

fn main() {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let objects = acyclic_objects::MemoryObjects::memory();
    let authority = Arc::new(acyclic_fs::StreamAuthorityStore::new(stream.clone()));
    let objects = Arc::new(acyclic_fs::LogicalObjectStore::new(
        Arc::new(objects)));
    let filesystem =
        acyclic_fs::Fs::new(authority, objects, acyclic_fs::EmbeddedCapabilities::MEMORY);
    let future = acyclic_harness_conformance::collect_on(
        serde_json::json!({"driver": "unknown"}),
        Vec::new(),
        filesystem,
        stream,
    );
    drop(std::hint::black_box(future));
}
