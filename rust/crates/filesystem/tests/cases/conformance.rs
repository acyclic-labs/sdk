//! Disposable reference adapter for the shared public filesystem suite.
use acyclic_fs::{EmbeddedCapabilities, conformance::*};
async fn memory() -> Result<impl Backend, Failure> {
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    Ok(from_providers(
        acyclic_stream::MemoryStream::default(),
        objects,
        bucket,
        EmbeddedCapabilities::MEMORY,
    ))
}
suite!(public_memory_suite, memory);
#[tokio::test]
async fn shared_benchmark_counts_both_real_providers() -> Result<(), Failure> {
    let backend = memory().await?;
    let measured = bench(&backend).await?;
    assert_eq!(measured.reads, 256);
    assert_eq!(measured.requests.control(), 0);
    let all = backend.requests().requests();
    assert!(all.data() > 0);
    assert!(all.control() > 0);
    Ok(())
}
