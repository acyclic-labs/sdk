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

#[cfg(feature = "local")]
mod local {
    use super::*;
    use acyclic_objects::v1::{ObjectsProvider as _, local::LocalObjects, wire};
    use acyclic_stream::{
        LocalStream,
        testing::{Counted, RequestLedger},
    };

    type LocalFixture = Fixture<
        acyclic_fs::distributed::StreamAuthorityStore<Counted<LocalStream>>,
        acyclic_fs::LogicalObjectStore<Counted<LocalObjects>>,
    >;
    struct DurableBackend {
        fixture: LocalFixture,
        // The actual providers must close before the owned directory is removed.
        _directory: tempfile::TempDir,
    }
    impl Backend for DurableBackend {
        type Authority = <LocalFixture as Backend>::Authority;
        type Objects = <LocalFixture as Backend>::Objects;
        fn filesystem(&self) -> &acyclic_fs::Fs<Self::Authority, Self::Objects> {
            self.fixture.filesystem()
        }
        fn requests(&self) -> RequestLedger {
            self.fixture.requests()
        }
    }
    async fn fresh() -> Result<DurableBackend, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let stream =
            LocalStream::open(directory.path().join("streams"), Default::default()).await?;
        let objects =
            LocalObjects::open(directory.path().join("objects"), Default::default(), None).await?;
        let bucket = objects
            .create_bucket(wire::CreateBucketRequest {
                name: "filesystem-conformance".into(),
                mutation: None,
            })
            .await?;
        Ok(DurableBackend {
            fixture: from_providers(
                stream,
                objects,
                bucket
                    .bucket
                    .ok_or("created bucket has no public reference")?,
                EmbeddedCapabilities { durable: true },
            ),
            _directory: directory,
        })
    }
    suite!(public_local_suite, fresh);
    #[tokio::test]
    async fn durable_immutable_read_budget() -> Result<(), Box<dyn std::error::Error>> {
        let measured = bench(&fresh().await?).await?;
        assert_eq!(measured.reads, 256);
        assert_eq!(measured.requests.control(), 0);
        Ok(())
    }
}
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
