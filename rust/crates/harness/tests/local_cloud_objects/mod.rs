//! The real Objects Edge and MemoryObjects satisfy the same harness oracle.
use acyclic_objects::MemoryObjects;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};

mod content;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
struct Remote {
    endpoint: String,
    bearer: String,
    /// A second account's bearer, for the cross-account negative probes.
    second_bearer: String,
    ca_file: PathBuf,
}

#[derive(Deserialize)]
struct Endpoints {
    objects: Remote,
}

fn memory() -> TestResult<Arc<MemoryObjects>> {
    Ok(Arc::new(MemoryObjects::memory()))
}

proptest::proptest! {
    #[test]
    fn arbitrary_content_round_trips_and_a_changed_read_is_rejected(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 4..64),
    ) {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        let runtime = RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all().build().expect("runtime")
        });
        runtime.block_on(async {
            let exact = content::Content::exact(&bytes);
            let first = content::run(memory()?, "harness-content", &exact).await?;
            assert_eq!(first, content::run(memory()?, "harness-content", &exact).await?);
            let mut changed = bytes.clone();
            changed[0] ^= 1;
            let wrong = content::Content { staged: &bytes, expected: &changed };
            assert!(content::run(memory()?, "harness-content", &wrong).await.is_err());
            TestResult::Ok(())
        }).expect("content property");
    }
}

#[tokio::test]
#[ignore = "job=stress-cloud-contracts; needs a local cloud: node scripts/local-cloud.mjs"]
async fn objects_and_harness_content_match_the_local_cloud() -> TestResult {
    let file = acyclic_harness::test_support::environment("ACYCLIC_LOCAL_CLOUD")
        .ok_or("ACYCLIC_LOCAL_CLOUD must name the local cloud endpoints file")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
    let objects = endpoints.objects.objects().await?;
    let second = endpoints.objects.second().await?;
    acyclic_objects::conformance::verify(objects.as_ref(), second.as_ref(), "conformance").await?;
    let bytes = content::Content::exact(b"owner bytes");
    let cloud = content::run(objects, "harness-content", &bytes).await?;
    let local = content::run(memory()?, "harness-content", &bytes).await?;
    assert_eq!(cloud, local);
    println!("local cloud: Objects conformance and exact harness content matched");
    Ok(())
}
