//! The real Objects Edge and MemoryObjects satisfy the same harness oracle.
use acyclic_objects::v1::{MemoryObjects, MemoryOptions};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};

mod content;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
struct Remote {
    endpoint: String,
    bearer: String,
    ca_file: PathBuf,
}

#[derive(Deserialize)]
struct Endpoints {
    objects: Remote,
}

fn memory() -> TestResult<Arc<MemoryObjects>> {
    Ok(Arc::new(MemoryObjects::new(MemoryOptions::default())?))
}

proptest::proptest! {
    #[test]
    fn arbitrary_content_round_trips_and_a_changed_read_is_rejected(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 4..64),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().expect("runtime");
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
async fn harness_content_matches_its_oracle_and_detects_divergence() -> TestResult {
    let bytes = content::Content::exact(b"owner bytes");
    let first = content::run(memory()?, "harness-content", &bytes).await?;
    let again = content::run(memory()?, "harness-content", &bytes).await?;
    assert_eq!(first, again);
    let other = content::Content::exact(b"other owner bytes");
    assert_ne!(
        content::run(memory()?, "harness-content", &other).await?,
        first
    );
    let wrong = content::Content {
        staged: b"owner bytes",
        expected: b"owner bytez",
    };
    assert!(
        content::run(memory()?, "harness-content", &wrong)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "needs a local cloud: node scripts/local-cloud.mjs"]
async fn objects_and_harness_content_match_the_local_cloud() -> TestResult {
    let file = std::env::var_os("ACYCLIC_LOCAL_CLOUD")
        .ok_or("ACYCLIC_LOCAL_CLOUD must name the local cloud endpoints file")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
    let objects = endpoints.objects.objects().await?;
    acyclic_objects::v1::conformance::verify(objects.as_ref(), "conformance").await?;
    let bytes = content::Content::exact(b"owner bytes");
    let cloud = content::run(objects, "harness-content", &bytes).await?;
    let local = content::run(memory()?, "harness-content", &bytes).await?;
    assert_eq!(cloud, local);
    println!("local cloud: Objects conformance and exact harness content matched");
    Ok(())
}
