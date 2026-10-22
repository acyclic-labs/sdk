//! The existing executor scenarios with both file backends on public services.
use super::{TestResult, execution, scenario::Scenario, scenario_data};
use acyclic_fs::{EmbeddedCapabilities, Fs, LogicalObjectStore, StreamAuthorityStore};
use acyclic_harness::{AgentId, filesystem::MemoryHarnessStorage};
use acyclic_objects::v1::{MemoryObjects, MemoryOptions, ObjectsProvider, grpc::GrpcObjects, wire};
use acyclic_stream::{MemoryStream, StreamProvider, grpc::Client};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

type ServiceFs<P, O> = Fs<StreamAuthorityStore<P>, LogicalObjectStore<O>>;

async fn filesystem<P: StreamProvider, O: ObjectsProvider + 'static>(
    stream: Arc<P>,
    objects: Arc<O>,
    durable: bool,
) -> TestResult<ServiceFs<P, O>> {
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "harness-files".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("created bucket has no identity")?;
    Ok(Fs::new(
        StreamAuthorityStore::new(stream),
        LogicalObjectStore::new(objects, bucket),
        EmbeddedCapabilities { durable },
    ))
}

fn cases() -> TestResult<Vec<(PathBuf, Value)>> {
    let root = acyclic_harness::test_support::environment("CARGO_MANIFEST_DIR")
        .map_or_else(
            || PathBuf::from(acyclic_harness::build_metadata::MANIFEST_DIR),
            PathBuf::from,
        )
        .join("tests/scenarios");
    let mut paths = Vec::new();
    for family in ["core", "recovery"] {
        scenario_data::discover(&root.join(family), &mut paths)?;
    }
    paths.sort();
    let mut cases = Vec::new();
    for path in paths {
        let mut value = scenario_data::load_value(&root, &path)?;
        let driver = value
            .as_object_mut()
            .ok_or("scenario must be an object")?
            .remove("driver");
        if matches!(
            driver.as_ref().map(Value::as_str),
            None | Some(Some("executor"))
        ) {
            cases.push((path, value));
        }
    }
    Ok(cases)
}

async fn compare<P: StreamProvider, O: ObjectsProvider + 'static>(
    stream: Arc<P>,
    filesystem: ServiceFs<P, O>,
) -> TestResult<usize> {
    let cases = cases()?;
    let count = cases.len();
    let results = futures::future::join_all(cases.into_iter().map(|(path, value)| {
        let stream = stream.clone();
        let filesystem = filesystem.clone();
        async move {
            let local = execution::run_on(
                Scenario::from_value(value.clone())?,
                Arc::new(MemoryStream::default()),
            )
            .await?
            .trace()
            .await?;
            let remote =
                execution::run_with_filesystem(Scenario::from_value(value)?, filesystem, stream)
                    .await?
                    .trace()
                    .await?;
            if local != remote {
                return Err(format!("{}: file-backed traces differ", path.display()).into());
            }
            TestResult::Ok(())
        }
    }))
    .await;
    for result in results {
        result?;
    }
    Ok(count)
}

#[tokio::test]
async fn service_backed_files_match_and_wrong_file_bytes_are_rejected() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let objects = Arc::new(MemoryObjects::new(MemoryOptions::default())?);
    let files = filesystem(stream.clone(), objects, false).await?;
    assert!(compare(stream.clone(), files.clone()).await? > 0);
    let (_, mut wrong) = cases()?
        .into_iter()
        .find(|(path, _)| path.ends_with("preserve-seed.json"))
        .ok_or("missing file oracle scenario")?;
    *wrong
        .pointer_mut("/expect/files/src~1lib.rs")
        .ok_or("file oracle missing")? = Value::String("wrong file bytes".into());
    assert!(
        execution::run_with_filesystem(Scenario::from_value(wrong)?, files, stream,)
            .await
            .is_err_and(|error| error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidData))
    );
    Ok(())
}

proptest::proptest! {
    #[test]
    fn provider_selected_files_preserve_arbitrary_text(
        chars in proptest::collection::vec(proptest::prelude::any::<char>(), 0..32),
    ) {
        let text: String = chars.into_iter().collect();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all().build().map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
        runtime.block_on(async {
            let stream = Arc::new(MemoryStream::default());
            let objects = Arc::new(MemoryObjects::new(MemoryOptions::default())?);
            let files = filesystem(stream.clone(), objects, false).await?;
            let storage = MemoryHarnessStorage::from_providers(
                AgentId::new(), 1_048_576, files, stream,
            ).await?;
            execution::publish(&storage, &BTreeMap::from([("src/lib.rs".into(), text.clone())])).await?;
            execution::check_files(
                &BTreeMap::from([("src/lib.rs".into(), Some(text.clone()))]), &storage,
            ).await?;
            assert!(execution::check_files(
                &BTreeMap::from([("src/lib.rs".into(), Some(format!("{text}\0")))]), &storage,
            ).await.is_err_and(|error| error.downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::InvalidData)));
            TestResult::Ok(())
        }).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}

#[derive(Deserialize)]
struct Endpoint {
    endpoint: String,
    bearer: String,
    ca_file: PathBuf,
}
#[derive(Deserialize)]
struct Endpoints {
    streams: Endpoint,
    objects: Endpoint,
}

#[tokio::test]
#[ignore = "needs Cloud request 12: public Objects batches; request 10: authority Session kinds"]
async fn executor_files_and_journal_match_on_the_local_cloud() -> TestResult {
    let file = acyclic_harness::test_support::environment("ACYCLIC_LOCAL_CLOUD")
        .ok_or("ACYCLIC_LOCAL_CLOUD must name the endpoints file")?;
    let endpoints: Endpoints = serde_json::from_slice(&std::fs::read(file)?)?;
    let streams = endpoints.streams;
    let stream = Arc::new(
        Client::connect_eager_with_ca_certificate(
            &streams.endpoint,
            &streams.bearer,
            std::fs::read(streams.ca_file)?,
        )
        .await?,
    );
    let objects = endpoints.objects;
    let objects = Arc::new(
        GrpcObjects::connect(
            &objects.endpoint,
            &objects.bearer,
            Some(&std::fs::read(objects.ca_file)?),
            None,
        )
        .await?,
    );
    let files = filesystem(stream.clone(), objects, true).await?;
    let count = compare(stream, files).await?;
    println!(
        "local cloud: {count} executor scenarios matched with remote file authority, objects and journal"
    );
    Ok(())
}
