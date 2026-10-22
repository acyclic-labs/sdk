//! Shared scenario families over independently selected, constructible providers.
use super::*;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, LogicalObjectStore, StreamAuthorityStore,
};
use acyclic_objects::v1::{MemoryObjects, MemoryOptions};

async fn row<P, A, O>(name: &str, files: Fs<A, O>, stream: Arc<P>) -> TestResult
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let cases = scenarios()?;
    assert_eq!(cases.len(), 30, "scenario inventory changed");
    let results = futures::future::join_all(cases.iter().map(|(path, case)| {
        let files = files.clone();
        let stream = stream.clone();
        async move {
            let root = format!("matrix/{name}/{}", path.strip_prefix(root())?.display())
                .replace('\\', "/")
                .replace('.', "-");
            let expected = trace(case, Arc::new(MemoryStream::default()), &root).await?;
            let observed = match case {
                Case::Executor(value) => Value::Array(
                    execution::run_with_filesystem(
                        Scenario::from_value(value.clone())?,
                        files,
                        stream,
                    )
                    .await?
                    .trace()
                    .await?,
                ),
                Case::Subagents(case) => subagents::run_on(case, stream, &root).await?,
            };
            if observed != expected {
                return Err(format!("{name}/{}: scenario traces differ", path.display()).into());
            }
            TestResult::Ok(())
        }
    }))
    .await;
    for result in results {
        result?;
    }
    eprintln!(
        "substrate matrix {name}: {} shared cases passed",
        cases.len()
    );
    Ok(())
}

async fn files<P: StreamProvider>(name: &str, stream: Arc<P>) -> TestResult {
    row(&format!("memory/{name}"), Fs::memory(), stream.clone()).await?;
    let directory = tempfile::tempdir()?;
    let local = Fs::local(acyclic_fs::LocalOptions::new(directory.path())).await?;
    row(&format!("disk/{name}"), local, stream.clone()).await?;
    // Remote authority and local Objects are independently bound. Public remote
    // Objects batches and an Actors host are separate, explicit qualification gaps.
    let authority = loopback(Arc::new(MemoryStream::default())).await?;
    let objects = Arc::new(MemoryObjects::new(MemoryOptions::default())?);
    use acyclic_objects::v1::ObjectsProvider as _;
    let bucket = objects
        .create_bucket(acyclic_objects::v1::wire::CreateBucketRequest {
            name: "matrix-files".into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("matrix bucket missing")?;
    let remote_authority = Fs::new(
        StreamAuthorityStore::new(authority.client.clone()),
        LogicalObjectStore::new(objects, bucket),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    row(&format!("grpc-authority/{name}"), remote_authority, stream).await
}

#[tokio::test]
async fn shared_families_with_memory_journals() -> TestResult {
    files("memory", Arc::new(MemoryStream::default())).await
}

#[tokio::test]
async fn shared_families_with_disk_journals() -> TestResult {
    let directory = tempfile::tempdir()?;
    let stream =
        Arc::new(acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?);
    files("disk", stream).await
}

#[tokio::test]
async fn shared_families_with_grpc_journals() -> TestResult {
    let remote = loopback(Arc::new(MemoryStream::default())).await?;
    files("grpc", remote.client.clone()).await
}
