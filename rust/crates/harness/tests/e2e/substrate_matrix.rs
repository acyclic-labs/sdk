//! Shared scenario families over independently selected, constructible providers.
use super::*;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, LogicalObjectStore, StreamAuthorityStore,
};
use acyclic_objects::MemoryObjects;

async fn row<P, A, O>(
    name: &str,
    files: Fs<A, O>,
    stream: Arc<P>,
    cases: &[(PathBuf, Value, Value)],
) -> TestResult
where
    P: acyclic_harness::store::JournalProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let results: Vec<TestResult> =
        futures::future::join_all(cases.iter().map(|(path, case, expected)| {
            let files = files.clone();
            let stream = stream.clone();
            async move {
                let result = async {
                    let observed = Value::Array(
                        execution::run_with_filesystem(
                            Scenario::from_value(case.clone())?,
                            files,
                            stream,
                        )
                        .await?
                        .trace()
                        .await?,
                    );
                    if &observed != expected {
                        return Err("scenario traces differ".into());
                    }
                    TestResult::Ok(())
                }
                .await;
                result.map_err(|error| format!("{name}/{}: {error}", path.display()).into())
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

async fn files<P: acyclic_harness::store::JournalProvider>(
    name: &str,
    stream: Arc<P>,
    cases: &[(PathBuf, Value, Value)],
) -> TestResult {
    row(
        &format!("memory/{name}"),
        Fs::memory(),
        stream.clone(),
        cases,
    )
    .await?;
    let directory = tempfile::tempdir()?;
    let local = Fs::local(acyclic_fs::LocalOptions::new(directory.path())).await?;
    row(&format!("disk/{name}"), local, stream.clone(), cases).await?;
    // Remote authority and local Objects are independently bound. Public remote
    // Objects batches and an Actors host are separate, explicit qualification gaps.
    let authority = loopback(Arc::new(MemoryStream::default())).await?;
    let objects = Arc::new(MemoryObjects::memory());
    let remote_authority = Fs::new(
        StreamAuthorityStore::new(authority.client.clone()),
        LogicalObjectStore::new(objects),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    row(
        &format!("grpc-authority/{name}"),
        remote_authority,
        stream,
        cases,
    )
    .await
}

#[tokio::test]
async fn shared_families_with_independent_filesystem_and_journal_providers() -> TestResult {
    let cases = scenarios()?;
    assert_eq!(cases.len(), 27, "scenario inventory changed");
    // The reference is independent of both selected providers. Each original
    // scenario runs once here, then every matrix entry checks its exact trace.
    let cases = futures::future::try_join_all(cases.into_iter().map(|(path, case)| async move {
        let expected = trace(&case, Arc::new(MemoryStream::default()))
            .await
            .map_err(|error| format!("reference/{}: {error}", path.display()))?;
        TestResult::<_>::Ok((path, case, expected))
    }))
    .await?;
    let directory = tempfile::tempdir()?;
    let disk =
        Arc::new(acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?);
    let remote = loopback(Arc::new(MemoryStream::default())).await?;
    let results = futures::join!(
        files("memory", Arc::new(MemoryStream::default()), &cases),
        files("disk", disk, &cases),
        files("grpc", remote.client.clone(), &cases),
    );
    results.0?;
    results.1?;
    results.2
}
