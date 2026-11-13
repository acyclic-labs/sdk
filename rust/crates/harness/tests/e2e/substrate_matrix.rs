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
    let started = std::time::Instant::now();
    let results: Vec<TestResult> =
        futures::future::join_all(cases.iter().map(|(path, case, expected)| {
            let files = files.clone();
            let stream = stream.clone();
            async move {
                let started = std::time::Instant::now();
                let result = async {
                    let outcome = execution::run_with_filesystem(
                        Scenario::from_value(case.clone())?,
                        files,
                        stream,
                    )
                    .await?;
                    let executed = started.elapsed();
                    let observed = Value::Array(outcome.trace().await?);
                    eprintln!(
                        "substrate case {name}/{}: execute={executed:?} replay={:?}",
                        path.display(),
                        started.elapsed() - executed,
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
        "substrate matrix {name}: {} shared cases passed in {:?}",
        cases.len(),
        started.elapsed()
    );
    Ok(())
}

async fn journals<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    name: &str,
    files: Fs<A, O>,
    streams: &(
        Arc<MemoryStream>,
        Arc<acyclic_stream::LocalStream>,
        Arc<Client>,
    ),
    cases: &[(PathBuf, Value, Value)],
) -> TestResult {
    // Every scenario admits a distinct AgentId and OperationId through the
    // original executor path, so provider rows have no ordering dependency.
    // Join every row before releasing its filesystem or loopback server, even
    // when another row reports an error.
    let memory_name = format!("{name}/memory");
    let disk_name = format!("{name}/disk");
    let remote_name = format!("{name}/grpc");
    let results = futures::join!(
        row(&memory_name, files.clone(), streams.0.clone(), cases),
        row(&disk_name, files.clone(), streams.1.clone(), cases),
        row(&remote_name, files, streams.2.clone(), cases),
    );
    results.0?;
    results.1?;
    results.2
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
    let streams = (
        Arc::new(MemoryStream::default()),
        disk,
        remote.client.clone(),
    );
    let files_directory = tempfile::tempdir()?;
    let local = Fs::local(acyclic_fs::LocalOptions::new(files_directory.path())).await?;
    // Remote authority and local Objects are independently bound. Public remote
    // Objects batches and an Actors host are separate, explicit qualification gaps.
    let authority = loopback(Arc::new(MemoryStream::default())).await?;
    let remote_authority = Fs::new(
        StreamAuthorityStore::new(authority.client.clone()),
        LogicalObjectStore::new(Arc::new(MemoryObjects::memory())),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let results = futures::join!(
        journals("memory", Fs::memory(), &streams, &cases),
        journals("disk", local, &streams, &cases),
        journals("grpc-authority", remote_authority, &streams, &cases),
    );
    results.0?;
    results.1?;
    results.2
}
