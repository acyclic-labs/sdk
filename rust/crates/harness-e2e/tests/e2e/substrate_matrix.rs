//! Shared scenario families over independently selected, constructible providers.
//!
//! The nine provider pairs are the proof: one executor, one scenario set, and
//! the same trace whichever filesystem and journal are bound. Each pair is a
//! distinct `(P, A, O)` instantiation of the harness, so this file is the
//! crate's largest single compile cost; keep the pairs and cut elsewhere.
use super::super::{case_workers, scenario_runtime, scenario_worker_budget, stall};
use super::*;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, LogicalObjectStore, StreamAuthorityStore,
};
use acyclic_objects::MemoryObjects;

async fn scenario_trace<P, A, O>(case: &Value, files: Fs<A, O>, stream: Arc<P>) -> TestResult<Value>
where
    P: acyclic_harness::store::JournalProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    Ok(Value::Array(
        execution::run_with_filesystem(Scenario::from_value(case.clone())?, files, stream)
            .await?
            .trace()
            .await?,
    ))
}

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
                let result = async {
                    let observed = scenario_trace(case, files, stream).await?;
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

type MatrixRun<'a> =
    Box<dyn FnOnce() -> futures::future::LocalBoxFuture<'a, TestResult> + Send + 'a>;

struct MatrixJob<'a> {
    providers: &'a str,
    run: MatrixRun<'a>,
}

fn matrix_job<'a, P, A, O>(
    name: &'a str,
    files: Fs<A, O>,
    stream: Arc<P>,
    cases: &'a [(PathBuf, Value, Value)],
) -> MatrixJob<'a>
where
    P: acyclic_harness::store::JournalProvider + 'a,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    MatrixJob {
        providers: name,
        run: Box::new(move || Box::pin(row(name, files, stream, cases))),
    }
}

#[test]
fn shared_families_with_independent_filesystem_and_journal_providers() -> TestResult {
    let cases = scenarios()?;
    assert_eq!(cases.len(), 27, "scenario inventory changed");
    const COMPARED_PAIRS: usize = 8;
    let (services, drivers) = case_workers::layout(scenario_worker_budget(), COMPARED_PAIRS);
    let runtime = scenario_runtime(services)?;
    // Memory Filesystem and Stream run the scenario oracles and provide the
    // reference trace compared by every other pair of providers.
    let memory_files = Fs::memory();
    let memory_stream = Arc::new(MemoryStream::default());
    let cases = runtime.block_on(futures::future::try_join_all(cases.into_iter().map(
        |(path, case)| {
            let files = memory_files.clone();
            let stream = memory_stream.clone();
            async move {
                let expected = scenario_trace(&case, files, stream)
                    .await
                    .map_err(|error| format!("memory/memory/{}: {error}", path.display()))?;
                TestResult::<_>::Ok((path, case, expected))
            }
        },
    )))?;
    // The same real native files the rest of the suite uses: journal syncs and
    // cold reopen stay real, and eight concurrent rows do not queue on the
    // host's persistent disk.
    let directory = crate::local_workspace::temporary()?;
    let files_directory = crate::local_workspace::temporary()?;
    let (disk, remote, local, authority) = runtime.block_on(async {
        let disk = Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?,
        );
        let remote = loopback(Arc::new(MemoryStream::default())).await?;
        let local = Fs::local(acyclic_fs::LocalOptions::new(files_directory.path())).await?;
        let authority = loopback(Arc::new(MemoryStream::default())).await?;
        TestResult::<_>::Ok((disk, remote, local, authority))
    })?;
    let grpc = remote.client.clone();
    // Remote authority and local Objects are independently bound. Public remote
    // Objects batches and an Actors host are separate, explicit qualification gaps.
    let remote_authority = Fs::new(
        StreamAuthorityStore::new(authority.client.clone()),
        LogicalObjectStore::new(Arc::new(MemoryObjects::memory())),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    // Each pair admits distinct agent and operation IDs. Schedule the pairs
    // independently and join them before releasing the shared providers.
    let jobs: [_; COMPARED_PAIRS] = [
        matrix_job("disk/memory", local.clone(), memory_stream.clone(), &cases),
        matrix_job("disk/disk", local.clone(), disk.clone(), &cases),
        matrix_job("disk/grpc", local, grpc.clone(), &cases),
        matrix_job("memory/disk", memory_files.clone(), disk.clone(), &cases),
        matrix_job("memory/grpc", memory_files, grpc.clone(), &cases),
        matrix_job(
            "grpc-authority/memory",
            remote_authority.clone(),
            memory_stream,
            &cases,
        ),
        matrix_job(
            "grpc-authority/disk",
            remote_authority.clone(),
            disk,
            &cases,
        ),
        matrix_job("grpc-authority/grpc", remote_authority, grpc, &cases),
    ];
    let mut guard = crate::progress_guard::Guard::new();
    let io = guard.io.clone();
    let registry = guard.registry.clone();
    let results = case_workers::run(
        &runtime,
        jobs.into(),
        COMPARED_PAIRS,
        drivers,
        &mut guard,
        |job| {
            stall::run(
                io.clone(),
                registry.clone(),
                PathBuf::from(job.providers),
                (job.run)(),
            )
        },
        |_| {},
    )?;
    results.into_iter().collect()
}
