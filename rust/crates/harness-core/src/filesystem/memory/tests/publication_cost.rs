//! Count the production runtime's content reads after a successful write.
use super::*;

#[test]
fn successful_runtime_publication_does_not_reread_its_just_written_body() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut total_reads = 0;
    for once in [false, true] {
        let opening = MemoryHarnessStorage::new(AgentId::new(), 1024 * 1024);
        // Three phases retain identity/limit, with their state tags, the
        // provider handles and one erased body. Keep convenience constructors
        // lazy: their original provider creation must run under the caller's
        // first-poll clock and entropy scope.
        let bound = 3
            * (std::mem::size_of::<AgentId>()
                + std::mem::size_of::<u64>()
                + std::mem::size_of::<usize>())
            + std::mem::size_of::<Fs<MemoryAuthorityBackend, MemoryObjectBackend>>()
            + std::mem::size_of::<Arc<MemoryStream>>()
            + 2 * std::mem::size_of::<usize>();
        let bytes = std::mem::size_of_val(&opening);
        eprintln!("STORAGE_OPEN future_bytes={bytes} bound={bound}");
        assert!(
            bytes <= bound,
            "storage initialization propagated into its caller: {bytes} > {bound}"
        );
        let storage = runtime.block_on(opening)?;
        let started = std::time::Instant::now();
        let (result, trace) =
            acyclic_test_support::differential::observe::<_, _, (&'static str, u64)>(
                || storage,
                |storage| {
                    runtime.block_on(run_admitted_file_case(
                        &storage,
                        Limits::default(),
                        move |task| async move {
                            let bytes = vec![b'w'; 1024 * 1024];
                            if once {
                                task.stage_file_once(
                                    OperationId::new(),
                                    "runtime-cost",
                                    &bytes,
                                    "text/plain",
                                    "runtime-cost",
                                    None,
                                )
                                .await?;
                            } else {
                                task.stage_file(
                                    None,
                                    OperationId::new(),
                                    "runtime-cost",
                                    &bytes,
                                    "text/plain",
                                    "runtime-cost",
                                )
                                .await?;
                            }
                            Ok(())
                        },
                    ))
                },
            );
        result?;
        let reads: Vec<_> = trace
            .into_iter()
            .filter(|(operation, _)| *operation == "resolved content body")
            .collect();
        eprintln!(
            "RUNTIME_PUBLICATION once={once} payload_bytes=1048576 read_calls={} requested_bytes={} elapsed_us={}",
            reads.len(),
            reads.iter().map(|(_, bytes)| *bytes).sum::<u64>(),
            started.elapsed().as_micros()
        );
        total_reads += reads.len();
    }
    assert_eq!(
        total_reads, 0,
        "successful runtime publication must not reread its just-written body"
    );
    Ok(())
}
