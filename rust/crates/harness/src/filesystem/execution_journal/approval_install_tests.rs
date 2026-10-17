use super::*;
use acyclic_test_support::differential;
use proptest::prelude::*;

async fn initialized(reason: &str) -> Result<(Journal, InteractionId, OperationId, Vec<u8>, u64)> {
    let fixture = Fixture::new(
        Arc::new(MemoryStream::default()),
        acyclic_fs::Fs::memory(),
        serde_json::json!({"type":"integer"}),
        None,
        &[],
    )
    .await?;
    fixture.running(TASK).await?;
    let conversation = conversation();
    bind(&fixture, &conversation).await?;
    let files = Arc::new(
        TaskFiles::open(
            fixture.stream.clone(),
            &fixture.storage,
            &fixture.issuer.verifier(&mut None)?,
            &owner_scope(&fixture).await?,
            65_536,
        )
        .await?,
    );
    let journal = journal(&fixture, &conversation, files.clone()).await?;
    let id = InteractionId::from_bytes([86; 16]);
    let operation = OperationId::from_bytes([TASK; 16]);
    let request = Interaction::approval("Install".into(), operation, [5; 32])?;
    journal.open_interaction(id, &request, None)?.await?;
    let request = approvals(&files).await?.remove(0).bytes;
    let sha = FileDescriptor::from_bytes(&request, "application/json", &mut None)?;
    let body = ApprovalDecisionFile::new("human", sha.sha256(), 1, true, Some(reason.into()))?;
    let decision = decision(id, "human", &body);
    journal
        .write_approval_decision(&decision, &responder(&conversation, "human", &[id])?)
        .await?;
    Ok((
        journal,
        id,
        operation,
        decision.bytes.to_vec(),
        request.len() as u64,
    ))
}

fn once(trace: &[(&str, u64)], request: u64, decision: u64, allocations: u64, bytes: u64) -> bool {
    let values = |event| {
        trace
            .iter()
            .filter_map(|(name, count)| (*name == event).then_some(*count))
            .collect::<Vec<_>>()
    };
    values("interaction-file-read") == [request, decision]
        && values("approval-receipt") == [1]
        && values("decision-decode-allocations") == [allocations]
        && values("decision-decode-bytes") == [bytes]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn installation_reads_and_allocates_one_decision(reason in ".{0,1024}") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for duplicate in [false, true] {
            // Each run owns fresh conversation, decoder and task-file state.
            let ((result, rejected, expected, request, decision), trace) = differential::observe::<_,_,(&'static str,u64)>(
                || runtime.block_on(initialized(&reason)).unwrap(),
                |(journal, id, operation, encoded, request)| {
                    let expected = acyclic_test_support::measure(|| {
                        drop(crate::contract::json_from_slice::<ApprovalDecisionFile>(&encoded).unwrap());
                    });
                    let rejected = matches!(runtime.block_on(
                        journal.verify_tool_approval_with_io(id, operation, [6;32], None)),
                        Err(Error::Unauthorized(_)));
                    let cancellation = acyclic_fs::CancellationToken::new();
                    let work = acyclic_fs::NativeMaterializationWork::new(
                        acyclic_fs::performance::work_budget(65_536, 64 << 20), &cancellation,
                    ).unwrap();
                    let io = crate::filesystem::PreparationIo::from_native_work(&work).unwrap();
                    let held = work.live_allocation_bytes();
                    let owned = io.owned_bytes().unwrap();
                    let native = runtime.block_on(journal.verify_tool_approval_with_io(
                        id, operation, [6;32], Some(&io),
                    )).map_err(|error| error.code());
                    let rejected = rejected && native == Err(crate::wire::ErrorCode::Unauthorized)
                        && io.owned_bytes().unwrap() == owned && work.live_allocation_bytes() == held;
                    drop(io);
                    // Negative control restores the redundant verification on
                    // the real path, including its read, decode and allocation.
                    if duplicate {
                        runtime.block_on(journal.verify_tool_approval_with_io(id, operation, [5;32], None)).unwrap();
                    }
                    let result = runtime.block_on(journal.verify_tool_approval_with_io(id, operation, [5;32], None));
                    (result, rejected, expected, request, encoded.len() as u64)
                });
            prop_assert!(result.is_ok());
            prop_assert!(rejected);
            prop_assert!(expected.count_total > 0);
            prop_assert_eq!(once(&trace, request, decision, expected.count_total, expected.bytes_total), !duplicate);
        }
    }
}
