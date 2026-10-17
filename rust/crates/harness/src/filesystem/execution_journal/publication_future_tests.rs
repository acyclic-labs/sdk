use super::*;
use crate::{AgentId, conversation::VolumeOwner, resources::ProviderRef};
use proptest::prelude::*;

type Journal = FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

async fn journal() -> Result<Journal> {
    let provider = ProviderRef::new("future".into(), "filesystem".into(), "2".into(), &mut None)?;
    let host = FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        provider,
        "future".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = crate::core::AuthorityIssuer::new(
        "future",
        [2; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: crate::ConversationId::from_bytes([3; 16]).to_string(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        None,
        &mut None,
    )?;
    FilesystemExecutionJournal::new(
        Arc::new(acyclic_stream::MemoryStream::default()),
        host,
        &SchemaRegistry::new(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &volume,
            verifier: &issuer.verifier(&mut None)?,
            scope: &scope,
            maximum_bytes: 4096,
            io: None,
        },
    )
}

mod legacy;
use serde_json::Value;

async fn prepare(
    journal: &Journal,
    operation: OperationId,
    event: &ExecutionEvent,
    bytes: &[u8],
) -> Result<FileRef> {
    journal.append(operation, "start", event, None)?.await?;
    journal.append(operation, "start", event, None)?.await?;
    journal
        .stage(
            operation,
            "payload",
            bytes,
            "application/octet-stream",
            None,
        )?
        .await
}

async fn run(
    journal: &Journal,
    mode: u8,
    old: bool,
    tail: u64,
    (operation, key, event): (OperationId, &str, &ExecutionEvent),
    file: &FileRef,
    bytes: &[u8],
) -> Result<Value> {
    match mode {
        0 => {
            if old {
                legacy::append(journal, operation, key, event, None)?.await?;
            } else {
                journal.append(operation, key, event, None)?.await?;
            }
            Ok(Value::Null)
        }
        1 => {
            let result = if old {
                legacy::append_if_tail(journal, operation, tail, key, event, None)?.await?
            } else {
                journal
                    .append_if_tail(operation, tail, key, event, None)?
                    .await?
            };
            Ok(Value::Bool(result))
        }
        2 => {
            let result = if old {
                legacy::replay(journal, operation, tail, 64, None)?.await?
            } else {
                journal.replay(operation, tail, 64, None)?.await?
            };
            serde_json::to_value(result).map_err(|error| Error::Invalid(error.to_string()))
        }
        3 => {
            let result = if old {
                legacy::load(journal, file, None)?.await?
            } else {
                journal.load(file, None)?.await?
            };
            Ok(serde_json::json!(result.as_ref()))
        }
        _ => {
            let result = if old {
                legacy::stage(
                    journal,
                    operation,
                    "payload",
                    bytes,
                    "application/octet-stream",
                    None,
                )?
                .await?
            } else {
                journal
                    .stage(
                        operation,
                        "payload",
                        bytes,
                        "application/octet-stream",
                        None,
                    )?
                    .await?
            };
            serde_json::to_value(result).map_err(|error| Error::Invalid(error.to_string()))
        }
    }
}

fn construct(
    journal: &Journal,
    mode: u8,
    old: bool,
    operation: OperationId,
    event: &ExecutionEvent,
    file: &FileRef,
    io: &PreparationIo,
) -> Result<()> {
    match mode {
        0 => drop(if old {
            legacy::append(journal, operation, "key", event, Some(io))?
        } else {
            journal.append(operation, "key", event, Some(io))?
        }),
        1 => drop(if old {
            legacy::append_if_tail(journal, operation, 0, "key", event, Some(io))?
        } else {
            journal.append_if_tail(operation, 0, "key", event, Some(io))?
        }),
        2 => drop(if old {
            legacy::replay(journal, operation, 0, 64, Some(io))?
        } else {
            journal.replay(operation, 0, 64, Some(io))?
        }),
        3 => drop(if old {
            legacy::load(journal, file, Some(io))?
        } else {
            journal.load(file, Some(io))?
        }),
        _ => drop(if old {
            legacy::stage(journal, operation, "key", b"", "text/plain", Some(io))?
        } else {
            journal.stage(operation, "key", b"", "text/plain", Some(io))?
        }),
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Full operations preserve outputs/errors and reduce requested storage;
    // storage work is included in these counts, separately from wrapper boxes.
    #[test]
    fn journal_eliminates_only_the_extra_future_box(
        key in "[a-z]{1,16}", mut digest in any::<[u8;32]>(),
        tail in prop_oneof![Just(0), Just(1), Just(u64::MAX), any::<u64>()], failure in 0u8..3,
        bytes in prop::collection::vec(any::<u8>(), 0..1024),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let journal = runtime.block_on(journal()).unwrap();
        let operation = OperationId::from_bytes([4;16]);
        if let Some(first) = digest.first_mut() { *first |= 1; }
        let event = ExecutionEvent::Started { request_digest: digest };
        runtime.block_on(journal.append(operation, &key, &event, None).unwrap()).unwrap();
        let file = runtime.block_on(journal.stage(operation, "payload", &bytes, "application/octet-stream", None).unwrap()).unwrap();
        let mut candidate = event.clone();
        if failure == 1 { candidate = ExecutionEvent::Started { request_digest: [0;32] }; }
        let key = if failure == 2 { "" } else { &key };
        for mode in 0..5 {
            for old in [true, false] {
                let _ = runtime.block_on(run(&journal, mode, old, tail, (operation, key, &candidate), &file, &bytes));
            }
            let mut expected = Ok(Value::Null);
            let before = acyclic_test_support::measure(|| {
                expected = runtime.block_on(run(&journal, mode, true, tail, (operation, key, &candidate), &file, &bytes));
            });
            let mut actual = Ok(Value::Null);
            let after = acyclic_test_support::measure(|| {
                actual = runtime.block_on(run(&journal, mode, false, tail, (operation, key, &candidate), &file, &bytes));
            });
            prop_assert_eq!(actual.map_err(|e| e.to_string()), expected.map_err(|e| e.to_string()));
            prop_assert!(after.count_total <= before.count_total,
                "method {}: before {:?}, after {:?}", mode, before, after);
            prop_assert!(after.bytes_total < before.bytes_total,
                "method {}: before {:?}, after {:?}", mode, before, after);
        }
    }

    // Fail through each real entrypoint before storage work: isolate the
    // wrapper's exact one-box saving from the full operation above.
    #[test]
    fn journal_wrapper_removes_exactly_one_allocation(mut digest in any::<[u8;32]>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let mut journal = runtime.block_on(journal()).unwrap();
        let operation = OperationId::from_bytes([4;16]);
        if let Some(first) = digest.first_mut() { *first |= 1; }
        let event = ExecutionEvent::Started { request_digest: digest };
        let file = runtime.block_on(prepare(&journal, operation, &event, b"payload")).unwrap();
        journal.execution = Some((OperationId::from_bytes([5;16]), operation));
        journal.volume = VolumeRef::new(journal.volume.provider().clone(), "foreign".into(),
            VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::from_bytes([1;16])), &mut None).unwrap();
        let oversized = vec![0;4097];
        for mode in 0..5 {
            let mut expected = Ok(Value::Null);
            let before = acyclic_test_support::measure(|| {
                expected = runtime.block_on(run(&journal, mode, true, 0, (operation, "key", &event), &file, &oversized));
            });
            let mut actual = Ok(Value::Null);
            let after = acyclic_test_support::measure(|| {
                actual = runtime.block_on(run(&journal, mode, false, 0, (operation, "key", &event), &file, &oversized));
            });
            prop_assert!(actual.is_err());
            prop_assert_eq!(actual.map_err(|e| e.to_string()), expected.map_err(|e| e.to_string()));
            let bound = before.count_total - 1;
            prop_assert_eq!(after.count_total, bound, "method {}: before {:?}, after {:?}", mode, before, after);
            prop_assert_ne!(before.count_total, bound, "old nested box passed the same bound");
        }
    }

    // Every old/new factory is compared at its exact admission boundary, after
    // drop, and with cancellation. The smaller unmetered frame must not change
    // original accounting or move the success/failure threshold.
    #[test]
    fn journal_accounting_admits_the_original_frames(cancelled in any::<bool>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let journal = runtime.block_on(journal()).unwrap();
        let operation = OperationId::from_bytes([4;16]);
        let event = ExecutionEvent::Started { request_digest: [9;32] };
        let file = runtime.block_on(prepare(&journal, operation, &event, b"payload")).unwrap();
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(acyclic_fs::performance::work_budget(1_000_000, 1<<24), &cancellation).unwrap();
        for mode in 0..5 {
            let frame = acyclic_test_support::measure(|| { construct(&journal, mode, true, operation, &event, &file, &io).unwrap(); }).bytes_total;
            for maximum in [frame.saturating_sub(1), frame, frame+1] {
                let mut outcomes = Vec::new();
                for old in [true, false] {
                    let cancellation = acyclic_fs::CancellationToken::new();
                    let io = PreparationIo::from_budget(acyclic_fs::performance::work_budget(1_000_000, maximum), &cancellation).unwrap();
                    if cancelled { cancellation.cancel(); }
                    let result = construct(&journal, mode, old, operation, &event, &file, &io).map_err(|e| e.to_string());
                    prop_assert_eq!(io.owned_bytes().unwrap(), 0);
                    outcomes.push((result, io.receipt()));
                }
                prop_assert_eq!(outcomes.first(), outcomes.last(), "method {}", mode);
            }
        }
    }
}

fn records_poll_parent(mode: u8, early: bool) -> Result<bool> {
    use futures::FutureExt as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let journal = runtime.block_on(journal())?;
    let operation = OperationId::from_bytes([4; 16]);
    let event = ExecutionEvent::Started {
        request_digest: [9; 32],
    };
    let file = runtime.block_on(prepare(&journal, operation, &event, b"payload"))?;
    let (seen, _capture) = crate::obs::capture::install();
    let construction = obs_span!(INFO, "acyclic.harness.test.construct");
    let polling = obs_span!(INFO, "acyclic.harness.test.poll");
    let future: Result<BoxFuture<'_, Result<()>>> = construction.in_scope(|| {
        let future: BoxFuture<'_, Result<()>> = match mode {
            0 => journal.append(operation, "start", &event, None)?.boxed(),
            1 => journal
                .append_if_tail(operation, 0, "start", &event, None)?
                .map(|r| r.map(|_| ()))
                .boxed(),
            2 => journal
                .replay(operation, 0, 64, None)?
                .map(|r| r.map(|_| ()))
                .boxed(),
            3 => journal.load(&file, None)?.map(|r| r.map(|_| ())).boxed(),
            _ => journal
                .stage(
                    operation,
                    "payload",
                    b"payload",
                    "application/octet-stream",
                    None,
                )?
                .map(|r| r.map(|_| ()))
                .boxed(),
        };
        if early {
            // Broken control drives the production entrypoint under a context
            // captured at construction, rather than its actual first poll.
            let span = obs_span!(INFO, "acyclic.harness.test.early");
            Ok(crate::obs::traced_future(span, future).boxed())
        } else {
            Ok(future)
        }
    });
    polling.in_scope(|| runtime.block_on(future?))?;
    let name = match mode {
        0 => "acyclic.harness.journal.append",
        1 => "acyclic.harness.journal.append_if_tail",
        2 => "acyclic.harness.journal.replay",
        3 => "acyclic.harness.journal.load",
        _ => "acyclic.harness.journal.stage",
    };
    Ok(
        crate::obs::capture::has(&seen, name, "parent", "acyclic.harness.test.poll")
            && crate::obs::capture::has(&seen, name, "outcome", "ok"),
    )
}

#[test]
fn journal_tracing_captures_the_poll_context() -> Result<()> {
    for mode in 0..5 {
        assert!(records_poll_parent(mode, false)?);
        assert!(
            !records_poll_parent(mode, true)?,
            "early context control escaped detection"
        );
    }
    Ok(())
}
