use super::*;
use acyclic_test_support::differential;
use proptest::prelude::*;

// A verifier can replay history after teardown, but admissions still require
// the runtime that owns its signing custody. Keep both runtimes alive through
// setup and the measured journal operations.
struct Initialized {
    journal: Journal,
    id: InteractionId,
    operation: OperationId,
    encoded: Vec<u8>,
    request_bytes: u64,
    _fixture: Fixture,
    _conversation: AuthorityIssuer,
}

async fn initialized(reason: &str, io: Option<&PreparationIo>) -> Result<Initialized> {
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
    let request = Interaction::approval(
        "Install".into(),
        operation,
        [5; 32],
        crate::interaction::ApprovalSubject::Opaque {},
    )?;
    journal.open_interaction(id, &request, None)?.await?;
    let request = approvals(&files).await?.remove(0).bytes;
    let sha = FileDescriptor::from_bytes(&request, "application/json", &mut None)?;
    let body = ApprovalDecisionFile::new("human", sha.sha256(), 1, true, Some(reason.into()))?;
    let decision = decision(id, "human", &body);
    journal
        .write_approval_decision(&decision, &responder(&conversation, "human", &[id])?)
        .await?;
    let journal = if io.is_some() {
        journal_with_io(&fixture, &conversation, files, io).await?
    } else {
        journal
    };
    Ok(Initialized {
        journal,
        id,
        operation,
        encoded: decision.bytes.to_vec(),
        request_bytes: request.len() as u64,
        _fixture: fixture,
        _conversation: conversation,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn verdict_reconstructs_once_and_only_flushes_a_resolved_approval(reason in ".{0,256}") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        for native in [false, true] {
            for pending in [false, true] {
                let (initialized, id, work) = runtime.block_on(async {
                    let cancellation = acyclic_fs::CancellationToken::new();
                    let work = acyclic_fs::NativeMaterializationWork::new(
                        acyclic_fs::performance::work_budget(65_536, 64 << 20), &cancellation,
                    ).unwrap();
                    let io = native.then(|| PreparationIo::from_native_work(&work).unwrap());
                    let initialized = initialized(&reason, io.as_ref()).await.unwrap();
                    let journal = &initialized.journal;
                    let operation = initialized.operation;
                    let id = if pending {
                        let id = InteractionId::from_bytes([87;16]);
                        journal.open_interaction(id, &Interaction::approval(
                            "Pending".into(), operation, [5;32], crate::interaction::ApprovalSubject::Opaque {},
                        ).unwrap(), io.as_ref()).unwrap().await.unwrap();
                        id
                    } else { initialized.id };
                    drop(io);
                    (initialized, id, work)
                });
                // The paired reads share immutable history, while every read
                // still reconstructs and proves its own outcome.
                for duplicate in [false, true] {
                    let (outcome, trace) = differential::observe::<_,_,(&'static str,u64)>(
                        || (&initialized, id, &work),
                        |(initialized, id, original_work)| {
                            let journal = &initialized.journal;
                            let io = native.then(|| PreparationIo::from_native_work(original_work).unwrap());
                            if duplicate {
                                // Restore the removed, real history reconstruction
                                // before the ordinary verdict/proof path.
                                let host = journal.interactions().unwrap();
                                let aggregate = runtime.block_on(host.aggregate(io.as_ref())).unwrap();
                                host.read_outcome(&aggregate.0, id, io.as_ref()).unwrap();
                            }
                            runtime.block_on(journal.interaction_outcome_with_io(id, io.as_ref())).unwrap()
                        });
                    prop_assert_eq!(outcome, (!pending).then_some(InteractionOutcome::Approved {}));
                    let reconstructions = trace.iter().filter(|(name,_)| *name == "interaction-reconstruction").count();
                    prop_assert_eq!(reconstructions, 1 + usize::from(duplicate));
                    prop_assert_eq!(reconstructions == 1, !duplicate);
                    prop_assert_eq!(trace.iter().filter(|(name,_)| *name == "approval-receipt").count(), usize::from(!pending));
                }
            }
        }
    }
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
        let (ordinary, native, work) = runtime.block_on(async {
            let ordinary = initialized(&reason, None).await.unwrap();
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(65_536, 64 << 20), &cancellation,
            ).unwrap();
            let io = PreparationIo::from_native_work(&work).unwrap();
            let native = initialized(&reason, Some(&io)).await.unwrap();
            drop(io);
            (ordinary, native, work)
        });
        for duplicate in [false, true] {
            let ((result, rejected, expected, request, decision), trace) = differential::observe::<_,_,(&'static str,u64)>(
                || (&ordinary, &native, &work),
                |(ordinary, native, original_work)| {
                    let journal = &ordinary.journal;
                    let id = ordinary.id;
                    let operation = ordinary.operation;
                    let encoded = &ordinary.encoded;
                    let request = ordinary.request_bytes;
                    let native_journal = &native.journal;
                    let expected = acyclic_test_support::measure(|| {
                        drop(crate::contract::json_from_slice::<ApprovalDecisionFile>(encoded).unwrap());
                    });
                    let rejected = matches!(runtime.block_on(
                        journal.verify_tool_approval_with_io(id, operation, [6;32], None)),
                        Err(Error::Unauthorized(ref reason)) if reason == "approval does not authorize this definition");
                    let io = crate::filesystem::PreparationIo::from_native_work(original_work).unwrap();
                    let held = original_work.live_allocation_bytes();
                    let owned = io.owned_bytes().unwrap();
                    let native = runtime.block_on(native_journal.verify_tool_approval_with_io(
                        id, operation, [6;32], Some(&io),
                    ));
                    let rejected = rejected && matches!(native, Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "approval does not authorize this definition",
                    }))
                        && io.owned_bytes().unwrap() == owned && original_work.live_allocation_bytes() == held;
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

#[test]
fn approval_settlement_reconstructs_once_for_every_entry_point() -> Result<()> {
    use crate::runtime::InteractionResolver as _;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for entry in 0..3 {
        for approved in [false, true] {
            let initialized = runtime.block_on(initialized("settlement", None))?;
            let journal = &initialized.journal;
            let host = journal.interactions()?;
            let id = InteractionId::from_bytes([88; 16]);
            let request = Interaction::approval(
                "Settle once".into(),
                initialized.operation,
                [5; 32],
                crate::interaction::ApprovalSubject::Opaque {},
            )?;
            runtime.block_on(journal.open_interaction(id, &request, None)?)?;
            let (ticket, _) = runtime.block_on(host.read(id, None))?.unwrap();
            let body = ApprovalDecisionFile::new(
                "human",
                ticket.request.descriptor().sha256(),
                1,
                approved,
                None,
            )?;
            let file = decision(id, "human", &body);
            let human = responder(&initialized._conversation, "human", &[id])?;
            for reconstruct_again in [false, true] {
                let (result, trace) = differential::observe::<_, _, differential::Operation>(
                    || (),
                    |()| {
                        runtime.block_on(async {
                            if reconstruct_again {
                                // Negative control restores a discarded reconstruction
                                // before settling the same authenticated binding.
                                host.read(id, None).await?;
                            }
                            match entry {
                                0 => journal.write_approval_decision(&file, &human).await,
                                1 => host
                                    .resolve_approval(
                                        OperationId::from_bytes([89; 16]),
                                        human.copy(&mut None)?,
                                        id,
                                        1,
                                        approved,
                                        None,
                                    )
                                    .await
                                    .map(|_| ()),
                                _ => {
                                    host.resolve_latest(
                                        id,
                                        InteractionResponse::Approval {
                                            approved,
                                            reason: None,
                                        },
                                        &human,
                                        journal.owner.as_ref().map(|(owner, _)| owner),
                                    )
                                    .await
                                }
                            }
                        })
                    },
                );
                result?;
                let count: u64 = trace
                    .iter()
                    .filter(|(kind, _)| *kind == "interaction-reconstruction")
                    .map(|(_, count)| count)
                    .sum();
                assert_eq!(
                    count,
                    1 + u64::from(reconstruct_again),
                    "entry={entry}, approved={approved}"
                );
            }
        }
    }
    Ok(())
}

async fn publish_writer_entry_point(
    second: &Journal,
    entry: usize,
    id: InteractionId,
    request: &Interaction,
    human: &Scope,
    file: Option<&FileValue>,
) -> Result<()> {
    use crate::runtime::InteractionResolver as _;
    let host = second.interactions()?;
    match entry {
        0 => {
            second
                .write_approval_decision(
                    file.ok_or_else(|| {
                        Error::Invalid("writer fixture has no decision file".into())
                    })?,
                    human,
                )
                .await
        }
        1 => host
            .resolve_approval(
                OperationId::from_bytes([89; 16]),
                human.copy(&mut None)?,
                id,
                1,
                false,
                None,
            )
            .await
            .map(|_| ()),
        2 => {
            host.resolve_latest(
                id,
                InteractionResponse::Approval {
                    approved: false,
                    reason: None,
                },
                human,
                None,
            )
            .await
        }
        _ => second.open_interaction(id, request, None)?.await,
    }
}

async fn observe_writer_attempt<F: std::future::Future<Output = Result<()>>>(
    mut publish: std::pin::Pin<&mut F>,
    entry: usize,
    released_cut: bool,
) -> std::task::Poll<Result<()>> {
    futures::future::poll_fn(|context| {
        let (polled, trace) = differential::observe::<_, _, differential::Operation>(
            || (),
            |()| std::future::Future::poll(publish.as_mut(), context),
        );
        let count = |name| {
            trace
                .iter()
                .filter(|(kind, _)| *kind == name)
                .map(|(_, count)| count)
                .sum::<u64>()
        };
        if count("interaction-writer-attempt") == 0 {
            assert!(
                polled.is_pending(),
                "entry point bypassed transaction: entry={entry}"
            );
            return std::task::Poll::Pending;
        }
        assert_eq!(
            count("interaction-writer-acquired"),
            u64::from(released_cut),
            "entry={entry}"
        );
        if !released_cut {
            assert!(
                polled.is_pending(),
                "writer bypassed custody: entry={entry}"
            );
        }
        std::task::Poll::Ready(polled)
    })
    .await
}

#[tokio::test]
async fn approval_writers_share_one_claim_across_composed_hosts_and_entry_points() -> Result<()> {
    for entry in 0..4 {
        for released_cut in [false, true] {
            let initialized = initialized("writer custody", None).await?;
            let first = &initialized.journal;
            let second = journal(
                &initialized._fixture,
                &initialized._conversation,
                first.task_files.as_ref().unwrap().clone(),
            )
            .await?;
            let host = second.interactions()?;
            let id = InteractionId::from_bytes([88; 16]);
            let request = Interaction::approval(
                "Concurrent publication".into(),
                initialized.operation,
                [5; 32],
                crate::interaction::ApprovalSubject::Opaque {},
            )?;
            if entry != 3 {
                first.open_interaction(id, &request, None)?.await?;
            }
            let human = responder(&initialized._conversation, "human", &[id])?;
            let file = if entry == 0 {
                let (ticket, _) = host.read(id, None).await?.unwrap();
                Some(decision(
                    id,
                    "human",
                    &ApprovalDecisionFile::new(
                        "human",
                        ticket.request.descriptor().sha256(),
                        1,
                        false,
                        None,
                    )?,
                ))
            } else {
                None
            };
            // The control holds the same verified cut without writer custody.
            let cut = if released_cut {
                Some(first.interactions()?.aggregate(None).await?)
            } else {
                None
            };
            let transaction = if released_cut {
                None
            } else {
                Some(first.interactions()?.transaction(None).await?)
            };
            let publish =
                publish_writer_entry_point(&second, entry, id, &request, &human, file.as_ref());
            tokio::pin!(publish);
            // Drive cooperative yields until this path actually attempts the
            // writer claim. Pending before that point says nothing about custody.
            let polled = observe_writer_attempt(publish.as_mut(), entry, released_cut).await;
            drop(transaction);
            match polled {
                std::task::Poll::Ready(result) => result?,
                std::task::Poll::Pending => publish.await?,
            }
            drop(cut);
        }
    }
    Ok(())
}

#[tokio::test]
async fn approval_writer_admits_storage_before_taking_custody() -> Result<()> {
    let initialized = initialized("writer allocation", None).await?;
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(65_536, 64 << 20);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        assert_eq!(io.owned_bytes()?, 0);
        // The old shape took custody before admitting any node storage.
        assert!(io.guard_owned(1).is_err());
        let transaction = initialized
            .journal
            .interactions()?
            .transaction(Some(&io))
            .await?;
        drop(transaction);
        assert_eq!(io.owned_bytes()?, 0);
    }
    Ok(())
}

#[test]
fn approval_open_reuses_one_verified_cut_for_new_and_existing_requests() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 64 << 20),
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = native
            .then(|| PreparationIo::from_native_work(&work))
            .transpose()?;
        let initialized = runtime.block_on(initialized("open transaction", io.as_ref()))?;
        let id = InteractionId::from_bytes([90; 16]);
        let request = Interaction::approval(
            "Open once".into(),
            initialized.operation,
            [5; 32],
            crate::interaction::ApprovalSubject::Opaque {},
        )?;
        for duplicate in [false, true] {
            let (result, trace) = differential::observe::<_, _, differential::Operation>(
                || (),
                |()| {
                    runtime.block_on(async {
                        if duplicate {
                            // Restore the removed discarded read before the exact retry.
                            let before = io
                                .as_ref()
                                .map(PreparationIo::owned_bytes)
                                .transpose()?
                                .unwrap_or(0);
                            let original = initialized
                                .journal
                                .interactions()?
                                .read_request(id, io.as_ref())
                                .await?;
                            let hold = io
                                .as_ref()
                                .map(|io| io.guard_owned(io.owned_since(before)?))
                                .transpose()?;
                            assert_eq!(original.as_ref(), Some(&request));
                            drop((original, hold));
                        }
                        initialized
                            .journal
                            .open_interaction_with_io(id, &request, io.as_ref())
                            .await
                    })
                },
            );
            result?;
            let count: u64 = trace
                .iter()
                .filter(|(kind, _)| *kind == "interaction-reconstruction")
                .map(|(_, count)| count)
                .sum();
            assert_eq!(count, 1 + u64::from(duplicate), "native={native}");
        }
    }
    Ok(())
}
