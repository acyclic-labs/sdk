//! Count actual device operations on both journal settlement entry points.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing_subscriber::layer::SubscriberExt as _;

#[derive(Clone, Default)]
struct Flushes(Arc<AtomicU64>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Flushes {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Count<'a>(&'a AtomicU64);
        impl tracing::field::Visit for Count<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                if field.name() == "fsyncs" {
                    self.0.fetch_add(value, Ordering::Relaxed);
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        event.record(&mut Count(&self.0));
    }
}

#[test]
fn approval_settlement_groups_staging_but_keeps_both_durable_proofs() -> Result<()> {
    settlement_flushes(true)
}

#[test]
fn approval_settlement_keeps_a_foreign_filesystem_authority_durable() -> Result<()> {
    settlement_flushes(false)
}

fn settlement_flushes(shared_provider: bool) -> Result<()> {
    // The callsite registry belongs to the process. Give this observer one
    // process and install it before any provider/span is constructed.
    let test = std::thread::current()
        .name()
        .ok_or_else(|| Error::Storage("flush proof has no libtest identity".into()))?
        .to_owned();
    const CHILD: &str = "ACYCLIC_APPROVAL_FLUSH_PROOF";
    if crate::native_environment::variable(CHILD).as_deref() != Some(std::ffi::OsStr::new(&test)) {
        let mut command = std::process::Command::new(
            std::env::current_exe().map_err(|error| Error::Storage(error.to_string()))?,
        );
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            command.creation_flags(0x08000000);
        }
        let output = command
            .args([&test, "--exact", "--test-threads=1", "--nocapture"])
            .env(CHILD, &test)
            .output()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(stdout.contains("1 passed; 0 failed"), "{stdout}\n{stderr}");
        assert_eq!(stderr.matches("APPROVALS_SETTLEMENT_FLUSHES").count(), 2);
        assert_eq!(stderr.matches("APPROVALS_REQUEST_FLUSHES").count(), 2);
        eprint!("{stderr}");
        return Ok(());
    }
    let flushes = Flushes::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(flushes.clone()))
        .map_err(|error| Error::Storage(error.to_string()))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    runtime.block_on(async {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let stream = Arc::new(
            acyclic_stream::LocalStream::open(directory.path(), Default::default())
                .await
                .map_err(|error| Error::Storage(error.to_string()))?,
        );
        let authority_directory =
            tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let authority = if shared_provider {
            stream.clone()
        } else {
            Arc::new(
                acyclic_stream::LocalStream::open(authority_directory.path(), Default::default())
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?,
            )
        };
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let backend = acyclic_fs::Fs::new(
            acyclic_fs::StreamAuthorityStore::new(authority),
            acyclic_fs::LogicalObjectStore::new(Arc::new(objects), bucket),
            acyclic_fs::EmbeddedCapabilities::MEMORY,
        );
        let fixture = Fixture::new(
            stream,
            backend,
            serde_json::json!({"type":"integer"}),
            None,
            &[],
        )
        .await?;
        fixture.running(TASK).await?;
        let conversation = conversation();
        bind(&fixture, &conversation).await?;
        let operation = OperationId::from_bytes([TASK; 16]);
        let journal = fixture.execution(fixture.owner(TASK).await?, operation)?;
        let files = Arc::new(
            TaskFiles::open(
                fixture.stream.clone(),
                &fixture.storage,
                &fixture.issuer.verifier(&mut None)?,
                &journal.scope,
                65_536,
            )
            .await?,
        );
        let scope = conversation.issue(
            "interaction-owner",
            &Capabilities::new([
                crate::contract::capability::INTERACTION_OPEN.to_owned(),
                fixture
                    .storage
                    .capability(VolumeOperation::Read, &mut None)?,
                fixture
                    .storage
                    .capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let journal = journal
            .with_task_files(files.clone())?
            .with_interaction_owner(
                &conversation.verifier(&mut None)?,
                &SchemaRegistry::new(),
                &scope,
                &fixture.storage,
                None,
            )?;
        for approved in [false, true] {
            let mut measured = Vec::new();
            let mut opening = Vec::new();
            let mut former_native = None;
            for entry in 0..4 {
                let id = InteractionId::from_bytes([100 + entry + 4 * u8::from(approved); 16]);
                let request = Interaction::approval(
                    "Grouped settlement".into(),
                    operation,
                    [5; 32],
                    ApprovalSubject::Opaque {},
                )?;
                let host = journal.interactions()?;
                flushes.0.store(0, Ordering::Relaxed);
                match entry {
                    0 => journal.open_interaction(id, &request, None)?.await?,
                    1 => {
                        let cancellation = acyclic_fs::CancellationToken::new();
                        let work = acyclic_fs::NativeMaterializationWork::new(
                            acyclic_fs::performance::work_budget(65_536, 64 << 20),
                            &cancellation,
                        )
                        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                        let io = PreparationIo::from_native_work(&work)?;
                        // The former fixture supplied an ordinary journal to
                        // another ledger. It must fail before either record.
                        assert!(matches!(
                            journal.open_interaction_with_io(id, &request, Some(&io)).await,
                            Err(Error::Unauthorized(message))
                                if message == "journal owner uses another metadata ledger"
                        ));
                        assert!(host.read(id, None).await?.is_none());
                        assert_eq!(flushes.0.load(Ordering::Relaxed), 0);
                        let (task, fence) = journal
                            .owner
                            .as_ref()
                            .ok_or_else(|| Error::Storage("native proof has no owner".into()))?
                            .0
                            .task_binding();
                        let native = FilesystemExecutionJournal::for_task(
                            fixture.host.journal_owner(task, fence, Some(&io)).await?,
                            operation,
                            operation,
                            fixture.files.clone(),
                            crate::filesystem::FilesystemJournalBinding {
                                volume: &journal.volume,
                                verifier: &journal.verifier,
                                scope: &journal.scope,
                                maximum_bytes: journal.maximum_payload_bytes,
                                io: Some(&io),
                            },
                        )?
                        .with_task_files(files.clone())?
                        .with_interaction_owner(
                            &conversation.verifier(&mut None)?,
                            &SchemaRegistry::new(),
                            &scope,
                            &fixture.storage,
                            Some(&io),
                        )?;
                        flushes.0.store(0, Ordering::Relaxed);
                        // Compare the native path with its own former shape:
                        // native filesystem staging has a different I/O cost
                        // from the ordinary publisher.
                        let former_id = InteractionId::from_bytes([120 + 3 * u8::from(approved); 16]);
                        let native_host = native.interactions()?;
                        let mut transaction = native_host.transaction(Some(&io)).await?;
                        let ticket = native_host.stage_request(former_id, &request, None, Some(&io)).await?;
                        native_host.open_at(
                            super::super::interactions::interaction_open_operation(former_id, Some(&io))?,
                            None,
                            &ticket,
                            native.owner.as_ref().map(|(owner, _)| owner),
                            Some(&io),
                            &mut transaction,
                        ).await?;
                        native.publish_approval_request(former_id, &transaction.aggregate.0, Some(&io)).await?;
                        former_native = Some(flushes.0.load(Ordering::Relaxed));
                        drop(transaction);
                        drop(ticket);
                        flushes.0.store(0, Ordering::Relaxed);
                        native.open_interaction_with_io(id, &request, Some(&io)).await?;
                    }
                    _ => {
                        // Execute the former eager canonical open, then the
                        // identical mandatory task-file and canonical proofs.
                        let mut transaction = host.transaction(None).await?;
                        let ticket = host.stage_request(id, &request, None, None).await?;
                        host.open_at(
                            super::super::interactions::interaction_open_operation(id, None)?,
                            None,
                            &ticket,
                            journal.owner.as_ref().map(|(owner, _)| owner),
                            None,
                            &mut transaction,
                        )
                        .await?;
                        journal
                            .publish_approval_request(id, &transaction.aggregate.0, None)
                            .await?;
                    }
                }
                let count = flushes.0.load(Ordering::Relaxed);
                assert!(count > 0, "request flushes were not observed");
                opening.push(count);
                let opened = files
                    .snapshot(&Query::Approval { task: task(), request: request_id(id) }, 1)
                    .await?;
                assert_eq!(opened.files.len(), 1);
                assert!(files.inner.file_is_durable(&opened.files[0]).await?);
                let aggregate = host.aggregate(None).await?;
                assert!(aggregate.0.durable_receipt().await?.is_some());
                drop(aggregate);
                assert!(journal.interaction_outcome_with_io(id, None).await?.is_none());
                flushes.0.store(0, Ordering::Relaxed);
                journal.open_interaction(id, &request, None)?.await?;
                assert_eq!(flushes.0.load(Ordering::Relaxed), 0, "exact reopen flushed again");
                let ticket = host.read(id, None).await?.unwrap().0;
                let human = responder(&conversation, "human", &[id])?;
                let decision = decision(
                    id,
                    "human",
                    &ApprovalDecisionFile::new(
                        "human",
                        ticket.request.descriptor().sha256(),
                        1,
                        approved,
                        None,
                    )?,
                );
                flushes.0.store(0, Ordering::Relaxed);
                match entry {
                    0 => journal.write_approval_decision(&decision, &human).await?,
                    1 => {
                        journal
                            .resolve_interaction(
                                id,
                                InteractionResponse::Approval {
                                    approved,
                                    reason: None,
                                },
                                &human,
                            )
                            .await?;
                    }
                    2 => {
                        // Execute the preceding grouped settlement shape,
                        // including its redundant canonical-before-file barrier.
                        acyclic_stream::deferring_provider_durability(
                            &*fixture.stream,
                            host.resolve_latest(
                                id,
                                InteractionResponse::Approval {
                                    approved,
                                    reason: None,
                                },
                                &human,
                                journal.owner.as_ref().map(|(owner, _)| owner),
                            ),
                        ).await?;
                        let aggregate = host.aggregate(None).await?;
                        assert!(aggregate.0.durable_receipt().await?.is_some());
                        drop(aggregate);
                        journal.published_decision(id, None).await?;
                    }
                    _ => {
                        // Execute the former eager filesystem staging and
                        // canonical append, followed by the identical proof.
                        host.resolve_latest(
                            id,
                            InteractionResponse::Approval {
                                approved,
                                reason: None,
                            },
                            &human,
                            journal.owner.as_ref().map(|(owner, _)| owner),
                        )
                        .await?;
                        journal.published_decision(id, None).await?;
                    }
                }
                let count = flushes.0.load(Ordering::Relaxed);
                assert!(count > 0, "the native flush trace was not observed");
                measured.push(count);
                let observed = files
                    .snapshot(
                        &Query::Approval {
                            task: task(),
                            request: request_id(id),
                        },
                        8,
                    )
                    .await?;
                assert_eq!(observed.files.len(), 2);
                for file in &observed.files {
                    assert!(
                        files.inner.file_is_durable(file).await?,
                        "settlement returned an unflushed file"
                    );
                }
                let proof = journal.published_decision(id, None).await?;
                assert_eq!(proof.approved(), approved);
                assert_eq!(
                    journal.interaction_outcome_with_io(id, None).await?,
                    Some(if approved {
                        InteractionOutcome::Approved {}
                    } else {
                        InteractionOutcome::Declined {}
                    })
                );
            }
            assert_eq!(opening[0] + 1, opening[2], "public open did not group publication");
            let former_native = former_native.ok_or_else(|| Error::Storage("native control was not executed".into()))?;
            assert_eq!(opening[1] + 1, former_native, "native open did not group publication");
            eprintln!(
                "APPROVALS_REQUEST_FLUSHES shared={shared_provider} approved={approved} public={} native={} former={} former_native={former_native}",
                opening[0], opening[1], opening[2],
            );
            assert_eq!(measured[0] + 1, measured[2], "file settlement kept a redundant canonical barrier");
            assert_eq!(measured[1] + 1, measured[2], "direct settlement kept a redundant canonical barrier");
            if shared_provider {
                assert_eq!(measured[2] + 1, measured[3], "settlement did not group staging: {measured:?}");
            } else {
                assert_eq!(measured[2], measured[3], "settlement deferred a foreign authority");
            }
            eprintln!(
                "APPROVALS_SETTLEMENT_FLUSHES shared={shared_provider} approved={approved} file={} direct={} former_grouped={} former_eager={}",
                measured[0], measured[1], measured[2], measured[3]
            );
        }
        Ok(())
    })
}
