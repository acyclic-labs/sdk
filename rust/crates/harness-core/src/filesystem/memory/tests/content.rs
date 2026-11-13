use super::*;
use crate::IdempotencyKey;

#[tokio::test]
async fn retained_writer_admission_requires_the_host_provider() -> Result<()> {
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 4096).await?;
    storage
        .host
        .authorize_retained_writer(&storage.scope, &storage.volume)
        .await?;
    let foreign = VolumeRef::new(
        crate::resources::ProviderRef::new(
            "elsewhere".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?,
        storage.volume.id().to_owned(),
        storage.volume.class(),
        storage.volume.owner().clone(),
        &mut None,
    )?;
    let refused = storage
        .host
        .authorize_retained_writer(&storage.scope, &foreign)
        .await
        .expect_err("a foreign provider was opened under the host's binding");
    assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    storage
        .host
        .authorize_volume_creation(&storage.scope, &storage.volume)?;
    let refused = storage
        .host
        .authorize_volume_creation(&storage.scope, &foreign)
        .expect_err("legacy allocation accepted another provider");
    assert_eq!(refused.code(), crate::wire::ErrorCode::Unauthorized);
    let journal = crate::conversation::journal_volume(
        storage.volume.provider().clone(),
        &crate::agent::AgentId::from_bytes(agent.into_bytes()),
    )?;
    let creation = acyclic_fs::access::Capability::whole_volume(
        None,
        &[
            acyclic_fs::access::Action::Create,
            acyclic_fs::access::Action::Write,
        ],
    );
    let creation_scope = storage.issuer.bootstrap_scope(
        "allocate-only",
        &Capabilities::new([creation
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?]),
        Some(agent),
        &mut None,
    )?;
    storage
        .host
        .authorize_volume_creation(&creation_scope, &journal)?;
    Ok(())
}
#[tokio::test]
async fn original_publication_receipts_require_write_but_not_directory_listing() -> Result<()> {
    use acyclic_fs::access::{Action, Capability};
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 4096).await?;
    let capability = Capability::whole_volume(
        None,
        &[Action::Read, Action::Create, Action::Write, Action::Admin],
    );
    let scope = storage.issuer.bootstrap_scope(
        "receipt-writer",
        &Capabilities::new([capability
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?]),
        Some(agent),
        &mut None,
    )?;
    let publisher = FilesystemContentPublisher::new(
        storage.host.clone(),
        &storage.volume,
        &storage.verifier(),
        &scope,
        4096,
        None,
    )?;
    let operation = OperationId::new();
    let original = publisher
        .stage_once(
            operation,
            "original",
            b"original",
            "text/plain",
            "original",
            Default::default(),
        )?
        .await?
        .0;
    assert_eq!(
        publisher
            .observe_staged_once(operation, None, Some(4096), Default::default())?
            .await?
            .0,
        Some(original.clone())
    );
    assert_eq!(
        publisher
            .stage_once(
                operation,
                "original",
                b"original",
                "text/plain",
                "original",
                Default::default(),
            )?
            .await?
            .0,
        original
    );
    let mut elsewhere = capability;
    elsewhere.layers[0][0].resource.path = "elsewhere".into();
    let ceiling = Capabilities::new([elsewhere
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?]);
    let denied = async {
        publisher
            .observe_staged_once(
                operation,
                None,
                Some(4096),
                crate::conversation::ContentAccess {
                    ceiling: Some(&ceiling),
                    io: None,
                },
            )?
            .await
    }
    .await
    .err()
    .expect("receipt recovery ignored the recorded path's write ceiling");
    assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
    let grant = ContentGrant::verify(
        &storage.verifier(),
        &scope,
        &storage.volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let key = crate::filesystem::content_publisher::once_key(operation, None)?;
    let metadata = storage
        .host
        .content_for_operation(
            &storage.volume,
            &grant,
            &key,
            "original",
            Some(4096),
            Default::default(),
        )
        .await
        .err()
        .expect("standalone metadata lookup bypassed directory disclosure");
    assert_eq!(metadata.code(), crate::wire::ErrorCode::Unauthorized);
    Ok(())
}

#[tokio::test]
async fn private_publication_receipts_keep_the_observing_writer() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
    // Exercise a valid host whose journal and private publisher share a volume.
    let journal: Arc<dyn crate::executor::ExecutionJournal> =
        Arc::new(crate::filesystem::FilesystemExecutionJournal::new(
            Arc::clone(&storage.stream),
            Arc::clone(&storage.host),
            crate::filesystem::FilesystemJournalBinding {
                volume: &storage.volume,
                verifier: &storage.verifier(),
                scope: &storage.scope,
                io: None,
            },
        )?);
    let turn = OperationId::new();
    let effect = OperationId::new();
    run_admitted_file_case(&storage, Limits::default(), move |task| {
        let journal = Arc::clone(&journal);
        async move {
            let context = crate::runtime::ToolContext::new(
                Arc::new(task.with_execution_journal(journal, turn)?),
                turn,
                "publish",
            )?;
            let original = context
                .stage_file_once(
                    effect,
                    "receipts/one",
                    b"once",
                    "text/plain",
                    "receipt",
                    None,
                )
                .await?
                .0;
            let observed = context
                .observe_staged_file_once(effect, None)
                .await?
                .0
                .ok_or_else(|| Error::NotFound("original publication receipt".into()))?;
            assert_eq!(observed, original);
            assert_eq!(
                context
                    .stage_file_once(
                        effect,
                        "receipts/one",
                        b"once",
                        "text/plain",
                        "receipt",
                        None
                    )
                    .await?
                    .0,
                original
            );
            assert_eq!(
                context.task().read_file(&original, None).await?.as_ref(),
                b"once"
            );
            let denied = context
                .task()
                .stage_file(
                    Some(original.volume()),
                    OperationId::new(),
                    "receipts/forged",
                    b"forged",
                    "text/plain",
                    "forged",
                )
                .await
                .expect_err("receipt replay delegated the read-only journal writer");
            assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
            Ok(())
        }
    })
    .await
}

#[tokio::test]
async fn tool_output_activity_shares_chunk_publication_and_rejects_partial_updates() -> Result<()> {
    use crate::runtime::{
        Bindings, ToolContext, ToolOutputActivity, ToolOutputLog, tool_output_activity_path,
    };
    for enabled in [false, true] {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let content = storage.content_bindings();
        let volume = content.volume.clone();
        let mut bindings = Bindings::local();
        bindings.scope = RuntimeScope::new(
            Capabilities::new([
                storage.read_capability.clone(),
                storage.write_capability.clone(),
                volume.directory_read_capability("", &mut None)?,
            ]),
            Limits::default(),
        )?;
        bindings.content = Some(content.clone());
        let task = Arc::new(
            bindings
                .build()?
                .agent_context(&Capabilities::new(Vec::<String>::new()), content)?,
        );
        let call = OperationId::from_bytes([51; 16]);
        let mut context = ToolContext::new(task, call, "output")?;
        context.output_activity = enabled;
        let mut output = ToolOutputLog::new(context.clone());
        let first = output
            .publish_chunk("stdout/0000000000000000", b"first")
            .await?;
        assert_eq!(context.read_file(&first, None).await?.as_ref(), b"first");
        let path = tool_output_activity_path(context.turn_id(), call);
        if !enabled {
            assert!(
                context
                    .read_private_path(&volume, "", &path, None)
                    .await
                    .is_err()
            );
            assert_eq!(
                context
                    .read_output_file_once("stdout/0000000000000000")
                    .await?
                    .map(|(file, _)| file),
                Some(first)
            );
            continue;
        }
        let (activity, bytes) = context.read_private_path(&volume, "", &path, None).await?;
        let cursor: ToolOutputActivity =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            (cursor.version, cursor.call, cursor.turn, cursor.through),
            (1, call, context.turn_id(), 1)
        );
        assert_eq!(
            activity.version(),
            first.version(),
            "activity was published outside its chunk's original generation"
        );
        // A fresh owner recovers the exact original batch after later reads,
        // rather than conditionally creating a new cursor beside old output.
        let mut replay = ToolOutputLog::new(context.clone());
        assert_eq!(
            replay
                .publish_chunk("stdout/0000000000000000", b"first")
                .await?,
            first
        );
        assert!(
            output
                .publish_chunk("stdout/0000000000000000", b"replacement")
                .await
                .is_err()
        );
        let (_, unchanged) = context.read_private_path(&volume, "", &path, None).await?;
        assert_eq!(
            unchanged.as_ref(),
            bytes.as_ref(),
            "refused output advanced its activity cursor"
        );
        let second = output
            .publish_chunk("stderr/0000000000000000", b"second")
            .await?;
        let (next, bytes) = context.read_private_path(&volume, "", &path, None).await?;
        assert_eq!(next.version(), second.version());
        assert_eq!(
            serde_json::from_slice::<ToolOutputActivity>(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .through,
            2
        );
        assert_eq!(
            serde_json::from_slice::<ToolOutputActivity>(
                &context.read_file(&activity, None).await?
            )
            .map_err(|error| Error::Invalid(error.to_string()))?
            .through,
            1,
            "later output rewrote the original activity receipt"
        );
    }
    Ok(())
}

#[tokio::test]
async fn streamed_publication_replays_one_contract_and_refuses_changed_content() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), None).await?;
    let operation = OperationId::new();
    let bytes: Vec<u8> = (0..65537).map(|index| (index % 251) as u8).collect();
    let mut source = futures::io::Cursor::new(bytes.as_slice());
    let (original, _) = storage
        .publisher
        .stage_stream_once(
            operation,
            "streamed",
            &mut source,
            "application/octet-stream",
            "streamed",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert_eq!(storage.read(&original).await?.as_ref(), bytes.as_slice());
    // Both public entrances share one request identity and original receipt.
    let (replayed, _) = storage
        .publisher
        .stage_once(
            operation,
            "streamed",
            &bytes,
            "application/octet-stream",
            "streamed",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    assert_eq!(replayed, original);
    let mut replay = futures::io::Cursor::new(bytes.as_slice());
    assert_eq!(
        storage
            .publisher
            .stage_stream_once(
                operation,
                "streamed",
                &mut replay,
                "application/octet-stream",
                "streamed",
                crate::conversation::ContentAccess::default(),
            )?
            .await?
            .0,
        original
    );
    let mut changed = futures::io::Cursor::new(b"changed".as_slice());
    assert!(matches!(
        storage
            .publisher
            .stage_stream_once(
                operation,
                "streamed",
                &mut changed,
                "application/octet-stream",
                "streamed",
                crate::conversation::ContentAccess::default(),
            )?
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(storage.read(&original).await?.as_ref(), bytes.as_slice());
    // Intersecting staging capacity must not widen a smaller publisher credit.
    let limited = FilesystemContentPublisher::new(
        storage.host.clone(),
        &storage.volume,
        &storage.verifier(),
        &storage.scope,
        4,
        None,
    )?;
    let refused = OperationId::new();
    let mut oversized = futures::io::Cursor::new(b"12345".as_slice());
    assert!(
        limited
            .stage_stream_once(
                refused,
                "over-credit",
                &mut oversized,
                "text/plain",
                "over-credit",
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .is_err()
    );
    assert!(
        limited
            .observe_staged_once(
                refused,
                None,
                Some(4),
                crate::conversation::ContentAccess::default()
            )?
            .await?
            .0
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn streaming_producer_retirement_and_cancellation_leave_no_publication() -> Result<()> {
    use futures::{FutureExt, TryStreamExt};
    for cancel in [false, true] {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let operation = OperationId::new();
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = crate::filesystem::PreparationIo::for_unmeasured(&cancellation)?;
        let (started, reading) = futures::channel::oneshot::channel();
        let (producer, receiver) = futures::channel::oneshot::channel::<Vec<u8>>();
        let stream = futures::stream::once(async move {
            started
                .send(())
                .map_err(|_| std::io::Error::other("read observer retired"))?;
            receiver.await.map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "content producer retired; recovery: recreate the complete original source",
                )
            })
        });
        let mut source = Box::pin(stream).into_async_read();
        let publication = storage
            .publisher
            .stage_stream_once(
                operation,
                "retired",
                &mut source,
                "application/octet-stream",
                "retired",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io),
                },
            )?
            .fuse();
        let mut publication = core::pin::pin!(publication);
        let reading = reading.fuse();
        let mut reading = core::pin::pin!(reading);
        futures::select_biased! {
            ready = reading => ready.map_err(|_| Error::Storage("source entrance retired; recovery: run streaming_producer_retirement_and_cancellation_leave_no_publication".into()))?,
            _ = publication => return Err(Error::Storage("publication settled before its source event; recovery: run streaming_producer_retirement_and_cancellation_leave_no_publication".into())),
        }
        if cancel {
            cancellation.cancel();
        } else {
            drop(producer);
        }
        assert!(publication.await.is_err());
        assert!(
            storage
                .publisher
                .observe_staged_once(
                    operation,
                    None,
                    Some(4096),
                    crate::conversation::ContentAccess::default()
                )?
                .await?
                .0
                .is_none()
        );
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Recovery authenticates the publication operation, not the later head or
    // a placeholder version; the requested path must still match its receipt.
    #[test]
    fn publication_recovery_preserves_operation_identity(
        key in "[a-z0-9]{1,32}",
        body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..1024),
    ) {
        let result = futures::executor::block_on(async {
            let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
            let write = ContentGrant::verify(
                &storage.verifier(), &storage.scope, &storage.volume,
                VolumeOperation::Write, &mut None,
            )?;
            let key = IdempotencyKey::new(key)?;
            let original = storage.host.put_content(
                &storage.volume, &write, "original", &body, "application/octet-stream",
                "original", Some(1024), &key, None, crate::conversation::ContentAccess::default(),
            ).await?;
            storage.host.put_content(
                &storage.volume, &write, "original", b"replacement", "text/plain",
                "replacement", Some(1024), &IdempotencyKey::new("later-head/edit")?,
                None, crate::conversation::ContentAccess::default(),
            ).await?;
            assert_eq!(storage.host.content_for_operation(
                &storage.volume, &write, &key, "original", Some(1024), crate::conversation::ContentAccess::default(),
            ).await?, Some(original));
            assert!(storage.host.content_for_operation(
                &storage.volume, &write, &key, "different", Some(1024), crate::conversation::ContentAccess::default(),
            ).await.is_err());
            Ok::<_, Error>(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

#[tokio::test]
async fn stock_publisher_finite_once_observes_original_metadata_after_head_changes()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let operation = OperationId::new();
    let cancellation = acyclic_fs::CancellationToken::new();
    let filesystem = storage.host.filesystem.clone();
    let reference = crate::filesystem::bounded_io::content_workspace(
        &storage.host.provider,
        &storage.volume,
        None,
    )?;
    let root = storage.host.open(&reference, None).await?;
    let store = acyclic_fs::StreamGitCompatStore::new(acyclic_fs::StreamAuthorityStore::new(
        filesystem.stream_provider(),
    ));
    let graph = acyclic_fs::WorkspaceGraph::new(store.clone());
    graph
        .register_root(
            &root,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?;
    let branch = graph
        .fork(
            &root,
            "uploads",
            acyclic_fs::IdempotencyKey::new(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let repository = acyclic_fs::GitCompatRepository::new(root.id(), store.clone());
    repository
        .register_branch_workspace("uploads", branch.id(), None, true)
        .await?;
    let original_key = crate::filesystem::content_publisher::once_key(operation, None)?;
    let original_key = crate::filesystem::bounded_io::operation_key(&original_key, None)?;
    let colliding_string = IdempotencyKey::new(format!(
        "filesystem-content-workspace-v1:{}",
        hex::encode(original_key.into_bytes())
    ))?;
    let write = ContentGrant::verify(
        &storage.verifier(),
        &storage.scope,
        &storage.volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    // A real user upload can choose an internal-looking string. Routing keys
    // must live in a distinct primitive domain, rather than reserve that name.
    storage
        .host
        .put_content(
            &storage.volume,
            &write,
            "caller-selected-key",
            b"root upload",
            "text/plain",
            "caller-selected-key",
            Some(1024),
            &colliding_string,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    storage.host = FilesystemHost::new(filesystem, &storage.host.provider, None)?
        .with_project_families(store.clone())?;
    storage.publisher = FilesystemContentPublisher::new(
        storage.host.clone(),
        &storage.volume,
        &storage.verifier(),
        &storage.scope,
        1024,
        None,
    )?;
    storage.content_verifier = FilesystemContentVerifier::new(
        storage.host.clone(),
        &storage.verifier(),
        &storage.scope,
        1024,
        None,
    )?;
    let committed_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancellation)?;
    let (committed, _committed_hold) = storage
        .publisher
        .stage_once(
            operation,
            "once.json",
            b"original",
            "application/json",
            "original.json",
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&committed_io),
            },
        )?
        .await?;
    assert!(committed_io.receipt().backend_write_operations > 0);
    let original_key =
        crate::filesystem::content_publisher::once_key(operation, Some(&committed_io))?;
    let original_key =
        crate::filesystem::bounded_io::operation_key(&original_key, Some(&committed_io))?;
    assert!(
        committed_io
            .sdk(
                root.operation_generation(original_key, &committed_io, &cancellation)
                    .await
            )?
            .is_none(),
        "the old root-only lookup misses this actual branch upload"
    );
    assert_eq!(
        storage
            .publisher
            .observe_staged_once(
                operation,
                None,
                Some(1024),
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&committed_io),
                },
            )?
            .await?
            .0
            .as_ref(),
        Some(&committed),
        "recovery must find the branch publication before checkout changes"
    );
    let replacement_operation = OperationId::new();
    let (replacement, _replacement_hold) = storage
        .publisher
        .stage_at(
            replacement_operation,
            &committed,
            b"replacement",
            None,
            Some(&committed_io),
        )?
        .await?;
    assert_eq!(
        storage
            .content_verifier
            .read(&replacement, 1024, None, Some(&committed_io))?
            .await?
            .as_ref(),
        b"replacement"
    );
    assert_eq!(
        storage
            .publisher
            .stage_at(
                replacement_operation,
                &committed,
                b"replacement",
                None,
                Some(&committed_io),
            )?
            .await?
            .0,
        replacement
    );
    // A digest-only projection of this actual reference must fail before IO.
    let mut digest_only = committed.clone();
    let generation = crate::filesystem::bounded_io::generation_version(committed.version(), None)?;
    digest_only.pin_version(hex::encode(generation.digest().as_bytes()), &mut None)?;
    let writes = committed_io.receipt().backend_write_operations;
    assert!(matches!(
        storage
            .publisher
            .stage_at(
                OperationId::new(),
                &digest_only,
                b"never",
                None,
                Some(&committed_io),
            )?
            .await,
        Err(error) if error.code() == crate::wire::ErrorCode::Invalid
    ));
    assert_eq!(committed_io.receipt().backend_write_operations, writes);
    repository
        .register_branch_workspace("main", root.id(), None, true)
        .await?;
    storage
        .publisher
        .stage_once(
            OperationId::new(),
            "once.json",
            b"later",
            "text/plain",
            "later.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let reopened_host = FilesystemHost::new(
        storage.host.filesystem.clone(),
        &storage.host.provider,
        None,
    )?
    .with_project_families(store)?;
    let reopened_publisher = FilesystemContentPublisher::new(
        reopened_host,
        &storage.volume,
        &storage.verifier(),
        &storage.scope,
        1024,
        None,
    )?;
    let observed_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancellation)?;
    let (recovered, _recovered_hold) = reopened_publisher
        .observe_staged_once(
            operation,
            None,
            Some(1024),
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&observed_io),
            },
        )?
        .await?;
    assert_eq!(recovered.as_ref(), Some(&committed));
    let root_before = root
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .id();
    let branch_before = branch
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .id();
    assert_eq!(
        reopened_publisher
            .stage_once(
                operation,
                "once.json",
                b"original",
                "application/json",
                "original.json",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&committed_io),
                },
            )?
            .await?
            .0,
        committed
    );
    assert_eq!(
        root.head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id(),
        root_before
    );
    assert_eq!(
        branch
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .id(),
        branch_before
    );
    let read_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancellation)?;
    assert_eq!(
        storage
            .content_verifier
            .read(&committed, 1024, None, Some(&read_io))?
            .await?
            .as_ref(),
        b"original"
    );
    assert!(
        reopened_publisher
            .stage_once(
                operation,
                "once.json",
                b"original",
                "text/plain",
                "changed.txt",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&committed_io)
                }
            )?
            .await
            .is_err()
    );
    let cancelled = acyclic_fs::CancellationToken::new();
    let cancelled_io =
        crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancelled)?;
    cancelled.cancel();
    assert!(
        reopened_publisher
            .stage_once(
                OperationId::new(),
                "cancelled",
                b"never",
                "text/plain",
                "never",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&cancelled_io)
                }
            )
            .is_err()
    );
    assert_eq!(cancelled_io.receipt().backend_write_operations, 0);
    let denied_operation = OperationId::new();
    let mut denied = FINITE_CONTENT_BUDGET;
    denied.peak_allocation_bytes = 0;
    let denied_io = crate::filesystem::PreparationIo::from_budget(denied, &cancellation)?;
    assert!(
        reopened_publisher
            .stage_once(
                denied_operation,
                "denied",
                b"never",
                "text/plain",
                "never",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&denied_io)
                }
            )
            .is_err()
    );
    assert!(
        reopened_publisher
            .observe_staged_once(
                denied_operation,
                None,
                Some(1024),
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&observed_io)
                }
            )?
            .await?
            .0
            .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn stock_native_content_keeps_original_ledger_and_output_custody() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    storage.publisher.supports_stage(Some(&io))?;
    assert_eq!(work.receipt().backend_write_operations, 0);
    struct OrdinaryOnlyPublisher {
        volume: VolumeRef,
    }
    impl ContentPublisher for OrdinaryOnlyPublisher {
        fn volume(&self) -> &VolumeRef {
            &self.volume
        }
    }
    let ordinary = OrdinaryOnlyPublisher {
        volume: storage.volume.clone(),
    };
    assert!(matches!(
        ordinary.supports_stage(Some(&io)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert!(matches!(
        ordinary.stage_once(
            OperationId::new(),
            "unsupported",
            b"never",
            "text/plain",
            "never",
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io)
            }
        ),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    let mut frame_budget = FINITE_CONTENT_BUDGET;
    frame_budget.allocation_operations = work.receipt().allocation_operations;
    let frame_work = acyclic_fs::NativeMaterializationWork::new(
        frame_budget,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let frame_io = crate::filesystem::PreparationIo::from_native_work(&frame_work)?;
    assert!(
        storage
            .publisher
            .stage_once(
                OperationId::new(),
                "frame-denied",
                b"never",
                "text/plain",
                "never",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&frame_io)
                }
            )
            .is_err()
    );
    assert_eq!(frame_work.receipt().backend_write_operations, 0);
    let operation = OperationId::new();
    let (file, file_hold) = storage
        .publisher
        .stage_once(
            operation,
            "native.json",
            b"original",
            "application/json",
            "original.json",
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io),
            },
        )?
        .await?;
    assert!(file_hold.as_ref().is_some_and(|hold| hold.bytes() > 0));
    assert!(work.receipt().backend_write_operations > 0);
    let before_read = work.live_allocation_bytes();
    let bytes = storage
        .content_verifier
        .read(&file, 1024, None, Some(&io))?
        .await?;
    assert_eq!(bytes.as_ref(), b"original");
    assert!(bytes.matches_meter(&work.runtime_meter()));
    assert!(work.live_allocation_bytes() > before_read);
    let with_output = work.live_allocation_bytes();
    drop(bytes);
    assert!(work.live_allocation_bytes() < with_output);
    assert!(
        storage
            .publisher
            .stage_once(
                operation,
                "native.json",
                b"original",
                "text/plain",
                "changed",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io)
                }
            )?
            .await
            .is_err()
    );
    drop(file);
    drop(file_hold);
    cancellation.cancel();
    assert!(
        storage
            .publisher
            .stage_once(
                OperationId::new(),
                "cancelled",
                b"never",
                "text/plain",
                "never",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io)
                }
            )
            .is_err()
    );
    let mut denied = FINITE_CONTENT_BUDGET;
    denied.backend_write_operations = 0;
    let denied_work =
        acyclic_fs::NativeMaterializationWork::new(denied, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let denied_io = crate::filesystem::PreparationIo::from_native_work(&denied_work)?;
    assert!(
        storage
            .publisher
            .stage_once(
                OperationId::new(),
                "denied",
                b"never",
                "text/plain",
                "never",
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&denied_io)
                }
            )?
            .await
            .is_err()
    );
    assert_eq!(denied_work.receipt().backend_write_operations, 0);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_host_original_key_recovers_immutable_upload_after_head_edit() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
    let write = ContentGrant::verify(
        &storage.verifier(),
        &storage.scope,
        &storage.volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let key = IdempotencyKey::new("acyclic.pi/input/root-task/original-operation/label")?;
    let mut path_budget = FINITE_CONTENT_BUDGET;
    path_budget.allocation_operations = work.receipt().allocation_operations;
    let path_work = acyclic_fs::NativeMaterializationWork::new(path_budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let path_io = crate::filesystem::PreparationIo::from_native_work(&path_work)?;
    let path_prefix = path_work.receipt();
    let path = "../outside";
    let rejection = storage
        .host
        .put_content(
            &storage.volume,
            &write,
            path,
            b"never",
            "text/plain",
            "never",
            Some(1024),
            &key,
            None,
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&path_io),
            },
        )
        .await;
    assert!(
        matches!(
            rejection,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ),
        "{path}: {rejection:?}"
    );
    assert!(matches!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &key,
                "../outside",
                Some(1024),
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&path_io)
                }
            )
            .await,
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    let rejected = path_work.receipt();
    assert_eq!(
        rejected,
        acyclic_fs::WorkCounters {
            items_examined: rejected.items_examined,
            ..path_prefix
        }
    );
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    // The former reserved name is a regular file, not a host directory.
    let staged = storage
        .host
        .put_content(
            &storage.volume,
            &write,
            ".system/private",
            b"original",
            "application/json",
            "original.json",
            Some(1024),
            &key,
            None,
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io),
            },
        )
        .await?;
    let staged_hold = io.take_native_custody(staged.logical_owned_bytes()?)?;
    // Exercise ordinary authored-name delegation independently of the staged
    // content fixture's oversized-display-name boundary.
    {
        let delegated = storage.issuer.delegate_private_directory_read(
            &storage.scope,
            AgentId::new(),
            "authored-system-reader",
            &storage.volume,
            ".system",
        )?;
        let delegated_reader = crate::filesystem::FilesystemContentVerifier::new(
            storage.host.clone(),
            &storage.verifier(),
            &delegated,
            1024,
            None,
        )?;
        assert_eq!(
            delegated_reader
                .read_file(&staged, 1024, None, None)
                .await?
                .as_ref(),
            b"original"
        );
        let (outside, _outside_hold) = storage
            .publisher
            .stage_once(
                OperationId::new(),
                "scratch/outside",
                b"outside",
                "text/plain",
                "outside",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        let Err(denied) = delegated_reader.read_file(&outside, 1024, None, None).await else {
            panic!("delegated directory read escaped its prefix");
        };
        assert_eq!(denied.code(), crate::wire::ErrorCode::Unauthorized);
        assert!(
            ContentGrant::verify(
                &storage.verifier(),
                &delegated,
                &storage.volume,
                VolumeOperation::Write,
                &mut None,
            )
            .is_err()
        );
    }
    storage
        .host
        .put_content(
            &storage.volume,
            &write,
            ".system/private",
            b"later",
            "text/plain",
            "later.txt",
            Some(1024),
            &IdempotencyKey::new("head-edit")?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
    let recovered = storage
        .host
        .content_for_operation(
            &storage.volume,
            &write,
            &key,
            ".system/private",
            Some(1024),
            crate::conversation::ContentAccess {
                ceiling: None,
                io: Some(&io),
            },
        )
        .await?
        .ok_or_else(|| Error::NotFound("original native operation receipt".into()))?;
    let recovered_hold = io.take_native_custody(recovered.logical_owned_bytes()?)?;
    assert_eq!(recovered, staged);
    let content = storage
        .content_verifier
        .read(&recovered, 1024, None, Some(&io))?
        .await?;
    assert_eq!(content.as_ref(), b"original");
    drop(content);
    let writes = work.receipt().backend_write_operations;
    assert!(
        storage
            .host
            .put_content(
                &storage.volume,
                &write,
                ".system/private",
                b"original",
                "text/plain",
                "changed.txt",
                Some(1024),
                &key,
                None,
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io)
                }
            )
            .await
            .is_err()
    );
    assert_eq!(work.receipt().backend_write_operations, writes);
    assert!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &key,
                "inputs/changed.json",
                Some(1024),
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io)
                }
            )
            .await
            .is_err()
    );
    assert!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &IdempotencyKey::new("proven-absent-operation")?,
                ".system/private",
                Some(1024),
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io)
                }
            )
            .await?
            .is_none()
    );
    drop(recovered);
    drop(recovered_hold);
    drop(staged);
    drop(staged_hold);
    drop(io);
    let cancelled = acyclic_fs::CancellationToken::new();
    let cancelled_work =
        acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancelled)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&cancelled_work)?;
    let prefix = cancelled_work.receipt();
    cancelled.cancel();
    assert!(matches!(
        storage
            .host
            .content_for_operation(
                &storage.volume,
                &write,
                &key,
                ".system/private",
                Some(1024),
                crate::conversation::ContentAccess {
                    ceiling: None,
                    io: Some(&io)
                }
            )
            .await,
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert_eq!(cancelled_work.receipt(), prefix);
    drop(io);
    let with_storage = work.live_allocation_bytes();
    // Checkout registrations remain on the original ledger until that ledger drops.
    drop(storage);
    assert!(work.live_allocation_bytes() < with_storage);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn exhausted_native_custody_transfer_keeps_existing_guard_until_value_drops() -> Result<()> {
    let work = acyclic_fs::NativeMaterializationWork::new(
        FINITE_CONTENT_BUDGET,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let bytes = io.encode(&serde_json::Value::Null, 1024, true)?;
    let held = io.owned_bytes()?;
    let live_held = work.live_allocation_bytes() - baseline;
    let Err(error) = io.into_native_custody(held + work.remaining()?.peak_allocation_bytes + 1)
    else {
        panic!("custody beyond the original remaining quota must fail");
    };
    let failure = (bytes, error);
    assert!(matches!(
        &failure.1,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(_),
            custody: Some(_),
        }
    ));
    assert_eq!(work.live_allocation_bytes(), baseline + live_held);
    let empty_alias = failure.0.slice(0..0);
    drop(failure);
    assert!(work.live_allocation_bytes() > baseline);
    drop(empty_alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn rejected_foreign_native_custody_preserves_original_owned_data_guard() -> Result<()> {
    let token = acyclic_fs::CancellationToken::new();
    let receiving = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let producing = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let receiver_io = crate::filesystem::PreparationIo::from_native_work(&receiving)?;
    let receiver_baseline = receiving.live_allocation_bytes();
    let producer_baseline = producing.live_allocation_bytes();
    let producer_io = crate::filesystem::PreparationIo::from_native_work(&producing)?;
    let bytes = producer_io.encode(&serde_json::Value::Null, 1024, true)?;
    drop(producer_io);
    let (metadata, incoming) = producing
        .display("source carrier metadata")
        .map_err(Error::from)?;
    let held = producing.live_allocation_bytes() - producer_baseline;
    let Err(error) = receiver_io.absorb_native_custody(incoming) else {
        panic!("distinct original allocation ledgers must not merge");
    };
    let failure = (bytes, metadata, error);
    assert!(matches!(
        &failure.2,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: Some(_),
        }
    ));
    assert_eq!(receiving.live_allocation_bytes(), receiver_baseline);
    assert_eq!(producing.live_allocation_bytes(), producer_baseline + held);
    drop(failure);
    assert_eq!(producing.live_allocation_bytes(), producer_baseline);
    Ok(())
}
