//! Public named-creation work bounds and failure receipts.
use acyclic_fs::model::{Lifecycle, VolumeConfig};
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, OperationId, WorkBudget, WorkCounters, WorkspaceError,
};

fn finite_budget() -> Result<WorkBudget, serde_json::Error> {
    let mut value = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    if let serde_json::Value::Object(fields) = &mut value {
        for allowance in fields.values_mut() {
            *allowance = serde_json::json!(100_000_000_u64);
        }
    }
    serde_json::from_value(value)
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
#[tokio::test]
async fn materialization_failures_retain_the_pinned_checkout_work()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("measured-materialization").await?;
    workspace
        .write_text("/original.txt", "pinned bytes")
        .await?;
    let generation = workspace.head().await?;
    let budget = finite_budget()?;
    let pinned = workspace
        .generation_measured(generation.id(), budget, &CancellationToken::new())
        .await?;
    assert!(pinned.work.object_bytes_read > 0);
    let destination = tempfile::tempdir()?;
    let invalid = acyclic_fs::MaterializeOptions::native(destination.path().join("absent"));
    for selected_path in [false, true] {
        let failure = if selected_path {
            generation
                .materialize_path("/original.txt", &invalid, budget, &CancellationToken::new())
                .await
                .err()
        } else {
            generation
                .materialize(&invalid, budget, &CancellationToken::new())
                .await
                .err()
        }
        .ok_or("materialization accepted an absent destination")?;
        assert_eq!(
            *failure.work,
            pinned.work.checked_add(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::default()
            })?
        );
        failure.work.verify(budget)?;
    }
    let valid = acyclic_fs::MaterializeOptions::native(destination.path());
    let mut root_only = budget;
    root_only.backend_read_operations = pinned.work.backend_read_operations + 1;
    let failure = generation
        .materialize(&valid, root_only, &CancellationToken::new())
        .await
        .err()
        .ok_or("materialization admitted the unbudgeted destination scan")?;
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert_eq!(
        failure.work.backend_read_operations,
        root_only.backend_read_operations
    );
    assert_eq!(std::fs::read_dir(destination.path())?.count(), 0);
    let failure = generation
        .materialize_path("/missing.txt", &valid, budget, &CancellationToken::new())
        .await
        .err()
        .ok_or("materialization accepted a missing selected path")?;
    assert!(failure.work.backend_read_operations > pinned.work.backend_read_operations);
    assert!(failure.work.object_bytes_read >= pinned.work.object_bytes_read);
    failure.work.verify(budget)?;
    let mut limited = budget;
    limited.backend_read_operations = pinned.work.backend_read_operations;
    let failure = generation
        .materialize_path("/original.txt", &valid, limited, &CancellationToken::new())
        .await
        .err()
        .ok_or("materialization exceeded the original checkout read allowance")?;
    assert!(
        matches!(failure.error, WorkspaceError::Work(_)),
        "{:?}",
        failure.error
    );
    assert_eq!(
        failure.work.backend_read_operations,
        pinned.work.backend_read_operations
    );
    assert_eq!(
        failure.work.object_bytes_read,
        pinned.work.object_bytes_read
    );
    // Destination admission now rejects before any host read or path lookup.
    assert_eq!(*failure.work, pinned.work);
    failure.work.verify(limited)?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = generation
        .materialize(&valid, budget, &cancelled)
        .await
        .err()
        .ok_or("pre-cancelled materialization succeeded")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(workspace.head().await?.id(), generation.id());
    assert!(std::fs::read_dir(destination.path())?.next().is_none());
    Ok(())
}

#[tokio::test]
async fn generation_pinning_retains_spent_failure_work_and_exact_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("measured-pinning").await?;
    workspace
        .write_text("/original.txt", "original generation")
        .await?;
    let original = workspace.head().await?.id();
    workspace.write_text("/later.txt", "later head").await?;
    let current = workspace.head().await?.id();
    assert_ne!(original, current);
    let budget = finite_budget()?;
    for select_head in [false, true] {
        let read = |allowance, token: CancellationToken| {
            let workspace = workspace.clone();
            async move {
                if select_head {
                    workspace.head_measured(allowance, &token).await
                } else {
                    workspace
                        .generation_measured(original, allowance, &token)
                        .await
                }
            }
        };
        let pinned = read(budget, CancellationToken::new()).await?;
        pinned.work.verify(budget)?;
        assert_eq!(
            pinned.value.id(),
            if select_head { current } else { original }
        );
        assert!(pinned.work.backend_read_operations > 0);
        // Both selected roots have a parent. Reading their authenticated bytes
        // precedes the bounded allocation/copy needed to decode that parent.
        assert!(pinned.work.bytes_copied > pinned.work.object_bytes_read);
        let mut partial = budget;
        partial.bytes_copied = pinned.work.bytes_copied - 1;
        let failure = read(partial, CancellationToken::new())
            .await
            .err()
            .ok_or("generation pin exceeded the parent decoding allowance")?;
        assert!(matches!(failure.error, WorkspaceError::Work(_)));
        assert!(failure.work.backend_read_operations > 0);
        assert!(failure.work.object_bytes_read > 0);
        failure.work.verify(partial)?;
        let mut denied = budget;
        denied.backend_read_operations = 0;
        let failure = read(denied, CancellationToken::new())
            .await
            .err()
            .ok_or("generation pin admitted zero reads")?;
        assert!(matches!(failure.error, WorkspaceError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::default());
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let failure = read(budget, cancelled)
            .await
            .err()
            .ok_or("pre-cancelled generation pin succeeded")?;
        assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    assert_eq!(workspace.head().await?.id(), current);
    Ok(())
}

#[tokio::test]
async fn named_creation_retains_finite_success_and_spent_exhaustion()
-> Result<(), Box<dyn std::error::Error>> {
    let budget = finite_budget()?;
    assert!(budget.has_finite_allowances());
    let fs = Fs::memory();
    let created = fs
        .create_workspace_measured("measured-create", budget, &CancellationToken::new())
        .await?;
    created.work.verify(budget)?;
    assert!(created.work.authority_records_appended > 0);
    assert!(created.work.authority_records_read > 0);
    assert!(created.work.object_bytes_written > 0);
    assert_eq!(
        fs.open_workspace("measured-create").await?.id(),
        created.value.id()
    );
    assert_eq!(
        fs.create_workspace("measured-create").await?.id(),
        created.value.id()
    );

    let mut limited = budget;
    limited.authority_records_read = 0;
    let failed = Fs::memory()
        .create_workspace_measured("spent-create", limited, &CancellationToken::new())
        .await;
    let failure = failed
        .err()
        .ok_or("zero authority read budget admitted creation")?;
    assert!(
        matches!(failure.error, WorkspaceError::Work(_)),
        "{:?}",
        failure.error
    );
    assert!(failure.work.object_bytes_written > 0);
    failure.work.verify(limited)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_creation_spends_nothing_and_does_not_publish()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed = fs
        .create_workspace_measured("cancelled-create", finite_budget()?, &cancelled)
        .await;
    let failure = failed.err().ok_or("pre-cancelled creation succeeded")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    assert!(fs.open_workspace("cancelled-create").await.is_err());
    Ok(())
}

#[tokio::test]
async fn original_creation_retry_never_adopts_a_later_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let operation = OperationId::new();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let budget = finite_budget()?;
    let cancellation = CancellationToken::new();
    let created = fs
        .create_workspace_with_operation_measured(
            "original-create",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?;
    created.work.verify(budget)?;
    let initial = created.value.id();
    let workspace = created.value.workspace();
    let original = workspace
        .operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()))
        .await?
        .ok_or("original creation operation was not retained")?;
    assert_eq!(original.id(), initial);
    workspace
        .write_text("/later.txt", "later publication")
        .await?;
    assert_ne!(workspace.head().await?.id(), initial);
    let retried = fs
        .create_workspace_with_operation_measured(
            "original-create",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?;
    retried.work.verify(budget)?;
    assert_eq!(retried.value.id(), initial);
    assert_eq!(retried.value.workspace_id(), workspace.id());
    let head_before_rejection = workspace.head().await?.id();
    let mut changed_config = config;
    changed_config.hard_links = !config.hard_links;
    for (requested_config, requested_operation) in [
        (config, OperationId::new()),
        (changed_config, operation),
    ] {
        let rejected = fs
            .create_workspace_with_operation_measured(
                "original-create",
                requested_config,
                requested_operation,
                budget,
                &cancellation,
            )
            .await
            .err()
            .ok_or("foreign operation or reused operation/config adopted creation")?;
        assert!(matches!(&rejected.error, WorkspaceError::Engine(message)
            if message == "volume creation was rejected by authority state"));
        assert!(rejected.work.authority_records_appended > 0);
        rejected.work.verify(budget)?;
        assert_eq!(workspace.head().await?.id(), head_before_rejection);
        assert_eq!(
            workspace
                .operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()))
                .await?
                .ok_or("rejection rewrote the original operation")?
                .id(),
            initial
        );
    }
    Ok(())
}

#[tokio::test]
async fn original_creation_recovers_after_post_commit_receipt_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let budget = finite_budget()?;
    let cancellation = CancellationToken::new();
    let baseline = Fs::memory()
        .create_workspace_with_operation_measured(
            "receipt-create",
            config,
            OperationId::new(),
            budget,
            &cancellation,
        )
        .await?;
    let mut limited = budget;
    limited.authority_records_read = baseline
        .work
        .authority_records_read
        .checked_sub(1)
        .ok_or("creation authenticated no authority records")?;
    let fs = Fs::memory();
    let operation = OperationId::new();
    let failure = fs
        .create_workspace_with_operation_measured(
            "receipt-create",
            config,
            operation,
            limited,
            &cancellation,
        )
        .await
        .err()
        .ok_or("insufficient response budget succeeded")?;
    assert!(
        matches!(failure.error, WorkspaceError::Work(_)),
        "{:?}",
        failure.error
    );
    failure.work.verify(limited)?;
    assert!(failure.work.authority_records_appended > 0);
    let workspace = fs.open_workspace("receipt-create").await?;
    let original = workspace
        .operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()))
        .await?
        .ok_or("failed response did not follow the original committed creation")?;
    let recovered = fs
        .create_workspace_with_operation_measured(
            "receipt-create",
            config,
            operation,
            budget,
            &cancellation,
        )
        .await?;
    recovered.work.verify(budget)?;
    assert_eq!(recovered.value.id(), original.id());
    assert_eq!(recovered.value.workspace_id(), original.workspace_id());
    Ok(())
}

#[tokio::test]
async fn named_open_retains_cumulative_work_and_original_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let created = fs.create_workspace("measured-open").await?;
    let budget = finite_budget()?;
    let opened = fs
        .open_workspace_measured("measured-open", budget, &CancellationToken::new())
        .await?;
    assert_eq!(opened.value.id(), created.id());
    opened.work.verify(budget)?;
    assert!(opened.work.authority_records_read > 1);
    assert!(opened.work.backend_read_operations > 0);
    let mut exhausted = budget;
    exhausted.authority_records_read = opened.work.authority_records_read - 1;
    let failure = fs
        .open_workspace_measured("measured-open", exhausted, &CancellationToken::new())
        .await
        .err()
        .ok_or("opening exceeded cumulative authority budget")?;
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert!(failure.work.authority_records_read > 0);
    failure.work.verify(exhausted)?;
    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = fs
        .open_workspace_measured("measured-open", denied, &CancellationToken::new())
        .await
        .err()
        .ok_or("zero backend-read budget admitted open")?;
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = fs
        .open_workspace_measured("measured-open", budget, &cancelled)
        .await
        .err()
        .ok_or("pre-cancelled open succeeded")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(fs.open_workspace("measured-open").await?.id(), created.id());
    Ok(())
}

#[tokio::test]
async fn original_operation_recovery_is_bounded_and_never_adopts_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let operation = OperationId::new();
    let budget = finite_budget()?;
    let created = fs
        .create_workspace_with_operation_measured(
            "measured-recovery",
            VolumeConfig::portable(Lifecycle::Ephemeral),
            operation,
            budget,
            &CancellationToken::new(),
        )
        .await?;
    let workspace = created.value.workspace();
    workspace
        .write_text("/later.txt", "later generation")
        .await?;
    let head = workspace.head().await?.id();
    assert_ne!(head, created.value.id());
    let key = IdempotencyKey::from_bytes(operation.into_bytes());
    let recovered = workspace
        .operation_generation_measured(key, budget, &CancellationToken::new())
        .await?;
    recovered.work.verify(budget)?;
    assert!(recovered.work.backend_read_operations > 0);
    assert_eq!(
        recovered
            .value
            .ok_or("original operation disappeared")?
            .id(),
        created.value.id()
    );
    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = workspace
        .operation_generation_measured(key, denied, &CancellationToken::new())
        .await
        .err()
        .ok_or("recovery admitted zero backend-read budget")?;
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = workspace
        .operation_generation_measured(key, budget, &cancelled)
        .await
        .err()
        .ok_or("pre-cancelled recovery succeeded")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let absent = workspace
        .operation_generation_measured(
            IdempotencyKey::from_bytes(OperationId::new().into_bytes()),
            budget,
            &CancellationToken::new(),
        )
        .await?;
    assert!(absent.value.is_none());
    absent.work.verify(budget)?;
    assert_eq!(workspace.head().await?.id(), head);
    assert_eq!(
        workspace
            .operation_generation(key)
            .await?
            .ok_or("convenience lost original")?
            .id(),
        created.value.id()
    );
    Ok(())
}

#[cfg(all(feature = "distributed", feature = "native-watch", not(target_arch = "wasm32")))]
mod source_bound_lost_ack {
    use super::*;
    use acyclic_fs::demand::{DemandSource, SourceReference, native::NativeDemandSource};
    use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
    use acyclic_fs::{
        EmbeddedCapabilities, LazyWorkspace, LazyWorkspaceStore, LogicalObjectStore,
        MemoryLazyWorkspaceStore, StreamAuthorityStore,
    };
    use acyclic_stream::{
        AppendRequest, ChildrenRequest, CommitRequest, MemoryStream, ReadRequest, StreamError,
        StreamPath, StreamProvider,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // Only the acknowledgement is lost: all storage and idempotency decisions
    // remain those of the exact public Stream provider used by Fs::memory().
    struct LostCreationAck {
        inner: Arc<MemoryStream>,
        armed: AtomicBool,
        committed: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl StreamProvider for LostCreationAck {
        async fn inspect_idempotency(
            &self, key: acyclic_stream::IdempotencyKey,
        ) -> Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
            self.inner.inspect_idempotency(key).await
        }

        async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
            self.inner.tail(path).await
        }

        async fn bounds(&self, path: StreamPath) -> Result<acyclic_stream::StreamBounds, StreamError> {
            self.inner.bounds(path).await
        }

        async fn append(&self, request: AppendRequest)
            -> Result<acyclic_stream::AppendOutcome, StreamError>
        {
            self.inner.append(request).await
        }

        async fn fork(&self, request: acyclic_stream::ForkRequest)
            -> Result<acyclic_stream::ForkReceipt, StreamError>
        {
            self.inner.fork(request).await
        }

        async fn read(&self, request: ReadRequest) -> Result<acyclic_stream::RecordStream, StreamError> {
            self.inner.read(request).await
        }

        async fn follow(&self, path: StreamPath, from: u64)
            -> Result<acyclic_stream::RecordStream, StreamError>
        {
            self.inner.follow(path, from).await
        }

        async fn children(&self, request: ChildrenRequest)
            -> Result<acyclic_stream::ChildStream, StreamError>
        {
            self.inner.children(request).await
        }

        async fn children_page(&self, request: acyclic_stream::ChildrenPageRequest)
            -> Result<acyclic_stream::ChildrenPage, StreamError>
        {
            self.inner.children_page(request).await
        }

        async fn commit(&self, request: CommitRequest)
            -> Result<acyclic_stream::CommitOutcome, StreamError>
        {
            let result = self.inner.commit(request).await?;
            if matches!(&result, acyclic_stream::CommitOutcome::Committed(_)) {
                self.committed.fetch_add(1, Ordering::SeqCst);
                if self.armed.swap(false, Ordering::SeqCst) {
                    return Err(StreamError::Unavailable);
                }
            }
            Ok(result)
        }

        async fn commit_before(&self, request: CommitRequest, deadline_unix_millis: u64)
            -> Result<acyclic_stream::CommitOutcome, StreamError>
        {
            self.inner.commit_before(request, deadline_unix_millis).await
        }

        async fn read_commit(&self, id: acyclic_stream::CommitId)
            -> Result<acyclic_stream::CommittedEnvelope, StreamError>
        {
            self.inner.read_commit(id).await
        }
    }

    #[tokio::test]
    async fn lost_stream_creation_ack_reopens_same_source_bound_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let budget = finite_budget()?;
        let config = VolumeConfig::portable(Lifecycle::Ephemeral);
        let operation = OperationId::new();
        let cancellation = CancellationToken::new();
        // Deny the first post-commit authority read on a separate, identical
        // real producer. Its receipt is exactly the pre-ack creation work.
        let mut response_denied = budget;
        response_denied.authority_records_read = 0;
        let reference_failure = Fs::memory()
            .create_workspace_with_operation_measured(
                "stream-lost-ack", config, operation, response_denied, &cancellation,
            ).await.err().ok_or("post-commit authority denial unexpectedly succeeded")?;
        assert!(matches!(reference_failure.error, WorkspaceError::Work(_)));
        assert!(reference_failure.work.authority_records_appended > 0);
        assert_eq!(reference_failure.work.authority_records_read, 0);
        reference_failure.work.verify(response_denied)?;

        let stream = Arc::new(MemoryStream::default());
        let fault = Arc::new(LostCreationAck {
            inner: stream.clone(),
            armed: AtomicBool::new(true),
            committed: AtomicUsize::new(0),
        });
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let objects = Arc::new(objects);
        let fs = Fs::new(
            StreamAuthorityStore::new(fault.clone()),
            LogicalObjectStore::new(objects.clone(), bucket.clone()),
            EmbeddedCapabilities::MEMORY,
        );
        let directory = tempfile::tempdir()?;
        std::fs::create_dir_all(directory.path().join("unresolved/deeper"))?;
        std::fs::write(directory.path().join("unresolved/deeper/file"), b"unread source")?;
        let reference = SourceReference { identity: [61; 16], epoch: 4 };
        let source = Arc::new(NativeDemandSource::open_with_reference_measured(
            directory.path().to_path_buf(), FilesystemProfile::Portable, VolumeLimits::default(),
            reference, None, budget, &cancellation,
        ).await?.value);
        let store = MemoryLazyWorkspaceStore::default();
        let failure = LazyWorkspace::attach_with_operation_measured(
            &fs, "stream-lost-ack", source.clone(), store.clone(), config, operation,
            budget, &cancellation,
        ).await.err().ok_or("lost real Stream acknowledgement succeeded")?;
        assert!(matches!(&failure.error, acyclic_fs::LazyWorkspaceError::Workspace(message)
            if message.contains("stream unavailable")));
        assert_eq!(*failure.work, *reference_failure.work);
        failure.work.verify(budget)?;
        assert_eq!(fault.committed.load(Ordering::SeqCst), 1);
        let workspace = fs.open_workspace("stream-lost-ack").await?;
        let initial = workspace.operation_generation(
            IdempotencyKey::from_bytes(operation.into_bytes()),
        ).await?.ok_or("lost acknowledgement did not retain original allocation")?;
        let initial_id = initial.id();
        let workspace_id = initial.workspace_id();
        assert!(store.load_lazy_workspace(workspace_id).await?.is_none());
        workspace.write_text("/later.txt", "head after lost acknowledgement").await?;
        let head = workspace.head().await?.id();
        assert_ne!(head, initial_id);
        drop(initial);
        drop(workspace);
        drop(fs);
        drop(source);
        drop(fault);

        // Reconstruct both production adapters, without the fault wrapper.
        let reopened = Fs::from_memory_providers(stream, objects, bucket);
        let source = Arc::new(NativeDemandSource::open_with_reference_measured(
            directory.path().to_path_buf(), FilesystemProfile::Portable, VolumeLimits::default(),
            reference, None, budget, &cancellation,
        ).await?.value);
        assert_eq!(source.reference(), reference);
        let recovered = LazyWorkspace::attach_with_operation_measured(
            &reopened, "stream-lost-ack", source, store.clone(), config, operation,
            budget, &cancellation,
        ).await?;
        recovered.work.verify(budget)?;
        assert_eq!(recovered.work.source_bytes_read, 0);
        assert_eq!(recovered.work.source_entries_visited, 0);
        let (lazy, original) = recovered.value;
        assert_eq!(original.id(), initial_id);
        assert_eq!(original.workspace_id(), workspace_id);
        assert_eq!(lazy.workspace().head().await?.id(), head);
        assert_eq!(lazy.snapshot().await?.source, reference);
        assert_eq!(
            store.load_lazy_workspace(workspace_id).await?.ok_or("binding absent after restart")?.source,
            reference
        );
        let mut changed_config = config;
        changed_config.hard_links = !config.hard_links;
        for (requested_config, requested_operation) in [
            (config, OperationId::new()),
            (changed_config, operation),
        ] {
            let failure = reopened.create_workspace_with_operation_measured(
                "stream-lost-ack", requested_config, requested_operation, budget, &cancellation,
            ).await.err().ok_or("restart accepted a foreign or reconfigured original creation")?;
            assert!(matches!(&failure.error, WorkspaceError::Engine(message)
                if message == "volume creation was rejected by authority state"));
            failure.work.verify(budget)?;
            assert_eq!(lazy.workspace().head().await?.id(), head);
            let retained = lazy.workspace().operation_generation_measured(
                IdempotencyKey::from_bytes(operation.into_bytes()), budget, &cancellation,
            ).await?;
            retained.work.verify(budget)?;
            assert_eq!(retained.value.ok_or("restart lost original operation")?.id(), initial_id);
        }
        Ok(())
    }
}
