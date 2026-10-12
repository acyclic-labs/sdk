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

#[tokio::test]
async fn generation_pinning_retains_spent_failure_work_and_exact_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs.create_workspace("measured-pinning", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    workspace
        .write_text("/original.txt", "original generation")
        .await?;
    let original = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    workspace.write_text("/later.txt", "later head").await?;
    let current = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    assert_ne!(original, current);
    let budget = finite_budget()?;
    for select_head in [false, true] {
        let read = |allowance, token: CancellationToken| {
            let workspace = workspace.clone();
            async move {
                if select_head {
                    workspace.head(allowance, &token).await
                } else {
                    workspace
                        .generation(original, allowance, &token)
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
    assert_eq!(workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), current);
    Ok(())
}

#[tokio::test]
async fn named_creation_retains_finite_success_and_spent_exhaustion()
-> Result<(), Box<dyn std::error::Error>> {
    let budget = finite_budget()?;
    assert!(budget.has_finite_allowances());
    let fs = Fs::memory();
    let created = fs
        .create_workspace("measured-create", budget, &CancellationToken::new())
        .await?;
    created.work.verify(budget)?;
    assert!(created.work.authority_records_appended > 0);
    assert!(created.work.authority_records_read > 0);
    assert!(created.work.object_bytes_written > 0);
    assert_eq!(
        fs.open_workspace("measured-create", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(),
        created.value.id()
    );
    assert_eq!(
        fs.create_workspace("measured-create", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(),
        created.value.id()
    );

    let mut limited = budget;
    limited.authority_records_read = 0;
    let failed = Fs::memory()
        .create_workspace("spent-create", limited, &CancellationToken::new())
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
        .create_workspace("cancelled-create", finite_budget()?, &cancelled)
        .await;
    let failure = failed.err().ok_or("pre-cancelled creation succeeded")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    assert!(fs.open_workspace("cancelled-create", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err());
    Ok(())
}

#[tokio::test]
async fn named_open_retains_cumulative_work_and_original_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let created = fs.create_workspace("measured-open", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let budget = finite_budget()?;
    let opened = fs
        .open_workspace("measured-open", budget, &CancellationToken::new())
        .await?;
    assert_eq!(opened.value.id(), created.id());
    opened.work.verify(budget)?;
    assert!(opened.work.authority_records_read > 1);
    assert!(opened.work.backend_read_operations > 0);
    let mut exhausted = budget;
    exhausted.authority_records_read = opened.work.authority_records_read - 1;
    let failure = fs
        .open_workspace("measured-open", exhausted, &CancellationToken::new())
        .await
        .err()
        .ok_or("opening exceeded cumulative authority budget")?;
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert!(failure.work.authority_records_read > 0);
    failure.work.verify(exhausted)?;
    let mut denied = budget;
    denied.backend_read_operations = 0;
    let failure = fs
        .open_workspace("measured-open", denied, &CancellationToken::new())
        .await
        .err()
        .ok_or("zero backend-read budget admitted open")?;
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = fs
        .open_workspace("measured-open", budget, &cancelled)
        .await
        .err()
        .ok_or("pre-cancelled open succeeded")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(fs.open_workspace("measured-open", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), created.id());
    Ok(())
}

#[tokio::test]
async fn original_operation_recovery_is_bounded_and_never_adopts_head()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let operation = OperationId::new();
    let budget = finite_budget()?;
    let created = fs
        .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>("measured-recovery", VolumeConfig::portable(Lifecycle::Ephemeral), operation, budget, &CancellationToken::new(), None)
        .await?;
    let workspace = created.value.workspace();
    workspace
        .write_text("/later.txt", "later generation")
        .await?;
    let head = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
    assert_ne!(head, created.value.id());
    let key = IdempotencyKey::from_bytes(operation.into_bytes());
    let recovered = workspace
        .operation_generation(key, budget, &CancellationToken::new())
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
        .operation_generation(key, denied, &CancellationToken::new())
        .await
        .err()
        .ok_or("recovery admitted zero backend-read budget")?;
    assert!(matches!(failure.error, WorkspaceError::Work(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = workspace
        .operation_generation(key, budget, &cancelled)
        .await
        .err()
        .ok_or("pre-cancelled recovery succeeded")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    let absent = workspace
        .operation_generation(
            IdempotencyKey::from_bytes(OperationId::new().into_bytes()),
            budget,
            &CancellationToken::new(),
        )
        .await?;
    assert!(absent.value.is_none());
    absent.work.verify(budget)?;
    assert_eq!(workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), head);
    assert_eq!(
        workspace
            .operation_generation(key, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .ok_or("convenience lost original")?
            .id(),
        created.value.id()
    );
    Ok(())
}

#[cfg(all(feature = "distributed", feature = "native-watch", not(target_arch = "wasm32")))]
mod source_bound_lost_ack {
    use super::*;
    use acyclic_fs::facade::FsWork;
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
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>("stream-lost-ack", config, operation, response_denied, &cancellation, None).await.err().ok_or("post-commit authority denial unexpectedly succeeded")?;
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
        let source = Arc::new(NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(),
            Some(reference), None, budget, &cancellation,
        ).await?.value);
        let store = MemoryLazyWorkspaceStore::default();
        let mut accounting = budget.accounting(&cancellation);
        let result = LazyWorkspace::attach(
            &fs, "stream-lost-ack", source.clone(), store.clone(), config, operation,
            &mut accounting,
        ).await;
        let failure = budget.finish(result, &accounting).err().ok_or("lost real Stream acknowledgement succeeded")?;
        assert!(matches!(&failure.error, acyclic_fs::LazyWorkspaceError::WorkspaceCause(error)
            if error.to_string().contains("stream unavailable")));
        assert_eq!(*failure.work, *reference_failure.work);
        failure.work.verify(budget)?;
        assert_eq!(fault.committed.load(Ordering::SeqCst), 1);
        let workspace = fs.open_workspace("stream-lost-ack", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let initial = workspace.operation_generation(IdempotencyKey::from_bytes(operation.into_bytes()), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.ok_or("lost acknowledgement did not retain original allocation")?;
        let initial_id = initial.id();
        let workspace_id = initial.workspace_id();
        assert!(store.load_lazy_workspace(workspace_id, &mut WorkBudget::UNBOUNDED.accounting(&cancellation)).await?.is_none());
        workspace.write_text("/later.txt", "head after lost acknowledgement").await?;
        let head = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id();
        assert_ne!(head, initial_id);
        drop(initial);
        drop(workspace);
        drop(fs);
        drop(source);
        drop(fault);

        // Reconstruct both production adapters, without the fault wrapper.
        let reopened = Fs::from_memory_providers(stream, objects, bucket);
        let source = Arc::new(NativeDemandSource::open(
            std::borrow::Cow::Owned(directory.path().to_path_buf()), FilesystemProfile::Portable, VolumeLimits::default(),
            Some(reference), None, budget, &cancellation,
        ).await?.value);
        assert_eq!(source.reference(), reference);
        let mut accounting = budget.accounting(&cancellation);
        let result = LazyWorkspace::attach(
            &reopened, "stream-lost-ack", source, store.clone(), config, operation,
            &mut accounting,
        ).await;
        let recovered = budget.finish(result, &accounting)?;
        recovered.work.verify(budget)?;
        assert_eq!(recovered.work.source_bytes_read, 0);
        assert_eq!(recovered.work.source_entries_visited, 0);
        let (lazy, original) = recovered.value;
        assert_eq!(original.id(), initial_id);
        assert_eq!(original.workspace_id(), workspace_id);
        assert_eq!(lazy.workspace().head(WorkBudget::UNBOUNDED, &cancellation).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), head);
        assert_eq!(lazy.snapshot(&mut WorkBudget::UNBOUNDED.accounting(&cancellation)).await?.source, reference);
        assert_eq!(
            store.load_lazy_workspace(workspace_id, &mut WorkBudget::UNBOUNDED.accounting(&cancellation)).await?.ok_or("binding absent after restart")?.source,
            reference
        );
        let mut changed_config = config;
        changed_config.hard_links = !config.hard_links;
        for (requested_config, requested_operation) in [
            (config, OperationId::new()),
            (changed_config, operation),
        ] {
            let failure = reopened.create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>("stream-lost-ack", requested_config, requested_operation, budget, &cancellation, None).await.err().ok_or("restart accepted a foreign or reconfigured original creation")?;
            assert!(matches!(&failure.error, WorkspaceError::Engine(message)
                if message == "volume creation was rejected by authority state"));
            failure.work.verify(budget)?;
            assert_eq!(lazy.workspace().head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id(), head);
            let retained = lazy.workspace().operation_generation(
                IdempotencyKey::from_bytes(operation.into_bytes()), budget, &cancellation,
            ).await?;
            retained.work.verify(budget)?;
            assert_eq!(retained.value.ok_or("restart lost original operation")?.id(), initial_id);
        }
        Ok(())
    }
}
