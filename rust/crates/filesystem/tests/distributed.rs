//! Black-box conformance for canonical distributed adapters over public memory providers.
#![cfg(feature = "distributed")]

use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityId, AuthorityStoreError,
    CancellationToken, CreateAuthorityOutcome, Digest, DistributedFs, EmbeddedCapabilities, Epoch,
    FenceOutcome, ForkOptions, Fs, GenerationFork, GenerationForkSource, GenerationId, Head,
    IdempotencyKey, ObjectId, ObjectKind, OperationFailure, OperationId, ProposedCommit,
    ReplayLimit, Sequence, WorkBudget, WorkspaceDelete, object_digest,
};
use acyclic_fs::{LogicalObjectStore, StreamAuthorityStore};
use acyclic_objects::v2::{MemoryObjects, ObjectsProvider, wire};
use acyclic_stream::{
    AppendRequest, ChildrenRequest, MemoryStream, ReadRequest, StreamError, StreamPath,
    StreamProvider,
};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

struct PausingStream {
    inner: MemoryStream,
    pause_record_commit: AtomicBool,
    entered: Notify,
    resume: Notify,
    record_path: StreamPath,
}

#[async_trait::async_trait]
impl StreamProvider for PausingStream {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }

    async fn bounds(&self, path: StreamPath) -> Result<acyclic_stream::StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }

    async fn append(
        &self,
        request: AppendRequest,
    ) -> Result<acyclic_stream::AppendOutcome, StreamError> {
        self.inner.append(request).await
    }

    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> Result<acyclic_stream::ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.read(request).await
    }

    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
    ) -> Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }

    async fn children(
        &self,
        request: ChildrenRequest,
    ) -> Result<acyclic_stream::ChildStream, StreamError> {
        self.inner.children(request).await
    }

    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> Result<acyclic_stream::ChildrenPage, StreamError> {
        self.inner.children_page(request).await
    }

    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> Result<acyclic_stream::CommitOutcome, StreamError> {
        if request.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                acyclic_stream::CommitMutation::Append { path, .. } if path == &self.record_path
            )
        }) && self.pause_record_commit.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        self.inner.commit(request).await
    }

    async fn commit_before(
        &self,
        request: acyclic_stream::CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<acyclic_stream::CommitOutcome, StreamError> {
        self.inner
            .commit_before(request, deadline_unix_millis)
            .await
    }

    async fn read_commit(
        &self,
        commit_id: acyclic_stream::CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "linear conformance walkthrough over one authority's lifecycle; each step is a \
              distinct sequential assertion building on the prior step's state, and splitting \
              it apart would only scatter the narrative across functions without clarifying it"
)]
async fn authority_lifecycle_is_native_stream_backed_and_exactly_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(Arc::clone(&provider));
    let authority_id = AuthorityId::from_bytes([7; 16]);
    let cancellation = CancellationToken::new();
    assert_eq!(
        store
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        CreateAuthorityOutcome::Created(Head::genesis(Epoch::GENESIS))
    );
    assert_eq!(store.authorities(8).await?, vec![authority_id]);
    assert_eq!(
        provider
            .tail(StreamPath::new(format!(
                "fs/authorities/{}",
                hex::encode(authority_id.into_bytes())
            ))?)
            .await?,
        1
    );

    let proposal = ProposedCommit {
        operation_id: OperationId::from_bytes([8; 16]),
        fingerprint: Digest::from_bytes([9; 32]),
        payload: Bytes::from_static(b"generation"),
    };
    let committed = store
        .compare_and_append(
            authority_id,
            Epoch::GENESIS,
            Head::genesis(Epoch::GENESIS),
            proposal.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let AppendOutcome::Committed(commit) = committed else {
        return Err("first operation was not committed".into());
    };
    assert_eq!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                proposal.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::AlreadyCommitted(commit.clone())
    );
    let second_authority = AuthorityId::from_bytes([17; 16]);
    store
        .create_authority(
            second_authority,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert!(matches!(
        store
            .compare_and_append(
                second_authority,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                proposal.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::Committed(_)
    ));
    let conflicting = ProposedCommit {
        fingerprint: Digest::from_bytes([10; 32]),
        ..proposal.clone()
    };
    assert!(matches!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                conflicting.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::IdempotencyConflict { .. }
    ));
    let authority_path = StreamPath::new(format!(
        "fs/authorities/{}",
        hex::encode(authority_id.into_bytes())
    ))?;
    let children = provider
        .children(ChildrenRequest {
            parent: Some(authority_path),
            limit: 16,
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        children
            .iter()
            .all(|child| !child.path.as_str().contains("/operations")),
        "native Stream idempotency must replace shadow operation paths"
    );
    assert_eq!(
        store
            .replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 4,
                    payload_bytes: 1024,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        vec![commit.clone()]
    );
    let head = Head {
        epoch: commit.epoch,
        sequence: commit.sequence,
        digest: commit.digest,
    };
    assert!(matches!(
        store
            .fence(authority_id, head, WorkBudget::UNBOUNDED, &cancellation,)
            .await?
            .value,
        FenceOutcome::Advanced(_)
    ));
    assert_eq!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                proposal.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::AlreadyCommitted(commit)
    );
    assert!(matches!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                conflicting,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::IdempotencyConflict { .. }
    ));
    assert!(matches!(
        store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                ProposedCommit {
                    operation_id: OperationId::from_bytes([18; 16]),
                    ..proposal
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value,
        AppendOutcome::Fenced { .. }
    ));
    store
        .create_authority(
            AuthorityId::from_bytes([11; 16]),
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert!(store.authorities(1).await.is_err());
    Ok(())
}

#[tokio::test]
async fn deleting_workspace_preserves_committed_stream_history()
-> Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let objects = Arc::new(MemoryObjects::new(Default::default())?);
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "delete-history".to_owned(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let authority_store = StreamAuthorityStore::new(Arc::clone(&streams));
    let fs = Fs::new(
        authority_store.clone(),
        LogicalObjectStore::new(objects, bucket),
        EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs.create_workspace("history").await?;
    workspace.write_text("/kept.txt", "committed").await?;
    let authority = AuthorityId::from_bytes(workspace.id().into_bytes());
    let records = StreamPath::new(format!(
        "fs/authorities/{}/records",
        hex::encode(authority.into_bytes())
    ))?;
    let before = streams.tail(records.clone()).await?;
    let key = IdempotencyKey::new();
    assert_eq!(workspace.delete(key).await?, WorkspaceDelete::Deleted);
    assert_eq!(
        workspace.delete(key).await?,
        WorkspaceDelete::AlreadyDeleted
    );
    assert!(fs.open_workspace("history").await.is_err());
    assert!(!authority_store.authorities(16).await?.contains(&authority));

    let after = streams.tail(records.clone()).await?;
    assert!(
        after > before,
        "workspace deletion must append its tombstone"
    );
    let retained = streams
        .read(ReadRequest {
            path: records,
            from: 0,
            limit: u32::try_from(after)?,
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(retained.len(), usize::try_from(after)?);
    Ok(())
}

#[tokio::test]
async fn retirement_fences_an_inflight_authority_append() -> Result<(), Box<dyn std::error::Error>>
{
    let authority = AuthorityId::from_bytes([39; 16]);
    let records = StreamPath::new(format!(
        "fs/authorities/{}/records",
        hex::encode(authority.into_bytes())
    ))?;
    let provider = Arc::new(PausingStream {
        inner: MemoryStream::default(),
        pause_record_commit: AtomicBool::new(false),
        entered: Notify::new(),
        resume: Notify::new(),
        record_path: records.clone(),
    });
    let store = StreamAuthorityStore::new(Arc::clone(&provider));
    let cancellation = CancellationToken::new();
    store
        .create_authority(
            authority,
            Epoch::GENESIS,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let before = provider.tail(records.clone()).await?;
    provider.pause_record_commit.store(true, Ordering::SeqCst);
    let writer_store = StreamAuthorityStore::new(Arc::clone(&provider));
    let writer = tokio::spawn(async move {
        writer_store
            .compare_and_append(
                authority,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                ProposedCommit {
                    operation_id: OperationId::from_bytes([40; 16]),
                    fingerprint: Digest::from_bytes([41; 32]),
                    payload: Bytes::from_static(b"inflight"),
                },
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
    });
    provider.entered.notified().await;
    store
        .retire_authority(authority, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    provider.resume.notify_one();
    assert!(matches!(
        writer.await?,
        Err(OperationFailure {
            error: AuthorityStoreError::Retired,
            ..
        })
    ));
    assert_eq!(provider.tail(records).await?, before);
    assert!(!store.authorities(4).await?.contains(&authority));
    Ok(())
}

#[tokio::test]
async fn immutable_objects_use_the_exact_public_objects_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryObjects::new(Default::default())?);
    let bucket = provider
        .create_bucket(wire::CreateBucketRequest {
            name: "adapter".to_owned(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let store = LogicalObjectStore::new(provider, bucket);
    let bytes = Bytes::from_static(b"authenticated");
    let object_id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &bytes),
    };
    let cancellation = CancellationToken::new();
    store
        .put(
            object_id,
            bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let read = store
        .read(object_id, 64, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(read.value.bytes, bytes);
    assert!(
        store
            .contains(object_id, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
    );
    let wrong = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([11; 32]),
    };
    assert!(
        store
            .put(
                wrong,
                Bytes::from_static(b"not-that-digest"),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn workspace_fork_uses_one_native_stream_prefix_and_independent_suffixes()
-> Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let objects = Arc::new(MemoryObjects::new(Default::default())?);
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "filesystem".to_owned(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let fs = Fs::new(
        StreamAuthorityStore::new(Arc::clone(&streams)),
        LogicalObjectStore::new(objects, bucket),
        EmbeddedCapabilities::MEMORY,
    );
    let source = fs.create_workspace("source").await?;
    source.write_text("/shared", "base").await?;
    let selected = source.head().await?;
    let child = source
        .fork(
            "child",
            ForkOptions::from_generation(selected.clone(), IdempotencyKey::from_bytes([0x51; 16])),
        )
        .await?;

    let lineage = |workspace: acyclic_fs::WorkspaceId| {
        StreamPath::new(format!(
            "fs/authorities/{}/lineage",
            hex::encode(workspace.into_bytes())
        ))
    };
    let source_lineage = lineage(source.id())?;
    let child_lineage = lineage(child.id())?;
    let inherited = streams.tail(source_lineage.clone()).await?;
    let mut source_records = streams
        .read(ReadRequest {
            path: source_lineage,
            from: 0,
            limit: u32::try_from(inherited)?,
        })
        .await?;
    let mut child_records = streams
        .read(ReadRequest {
            path: child_lineage,
            from: 0,
            limit: u32::try_from(inherited)?,
        })
        .await?;
    while let Some(source_record) = source_records.next().await {
        let source_record = source_record?;
        let child_record = child_records
            .next()
            .await
            .ok_or("native fork omitted an inherited record")??;
        assert_eq!(child_record, source_record);
    }
    assert!(child_records.next().await.is_none());
    assert_eq!(
        child.read("/shared", 16).await?,
        Bytes::from_static(b"base")
    );

    source.write_text("/source-only", "source").await?;
    let retry = source
        .fork(
            "child",
            ForkOptions::from_generation(selected, IdempotencyKey::from_bytes([0x51; 16])),
        )
        .await?;
    assert_eq!(retry.id(), child.id());
    child.write_text("/child-only", "child").await?;
    assert!(child.read("/source-only", 16).await.is_err());
    assert!(source.read("/child-only", 16).await.is_err());
    Ok(())
}

#[tokio::test]
async fn distributed_facade_leases_authorize_workspace_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let objects = Arc::new(MemoryObjects::new(Default::default())?);
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "distributed-leases".to_owned(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let fs = Fs::new(
        StreamAuthorityStore::new(streams),
        LogicalObjectStore::new(objects, bucket),
        EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs.create_workspace("leased").await?;
    let parent = workspace.head().await?;
    let distributed = DistributedFs::new(fs, ());
    let operations = distributed.operations();
    let lease = operations
        .begin(workspace.id(), parent.id(), "tool", 1, u64::MAX)
        .await?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::from_bytes([0x81; 16]))
        .await?;
    transaction
        .write_text("/published", "through lease")
        .await?;
    assert!(matches!(
        transaction
            .commit_with_permit(lease.publication_permit())
            .await?,
        acyclic_fs::TransactionCommit::Committed(_)
    ));
    assert_eq!(
        workspace.read("/published", 32).await?,
        Bytes::from_static(b"through lease")
    );
    Ok(())
}

#[tokio::test]
async fn generation_fork_rejects_an_authority_without_lineage_locators()
-> Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(streams);
    let source = AuthorityId::from_bytes([0x61; 16]);
    let destination = AuthorityId::from_bytes([0x62; 16]);
    let cancellation = CancellationToken::new();
    store
        .create_authority(source, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let Err(error) = store
        .fork_generation_authority(
            GenerationForkSource {
                authority: source,
                generation: GenerationId::new(Digest::from_bytes([0x63; 32])),
                lineage: GenerationFork::PublishedPrefix,
            },
            destination,
            OperationId::from_bytes([0x64; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
    else {
        return Err("missing lineage locator unexpectedly forked".into());
    };
    assert!(matches!(error.error, AuthorityStoreError::Rejected(_)));
    assert!(matches!(
        store
            .head(destination, WorkBudget::UNBOUNDED, &cancellation)
            .await,
        Err(OperationFailure {
            error: AuthorityStoreError::Missing,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn failed_atomic_generation_fork_leaves_no_destination_lineage()
-> Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let objects = Arc::new(MemoryObjects::new(Default::default())?);
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: "atomic-fork".to_owned(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let store = StreamAuthorityStore::new(Arc::clone(&streams));
    let fs = Fs::new(
        store.clone(),
        LogicalObjectStore::new(objects, bucket),
        EmbeddedCapabilities::MEMORY,
    );
    let source = fs.create_workspace("atomic-source").await?;
    source.write_text("/shared", "base").await?;
    let selected = source.head().await?;
    let destination_id = fs.workspace_id("blocked-destination")?;
    let destination_authority = AuthorityId::from_bytes(destination_id.into_bytes());
    let prefix = format!(
        "fs/authorities/{}",
        hex::encode(destination_authority.into_bytes())
    );
    streams
        .append(AppendRequest {
            path: StreamPath::new(format!("{prefix}/records"))?,
            records: vec![Bytes::from_static(b"conflict")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let source_authority = AuthorityId::from_bytes(source.id().into_bytes());
    assert!(
        store
            .fork_generation_authority(
                GenerationForkSource {
                    authority: source_authority,
                    generation: selected.id(),
                    lineage: GenerationFork::PublishedPrefix,
                },
                destination_authority,
                OperationId::from_bytes([0x65; 16]),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .is_err()
    );
    assert_eq!(
        streams
            .tail(StreamPath::new(format!("{prefix}/lineage"))?)
            .await,
        Err(StreamError::NotFound)
    );
    Ok(())
}
