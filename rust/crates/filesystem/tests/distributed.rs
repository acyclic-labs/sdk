//! Black-box conformance for canonical distributed adapters over public memory providers.
#![cfg(feature = "distributed")]

use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityId, CancellationToken,
    CreateAuthorityOutcome, Digest, EmbeddedCapabilities, Epoch, FenceOutcome, ForkOptions, Fs,
    Head, IdempotencyKey, ObjectId, ObjectKind, OperationId, ProposedCommit, ReplayLimit, Sequence,
    WorkBudget, object_digest,
};
use acyclic_fs::{ProviderObjectStore, StreamAuthorityStore};
use acyclic_objects::{MemoryObjects, ObjectsProvider};
use acyclic_stream::{MemoryStream, ReadRequest, StreamPath, StreamProvider};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

#[tokio::test]
async fn authority_lifecycle_is_native_stream_backed_and_exactly_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider);
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
    let conflicting = ProposedCommit {
        fingerprint: Digest::from_bytes([10; 32]),
        ..proposal
    };
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
    Ok(())
}

#[tokio::test]
async fn immutable_objects_use_the_exact_public_objects_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryObjects::default());
    let bucket = provider
        .create_bucket("adapter".to_owned(), Some("create-adapter".to_owned()))
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let store = ProviderObjectStore::new(provider, bucket);
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
    let objects = Arc::new(MemoryObjects::default());
    let bucket = objects
        .create_bucket(
            "filesystem".to_owned(),
            Some("create-filesystem".to_owned()),
        )
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let fs = Fs::new(
        StreamAuthorityStore::new(Arc::clone(&streams)),
        ProviderObjectStore::new(objects, bucket),
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
