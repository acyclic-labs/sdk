//! Black-box conformance for canonical distributed adapters over public memory providers.
#![cfg(feature = "distributed")]

use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityId, CancellationToken,
    CreateAuthorityOutcome, Digest, EmbeddedCapabilities, Epoch, FenceOutcome, ForkOptions, Fs,
    GenerationId, Head, IdempotencyKey, ObjectId, ObjectKind, OperationId, ProposedCommit,
    ReplayLimit, Sequence, WorkBudget, object_digest,
};
use acyclic_fs::{ProviderObjectStore, StreamAuthorityStore};
use acyclic_objects::{MemoryObjects, ObjectsProvider};
use acyclic_stream::{
    AppendRequest, ChildrenRequest, MemoryStream, ReadRequest, StreamError, StreamPath,
    StreamProvider,
};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

#[tokio::test]
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

#[tokio::test]
async fn generation_fork_falls_back_for_an_authority_without_lineage_locators()
-> Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(streams);
    let source = AuthorityId::from_bytes([0x61; 16]);
    let destination = AuthorityId::from_bytes([0x62; 16]);
    let cancellation = CancellationToken::new();
    store
        .create_authority(source, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let forked = store
        .fork_generation_authority(
            source,
            GenerationId::new(Digest::from_bytes([0x63; 32])),
            destination,
            OperationId::from_bytes([0x64; 16]),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(
        forked.value,
        CreateAuthorityOutcome::Created(Head::genesis(Epoch::GENESIS))
    );
    assert_eq!(
        store
            .head(destination, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value,
        Head::genesis(Epoch::GENESIS)
    );
    Ok(())
}

#[tokio::test]
async fn failed_atomic_generation_fork_leaves_no_destination_lineage()
-> Result<(), Box<dyn std::error::Error>> {
    let streams = Arc::new(MemoryStream::default());
    let objects = Arc::new(MemoryObjects::default());
    let bucket = objects
        .create_bucket(
            "atomic-fork".to_owned(),
            Some("create-atomic-fork".to_owned()),
        )
        .await?
        .bucket
        .ok_or("bucket identity missing")?;
    let store = StreamAuthorityStore::new(Arc::clone(&streams));
    let fs = Fs::new(
        store.clone(),
        ProviderObjectStore::new(objects, bucket),
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
                source_authority,
                selected.id(),
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
