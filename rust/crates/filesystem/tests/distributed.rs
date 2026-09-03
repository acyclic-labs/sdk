#![cfg(feature = "distributed")]

//! Black-box conformance for canonical distributed adapters over public memory providers.

use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityId, CancellationToken,
    CreateAuthorityOutcome, Digest, Epoch, FenceOutcome, Head, ObjectId, ObjectKind, OperationId,
    ProposedCommit, ReplayLimit, Sequence, WorkBudget, object_digest,
};
use acyclic_fs::{ProviderObjectStore, StreamAuthorityStore};
use acyclic_objects::{MemoryObjects, ObjectsProvider};
use acyclic_stream::MemoryStream;
use bytes::Bytes;
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
