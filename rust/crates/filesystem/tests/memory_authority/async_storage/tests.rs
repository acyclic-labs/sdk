use acyclic_fs::async_storage::read_many_sequential;
use acyclic_fs::{
    AppendOutcome, AsyncAuthorityStore, AsyncObjectStore, AuthorityStoreError, AuthorityWork,
    CancellationToken, CreateAuthorityOutcome, FenceOutcome, GuardedAppend, Head,
    MemoryAuthorityStore, MemoryObjectStore, ObjectReadRequest, ObjectStoreError, ProposedCommit,
    PublicationPermit, ReplayLimit, object_digest,
};
use acyclic_fs_format::authority::{Epoch, Sequence};
use acyclic_fs_format::{
    identity::{AuthorityId, Digest, OperationId},
    object_reference::{ObjectId, ObjectKind},
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};
use acyclic_stream::OwnedBytes;

#[tokio::test]
async fn memory_stores_expose_the_complete_async_contract() -> Result<(), Box<dyn std::error::Error>>
{
    let cancel = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    let objects = MemoryObjectStore::default();
    let mut ids = Vec::new();
    for bytes in [b"first".as_slice(), b"second".as_slice()] {
        let id = ObjectId {
            storage: None,
            kind: ObjectKind::BlobChunk,
            digest: object_digest(bytes),
        };
        objects
            .put(id, OwnedBytes::copy_from_slice(bytes), &mut work)
            .await?;
        ids.push(id);
    }
    let [first, second] = ids.as_slice() else {
        return Err("expected two object IDs".into());
    };
    assert!(objects.contains(*first, &mut work).await?);
    assert_eq!(
        objects.read(*first, 5, &mut work).await?.bytes.as_ref(),
        b"first"
    );
    let requests = [
        ObjectReadRequest {
            object_id: *second,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: *first,
            maximum_bytes: 5,
        },
    ];
    for values in [
        objects.read_many(&requests, &mut work).await?.0,
        read_many_sequential(&objects, &requests, &mut work)
            .await?
            .0,
    ] {
        let [second, first] = values.as_slice() else {
            return Err("expected two object reads".into());
        };
        assert_eq!(second.bytes.as_ref(), b"second");
        assert_eq!(first.bytes.as_ref(), b"first");
    }
    let authorities = MemoryAuthorityStore::default();
    let authority = AuthorityId::from_bytes([0x41; 16]);
    let created = authorities
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let CreateAuthorityOutcome::Created(head) = created else {
        return Err("authority existed".into());
    };
    assert_eq!(authorities.head(authority, &mut work).await?, head);
    let operation = OperationId::from_bytes([0x42; 16]);
    let appended = authorities
        .compare_and_append(
            GuardedAppend {
                authority_id: authority,
                epoch: Epoch::GENESIS,
                expected: head,
                commit: ProposedCommit {
                    publication: None,
                    operation_id: operation,
                    fingerprint: Digest::from_bytes([0x43; 32]),
                    payload: bytes::Bytes::from_static(b"payload"),
                },
                permit: PublicationPermit::Unrestricted,
            },
            &mut work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    assert!(matches!(appended, AppendOutcome::Committed(_)));
    assert_eq!(
        authorities
            .replay(
                authority,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 7
                },
                &mut work
            )
            .await?
            .0
            .len(),
        1
    );
    assert!(
        authorities
            .find_operation(authority, operation, &mut work)
            .await?
            .is_some()
    );
    let current = authorities.head(authority, &mut work).await?;
    assert!(
        matches!(authorities.fence(authority, current, &mut work).await?, FenceOutcome::Advanced(head) if head.epoch.get() == 2)
    );
    Ok(())
}

#[tokio::test]
async fn adapters_reject_cancellation_and_sequential_read_failures()
-> Result<(), Box<dyn std::error::Error>> {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancelled);
    let authority = MemoryAuthorityStore::default();
    let id = AuthorityId::from_bytes([0x51; 16]);
    let head = Head::genesis(Epoch::GENESIS);
    let operation = OperationId::from_bytes([0x52; 16]);
    let request = GuardedAppend {
        authority_id: id,
        epoch: Epoch::GENESIS,
        expected: head,
        commit: ProposedCommit {
            publication: None,
            operation_id: operation,
            fingerprint: Digest::from_bytes([0x53; 32]),
            payload: bytes::Bytes::from_static(b"cancelled"),
        },
        permit: PublicationPermit::Unrestricted,
    };
    for result in [
        authority
            .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await
            .map(|_| ())
            .map_err(|error| error.into_error(std::convert::identity)),
        authority.head(id, &mut work).await.map(|_| ()),
        authority
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await
            .map(|_| ())
            .map_err(|error| error.into_error(std::convert::identity)),
        authority
            .replay(
                id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 1,
                },
                &mut work,
            )
            .await
            .map(|_| ()),
        authority.fence(id, head, &mut work).await.map(|_| ()),
        authority
            .find_operation(id, operation, &mut work)
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(result, Err(AuthorityStoreError::Cancelled)));
    }
    assert_eq!(work.spent()?, WorkCounters::default());
    let objects = MemoryObjectStore::default();
    let object_id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(b"cancelled"),
    };
    let requests = [ObjectReadRequest {
        object_id,
        maximum_bytes: 9,
    }];
    for result in [
        objects
            .put(object_id, OwnedBytes::from_static(b"cancelled"), &mut work)
            .await,
        objects.read(object_id, 9, &mut work).await.map(|_| ()),
        objects.read_many(&requests, &mut work).await.map(|_| ()),
        objects.contains(object_id, &mut work).await.map(|_| ()),
    ] {
        assert!(matches!(result, Err(ObjectStoreError::Cancelled)));
    }
    assert_eq!(work.spent()?, WorkCounters::default());
    let live = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &live);
    assert!(matches!(
        read_many_sequential(&objects, &[], &mut work).await,
        Err(ObjectStoreError::EmptyReadBatch)
    ));
    assert!(matches!(
        read_many_sequential(&objects, &requests, &mut work).await,
        Err(ObjectStoreError::Missing)
    ));
    let mut denied = AuthorityWork::new(
        WorkBudget {
            items_examined: 0,
            ..WorkBudget::UNBOUNDED
        },
        &live,
    );
    assert!(matches!(
        read_many_sequential(&objects, &requests, &mut denied).await,
        Err(ObjectStoreError::Work(_))
    ));
    Ok(())
}
