use super::*;

#[test]
fn object_kinds_digests_and_read_views_are_canonical() {
    let kinds = [
        ObjectKind::Blob,
        ObjectKind::BlobChunk,
        ObjectKind::TreePage,
        ObjectKind::ExtentPage,
        ObjectKind::FileTablePage,
        ObjectKind::GenerationRoot,
        ObjectKind::Metadata,
        ObjectKind::AttributePage,
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        let tag = u8::try_from(index + 1).unwrap_or(u8::MAX);
        assert_eq!(kind.canonical_tag(), tag);
        assert_eq!(ObjectKind::from_canonical_tag(tag), Ok(kind));
    }
    assert_eq!(
        ObjectKind::from_canonical_tag(0),
        Err(ObjectKindError::Unknown(0))
    );
    assert_eq!(
        ObjectKind::from_canonical_tag(9),
        Err(ObjectKindError::Unknown(9))
    );
    assert_ne!(
        object_digest(ObjectKind::Blob, b"bytes"),
        object_digest(ObjectKind::BlobChunk, b"bytes")
    );
    assert_ne!(
        object_digest(ObjectKind::Blob, b"bytes"),
        object_digest(ObjectKind::Blob, b"other")
    );
    let read = ObjectRead {
        bytes: Bytes::from_static(b"view"),
        retention: ObjectReadRetention::Owned { logical_bytes: 4 },
    };
    assert_eq!(&*read, b"view");
    assert_ne!(read.retention, ObjectReadRetention::Shared);
}

#[cfg(all(feature = "memory", not(target_arch = "wasm32")))]
#[test]
fn shared_backend_handles_forward_without_semantic_wrappers()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = Arc::new(crate::MemoryObjectStore::default());
    let bytes = Bytes::from_static(b"shared-object");
    let object_id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &bytes),
    };
    ObjectStore::put(&objects, object_id, bytes.clone(), WorkBudget::UNBOUNDED)?;
    let read = ObjectStore::read(
        &objects,
        object_id,
        u64::try_from(bytes.len())?,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(read.value.bytes, bytes);
    assert!(ObjectStore::contains(&objects, object_id, WorkBudget::UNBOUNDED,)?.value);
    let batch = ObjectStore::read_many(
        &objects,
        &[ObjectReadRequest {
            object_id,
            maximum_bytes: 13,
        }],
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(batch.value.len(), 1);
    assert_eq!(batch.value[0].bytes, bytes);

    let authority = Arc::new(crate::MemoryAuthorityStore::default());
    let authority_id = AuthorityId::from_bytes([9; 16]);
    let created = AuthorityStore::create_authority(
        &authority,
        authority_id,
        Epoch::GENESIS,
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(created.value, CreateAuthorityOutcome::Created(_)));
    let head = match created.value {
        CreateAuthorityOutcome::Created(head) | CreateAuthorityOutcome::Existing(head) => head,
    };
    assert_eq!(
        AuthorityStore::head(&authority, authority_id, WorkBudget::UNBOUNDED)?.value,
        head
    );
    let operation_id = OperationId::from_bytes([10; 16]);
    let appended = AuthorityStore::compare_and_append(
        &authority,
        authority_id,
        Epoch::GENESIS,
        head,
        ProposedCommit {
            operation_id,
            fingerprint: Digest::from_bytes([11; 32]),
            payload: Bytes::from_static(b"forwarded"),
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(appended.value, AppendOutcome::Committed(_)));
    let replayed = AuthorityStore::replay(
        &authority,
        authority_id,
        Sequence::GENESIS,
        ReplayLimit {
            records: 1,
            payload_bytes: 9,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(replayed.value.len(), 1);
    assert_eq!(replayed.value[0].operation_id, operation_id);
    assert_eq!(
        AuthorityStore::find_operation(
            &authority,
            authority_id,
            operation_id,
            WorkBudget::UNBOUNDED,
        )?
        .value,
        Some(replayed.value[0].clone())
    );
    let current = AuthorityStore::head(&authority, authority_id, WorkBudget::UNBOUNDED)?.value;
    let fenced = AuthorityStore::fence(&authority, authority_id, current, WorkBudget::UNBOUNDED)?;
    let FenceOutcome::Advanced(fenced_head) = fenced.value else {
        return Err("fresh delegated fence conflicted".into());
    };
    assert_eq!(fenced_head.epoch.get(), 2);
    Ok(())
}
