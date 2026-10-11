use super::*;
use crate::foundation::FileId;
use crate::kernel::{
    DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    MetadataField, TreePage, encode_file_metadata, encode_file_table_page, encode_generation_root,
    encode_tree_page,
};
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::storage::{
    AuthorityStore, FenceOutcome, ObjectKind, ObjectStore, ObjectStoreError, object_digest,
};

fn metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Unavailable,
        posix_uid: MetadataField::Unavailable,
        posix_gid: MetadataField::Unavailable,
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: MetadataField::Unavailable,
        accessed_ns: MetadataField::Unavailable,
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn put(
    store: &MemoryObjectStore,
    kind: ObjectKind,
    bytes: Vec<u8>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let object = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    store.put(object, Bytes::from(bytes), WorkBudget::UNBOUNDED)?;
    Ok(object)
}

fn fixture(
    objects: &MemoryObjectStore,
    volume_id: VolumeId,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let metadata = put(
        objects,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let tree = put(
        objects,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_file_id = FileId::from_bytes([3; 16]);
    let table = put(
        objects,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                file_id: root_file_id,
                kind: FileKind::Directory,
                link_count: 1,
                metadata,
                payload: FilePayload::Directory { entries: tree },
            }]),
            8,
        )?,
    )?;
    put(
        objects,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id,
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        })?,
    )
}

fn limits() -> ClosureLimits {
    ClosureLimits {
        decode: DecodeLimits::default(),
        maximum_objects: 16,
        maximum_files: 16,
        maximum_object_bytes: 64 * 1024,
        profile: crate::model::FilesystemProfile::Portable,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
    }
}

#[test]
fn complete_generation_publishes_and_retries_exactly() -> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([2; 16]);
    let authority_id = volume_authority_id(volume_id);
    let head = authority
        .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value;
    let head = match head {
        crate::storage::CreateAuthorityOutcome::Created(head) => head,
        crate::storage::CreateAuthorityOutcome::Existing(_) => return Err("existing".into()),
    };
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected: head,
        operation_id: OperationId::from_bytes([4; 16]),
        generation_root: fixture(&objects, volume_id)?,
    };
    let first = publish_generation(
        &objects,
        &authority,
        request,
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(&first.outcome, AppendOutcome::Committed(_)));
    let retry = publish_generation(
        &objects,
        &authority,
        request,
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(retry.outcome, AppendOutcome::AlreadyCommitted(_)));
    assert!(retry.work.authority_records_read > 0);
    let conflicting_expected_epoch = publish_generation(
        &objects,
        &authority,
        PublishGenerationRequest {
            expected: Head {
                epoch: Epoch::new(2)?,
                ..head
            },
            ..request
        },
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        conflicting_expected_epoch.outcome,
        AppendOutcome::IdempotencyConflict { .. }
    ));
    let committed_head = match first.outcome {
        AppendOutcome::Committed(commit) => Head {
            epoch: commit.epoch,
            sequence: commit.sequence,
            digest: commit.digest,
        },
        _ => return Err("first publication was not committed".into()),
    };
    assert!(matches!(
        authority
            .fence(authority_id, committed_head, WorkBudget::UNBOUNDED)?
            .value,
        FenceOutcome::Advanced(_)
    ));
    let post_fence_retry = publish_generation(
        &objects,
        &authority,
        request,
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        post_fence_retry.outcome,
        AppendOutcome::AlreadyCommitted(_)
    ));
    let post_fence_altered = publish_generation(
        &objects,
        &authority,
        PublishGenerationRequest {
            expected: Head {
                epoch: Epoch::new(2)?,
                ..head
            },
            ..request
        },
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        post_fence_altered.outcome,
        AppendOutcome::IdempotencyConflict { .. }
    ));
    let post_fence_fresh = publish_generation(
        &objects,
        &authority,
        PublishGenerationRequest {
            operation_id: OperationId::from_bytes([5; 16]),
            ..request
        },
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(matches!(
        post_fence_fresh.outcome,
        AppendOutcome::Fenced { .. }
    ));
    Ok(())
}

#[test]
fn publication_payload_is_canonical_bounded_and_decodable() -> Result<(), Box<dyn std::error::Error>>
{
    let value = PublishedGeneration {
        volume_id: VolumeId::from_bytes([31; 16]),
        generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([32; 32]),
        },
    };
    let encoded = encode_publication_payload(value.volume_id, value.generation_root);
    assert_eq!(
        decode_published_generation(&encoded, u64::try_from(encoded.len()).unwrap_or(u64::MAX),)?,
        value
    );

    let too_small = u64::try_from(encoded.len().saturating_sub(1)).unwrap_or(0);
    assert!(decode_published_generation(&encoded, too_small).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_published_generation(&trailing, u64::MAX).is_err());
    Ok(())
}

#[test]
fn publication_payload_rejects_unknown_and_non_generation_object_kinds() {
    let volume_id = VolumeId::from_bytes([31; 16]);
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([32; 32]),
    };
    let encoded = encode_publication_payload(volume_id, root);
    let kind_offset = PAYLOAD_DOMAIN.len() + 2 + 16;

    let mut wrong_kind = encoded.clone();
    wrong_kind[kind_offset] = ObjectKind::Blob.canonical_tag();
    assert!(matches!(
        decode_published_generation(&wrong_kind, u64::MAX),
        Err(CanonicalDecodeError::Invariant(crate::kernel::codec::CanonicalInvariant::Static(
            "publication object is not a generation root",
        )))
    ));

    let mut unknown_kind = encoded;
    unknown_kind[kind_offset] = u8::MAX;
    assert!(matches!(
        decode_published_generation(&unknown_kind, u64::MAX),
        Err(CanonicalDecodeError::Invariant(crate::kernel::codec::CanonicalInvariant::Static(
            "unknown publication object kind",
        )))
    ));
}

#[test]
fn invalid_closure_never_touches_authority() -> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([6; 16]);
    let authority_id = volume_authority_id(volume_id);
    let head = match authority
        .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value
    {
        crate::storage::CreateAuthorityOutcome::Created(head) => head,
        crate::storage::CreateAuthorityOutcome::Existing(_) => return Err("existing".into()),
    };
    let result = publish_generation(
        &objects,
        &authority,
        PublishGenerationRequest {
            authority_id,
            volume_id,
            epoch: Epoch::GENESIS,
            expected: head,
            operation_id: OperationId::from_bytes([7; 16]),
            generation_root: ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: Digest::from_bytes([8; 32]),
            },
        },
        limits(),
        WorkBudget::UNBOUNDED,
    );
    assert!(matches!(
        result,
        Err(OperationFailure {
            error: PublicationError::Closure(ClosureError::Storage(_)),
            ..
        })
    ));
    assert_eq!(
        authority
            .head(authority_id, WorkBudget::UNBOUNDED)?
            .value
            .sequence,
        head.sequence
    );
    Ok(())
}

#[test]
fn mismatched_authority_rejects_before_closure_or_authority_work()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([41; 16]);
    let failure = publish_generation(
        &objects,
        &authority,
        PublishGenerationRequest {
            authority_id: AuthorityId::from_bytes([42; 16]),
            volume_id,
            epoch: Epoch::GENESIS,
            expected: Head::genesis(Epoch::GENESIS),
            operation_id: OperationId::from_bytes([43; 16]),
            generation_root: ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: Digest::from_bytes([44; 32]),
            },
        },
        limits(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("mismatched authority unexpectedly published")?;
    assert!(matches!(
        failure.error,
        PublicationError::AuthorityMismatch { .. }
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn authenticated_foreign_volume_never_reaches_authority_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let requested_volume = VolumeId::from_bytes([51; 16]);
    let actual_volume = VolumeId::from_bytes([52; 16]);
    let authority_id = volume_authority_id(requested_volume);
    let crate::storage::CreateAuthorityOutcome::Created(head) = authority
        .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value
    else {
        return Err("authority existed".into());
    };
    let root = fixture(&objects, actual_volume)?;
    let failure = publish_generation(
        &objects,
        &authority,
        PublishGenerationRequest {
            authority_id,
            volume_id: requested_volume,
            epoch: Epoch::GENESIS,
            expected: head,
            operation_id: OperationId::from_bytes([53; 16]),
            generation_root: root,
        },
        limits(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("foreign-volume generation published")?;
    assert!(matches!(
        failure.error,
        PublicationError::VolumeMismatch { expected, actual }
            if expected == requested_volume && actual == actual_volume
    ));
    assert!(failure.work.backend_read_operations > 0);
    assert_eq!(failure.work.authority_records_appended, 0);
    assert_eq!(
        authority.head(authority_id, WorkBudget::UNBOUNDED)?.value,
        head
    );
    Ok(())
}

#[test]
fn publication_encoding_copy_and_hash_are_admitted_before_authority_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([45; 16]);
    let authority_id = volume_authority_id(volume_id);
    let crate::storage::CreateAuthorityOutcome::Created(head) = authority
        .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value
    else {
        return Err("authority existed".into());
    };
    let generation_root = fixture(&objects, volume_id)?;
    let closure =
        prove_generation_closure(&objects, generation_root, limits(), WorkBudget::UNBOUNDED)?;
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected: head,
        operation_id: OperationId::from_bytes([46; 16]),
        generation_root,
    };
    assert_eq!(publication_payload_length(), PAYLOAD_DOMAIN.len() + 51);
    assert_eq!(
        fingerprint_input_length(None),
        publication_payload_length() + 72
    );
    for counter in [
        "bytes_copied",
        "bytes_hashed",
        "bytes_encoded",
        "allocation_operations",
        "peak_allocation_bytes",
    ] {
        let mut budget = WorkBudget::UNBOUNDED;
        match counter {
            "bytes_copied" => budget.bytes_copied = closure.work.bytes_copied,
            "bytes_hashed" => budget.bytes_hashed = closure.work.bytes_hashed,
            "bytes_encoded" => budget.bytes_encoded = closure.work.bytes_encoded,
            "allocation_operations" => {
                budget.allocation_operations = closure.work.allocation_operations;
            }
            "peak_allocation_bytes" => {
                // The full verifier's transient sets can dominate encoding
                // peak; reject its actual peak, not an assumed extra peak.
                budget.peak_allocation_bytes = closure.work.peak_allocation_bytes.saturating_sub(1);
            }
            _ => return Err("unknown test counter".into()),
        }
        let failure = publish_generation(&objects, &authority, request, limits(), budget)
            .err()
            .ok_or("unadmitted publication succeeded")?;
        assert!(matches!(failure.error, PublicationError::Work(_)
            | PublicationError::Closure(ClosureError::Work(_))));
        assert_eq!(
            authority.head(authority_id, WorkBudget::UNBOUNDED)?.value,
            head
        );
    }
    Ok(())
}

#[test]
fn async_and_sync_publication_are_identical_and_pre_cancel_is_inert()
-> Result<(), Box<dyn std::error::Error>> {
    let sync_objects = MemoryObjectStore::default();
    let async_objects = MemoryObjectStore::default();
    let sync_authority = MemoryAuthorityStore::default();
    let async_authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([22; 16]);
    let authority_id = volume_authority_id(volume_id);
    let sync_head = sync_authority
        .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value;
    let async_head = async_authority
        .create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
        .value;
    assert_eq!(sync_head, async_head);
    let crate::storage::CreateAuthorityOutcome::Created(head) = sync_head else {
        return Err("authority unexpectedly existed".into());
    };
    let sync_root = fixture(&sync_objects, volume_id)?;
    let async_root = fixture(&async_objects, volume_id)?;
    assert_eq!(sync_root, async_root);
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected: head,
        operation_id: OperationId::from_bytes([23; 16]),
        generation_root: sync_root,
    };
    let synchronous = publish_generation(
        &sync_objects,
        &sync_authority,
        request,
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = crate::async_storage::poll_ready(publish_generation_async(
        &async_objects,
        &async_authority,
        request,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous publication unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = crate::async_storage::poll_ready(publish_generation_async(
        &async_objects,
        &async_authority,
        PublishGenerationRequest {
            operation_id: OperationId::from_bytes([24; 16]),
            expected: async_authority
                .head(authority_id, WorkBudget::UNBOUNDED)?
                .value,
            ..request
        },
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelled asynchronous publication unexpectedly blocked")?
    .err()
    .ok_or("cancelled asynchronous publication unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PublicationError::Closure(ClosureError::Storage(ObjectStoreError::Cancelled))
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
enum NativePublicationTestError {
    #[error(transparent)]
    Fs(#[from] crate::FsError),
    #[error("the exact publication owner became stale")]
    OwnerStale,
}

#[cfg(not(target_arch = "wasm32"))]
fn native_fixture(
    objects: &MemoryObjectStore,
    volume_id: VolumeId,
) -> Result<(ObjectId, Vec<ObjectId>), Box<dyn std::error::Error>> {
    use crate::kernel::{
        BlobChunkRef, BlobNode, BlobPage, Extent, ExtentKind, ExtentPage, LogicalName,
        NameEncoding, TreeEntry, encode_blob_page, encode_extent_page,
    };
    let metadata = put(objects, ObjectKind::Metadata, encode_file_metadata(metadata())?)?;
    let chunk = put(objects, ObjectKind::BlobChunk, b"original closure content".to_vec())?;
    let logical_bytes = b"original closure content".len() as u64;
    let blob = put(objects, ObjectKind::Blob, encode_blob_page(&BlobPage {
        first_offset: 0, end_offset: logical_bytes,
        node: BlobNode::Leaf(vec![BlobChunkRef { first_offset: 0, end_offset: logical_bytes, chunk }]),
    }, 8)?)?;
    let extents = put(objects, ObjectKind::ExtentPage, encode_extent_page(&ExtentPage::Leaf(vec![Extent {
        offset: 0, length: logical_bytes,
        kind: ExtentKind::Content { object: blob, object_offset: 0 },
    }]), 8)?)?;
    let root_file_id = FileId::from_bytes([1; 16]);
    let child_file_id = FileId::from_bytes([2; 16]);
    let tree = put(objects, ObjectKind::TreePage, encode_tree_page(&TreePage::Leaf(vec![TreeEntry {
        name: LogicalName::new(NameEncoding::Utf8, b"content".to_vec(), 255)?,
        file_id: child_file_id, kind: FileKind::Regular,
    }]), 8)?)?;
    let table = put(objects, ObjectKind::FileTablePage, encode_file_table_page(&FileTablePage::Leaf(vec![
        FileRecord {
            file_id: root_file_id, kind: FileKind::Directory, link_count: 1, metadata,
            payload: FilePayload::Directory { entries: tree },
        },
        FileRecord {
            file_id: child_file_id, kind: FileKind::Regular, link_count: 1, metadata,
            payload: FilePayload::Regular { logical_bytes, extents },
        },
    ]), 8)?)?;
    let generation = put(objects, ObjectKind::GenerationRoot, encode_generation_root(&GenerationRoot {
        volume_id, root_file_id, file_table: table,
        parents: vec![crate::GenerationId::new(Digest::from_bytes([7; 32]))],
        required_features: 0,
    })?)?;
    let mut expected = vec![generation, table, tree, metadata, extents, blob, chunk];
    expected.sort_unstable_by(|left, right| left.kind.canonical_tag().cmp(&right.kind.canonical_tag())
        .then_with(|| left.digest.as_bytes().cmp(right.digest.as_bytes())));
    Ok((generation, expected))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_request(
    authority: &MemoryAuthorityStore, volume_id: VolumeId, generation_root: ObjectId,
) -> Result<PublishGenerationRequest, Box<dyn std::error::Error>> {
    let authority_id = volume_authority_id(volume_id);
    let created = authority.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?;
    let crate::storage::CreateAuthorityOutcome::Created(expected) = created.value else {
        return Err("native publication fixture authority already existed".into());
    };
    Ok(PublishGenerationRequest {
        authority_id, volume_id, epoch: Epoch::GENESIS, expected,
        operation_id: OperationId::from_bytes([8; 16]), generation_root,
    })
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_authenticates_actual_complete_closure_on_original_ledger()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([31; 16]);
    let (root, expected_objects) = native_fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let standing = original.claim(19)?;
    let mut calls = 0;
    let mut owner = |same: &crate::NativeMaterializationWork| {
        assert!(original.shares_ledger(same));
        assert_eq!(same.receipt().authority_records_appended, 0);
        calls += 1;
        std::future::ready(Ok::<(), NativePublicationTestError>(()))
    };
    let publication = publish_generation_async_guarded_with_native_work(
        &objects, &authority, request, PublicationIntent::unrestricted(None),
        limits(), &original, &cancellation, &mut owner, None, None,
    ).await?;
    assert!(matches!(publication.0.outcome, AppendOutcome::Committed(_)));
    assert_eq!(publication.0.proof.objects, expected_objects);
    assert_eq!(publication.0.proof.object_count, 7);
    assert_eq!(publication.0.proof.file_count, 2);
    assert!(publication.1.shares_ledger(&standing));
    assert_eq!(publication.0.work, original.receipt());
    assert_eq!(calls, 1);
    assert!(original.receipt().backend_read_operations >= 7);
    let live_with_output = original.live_allocation_bytes();
    let output_bytes = publication.1.bytes();
    drop(publication);
    assert_eq!(original.live_allocation_bytes(), live_with_output - output_bytes);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_owner_stale_after_await_fails_before_actual_cas()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicU64, Ordering};
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([32; 16]);
    let (root, _) = native_fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let owner_epoch = AtomicU64::new(1);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
    let mut started_tx = Some(started_tx);
    let mut continue_rx = Some(continue_rx);
    let mut guard = |same: &crate::NativeMaterializationWork| {
        assert!(original.shares_ledger(same));
        let observed = owner_epoch.load(Ordering::SeqCst);
        let sender = started_tx.take().expect("one attempted CAS");
        let receiver = continue_rx.take().expect("one attempted CAS");
        let epoch = &owner_epoch;
        async move {
            sender.send(()).expect("owner scheduling channel");
            receiver.await.expect("owner continuation");
            if epoch.load(Ordering::SeqCst) != observed {
                Err(NativePublicationTestError::OwnerStale)
            } else {
                Ok(())
            }
        }
    };
    let (result, ()) = tokio::join!(
        publish_generation_async_guarded_with_native_work(
            &objects, &authority, request, PublicationIntent::unrestricted(None),
            limits(), &original, &cancellation, &mut guard, None, None,
        ),
        async {
            started_rx.await.expect("actual postplanning owner callback");
            owner_epoch.store(2, Ordering::SeqCst);
            continue_tx.send(()).expect("owner resume");
        },
    );
    assert!(matches!(result, Err(NativePublicationTestError::OwnerStale)));
    assert_eq!(authority.head(request.authority_id, WorkBudget::UNBOUNDED)?.value, request.expected);
    assert_eq!(original.receipt().authority_records_appended, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_partial_closure_and_allocation_denials_release_owned_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([33; 16]);
    let (root, _) = native_fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let cancellation = CancellationToken::new();
    for early_allocation in [true, false] {
        let mut budget = WorkBudget::UNBOUNDED;
        if early_allocation {
            budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes();
        }
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let baseline = original.live_allocation_bytes();
        let mut calls = 0;
        let mut guard = |_: &crate::NativeMaterializationWork| {
            calls += 1;
            std::future::ready(Ok::<(), NativePublicationTestError>(()))
        };
        let mut admitted = limits();
        if !early_allocation { admitted.maximum_objects = 3; }
        let failure = publish_generation_async_guarded_with_native_work(
            &objects, &authority, request, PublicationIntent::unrestricted(None),
            admitted, &original, &cancellation, &mut guard, None, None,
        ).await.err().ok_or("unadmitted native publication succeeded")?;
        if early_allocation {
            assert!(matches!(failure, NativePublicationTestError::Fs(crate::FsError::Work(_))
                | NativePublicationTestError::Fs(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(_)))));
            assert_eq!(original.receipt().backend_read_operations, 0);
        } else {
            assert!(matches!(failure, NativePublicationTestError::Fs(crate::FsError::Closure(ClosureError::TooManyObjects { .. }))));
            assert!(original.receipt().backend_read_operations > 0);
        }
        assert_eq!(calls, 0);
        assert_eq!(original.receipt().authority_records_appended, 0);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert_eq!(authority.head(request.authority_id, WorkBudget::UNBOUNDED)?.value, request.expected);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_lost_ack_reopens_original_without_resealing_or_second_append()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::simulation::{Simulation, SimulationFault, SimulationOperation, SimulationOptions};
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([34; 16]);
    let (root, _) = native_fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let simulation = Simulation::wrap(authority, objects, SimulationOptions::default())?;
    let (authority, objects) = simulation.stores();
    simulation.schedule_next(SimulationOperation::AuthorityAppend, SimulationFault::AmbiguousAuthorityAppend)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut guard = |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), NativePublicationTestError>(()));
    let failure = publish_generation_async_guarded_with_native_work(
        &objects, &authority, request, PublicationIntent::unrestricted(None),
        limits(), &original, &cancellation, &mut guard, None, None,
    ).await.err().ok_or("acknowledgement loss unexpectedly returned success")?;
    assert!(matches!(failure, NativePublicationTestError::Fs(crate::FsError::Authority(AuthorityStoreError::Indeterminate { .. }))));
    assert_eq!(original.receipt().authority_records_appended, 1);
    let reopened = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let resolved = crate::AsyncAuthorityStore::find_operation_with_native_work(
        &authority, request.authority_id, request.operation_id, &reopened, &cancellation,
    ).await?;
    let commit = resolved.0.ok_or("lost acknowledgement operation was not durable")?;
    let predecessor = Head {
        epoch: commit.epoch,
        sequence: crate::Sequence::new(commit.sequence.get().checked_sub(1).ok_or("invalid committed sequence")?),
        digest: commit.previous_digest,
    };
    assert_eq!(predecessor, request.expected);
    let mut seals = 0;
    let mut stale_owner = |_: &crate::NativeMaterializationWork| {
        seals += 1;
        std::future::ready(Err::<(), _>(NativePublicationTestError::OwnerStale))
    };
    let publication = publish_generation_async_guarded_with_native_work(
        &objects, &authority, PublishGenerationRequest { expected: predecessor, ..request },
        PublicationIntent::unrestricted(None), limits(), &reopened, &cancellation,
        &mut stale_owner, Some(commit), None,
    ).await?;
    assert!(matches!(publication.0.outcome, AppendOutcome::AlreadyCommitted(_)));
    assert_eq!(seals, 0);
    assert_eq!(reopened.receipt().authority_records_appended, 0);
    assert_eq!(reopened.receipt().backend_write_operations, 0);
    Ok(())
}

/// Uses production immutable reads and durability, but intentionally has no
/// native custody capability. The generic default must reject, never skip it.
#[cfg(not(target_arch = "wasm32"))]
struct NativeMissingCustody<'a>(&'a MemoryObjectStore);

#[cfg(not(target_arch = "wasm32"))]
impl crate::AsyncObjectStore for NativeMissingCustody<'_> {
    async fn put(
        &self, object: ObjectId, bytes: Bytes, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<()> {
        crate::AsyncObjectStore::put(self.0, object, bytes, budget, cancellation).await
    }

    async fn read(
        &self, object: ObjectId, maximum: u64, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<crate::storage::ObjectRead> {
        crate::AsyncObjectStore::read(self.0, object, maximum, budget, cancellation).await
    }

    async fn read_many(
        &self, requests: &[crate::storage::ObjectReadRequest], budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<Vec<crate::storage::ObjectRead>> {
        crate::AsyncObjectStore::read_many(self.0, requests, budget, cancellation).await
    }

    async fn contains(
        &self, object: ObjectId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<bool> {
        crate::AsyncObjectStore::contains(self.0, object, budget, cancellation).await
    }

    async fn read_with_native_work(
        &self, object: ObjectId, maximum: u64, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectRead, ObjectStoreError> {
        crate::AsyncObjectStore::read_with_native_work(self.0, object, maximum, original, cancellation).await
    }

    async fn flush_before_publish_with_native_work(
        &self, scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        crate::AsyncObjectStore::flush_before_publish_with_native_work(self.0, scope, original, cancellation).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_missing_custody_capability_rejects_after_real_proof_before_owner_or_cas()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([35; 16]);
    let (root, _) = native_fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut calls = 0;
    let mut guard = |_: &crate::NativeMaterializationWork| {
        calls += 1;
        std::future::ready(Ok::<(), NativePublicationTestError>(()))
    };
    let failure = publish_generation_async_guarded_with_native_work(
        &NativeMissingCustody(&objects), &authority, request, PublicationIntent::unrestricted(None),
        limits(), &original, &cancellation, &mut guard, None, None,
    ).await.err().ok_or("missing native custody unexpectedly allowed publication")?;
    assert!(matches!(failure, NativePublicationTestError::Fs(crate::FsError::Object(ObjectStoreError::NativeUnavailable))));
    assert!(original.receipt().backend_read_operations >= 7);
    assert_eq!(original.receipt().authority_records_appended, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(calls, 0);
    assert_eq!(authority.head(request.authority_id, WorkBudget::UNBOUNDED)?.value, request.expected);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_root_preparation_retains_original_cause_and_releases_partial_buffers()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([36; 16]);
    let (root, _) = native_fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut calls = 0;
    let mut owner = |_: &crate::NativeMaterializationWork| {
        calls += 1;
        std::future::ready(Ok::<(), NativePublicationTestError>(()))
    };
    let mut preparation = |root: &GenerationRoot, proof: &mut crate::NativeAllocation, same: &crate::NativeMaterializationWork| {
        assert_eq!(root.parents.len(), 1);
        assert!(original.shares_ledger(same));
        assert!(same.shares_ledger(proof.work()));
        let before_preparation = same.live_allocation_bytes();
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(same, &cancellation);
        let prepared = super::super::persistent_point::Owned::<Vec<u8>>::allocate(127, &mut accounting)
            .map_err(NativePublicationTestError::from)?;
        assert_eq!(same.live_allocation_bytes(), before_preparation + prepared.bytes);
        drop(prepared);
        Err(NativePublicationTestError::OwnerStale)
    };
    let failure = publish_generation_async_guarded_with_native_work(
        &objects, &authority, request, PublicationIntent::unrestricted(None),
        limits(), &original, &cancellation, &mut owner, None, Some(&mut preparation),
    ).await.err().ok_or("failed root preparation unexpectedly published")?;
    assert!(matches!(failure, NativePublicationTestError::OwnerStale));
    assert_eq!(calls, 0);
    assert_eq!(original.receipt().authority_records_appended, 0);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(authority.head(request.authority_id, WorkBudget::UNBOUNDED)?.value, request.expected);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_publication_payload_decoder_accounts_actual_completed_field_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([37; 16]);
    let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([38; 32]) };
    let payload = encode_publication_payload(volume_id, root);
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 2;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    assert!(decode_published_generation_with_accounting(&payload, u64::MAX, &mut accounting).is_err());
    assert_eq!(original.receipt().bytes_copied, 2);
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);

    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let mut malformed = payload.clone();
    malformed[PAYLOAD_DOMAIN.len() + 2 + 16] = u8::MAX;
    assert!(matches!(
        decode_published_generation_with_accounting(&malformed, u64::MAX, &mut accounting),
        Err(crate::FsError::Decode(CanonicalDecodeError::Invariant(super::super::codec::CanonicalInvariant::Static(
            "unknown publication object kind",
        ))))
    ));
    assert_eq!(original.receipt().bytes_copied, 2 + 16 + 1);
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_publication_payload_decoder_rejects_domain_without_copy_and_keeps_late_cancelled_copy()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([39; 16]);
    let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([40; 32]) };
    let payload = encode_publication_payload(volume_id, root);
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut malformed = payload.clone();
    malformed[0] ^= 1;
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    assert!(decode_published_generation_with_accounting(&malformed, u64::MAX, &mut accounting).is_err());
    assert_eq!(original.receipt().bytes_copied, 0);
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
    let mut observer = super::super::file_read::OriginalReadAccounting::new(&mut accounting)?;
    let decoded = decode_published_generation_with_steps::<crate::FsError>(&payload, u64::MAX, |step| {
        if matches!(&step, super::super::codec::ReadAllocationStep::Copied(_)) { cancellation.cancel(); }
        observer.observe(step).map_err(super::super::file_read::ReadDecodeError::into_original_fs_error)
    });
    drop(observer.into_allocation());
    assert!(decoded.is_err());
    assert_eq!(original.receipt().bytes_copied, 2);
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_publication_payload_owner_denial_keeps_real_completed_buffers_and_drops_them()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let initialization = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let initial_operations = initialization.receipt().allocation_operations;
    drop(initialization);
    let mut budget = WorkBudget::UNBOUNDED;
    budget.allocation_operations = initial_operations + 2;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([42; 32]) };
    let volume_id = VolumeId::from_bytes([41; 16]);
    let authority = MemoryAuthorityStore::default();
    let request = native_request(&authority, volume_id, root)?;
    let mut accounting = PublicationAccounting::Native(&original);
    assert!(prepare_publication_commit(request, None, &mut accounting, &cancellation).is_err());
    assert_eq!(original.receipt().allocation_operations, initial_operations + 2);
    assert_eq!(original.receipt().bytes_encoded,
        (publication_payload_length() + fingerprint_input_length(None)) as u64);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt().authority_records_appended, 0);
    assert_eq!(authority.head(request.authority_id, WorkBudget::UNBOUNDED)?.value, request.expected);
    Ok(())
}
