use super::*;
use crate::foundation::{ProposedCommit, Sequence};
use crate::kernel::{
    Extent, ExtentKind, ExtentPage, GenerationExportManifestError, LogicalName, NameEncoding,
    prove_generation_closure,
    PublicationEvidenceError, TreeEntry, decode_generation_publication_evidence,
    encode_extent_page, encode_generation_export_manifest, encode_generation_publication_evidence,
};
use crate::model::{Lifecycle, VolumeConfig};
use crate::GuardedAppend;
use sha2::{Digest as _, Sha256};
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
    store.put(object, acyclic_stream::OwnedBytes::from(bytes), WorkBudget::UNBOUNDED)?;
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

/// Two distinct sparse files share one extent object. Their logical total
/// exceeds u64 without allocating or reading their holes.
fn evidence_fixture() -> Result<
    (PublishGenerationRequest, GenerationProof, GuardedAppend, VolumeConfig),
    Box<dyn std::error::Error>,
> {
    let objects = MemoryObjectStore::default();
    let volume_id = VolumeId::from_bytes([61; 16]);
    let root_file_id = FileId::from_bytes([1; 16]);
    let first_file_id = FileId::from_bytes([2; 16]);
    let second_file_id = FileId::from_bytes([3; 16]);
    let metadata = put(&objects, ObjectKind::Metadata, encode_file_metadata(metadata())?)?;
    let extents = put(
        &objects,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 0,
                length: u64::MAX,
                kind: ExtentKind::Hole,
            }]),
            8,
        )?,
    )?;
    let tree = put(
        &objects,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![
                TreeEntry {
                    name: LogicalName::new(NameEncoding::Utf8, b"first".to_vec(), 255)?,
                    file_id: first_file_id,
                    kind: FileKind::Regular,
                },
                TreeEntry {
                    name: LogicalName::new(NameEncoding::Utf8, b"second".to_vec(), 255)?,
                    file_id: second_file_id,
                    kind: FileKind::Regular,
                },
            ]),
            8,
        )?,
    )?;
    let table = put(
        &objects,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![
                FileRecord {
                    file_id: root_file_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: tree },
                },
                FileRecord {
                    file_id: first_file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Regular { logical_bytes: u64::MAX, extents },
                },
                FileRecord {
                    file_id: second_file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Regular { logical_bytes: u64::MAX, extents },
                },
            ]),
            8,
        )?,
    )?;
    let root = put(
        &objects,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id,
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        })?,
    )?;
    let request = PublishGenerationRequest {
        authority_id: volume_authority_id(volume_id),
        volume_id,
        epoch: Epoch::new(7)?,
        expected: Head {
            epoch: Epoch::new(7)?,
            sequence: Sequence::new(23),
            digest: Digest::from_bytes([62; 32]),
        },
        operation_id: OperationId::from_bytes([63; 16]),
        generation_root: root,
    };
    let proof = prove_generation_closure(&objects, root, limits(), WorkBudget::UNBOUNDED)?;
    let mut expected_objects = vec![metadata, extents, tree, table, root];
    expected_objects.sort_unstable();
    assert_eq!(proof.objects, expected_objects);
    assert_eq!(proof.object_count, 5);
    assert_eq!(proof.file_count, 3);
    assert_eq!(proof.logical_file_bytes, u128::from(u64::MAX) * 2);
    let cancellation = CancellationToken::new();
    let commit = prepare_publication_commit(request, None, None,
        &mut PublicationAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation), &cancellation)?;
    let append = GuardedAppend {
        authority_id: request.authority_id,
        epoch: request.epoch,
        expected: request.expected,
        commit,
        permit: PublicationPermit::Unrestricted,
    };
    let mut config = VolumeConfig::portable(Lifecycle::Durable);
    config.limits.maximum_objects_per_generation = 17;
    config.limits.maximum_files_per_generation = 19;
    Ok((request, proof, append, config))
}

#[test]
fn publication_evidence_preserves_original_complete_closure_and_every_permit()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, mut append, config) = evidence_fixture()?;
    for permit in [
        PublicationPermit::Unrestricted,
        PublicationPermit::Lease {
            authority_id: [71; 16],
            workspace_id: [72; 16],
            lease_id: [73; 16],
            expires_at_millis: u64::MAX - 1,
        },
        PublicationPermit::Reservation {
            operation_id: request.operation_id.into_bytes(),
            gate_tail: 42,
            expected: request.expected,
        },
    ] {
        append.permit = permit;
        let encoded =
            encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
        assert_eq!(encoded.sha256, <[u8; 32]>::from(Sha256::digest(&encoded.bytes)));
        let decoded = decode_generation_publication_evidence(
            &encoded.bytes,
            &encoded.sha256,
            encoded.bytes.len() as u64,
            proof.object_count,
        )?;
        assert_eq!(decoded.request, request);
        assert_eq!(decoded.append, append);
        assert_eq!(decoded.root, proof.root);
        assert_eq!(decoded.manifest.config, config);
        assert_eq!(decoded.manifest.volume_id, request.volume_id);
        assert_eq!(decoded.manifest.generation_root, request.generation_root);
        assert_eq!(decoded.manifest.objects, proof.objects);
        assert!(decoded.manifest.objects.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(decoded.manifest.file_count, proof.file_count);
        assert_eq!(decoded.manifest.generation_id, proof.generation_id);
        assert_eq!(decoded.logical_file_bytes, u128::from(u64::MAX) * 2);
        assert_eq!(
            encode_generation_publication_evidence(
                &decoded.request, &proof, &decoded.append, decoded.manifest.config,
                encoded.bytes.len() as u64,
            )?,
            encoded
        );
    }
    Ok(())
}

#[test]
fn publication_evidence_rejects_tampering_and_byte_and_object_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, append, config) = evidence_fixture()?;
    let encoded =
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
    let mut digest = encoded.sha256;
    digest[0] ^= 1;
    assert!(matches!(
        decode_generation_publication_evidence(&encoded.bytes, &digest, 64 * 1024, proof.object_count),
        Err(PublicationEvidenceError::Mismatch)
    ));
    for index in [0, encoded.bytes.len() / 2, encoded.bytes.len() - 1] {
        let mut altered = encoded.bytes.to_vec();
        altered[index] ^= 1;
        assert!(matches!(
            decode_generation_publication_evidence(
                &altered, &encoded.sha256, 64 * 1024, proof.object_count,
            ),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    let mut changed_append = append.clone();
    changed_append.commit.fingerprint = Digest::from_bytes([91; 32]);
    let changed = encode_generation_publication_evidence(
        &request, &proof, &changed_append, config, 64 * 1024,
    )?;
    assert!(matches!(
        decode_generation_publication_evidence(
            &changed.bytes, &encoded.sha256, 64 * 1024, proof.object_count,
        ),
        Err(PublicationEvidenceError::Mismatch)
    ));
    for bound in [0, encoded.bytes.len() as u64 - 1] {
        assert!(matches!(
            encode_generation_publication_evidence(&request, &proof, &append, config, bound),
            Err(PublicationEvidenceError::TooLarge)
        ));
        assert!(matches!(
            decode_generation_publication_evidence(
                &encoded.bytes, &encoded.sha256, bound, proof.object_count,
            ),
            Err(PublicationEvidenceError::TooLarge)
        ));
    }
    for bound in [0, proof.object_count - 1] {
        assert!(matches!(
            decode_generation_publication_evidence(
                &encoded.bytes, &encoded.sha256, 64 * 1024, bound,
            ),
            Err(PublicationEvidenceError::Manifest(GenerationExportManifestError::TooManyObjects))
        ));
    }
    Ok(())
}

#[test]
fn publication_evidence_rejects_disagreeing_request_proof_and_exact_append()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, append, config) = evidence_fixture()?;
    for changed in [
        PublishGenerationRequest { authority_id: AuthorityId::from_bytes([81; 16]), ..request },
        PublishGenerationRequest { volume_id: VolumeId::from_bytes([81; 16]), ..request },
        PublishGenerationRequest { operation_id: OperationId::from_bytes([81; 16]), ..request },
        PublishGenerationRequest { epoch: Epoch::new(8)?, ..request },
        PublishGenerationRequest { expected: Head::genesis(request.epoch), ..request },
        PublishGenerationRequest {
            generation_root: ObjectId {
                digest: Digest::from_bytes([81; 32]),
                ..request.generation_root
            },
            ..request
        },
    ] {
        assert!(matches!(
            encode_generation_publication_evidence(&changed, &proof, &append, config, 64 * 1024),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    let mut changed = proof.clone();
    changed.object_count += 1;
    assert!(matches!(
        encode_generation_publication_evidence(&request, &changed, &append, config, 64 * 1024),
        Err(PublicationEvidenceError::Mismatch)
    ));
    let mut changed = proof.clone();
    changed.root.volume_id = VolumeId::from_bytes([81; 16]);
    assert!(matches!(
        encode_generation_publication_evidence(&request, &changed, &append, config, 64 * 1024),
        Err(PublicationEvidenceError::Mismatch)
    ));
    for changed in [
        GuardedAppend { authority_id: AuthorityId::from_bytes([81; 16]), ..append.clone() },
        GuardedAppend { epoch: Epoch::new(8)?, ..append.clone() },
        GuardedAppend { expected: Head::genesis(request.epoch), ..append.clone() },
        GuardedAppend {
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([81; 16]), ..append.commit.clone()
            },
            ..append.clone()
        },
        GuardedAppend {
            commit: ProposedCommit {
                payload: bytes::Bytes::from(encode_publication_payload(
                    VolumeId::from_bytes([81; 16]), request.generation_root,
                )),
                ..append.commit.clone()
            },
            ..append.clone()
        },
    ] {
        assert!(matches!(
            encode_generation_publication_evidence(&request, &proof, &changed, config, 64 * 1024),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    Ok(())
}

#[test]
fn publication_evidence_rejects_noncanonical_manifest_and_trailing_data_with_fresh_digests()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, append, config) = evidence_fixture()?;
    let encoded =
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
    let mut reversed_proof = proof.clone();
    reversed_proof.objects.reverse();
    assert!(matches!(
        encode_generation_publication_evidence(
            &request, &reversed_proof, &append, config, 64 * 1024,
        ),
        Err(PublicationEvidenceError::Manifest(
            GenerationExportManifestError::NonCanonicalObjectOrder,
        ))
    ));
    let projection = decode_generation_publication_evidence(
        &encoded.bytes, &encoded.sha256, 64 * 1024, proof.object_count,
    )?;
    let manifest = encode_generation_export_manifest(&projection.manifest)?;
    let offset = encoded.bytes.windows(manifest.len())
        .position(|value| value == manifest)
        .ok_or("missing embedded manifest")?;
    let last_two_objects = offset + manifest.len() - 66;
    let mut unsorted = encoded.bytes.to_vec();
    unsorted[last_two_objects..last_two_objects + 66].rotate_left(33);
    let digest = Sha256::digest(&unsorted).into();
    assert!(matches!(
        decode_generation_publication_evidence(&unsorted, &digest, 64 * 1024, proof.object_count),
        Err(PublicationEvidenceError::Manifest(
            GenerationExportManifestError::NonCanonicalObjectOrder,
        ))
    ));
    let mut duplicate = encoded.bytes.to_vec();
    duplicate.copy_within(last_two_objects..last_two_objects + 33, last_two_objects + 33);
    let digest = Sha256::digest(&duplicate).into();
    assert!(matches!(
        decode_generation_publication_evidence(&duplicate, &digest, 64 * 1024, proof.object_count),
        Err(PublicationEvidenceError::Manifest(
            GenerationExportManifestError::NonCanonicalObjectOrder,
        ))
    ));
    let mut trailing = encoded.bytes.to_vec();
    trailing.push(0);
    let digest = Sha256::digest(&trailing).into();
    assert!(matches!(
        decode_generation_publication_evidence(&trailing, &digest, 64 * 1024, proof.object_count),
        Err(PublicationEvidenceError::Decode(CanonicalDecodeError::TrailingBytes))
    ));
    let truncated = &encoded.bytes[..encoded.bytes.len() - 1];
    let digest = Sha256::digest(truncated).into();
    assert!(matches!(
        decode_generation_publication_evidence(truncated, &digest, 64 * 1024, proof.object_count),
        Err(PublicationEvidenceError::Decode(_))
    ));
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
    let owner_epoch = std::sync::Arc::new(AtomicU64::new(1));
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
    let started_tx = std::sync::Arc::new(parking_lot::Mutex::new(Some(started_tx)));
    let continue_rx = std::sync::Arc::new(parking_lot::Mutex::new(Some(continue_rx)));
    let guarded_epoch = owner_epoch.clone();
    let owner_original = original.clone();
    let mut guard = move |same: &crate::NativeMaterializationWork| {
        assert!(owner_original.shares_ledger(same));
        let observed = guarded_epoch.load(Ordering::SeqCst);
        let sender = started_tx.lock().take().expect("one attempted CAS");
        let receiver = continue_rx.lock().take().expect("one attempted CAS");
        let epoch = guarded_epoch.clone();
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
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let owner_calls = calls.clone();
        let mut guard = move |_: &crate::NativeMaterializationWork| {
            owner_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
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
    let changed_root = fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let simulation = Simulation::wrap(authority, objects, SimulationOptions::default())?;
    let (authority, objects) = simulation.stores();
    simulation.schedule_next(SimulationOperation::AuthorityAppend, SimulationFault::AmbiguousAuthorityAppend)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let (data, custody) = original.copy_bytes(b"immutable original operation data")?;
    let evidence = acyclic_stream::OwnedBytes::from_vec_with_guard(data, custody, original.runtime_meter_owner())
        .expect("fresh evidence has fully initialized unique backing");
    let intent = PublicationIntent::unrestricted(Some(Digest::from_bytes([59; 32])))
        .with_original_evidence(Some(evidence));
    let mut guard = |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), NativePublicationTestError>(()));
    let failure = publish_generation_async_guarded_with_native_work(
        &objects, &authority, request, intent.clone(),
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
    {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&reopened, &cancellation);
        let decoded = decode_publication_evidence_with_accounting(&commit.payload, 4_096, &mut accounting)?;
        assert_eq!(decoded.original_evidence, Some(b"immutable original operation data".as_slice()));
        assert_eq!(contextual_publication_fingerprint_from_payload_with_accounting(
            request, intent.operation_context.ok_or("missing original context")?, &commit.payload, &mut accounting,
        )?, commit.fingerprint);
    }
    let later = publish_generation_async_guarded_with_native_work(
        &objects, &authority, PublishGenerationRequest {
            expected: Head { epoch: commit.epoch, sequence: commit.sequence, digest: commit.digest },
            operation_id: OperationId::from_bytes([60; 16]), generation_root: changed_root, ..request
        }, PublicationIntent::unrestricted(None), limits(), &original, &cancellation, &mut guard, None, None,
    ).await?;
    assert!(matches!(later.0.outcome, AppendOutcome::Committed(_)));
    let seals = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let owner_seals = seals.clone();
    let mut stale_owner = move |_: &crate::NativeMaterializationWork| {
        owner_seals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::future::ready(Err::<(), _>(NativePublicationTestError::OwnerStale))
    };
    let changed = publish_generation_async_guarded_with_native_work(
        &objects, &authority, PublishGenerationRequest { generation_root: changed_root, expected: predecessor, ..request },
        intent.clone(), limits(), &reopened, &cancellation, &mut stale_owner, Some(commit.clone()), None,
    ).await?;
    assert!(matches!(changed.0.outcome, AppendOutcome::IdempotencyConflict { .. }));
    let publication = publish_generation_async_guarded_with_native_work(
        &objects, &authority, PublishGenerationRequest { expected: predecessor, ..request },
        intent, limits(), &reopened, &cancellation,
        &mut stale_owner, Some(commit), None,
    ).await?;
    assert!(matches!(publication.0.outcome, AppendOutcome::AlreadyCommitted(_)));
    if let AppendOutcome::AlreadyCommitted(recovered) = &publication.0.outcome {
        assert_eq!(recovered.operation_id, request.operation_id);
        assert_eq!(recovered.previous_digest, request.expected.digest);
        assert_eq!(decode_published_generation(&recovered.payload, 4_096)?.generation_root, root);
    }
    assert_eq!(seals.load(std::sync::atomic::Ordering::SeqCst), 0);
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
        &self, object: ObjectId, bytes: acyclic_stream::OwnedBytes, budget: WorkBudget, cancellation: &CancellationToken,
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
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let owner_calls = calls.clone();
    let mut guard = move |_: &crate::NativeMaterializationWork| {
        owner_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(authority.head(request.authority_id, WorkBudget::UNBOUNDED)?.value, request.expected);
    Ok(())
}

#[test]
fn publication_evidence_v2_validates_complete_frame_before_legacy_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([52; 16]);
    let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([53; 32]) };
    let evidence = b"immutable original operation data";
    let mut bytes = Vec::new();
    write_publication_payload_with_evidence(&mut bytes, volume_id, root, Some((evidence, original_evidence_digest(evidence))));
    let decoded = decode_publication_evidence_with_steps::<CanonicalDecodeError>(&bytes, bytes.len() as u64, |_| Ok(()))?;
    assert_eq!(decoded.publication, PublishedGeneration { volume_id, generation_root: root });
    assert_eq!(decoded.original_evidence, Some(evidence.as_slice()));
    assert_eq!(decoded.evidence_digest, Some(original_evidence_digest(evidence)));
    assert_eq!(decoded.original_evidence.ok_or("missing original data")?.as_ptr(),
        bytes[publication_payload_length() + 4..].as_ptr());
    assert_eq!(decode_published_generation(&bytes, bytes.len() as u64)?, decoded.publication);
    assert!(matches!(decode_published_generation(&bytes, bytes.len() as u64 - 1),
        Err(CanonicalDecodeError::ObjectTooLarge { .. })));
    for offset in [PAYLOAD_DOMAIN.len(), PAYLOAD_DOMAIN.len() + 2 + 16, publication_payload_length() + 4, bytes.len() - 1] {
        let mut malformed = bytes.clone();
        malformed[offset] = u8::MAX;
        assert!(decode_published_generation(&malformed, u64::MAX).is_err());
    }
    for length in [0_u32, u32::MAX] {
        let mut malformed = bytes.clone();
        let offset = publication_payload_length();
        malformed[offset..offset + 4].copy_from_slice(&length.to_le_bytes());
        assert!(decode_published_generation(&malformed, bytes.len() as u64).is_err());
    }
    let mut suffixed = bytes.clone();
    suffixed.push(0);
    assert!(matches!(decode_published_generation(&suffixed, u64::MAX), Err(CanonicalDecodeError::TrailingBytes)));
    assert!(decode_published_generation(&bytes[..bytes.len() - 1], u64::MAX).is_err());
    let legacy = encode_publication_payload(volume_id, root);
    let decoded = decode_publication_evidence_with_steps::<CanonicalDecodeError>(&legacy, u64::MAX, |_| Ok(()))?;
    assert_eq!(decoded.original_evidence, None);
    assert_eq!(decoded.evidence_digest, None);
    assert_eq!(decoded.publication, PublishedGeneration { volume_id, generation_root: root });
    let mut suffixed = legacy.clone();
    suffixed.extend_from_slice(evidence);
    assert!(matches!(decode_published_generation(&suffixed, u64::MAX), Err(CanonicalDecodeError::TrailingBytes)));
    let large_evidence = vec![7; 4_096];
    let mut large = Vec::new();
    write_publication_payload_with_evidence(&mut large, volume_id, root,
        Some((&large_evidence, original_evidence_digest(&large_evidence))));
    assert!(matches!(decode_published_generation(&large, 4_096), Err(CanonicalDecodeError::ObjectTooLarge { .. })));
    assert_eq!(decode_published_generation(&large, large.len() as u64)?, decoded.publication);
    Ok(())
}
