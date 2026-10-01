use super::*;
use crate::GuardedAppend;
use crate::foundation::FileId;
use crate::kernel::{
    DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    MetadataField, TreePage, encode_file_metadata, encode_file_table_page, encode_generation_root,
    encode_tree_page,
};
use crate::kernel::{
    PublicationEvidenceError, decode_generation_publication_evidence,
    encode_generation_publication_evidence,
};
use crate::memory::{MemoryAuthorityStore, MemoryObjectStore};
use crate::model::{Lifecycle, VolumeConfig};
use crate::storage::{
    AuthorityStore, FenceOutcome, ObjectKind, ObjectStore, ObjectStoreError, object_digest,
};
use sha2::{Digest as _, Sha256};

fn evidence_fixture() -> Result<
    (
        PublishGenerationRequest,
        GenerationProof,
        GuardedAppend,
        VolumeConfig,
    ),
    Box<dyn std::error::Error>,
> {
    let objects = MemoryObjectStore::default();
    let volume_id = VolumeId::from_bytes([2; 16]);
    let authority_id = volume_authority_id(volume_id);
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected: Head::genesis(Epoch::GENESIS),
        operation_id: OperationId::from_bytes([4; 16]),
        generation_root: fixture(&objects, volume_id)?,
    };
    let proof = prove_generation_closure(
        &objects,
        request.generation_root,
        limits(),
        WorkBudget::UNBOUNDED,
    )?;
    let prepared = prepare_publication(proof.clone(), request, None, WorkBudget::UNBOUNDED)?;
    Ok((
        request,
        proof,
        GuardedAppend {
            authority_id,
            epoch: request.epoch,
            expected: request.expected,
            commit: prepared.commit,
            permit: PublicationPermit::Unrestricted,
        },
        VolumeConfig::portable(Lifecycle::Ephemeral),
    ))
}

#[test]
fn publication_evidence_round_trip_preserves_original_preparation_and_all_permits()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, mut append, config) = evidence_fixture()?;
    for permit in [
        PublicationPermit::Unrestricted,
        PublicationPermit::Lease {
            authority_id: [5; 16],
            workspace_id: [6; 16],
            lease_id: [7; 16],
            expires_at_millis: 1234,
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
        assert_eq!(
            encoded.sha256,
            <[u8; 32]>::from(Sha256::digest(&encoded.bytes))
        );
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
        assert_eq!(decoded.manifest.objects, proof.objects);
        assert_eq!(decoded.manifest.file_count, proof.file_count);
        assert_eq!(decoded.manifest.generation_id, proof.generation_id);
        assert_eq!(decoded.logical_file_bytes, proof.logical_file_bytes);
        assert_eq!(
            encode_generation_publication_evidence(
                &decoded.request,
                &proof,
                &decoded.append,
                decoded.manifest.config,
                64 * 1024,
            )?,
            encoded
        );
    }
    Ok(())
}

#[test]
fn publication_evidence_rejects_digest_changes_and_byte_and_object_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, append, config) = evidence_fixture()?;
    let encoded =
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
    let mut digest = encoded.sha256;
    digest[0] ^= 1;
    assert!(matches!(
        decode_generation_publication_evidence(
            &encoded.bytes,
            &digest,
            64 * 1024,
            proof.object_count,
        ),
        Err(PublicationEvidenceError::Mismatch)
    ));
    let mut altered = encoded.bytes.to_vec();
    altered[0] ^= 1;
    assert!(matches!(
        decode_generation_publication_evidence(
            &altered,
            &encoded.sha256,
            64 * 1024,
            proof.object_count,
        ),
        Err(PublicationEvidenceError::Mismatch)
    ));
    for bound in [0, encoded.bytes.len() as u64 - 1] {
        assert!(matches!(
            encode_generation_publication_evidence(&request, &proof, &append, config, bound,),
            Err(PublicationEvidenceError::TooLarge)
        ));
        assert!(matches!(
            decode_generation_publication_evidence(
                &encoded.bytes,
                &encoded.sha256,
                bound,
                proof.object_count,
            ),
            Err(PublicationEvidenceError::TooLarge)
        ));
    }
    assert!(matches!(
        decode_generation_publication_evidence(
            &encoded.bytes,
            &encoded.sha256,
            64 * 1024,
            proof.object_count - 1,
        ),
        Err(PublicationEvidenceError::Manifest(
            crate::kernel::GenerationExportManifestError::TooManyObjects,
        ))
    ));
    Ok(())
}

#[test]
fn publication_evidence_rejects_mismatched_request_proof_and_append()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, append, config) = evidence_fixture()?;
    for changed in [
        PublishGenerationRequest {
            volume_id: VolumeId::from_bytes([8; 16]),
            ..request
        },
        PublishGenerationRequest {
            operation_id: OperationId::from_bytes([8; 16]),
            ..request
        },
        PublishGenerationRequest {
            epoch: Epoch::new(2)?,
            ..request
        },
        PublishGenerationRequest {
            expected: Head::genesis(Epoch::new(2)?),
            ..request
        },
    ] {
        assert!(matches!(
            encode_generation_publication_evidence(&changed, &proof, &append, config, 64 * 1024,),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    let mut changed = proof.clone();
    changed.object_count += 1;
    assert!(matches!(
        encode_generation_publication_evidence(&request, &changed, &append, config, 64 * 1024,),
        Err(PublicationEvidenceError::Mismatch)
    ));
    let mut changed = append.clone();
    changed.commit.operation_id = OperationId::from_bytes([8; 16]);
    assert!(matches!(
        encode_generation_publication_evidence(&request, &proof, &changed, config, 64 * 1024,),
        Err(PublicationEvidenceError::Mismatch)
    ));
    Ok(())
}

#[test]
fn publication_evidence_rejects_noncanonical_manifest_and_trailing_bytes_even_with_new_digest()
-> Result<(), Box<dyn std::error::Error>> {
    let (request, proof, append, config) = evidence_fixture()?;
    let encoded =
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
    let projection = decode_generation_publication_evidence(
        &encoded.bytes,
        &encoded.sha256,
        64 * 1024,
        proof.object_count,
    )?;
    let manifest = crate::kernel::encode_generation_export_manifest(&projection.manifest)?;
    let offset = encoded
        .bytes
        .windows(manifest.len())
        .position(|value| value == manifest)
        .ok_or("missing embedded manifest")?;
    let mut altered = encoded.bytes.to_vec();
    let last_two_objects = offset + manifest.len() - 66;
    // Swap two adjacent 33-byte object identities without changing field lengths.
    altered[last_two_objects..last_two_objects + 66].rotate_left(33);
    let digest = Sha256::digest(&altered).into();
    assert!(matches!(
        decode_generation_publication_evidence(&altered, &digest, 64 * 1024, proof.object_count,),
        Err(PublicationEvidenceError::Manifest(
            crate::kernel::GenerationExportManifestError::NonCanonicalObjectOrder,
        ))
    ));
    let mut trailing = encoded.bytes.to_vec();
    trailing.push(0);
    let digest = Sha256::digest(&trailing).into();
    assert!(matches!(
        decode_generation_publication_evidence(&trailing, &digest, 64 * 1024, proof.object_count,),
        Err(PublicationEvidenceError::Decode(
            CanonicalDecodeError::TrailingBytes
        ))
    ));
    Ok(())
}

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
        Err(CanonicalDecodeError::Invariant(message))
            if message == "publication object is not a generation root"
    ));

    let mut unknown_kind = encoded;
    unknown_kind[kind_offset] = u8::MAX;
    assert!(matches!(
        decode_published_generation(&unknown_kind, u64::MAX),
        Err(CanonicalDecodeError::Invariant(message))
            if message == "unknown publication object kind"
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
                budget.peak_allocation_bytes = closure.work.peak_allocation_bytes;
            }
            _ => return Err("unknown test counter".into()),
        }
        let failure = publish_generation(&objects, &authority, request, limits(), budget)
            .err()
            .ok_or("unadmitted publication succeeded")?;
        assert!(matches!(failure.error, PublicationError::Work(_)));
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
