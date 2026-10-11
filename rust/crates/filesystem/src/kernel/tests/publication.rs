use super::*;
use crate::foundation::{DurableCommit, ProposedCommit, Sequence};
use crate::kernel::{
    Extent, ExtentKind, ExtentPage, GenerationExportManifestError, LogicalName, NameEncoding,
    PublicationEvidenceError, TreeEntry, decode_generation_publication_evidence,
    encode_extent_page, encode_generation_export_manifest, encode_generation_publication_evidence,
};
use crate::model::{Lifecycle, VolumeConfig};
use crate::storage::{
    AuthorityResult, ObjectFailure, ObjectRead, ObjectReadRequest, ObjectReceipt, ObjectResult,
    ReplayLimit,
};
use crate::{Collection, GenerationForkSource, GuardedAppend, PublicationHold, PublicationScope};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;
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
    let prepared = prepare_publication(proof.clone(), request, None, WorkBudget::UNBOUNDED)?;
    let append = GuardedAppend {
        authority_id: request.authority_id,
        epoch: request.epoch,
        expected: request.expected,
        commit: prepared.commit,
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
                payload: Bytes::from(encode_publication_payload(
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

struct PublicationBoundary {
    collection: Arc<Collection>,
    events: Mutex<Vec<&'static str>>,
}

impl PublicationBoundary {
    fn assert_held(&self) {
        // The real collection gate, not a synthetic "hold live" flag.
        assert!(crate::async_storage::poll_ready(self.collection.begin()).is_none());
    }

    async fn record(&self, expected: &[&'static str], event: &'static str) {
        let mut events = self.events.lock().await;
        assert_eq!(events.as_slice(), expected);
        events.push(event);
    }
}

struct CustodyObjectStore {
    inner: MemoryObjectStore,
    boundary: Arc<PublicationBoundary>,
    reject_hook: bool,
    original: Mutex<Option<(PublishGenerationRequest, GenerationProof, GuardedAppend)>>,
}

impl crate::AsyncObjectStore for CustodyObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        crate::AsyncObjectStore::put(&self.inner, object_id, bytes, budget, cancellation).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        crate::AsyncObjectStore::read(
            &self.inner, object_id, maximum_bytes, budget, cancellation,
        ).await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        crate::AsyncObjectStore::read_many(&self.inner, requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        crate::AsyncObjectStore::contains(&self.inner, object_id, budget, cancellation).await
    }

    fn collection(&self) -> Option<&Arc<Collection>> {
        Some(&self.boundary.collection)
    }

    async fn flush_before_publish(
        &self,
        scope: PublicationScope<'_>,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<PublicationHold> {
        cancellation.check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let PublicationScope::Closure { objects, proven_at } = scope else {
            panic!("generation publication must flush its exact proven closure");
        };
        let hold = self.boundary.collection.admit(objects, |_| false, proven_at).await
            .map_err(ObjectFailure::before_work)?
            .into_hold();
        self.boundary.record(&[], "flush").await;
        Ok(ObjectReceipt { value: hold, work: WorkCounters::default() })
    }

    async fn prepare_generation_publication(
        &self,
        request: &PublishGenerationRequest,
        proof: &GenerationProof,
        append: &GuardedAppend,
        scope: PublicationScope<'_>,
        _budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation.check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        self.boundary.assert_held();
        self.boundary.record(&["flush"], "hook").await;
        let PublicationScope::Closure { objects, proven_at } = scope else {
            panic!("custody hook must receive its exact proven closure");
        };
        assert_eq!(objects, proof.objects);
        assert_eq!(proven_at, self.boundary.collection.sweeps());
        assert_eq!(request.authority_id, append.authority_id);
        assert_eq!(request.epoch, append.epoch);
        assert_eq!(request.expected, append.expected);
        assert_eq!(request.operation_id, append.commit.operation_id);
        assert_eq!(append.permit, PublicationPermit::Unrestricted);
        assert_eq!(proof.root.volume_id, request.volume_id);
        assert_eq!(
            append.commit.payload.as_ref(),
            encode_publication_payload(request.volume_id, request.generation_root),
        );
        *self.original.lock().await = Some((*request, proof.clone(), append.clone()));
        if self.reject_hook {
            return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                "original publication custody unavailable".to_owned(),
            )));
        }
        Ok(ObjectReceipt { value: (), work: WorkCounters::default() })
    }
}

struct CustodyAuthorityStore {
    inner: MemoryAuthorityStore,
    boundary: Arc<PublicationBoundary>,
}

impl crate::AsyncAuthorityStore for CustodyAuthorityStore {
    async fn fork_generation_authority(
        &self,
        source: GenerationForkSource,
        destination_authority: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<crate::storage::CreateAuthorityOutcome> {
        crate::AsyncAuthorityStore::fork_generation_authority(
            &self.inner, source, destination_authority, operation_id, budget, cancellation,
        ).await
    }

    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<crate::storage::CreateAuthorityOutcome> {
        crate::AsyncAuthorityStore::create_authority(
            &self.inner, authority_id, genesis_epoch, budget, cancellation,
        ).await
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        crate::AsyncAuthorityStore::head(&self.inner, authority_id, budget, cancellation).await
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<AppendOutcome> {
        self.boundary.assert_held();
        self.boundary.record(&["flush", "hook"], "cas").await;
        // Only the actual Memory authority decides whether this append commits.
        crate::AsyncAuthorityStore::compare_and_append(
            &self.inner, authority_id, epoch, expected, commit, budget, cancellation,
        ).await
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        crate::AsyncAuthorityStore::replay(
            &self.inner, authority_id, after, limit, budget, cancellation,
        ).await
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        crate::AsyncAuthorityStore::fence(
            &self.inner, authority_id, expected, budget, cancellation,
        ).await
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        crate::AsyncAuthorityStore::find_operation(
            &self.inner, authority_id, operation_id, budget, cancellation,
        ).await
    }
}

#[test]
fn async_publication_flushes_then_prepares_original_custody_while_hold_covers_hook_and_cas()
-> Result<(), Box<dyn std::error::Error>> {
    for reject_hook in [false, true] {
        let boundary = Arc::new(PublicationBoundary {
            collection: Arc::new(Collection::default()),
            events: Mutex::new(Vec::new()),
        });
        let objects = CustodyObjectStore {
            inner: MemoryObjectStore::default(),
            boundary: Arc::clone(&boundary),
            reject_hook,
            original: Mutex::new(None),
        };
        let authority = CustodyAuthorityStore {
            inner: MemoryAuthorityStore::default(),
            boundary: Arc::clone(&boundary),
        };
        let volume_id = VolumeId::from_bytes([91; 16]);
        let authority_id = volume_authority_id(volume_id);
        let crate::storage::CreateAuthorityOutcome::Created(head) =
            authority.inner.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?
                .value
        else {
            return Err("fixture authority unexpectedly existed".into());
        };
        let request = PublishGenerationRequest {
            authority_id,
            volume_id,
            epoch: Epoch::GENESIS,
            expected: head,
            operation_id: OperationId::from_bytes([92; 16]),
            generation_root: fixture(&objects.inner, volume_id)?,
        };
        let result = crate::async_storage::poll_ready(publish_generation_async(
            &objects, &authority, request, limits(), WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )).ok_or("Memory publication unexpectedly blocked")?;
        let original = objects.original.try_lock().expect("completed hook releases original custody");
        let (original_request, original_proof, original_append) =
            original.as_ref().ok_or("custody hook did not receive preparation")?;
        assert_eq!(*original_request, request);
        if reject_hook {
            let failure = result.err().ok_or("failed custody hook allowed publication")?;
            assert_eq!(
                boundary.events.try_lock().expect("publication releases event lock").as_slice(),
                &["flush", "hook"],
            );
            assert_eq!(failure.work.authority_records_appended, 0);
            assert!(matches!(
                &failure.error,
                PublicationError::Objects(ObjectStoreError::Rejected(_))
            ));
            assert_eq!(authority.inner.head(authority_id, WorkBudget::UNBOUNDED)?.value, head);
            assert!(authority.inner.find_operation(
                authority_id, request.operation_id, WorkBudget::UNBOUNDED,
            )?.value.is_none());
        } else {
            let receipt = result?;
            assert_eq!(
                boundary.events.try_lock().expect("publication releases event lock").as_slice(),
                &["flush", "hook", "cas"],
            );
            assert_eq!(&receipt.proof, original_proof);
            let AppendOutcome::Committed(commit) = receipt.outcome else {
                return Err("actual Memory authority did not commit".into());
            };
            assert_eq!(commit.operation_id, original_append.commit.operation_id);
            assert_eq!(commit.fingerprint, original_append.commit.fingerprint);
            assert_eq!(commit.payload, original_append.commit.payload);
            assert_eq!(authority.inner.find_operation(
                authority_id, request.operation_id, WorkBudget::UNBOUNDED,
            )?.value, Some(commit.clone()));
            assert_eq!(
                authority.inner.head(authority_id, WorkBudget::UNBOUNDED)?.value,
                Head { epoch: commit.epoch, sequence: commit.sequence, digest: commit.digest },
            );
        }
        // Success and failure both release the hold only after the engine exits.
        assert!(crate::async_storage::poll_ready(boundary.collection.begin()).is_some());
    }
    Ok(())
}

#[test]
fn historical_publication_evidence_binds_original_memory_commit_not_later_head()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([101; 16]);
    let authority_id = volume_authority_id(volume_id);
    let crate::storage::CreateAuthorityOutcome::Created(head) =
        authority.create_authority(authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED)?.value
    else {
        return Err("fixture authority unexpectedly existed".into());
    };
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: Epoch::GENESIS,
        expected: head,
        operation_id: OperationId::from_bytes([102; 16]),
        generation_root: fixture(&objects, volume_id)?,
    };
    let proof = prove_generation_closure(
        &objects, request.generation_root, limits(), WorkBudget::UNBOUNDED,
    )?;
    let prepared = prepare_publication(proof.clone(), request, None, WorkBudget::UNBOUNDED)?;
    let append = GuardedAppend {
        authority_id,
        epoch: request.epoch,
        expected: head,
        commit: prepared.commit,
        permit: PublicationPermit::Unrestricted,
    };
    let evidence = crate::kernel::HistoricalGenerationEvidence::from(
        encode_generation_publication_evidence(
            &request, &proof, &append, VolumeConfig::portable(Lifecycle::Durable), 64 * 1024,
        )?,
    );
    let AppendOutcome::Committed(original) = authority.compare_and_append(
        authority_id, append.epoch, append.expected, append.commit.clone(), WorkBudget::UNBOUNDED,
    )?.value else {
        return Err("actual Memory authority did not commit original append".into());
    };
    let later = publish_generation(
        &objects, &authority,
        PublishGenerationRequest {
            operation_id: OperationId::from_bytes([103; 16]),
            expected: Head {
                epoch: original.epoch,
                sequence: original.sequence,
                digest: original.digest,
            },
            ..request
        },
        limits(), WorkBudget::UNBOUNDED,
    )?;
    let AppendOutcome::Committed(later) = later.outcome else {
        return Err("actual Memory authority did not advance".into());
    };
    let decoded = crate::kernel::decode_historical_generation_evidence(
        &evidence, authority_id, &original, 64 * 1024, proof.object_count,
    )?;
    assert_eq!(decoded.append, append);
    assert_eq!(decoded.root, proof.root);
    assert_eq!(decoded.manifest.objects, proof.objects);
    assert_eq!(decoded.logical_file_bytes, proof.logical_file_bytes);
    assert!(matches!(
        crate::kernel::decode_historical_generation_evidence(
            &evidence, authority_id, &later, 64 * 1024, proof.object_count,
        ),
        Err(PublicationEvidenceError::Mismatch)
    ));
    for altered in [
        DurableCommit { operation_id: later.operation_id, ..original.clone() },
        DurableCommit { fingerprint: Digest::from_bytes([104; 32]), ..original.clone() },
        DurableCommit { payload: Bytes::from_static(b"altered"), ..original.clone() },
        DurableCommit { epoch: Epoch::new(2)?, ..original.clone() },
        DurableCommit { sequence: later.sequence, ..original.clone() },
        DurableCommit { previous_digest: later.digest, ..original.clone() },
        DurableCommit { digest: later.digest, ..original.clone() },
    ] {
        assert!(matches!(
            crate::kernel::decode_historical_generation_evidence(
                &evidence, authority_id, &altered, 64 * 1024, proof.object_count,
            ),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    assert!(matches!(
        crate::kernel::decode_historical_generation_evidence(
            &evidence, AuthorityId::from_bytes([105; 16]), &original, 64 * 1024, proof.object_count,
        ),
        Err(PublicationEvidenceError::Mismatch)
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
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let owner_calls = calls.clone();
    let owner_original = original.clone();
    let mut owner = move |same: &crate::NativeMaterializationWork| {
        assert!(owner_original.shares_ledger(same));
        assert_eq!(same.receipt().authority_records_appended, 0);
        owner_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
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
        .map_err(|failure| failure.into_error())?;
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
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let owner_calls = calls.clone();
    let mut owner = move |_: &crate::NativeMaterializationWork| {
        owner_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
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
    assert!(prepare_publication_commit(request, None, None, &mut accounting, &cancellation).is_err());
    assert_eq!(original.receipt().allocation_operations, initial_operations + 2);
    assert_eq!(original.receipt().bytes_encoded,
        (publication_payload_length() + fingerprint_input_length(None)) as u64);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt().authority_records_appended, 0);
    assert_eq!(authority.head(request.authority_id, WorkBudget::UNBOUNDED)?.value, request.expected);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_permit_context_uses_canonical_recipe_and_admits_exact_hash_before_cpu()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let permits = [
        (PublicationPermit::Unrestricted, 0),
        (PublicationPermit::Lease {
            authority_id: [43; 16], workspace_id: [44; 16], lease_id: [45; 16], expires_at_millis: 46,
        }, b"acyclic-fs-publication-lease-v1\0".len() as u64 + 1 + 48 + 8),
        (PublicationPermit::Reservation {
            operation_id: [47; 16], gate_tail: 48,
            expected: Head { epoch: Epoch::GENESIS, sequence: crate::Sequence::new(49), digest: Digest::from_bytes([50; 32]) },
        }, b"acyclic-fs-publication-reservation-v1\0".len() as u64 + 1 + 16 + 8 + 8 + 8 + 32),
    ];
    for (permit, base_hash_bytes) in permits {
        for context in [None, Some(Digest::from_bytes([51; 32]))] {
            let hashed = base_hash_bytes + if base_hash_bytes != 0 && context.is_some() { 32 } else { 0 };
            let mut budget = WorkBudget::UNBOUNDED;
            budget.bytes_hashed = hashed;
            let original = crate::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error)?;
            let before = original.receipt();
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
            assert_eq!(permit_context_with_accounting(context, permit, &mut accounting)?, permit_context(context, permit));
            assert_eq!(original.receipt().bytes_hashed, hashed);
            assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
            if hashed != 0 {
                budget.bytes_hashed = hashed - 1;
                let denied = crate::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(error, _)| error)?;
                let before = denied.receipt();
                let mut accounting = crate::kernel::allocation::KernelAccounting::native(&denied, &cancellation);
                assert!(permit_context_with_accounting(context, permit, &mut accounting).is_err());
                assert_eq!(denied.receipt().bytes_hashed, 0);
                assert_eq!(denied.receipt().allocation_operations, before.allocation_operations);
            }
        }
    }
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
    let mut suffixed = legacy;
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

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_evidence_decoder_denies_hash_before_cpu_and_retains_completed_cancelled_hash()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([54; 16]);
    let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([55; 32]) };
    let evidence = b"immutable original operation data";
    let mut bytes = Vec::new();
    write_publication_payload_with_evidence(&mut bytes, volume_id, root, Some((evidence, original_evidence_digest(evidence))));
    let cancellation = CancellationToken::new();
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_hashed = 0;
    let original = crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    assert!(decode_published_generation_with_accounting(&bytes, bytes.len() as u64, &mut accounting).is_err());
    assert_eq!(original.receipt().bytes_hashed, 0);
    assert_eq!(original.receipt().bytes_copied, 2 + 16 + 1 + 32 + 4 + 32);
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let mut observer = super::super::file_read::OriginalReadAccounting::new(&mut accounting)?;
    let result = decode_publication_evidence_with_steps::<crate::FsError>(&bytes, bytes.len() as u64, |step| {
        if matches!(&step, super::super::codec::ReadAllocationStep::Hashed(_)) { cancellation.cancel(); }
        observer.observe(step).map_err(super::super::file_read::ReadDecodeError::into_original_fs_error)
    });
    drop(observer.into_allocation());
    assert!(result.is_err());
    assert_eq!(original.receipt().bytes_hashed, (EVIDENCE_DOMAIN.len() + evidence.len()) as u64);
    assert_eq!(original.receipt().bytes_copied, 2 + 16 + 1 + 32 + 4 + 32);
    assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn publication_payload_aliases_hold_actual_original_evidence_until_last_owner_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let (data, custody) = original.copy_bytes(b"immutable original operation data")?;
    let evidence = acyclic_stream::OwnedBytes::from_vec_with_guard(data, custody, original.runtime_meter_owner())
        .map_err(|failure| failure.into_error())?;
    assert!(original.live_allocation_bytes() > baseline);
    let authority = MemoryAuthorityStore::default();
    let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([57; 32]) };
    let request = native_request(&authority, VolumeId::from_bytes([56; 16]), root)?;
    let context = Digest::from_bytes([58; 32]);
    let mut accounting = PublicationAccounting::Native(&original);
    let commit = prepare_publication_commit(request, Some(context), Some(evidence), &mut accounting, &cancellation)?;
    let retained = original.live_allocation_bytes();
    let before_clone = original.receipt().allocation_operations;
    let alias = commit.clone();
    assert_eq!(original.receipt().allocation_operations, before_clone);
    drop(commit);
    assert_eq!(original.live_allocation_bytes(), retained);
    let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
    let decoded = decode_publication_evidence_with_accounting(&alias.payload, 4_096, &mut accounting)?;
    assert_eq!(decoded.original_evidence, Some(b"immutable original operation data".as_slice()));
    let before = original.receipt();
    assert_eq!(contextual_publication_fingerprint_from_payload_with_accounting(
        request, context, &alias.payload, &mut accounting,
    )?, alias.fingerprint);
    let after = original.receipt();
    assert_eq!(after.allocation_operations, before.allocation_operations);
    assert_eq!(after.bytes_copied, before.bytes_copied);
    assert_eq!(after.bytes_encoded, before.bytes_encoded);
    assert_eq!(after.bytes_hashed - before.bytes_hashed,
        (fingerprint_domain(Some(context)).len() + fingerprint_input_length(Some(context))
            - publication_payload_length() + alias.payload.len()) as u64);
    assert_eq!(original.live_allocation_bytes(), retained);
    assert_ne!(contextual_publication_fingerprint_with_accounting(request, context, &mut accounting)?, alias.fingerprint);
    let changed_evidence = b"altered original operation data";
    let mut changed = Vec::new();
    write_publication_payload_with_evidence(&mut changed, request.volume_id, request.generation_root,
        Some((changed_evidence, original_evidence_digest(changed_evidence))));
    assert_ne!(contextual_publication_fingerprint_from_payload_with_accounting(
        request, context, &changed, &mut accounting,
    )?, alias.fingerprint);
    drop(alias);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn original_borrowed_payload_fingerprint_preserves_legacy_recipe_without_allocation_and_denies_before_hash()
-> Result<(), Box<dyn std::error::Error>> {
    let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([61; 32]) };
    let authority = MemoryAuthorityStore::default();
    let request = native_request(&authority, VolumeId::from_bytes([62; 16]), root)?;
    let context = Digest::from_bytes([63; 32]);
    let payload = encode_publication_payload(request.volume_id, root);
    let expected = contextual_publication_fingerprint(request, context);
    let hashed = (fingerprint_domain(Some(context)).len() + fingerprint_input_length(Some(context))) as u64;
    let cancellation = CancellationToken::new();
    for maximum in [hashed, hashed - 1] {
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_hashed = maximum;
        let original = crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let before = original.receipt();
        let live = original.live_allocation_bytes();
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let result = contextual_publication_fingerprint_from_payload_with_accounting(request, context, &payload, &mut accounting);
        if maximum == hashed {
            assert_eq!(result?, expected);
            assert_eq!(original.receipt().bytes_hashed - before.bytes_hashed, hashed);
        } else {
            assert!(result.is_err());
            assert_eq!(original.receipt().bytes_hashed, before.bytes_hashed);
        }
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(original.receipt().bytes_encoded, before.bytes_encoded);
        assert_eq!(original.live_allocation_bytes(), live);
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_original_fingerprint_survives_real_reservation_rotation_without_authorizing_historical_permit()
-> Result<(), Box<dyn std::error::Error>> {
    let objects = MemoryObjectStore::default();
    let authority = MemoryAuthorityStore::default();
    let volume_id = VolumeId::from_bytes([64; 16]);
    let (root, _) = native_fixture(&objects, volume_id)?;
    let request = native_request(&authority, volume_id, root)?;
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|(error, _)| error)?;
    let old = crate::AsyncAuthorityStore::reserve_publication_with_native_work(
        &authority, request.authority_id, request.expected, request.operation_id, &original, &cancellation,
    ).await?;
    let old_permit = PublicationPermit::Reservation {
        operation_id: old.operation_id.into_bytes(), gate_tail: old.gate_tail, expected: old.expected,
    };
    let context = Digest::from_bytes([65; 32]);
    let (data, custody) = original.copy_bytes(b"immutable original reservation operation data")?;
    let evidence = acyclic_stream::OwnedBytes::from_vec_with_guard(data, custody, original.runtime_meter_owner())
        .map_err(|failure| failure.into_error())?;
    let expected_fingerprint = {
        let mut binding = crate::kernel::allocation::KernelAccounting::native(&original, &cancellation);
        let bound = permit_context_with_accounting(Some(context), old_permit, &mut binding)?;
        let mut accounting = PublicationAccounting::Native(&original);
        prepare_publication_commit(request, bound, Some(evidence.clone()), &mut accounting, &cancellation)?.fingerprint
    };
    crate::AsyncAuthorityStore::release_publication_with_native_work(&authority, old, &original, &cancellation).await?;
    let current = crate::AsyncAuthorityStore::reserve_publication_with_native_work(
        &authority, request.authority_id, request.expected, request.operation_id, &original, &cancellation,
    ).await?;
    assert_ne!(current.gate_tail, old.gate_tail);
    let current_permit = PublicationPermit::Reservation {
        operation_id: current.operation_id.into_bytes(), gate_tail: current.gate_tail, expected: current.expected,
    };
    let seals = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let owner_seals = seals.clone();
    let mut owner = move |_: &crate::NativeMaterializationWork| {
        owner_seals.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::future::ready(Ok::<(), NativePublicationTestError>(()))
    };
    let rejected = publish_generation_async_guarded_with_native_work(
        &objects, &authority, request,
        PublicationIntent::guarded(Some(context), old_permit).with_original_evidence(Some(evidence.clone()))
            .with_original_fingerprint_permit(Some(old_permit)),
        limits(), &original, &cancellation, &mut owner, None, None,
    ).await?;
    assert!(matches!(rejected.0.outcome, AppendOutcome::Conflict { .. }));
    assert_eq!(original.receipt().authority_records_appended, 0);
    let published = publish_generation_async_guarded_with_native_work(
        &objects, &authority, request,
        PublicationIntent::guarded(Some(context), current_permit).with_original_evidence(Some(evidence.clone()))
            .with_original_fingerprint_permit(Some(old_permit)),
        limits(), &original, &cancellation, &mut owner, None, None,
    ).await?;
    let commit = match published.0.outcome {
        AppendOutcome::Committed(commit) => commit,
        _ => return Err("current authenticated reservation did not publish".into()),
    };
    assert_eq!(commit.fingerprint, expected_fingerprint);
    assert_eq!(original.receipt().authority_records_appended, 1);
    assert!(seals.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let (data, custody) = original.copy_bytes(b"altered original reservation operation data")?;
    let altered = acyclic_stream::OwnedBytes::from_vec_with_guard(data, custody, original.runtime_meter_owner())
        .map_err(|failure| failure.into_error())?;
    let before = seals.load(std::sync::atomic::Ordering::SeqCst);
    let changed = publish_generation_async_guarded_with_native_work(
        &objects, &authority, request,
        PublicationIntent::guarded(Some(context), current_permit).with_original_evidence(Some(altered))
            .with_original_fingerprint_permit(Some(old_permit)),
        limits(), &original, &cancellation, &mut owner, Some(commit), None,
    ).await?;
    assert!(matches!(changed.0.outcome, AppendOutcome::IdempotencyConflict { .. }));
    assert_eq!(original.receipt().authority_records_appended, 1);
    assert_eq!(seals.load(std::sync::atomic::Ordering::SeqCst), before);
    Ok(())
}
