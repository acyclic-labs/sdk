use super::*;

#[test]
fn publication_payload_keeps_original_credit_until_the_last_retained_bytes_drop() -> TestResult {
    let cancel = CancellationToken::new();
    let mut ledger = crate::kernel::allocation::AllocationLedger::default();
    let mut work = WorkCounters::UNCHARGED;
    let volume_id = VolumeId::from_bytes([127; 16]);
    let request = PublishGenerationRequest {
        authority_id: volume_authority_id(volume_id),
        volume_id,
        epoch: Epoch::GENESIS,
        expected: Head::genesis(Epoch::GENESIS),
        operation_id: OperationId::from_bytes([128; 16]),
        generation_root: ObjectId {
            storage: None,
            kind: ObjectKind::GenerationRoot,
            digest: Digest::ZERO,
        },
    };
    let commit = {
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut ledger,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancel,
        );
        prepare_publication_commit(request, None, None, None, &mut accounting, &cancel)?
    };
    let retained = ledger.live_bytes();
    assert!(retained >= commit.payload.len() as u64);
    let second = commit.payload.clone();
    drop(commit);
    assert_eq!(
        ledger.live_bytes(),
        retained,
        "one clone retired shared publication bytes"
    );
    drop(second);
    assert!(
        ledger.live_bytes() < retained,
        "last payload leaked its original standalone credit"
    );
    Ok(())
}

fn encode_publication_payload(volume_id: VolumeId, generation_root: ObjectId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(publication_payload_length());
    write_publication_payload(&mut bytes, volume_id, generation_root, None, None);
    bytes
}

async fn evidence_fixture() -> TestResult<(
    PublishGenerationRequest,
    GenerationProof,
    GuardedAppend,
    VolumeConfig,
)> {
    let objects = MemoryObjectStore::default();
    let volume_id = VolumeId::from_bytes([61; 16]);
    let metadata = metadata_object(&objects).await?;
    let extents = crate::test_support::put_extent_page(
        &objects,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: u64::MAX,
            kind: ExtentKind::Hole,
        }]),
        8,
    )
    .await?;
    let first = FileId::from_bytes([2; 16]);
    let second = FileId::from_bytes([3; 16]);
    let [tree, table, root] = generation(
        &objects,
        volume_id,
        FileId::from_bytes([1; 16]),
        metadata,
        vec![entry(b"first", first)?, entry(b"second", second)?],
        vec![
            regular(first, metadata, u64::MAX, extents),
            regular(second, metadata, u64::MAX, extents),
        ],
        Vec::new(),
    )
    .await?;
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
    let proof = prove_generation_closure(
        &objects,
        root,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let mut expected_objects = vec![metadata, extents, tree, table, root];
    expected_objects.sort_unstable();
    assert_eq!(proof.objects, expected_objects);
    assert_eq!(crate::foundation::usize_to_u64(proof.objects.len()), 5);
    assert_eq!(proof.file_count, 3);
    assert_eq!(proof.logical_file_bytes, u128::from(u64::MAX) * 2);
    let cancellation = CancellationToken::new();
    let commit = prepare_publication_commit(
        request,
        None,
        None,
        None,
        &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
        &cancellation,
    )?;
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

#[tokio::test]
async fn publication_evidence_preserves_original_complete_closure_and_every_permit() -> TestResult {
    let (request, proof, mut append, config) = evidence_fixture().await?;
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
        assert_eq!(
            encoded.sha256,
            <[u8; 32]>::from(Sha256::digest(&encoded.bytes))
        );
        let decoded = decode_generation_publication_evidence(
            &encoded.bytes,
            &encoded.sha256,
            encoded.bytes.len() as u64,
            crate::foundation::usize_to_u64(proof.objects.len()),
        )?;
        assert_eq!(decoded.request, request);
        assert_eq!(decoded.append, append);
        assert_eq!(decoded.root, proof.root);
        assert_eq!(decoded.manifest.config, config);
        assert_eq!(decoded.manifest.volume_id, request.volume_id);
        assert_eq!(decoded.manifest.generation_root, request.generation_root);
        assert_eq!(decoded.manifest.objects, proof.objects);
        assert_eq!(decoded.manifest.file_count, proof.file_count);
        assert_eq!(decoded.manifest.generation_id, proof.generation_id);
        assert_eq!(decoded.logical_file_bytes, u128::from(u64::MAX) * 2);
        assert_eq!(
            encode_generation_publication_evidence(
                &decoded.request,
                &proof,
                &decoded.append,
                decoded.manifest.config,
                encoded.bytes.len() as u64,
            )?,
            encoded
        );
    }
    Ok(())
}

#[tokio::test]
async fn publication_evidence_codec_preserves_the_complete_sealed_root_reference() -> TestResult {
    let (mut request, mut proof, mut append, config) = evidence_fixture().await?;
    let plaintext_root = request.generation_root;
    request.generation_root.storage = Some(crate::ObjectStorage {
        locator: Digest::from_bytes([81; 32]),
        root_file_id: proof.root.root_file_id,
        origin_generation: plaintext_root.digest,
    });
    proof.generation_id = crate::GenerationId::from_root(request.generation_root);
    *proof
        .objects
        .iter_mut()
        .find(|id| **id == plaintext_root)
        .ok_or("missing root")? = request.generation_root;
    let cancel = CancellationToken::new();
    append.commit = prepare_publication_commit(
        request,
        None,
        None,
        None,
        &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel),
        &cancel,
    )?;
    let encoded =
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
    let decoded =
        decode_generation_publication_evidence(&encoded.bytes, &encoded.sha256, 64 * 1024, 64)?;
    assert_eq!(decoded.request, request);
    assert_eq!(decoded.manifest.generation_id, proof.generation_id);
    assert_eq!(decoded.manifest.objects, proof.objects);
    proof.generation_id = crate::GenerationId::from_root(plaintext_root);
    assert!(matches!(
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024),
        Err(PublicationEvidenceError::Mismatch)
    ));
    Ok(())
}

#[tokio::test]
async fn publication_evidence_rejects_tampering_and_byte_and_object_bounds() -> TestResult {
    let (request, proof, append, config) = evidence_fixture().await?;
    let encoded =
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
    let mut digest = encoded.sha256;
    digest[0] ^= 1;
    assert!(matches!(
        decode_generation_publication_evidence(
            &encoded.bytes,
            &digest,
            64 * 1024,
            crate::foundation::usize_to_u64(proof.objects.len())
        ),
        Err(PublicationEvidenceError::Mismatch)
    ));
    for index in [0, encoded.bytes.len() / 2, encoded.bytes.len() - 1] {
        let mut altered = encoded.bytes.to_vec();
        altered[index] ^= 1;
        assert!(matches!(
            decode_generation_publication_evidence(
                &altered,
                &encoded.sha256,
                64 * 1024,
                crate::foundation::usize_to_u64(proof.objects.len()),
            ),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    for bound in [0, encoded.bytes.len() as u64 - 1] {
        assert!(matches!(
            encode_generation_publication_evidence(&request, &proof, &append, config, bound),
            Err(PublicationEvidenceError::TooLarge)
        ));
        assert!(matches!(
            decode_generation_publication_evidence(
                &encoded.bytes,
                &encoded.sha256,
                bound,
                crate::foundation::usize_to_u64(proof.objects.len()),
            ),
            Err(PublicationEvidenceError::TooLarge)
        ));
    }
    for bound in [0, crate::foundation::usize_to_u64(proof.objects.len()) - 1] {
        assert!(matches!(
            decode_generation_publication_evidence(
                &encoded.bytes,
                &encoded.sha256,
                64 * 1024,
                bound,
            ),
            Err(PublicationEvidenceError::Manifest(
                GenerationExportManifestError::TooManyObjects
            ))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn publication_evidence_rejects_disagreeing_request_proof_and_exact_append() -> TestResult {
    let (request, proof, append, config) = evidence_fixture().await?;
    let mutations: [fn(&mut PublishGenerationRequest); 6] = [
        |request| request.authority_id = AuthorityId::from_bytes([81; 16]),
        |request| request.volume_id = VolumeId::from_bytes([81; 16]),
        |request| request.operation_id = OperationId::from_bytes([81; 16]),
        |request| request.epoch = Epoch::new(8).expect("nonzero epoch"),
        |request| request.expected = Head::genesis(request.epoch),
        |request| request.generation_root.digest = Digest::from_bytes([81; 32]),
    ];
    for mutate in mutations {
        let mut changed = request;
        mutate(&mut changed);
        assert!(matches!(
            encode_generation_publication_evidence(&changed, &proof, &append, config, 64 * 1024),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    let mut changed = proof.clone();
    changed.root.volume_id = VolumeId::from_bytes([81; 16]);
    assert!(matches!(
        encode_generation_publication_evidence(&request, &changed, &append, config, 64 * 1024),
        Err(PublicationEvidenceError::Mismatch)
    ));
    let mutations: [fn(&mut GuardedAppend); 4] = [
        |append| append.authority_id = AuthorityId::from_bytes([81; 16]),
        |append| append.epoch = Epoch::new(8).expect("nonzero epoch"),
        |append| append.expected = Head::genesis(append.epoch),
        |append| append.commit.operation_id = OperationId::from_bytes([81; 16]),
    ];
    for mutate in mutations {
        let mut changed = append.clone();
        mutate(&mut changed);
        assert!(matches!(
            encode_generation_publication_evidence(&request, &proof, &changed, config, 64 * 1024),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    let mut changed = append.clone();
    changed.commit.payload = bytes::Bytes::from(encode_publication_payload(
        VolumeId::from_bytes([81; 16]),
        request.generation_root,
    ));
    assert!(matches!(
        encode_generation_publication_evidence(&request, &proof, &changed, config, 64 * 1024),
        Err(PublicationEvidenceError::Mismatch)
    ));
    Ok(())
}

#[tokio::test]
async fn publication_evidence_rejects_trailing_and_truncated_frames_with_fresh_digests()
-> TestResult {
    let (request, proof, append, config) = evidence_fixture().await?;
    let encoded =
        encode_generation_publication_evidence(&request, &proof, &append, config, 64 * 1024)?;
    let mut trailing = encoded.bytes.to_vec();
    trailing.push(0);
    let digest = Sha256::digest(&trailing).into();
    assert!(matches!(
        decode_generation_publication_evidence(
            &trailing,
            &digest,
            64 * 1024,
            crate::foundation::usize_to_u64(proof.objects.len())
        ),
        Err(PublicationEvidenceError::Decode(
            CanonicalDecodeError::TrailingBytes
        ))
    ));
    let truncated = &encoded.bytes[..encoded.bytes.len() - 1];
    let digest = Sha256::digest(truncated).into();
    assert!(matches!(
        decode_generation_publication_evidence(
            truncated,
            &digest,
            64 * 1024,
            crate::foundation::usize_to_u64(proof.objects.len())
        ),
        Err(PublicationEvidenceError::Decode(_))
    ));
    Ok(())
}

#[tokio::test]
async fn original_evidence_binds_complete_creation_and_retention_references() -> TestResult {
    use crate::kernel::{
        RetentionCreated, RetentionKind, VolumeCreated, decode_historical_generation_evidence,
        encode_original_generation_evidence, encode_retention_created, encode_volume_created,
        retention_authority_id,
    };
    let (request, original, _, config) = evidence_fixture().await?;
    for sealed in [false, true] {
        let mut proof = original.clone();
        let mut root = request.generation_root;
        if sealed {
            root.storage = Some(crate::ObjectStorage {
                locator: Digest::from_bytes([91; 32]),
                root_file_id: proof.root.root_file_id,
                origin_generation: Digest::from_bytes([92; 32]),
            });
            proof.generation_id = crate::GenerationId::from_root(root);
            *proof
                .objects
                .iter_mut()
                .find(|id| **id == request.generation_root)
                .ok_or("missing root")? = root;
        }
        for retained in [false, true] {
            let (authority_id, payload) = if retained {
                (
                    retention_authority_id(
                        request.volume_id,
                        RetentionKind::Checkpoint,
                        "original",
                    ),
                    encode_retention_created(
                        &RetentionCreated {
                            volume_id: request.volume_id,
                            kind: RetentionKind::Checkpoint,
                            label: "original".into(),
                            generation_root: root,
                            config,
                        },
                        |_| Ok::<_, crate::FsError>(()),
                        |_| crate::FsError::from(CanonicalDecodeError::AllocationFailed),
                    )?,
                )
            } else {
                (
                    request.authority_id,
                    encode_volume_created(
                        VolumeCreated {
                            volume_id: request.volume_id,
                            initial_generation_root: root,
                            config,
                        },
                        |_| Ok::<_, crate::FsError>(()),
                        |_| crate::FsError::from(CanonicalDecodeError::AllocationFailed),
                    )?,
                )
            };
            let append = GuardedAppend {
                authority_id,
                epoch: request.epoch,
                expected: Head::genesis(request.epoch),
                commit: ProposedCommit {
                    publication: None,
                    operation_id: request.operation_id,
                    fingerprint: Digest::from_bytes(Sha256::digest(&payload).into()),
                    payload: Bytes::from(payload),
                },
                permit: PublicationPermit::Unrestricted,
            };
            let record = crate::DurableCommit {
                epoch: append.epoch,
                sequence: Sequence::new(1),
                operation_id: append.commit.operation_id,
                fingerprint: append.commit.fingerprint,
                previous_digest: append.expected.digest,
                digest: crate::authority_commit_digest(
                    authority_id,
                    append.epoch,
                    Sequence::new(1),
                    append.commit.operation_id,
                    append.commit.fingerprint,
                    append.expected.digest,
                    &append.commit.payload,
                ),
                payload: append.commit.payload.clone(),
                settled_at_micros: Some(1),
            };
            let evidence = encode_original_generation_evidence(&proof, &append, config, 64 * 1024)?;
            let decoded = decode_historical_generation_evidence(
                &evidence,
                authority_id,
                &record,
                64 * 1024,
                64,
            )?;
            assert_eq!(decoded.manifest.generation_root, root);
            assert_eq!(decoded.manifest.generation_id, proof.generation_id);
            assert_eq!(decoded.manifest.objects, proof.objects);
            assert_eq!(decoded.root, proof.root);
            assert_eq!(decoded.append, append);
            // Same plaintext digest with a changed locator must not authenticate.
            let mut changed = proof.clone();
            let mut wrong_root = root;
            wrong_root.storage = Some(crate::ObjectStorage {
                locator: Digest::from_bytes([93; 32]),
                root_file_id: proof.root.root_file_id,
                origin_generation: Digest::from_bytes([92; 32]),
            });
            changed.generation_id = crate::GenerationId::from_root(wrong_root);
            assert!(matches!(
                encode_original_generation_evidence(&changed, &append, config, 64 * 1024),
                Err(PublicationEvidenceError::Mismatch)
            ));
            let mut changed = proof.clone();
            changed.root.volume_id = VolumeId::from_bytes([94; 16]);
            assert!(matches!(
                encode_original_generation_evidence(&changed, &append, config, 64 * 1024),
                Err(PublicationEvidenceError::Mismatch)
            ));
        }
    }
    Ok(())
}
