use super::*;

#[tokio::test]
async fn accounting_uses_protected_original_archive_after_old_closure_is_absent()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "original-archive-accounting",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    workspace.write_text("/file", "original").await?;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let original_records = records(&authority, authority_id).await?;
    let initial = &original_records[0];
    let original = &original_records[1];
    let created = crate::kernel::decode_volume_created(&initial.payload, 4096, &mut |_| {
        Ok::<_, crate::kernel::VolumeCreatedError>(())
    })?;
    let original_root = root(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value
            .id(),
    );
    // Capture the actual original root and complete proof before advancing head.
    let proof = prove_original(&objects, original_root, created.config).await?;
    assert_eq!(proof.logical_file_bytes, 8);
    let append = original_append(authority_id, original);
    let request = crate::kernel::PublishGenerationRequest {
        authority_id,
        volume_id: workspace.id().volume_id(),
        epoch: append.epoch,
        expected: append.expected,
        operation_id: append.commit.operation_id,
        generation_root: original_root,
    };
    let evidence = crate::kernel::HistoricalGenerationEvidence::from(
        crate::kernel::encode_generation_publication_evidence(
            &request,
            &proof,
            &append,
            created.config,
            64 * 1024,
        )?,
    );
    // Initial/retention codec must not disguise a publication's original request.
    assert!(matches!(
        crate::kernel::encode_original_generation_evidence(
            &proof,
            &append,
            created.config,
            64 * 1024,
        ),
        Err(crate::kernel::PublicationEvidenceError::Mismatch)
    ));
    workspace
        .write_text("/file", "a much larger current generation")
        .await?;
    let current_records = records(&authority, authority_id).await?;
    let current = &current_records[2];
    let current_root = root(
        workspace
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value
            .id(),
    );
    assert_ne!(current_root, original_root);
    let current_proof = prove_original(&objects, current_root, created.config).await?;
    let current_append = original_append(authority_id, current);
    let current_evidence = crate::kernel::HistoricalGenerationEvidence::from(
        crate::kernel::encode_generation_publication_evidence(
            &crate::kernel::PublishGenerationRequest {
                epoch: current_append.epoch,
                expected: current_append.expected,
                operation_id: current_append.commit.operation_id,
                generation_root: current_root,
                ..request
            },
            &current_proof,
            &current_append,
            created.config,
            64 * 1024,
        )?,
    );
    let (empty, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let mut archive = OriginalArchive {
        inner: crate::LogicalObjectStore::new(Arc::new(empty), bucket),
        authority_id,
        original_digest: original.digest,
        evidence,
    };
    let cancellation = CancellationToken::new();
    assert!(
        !{
            let __object_args = (original_root, BUDGET, &cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = crate::AsyncObjectStore::contains(
                &archive.inner,
                __object_args.0,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        }?
        .value
    );
    assert!(
        !{
            let __object_args = (current_root, BUDGET, &cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = crate::AsyncObjectStore::contains(
                &archive.inner,
                __object_args.0,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        }?
        .value
    );
    let cursor = AccountingCursor {
        sequence: initial.sequence,
        digest: initial.digest,
        settled_at_micros: initial
            .settled_at_micros
            .ok_or("initial Stream timestamp missing")?,
    };
    let page = accounting_page(
        &authority,
        &archive,
        authority_id,
        cursor,
        1,
        BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(page.facts.len(), 1);
    assert!(!page.complete);
    assert_eq!(
        page.facts[0].transition,
        AccountingTransition::Published {
            volume_id: workspace.id().volume_id(),
            generation_root: original_root,
            logical_regular_file_bytes: 8,
        }
    );
    assert_eq!(page.facts[0].operation_id, original.operation_id);
    assert_eq!(
        Some(page.facts[0].settled_at_micros),
        original.settled_at_micros
    );
    assert_eq!(page.watermark_digest, current.digest);
    // A valid archive of the current head cannot serve an older committed fact.
    archive.evidence = current_evidence;
    assert!(matches!(
        accounting_page(
            &authority,
            &archive,
            authority_id,
            cursor,
            1,
            BUDGET,
            &cancellation,
        )
        .await,
        Err(AccountingError::Proof(_))
    ));
    Ok(())
}

#[tokio::test]
async fn original_initial_and_retention_evidence_bind_actual_stream_records_and_reject_sha_forgery()
-> Result<(), Box<dyn Error>> {
    let (fs, authority, objects) = fixture();
    let workspace = fs
        .create_workspace(
            "initial-retention-original",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let initial = records(&authority, authority_id).await?.remove(0);
    let created = crate::kernel::decode_volume_created(&initial.payload, 4096, &mut |_| {
        Ok::<_, crate::kernel::VolumeCreatedError>(())
    })?;
    let initial_proof =
        prove_original(&objects, created.initial_generation_root, created.config).await?;
    let initial_append = original_append(authority_id, &initial);
    let initial_evidence = crate::kernel::encode_original_generation_evidence(
        &initial_proof,
        &initial_append,
        created.config,
        64 * 1024,
    )?;
    workspace.write_text("/file", "retained original").await?;
    workspace.checkpoint("original").await?;
    let retained_authority = retention_authority_id(
        workspace.id().volume_id(),
        RetentionKind::Checkpoint,
        "original",
    );
    let retained_record = records(&authority, retained_authority).await?.remove(0);
    let retained = crate::kernel::decode_retention_created(&retained_record.payload, 4096)?;
    let retained_proof =
        prove_original(&objects, retained.generation_root, retained.config).await?;
    let retained_append = original_append(retained_authority, &retained_record);
    let retained_evidence = crate::kernel::encode_original_generation_evidence(
        &retained_proof,
        &retained_append,
        retained.config,
        64 * 1024,
    )?;
    for (authority_id, record, proof, append, evidence) in [
        (
            authority_id,
            initial,
            initial_proof,
            initial_append,
            initial_evidence,
        ),
        (
            retained_authority,
            retained_record,
            retained_proof,
            retained_append,
            retained_evidence,
        ),
    ] {
        let projection = crate::kernel::decode_historical_generation_evidence(
            &evidence,
            authority_id,
            &record,
            64 * 1024,
            4096,
        )?;
        assert_eq!(projection.append, append);
        assert_eq!(projection.root, proof.root);
        assert_eq!(projection.manifest.objects, proof.objects);
        assert_eq!(projection.logical_file_bytes, proof.logical_file_bytes);
        let mut forged_digest = evidence.clone();
        forged_digest.sha256[0] ^= 1;
        assert!(matches!(
            crate::kernel::decode_historical_generation_evidence(
                &forged_digest,
                authority_id,
                &record,
                64 * 1024,
                4096,
            ),
            Err(crate::kernel::PublicationEvidenceError::Mismatch)
        ));
        let mut forged_bytes = evidence.clone();
        let mut bytes = forged_bytes.bytes.to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        forged_bytes.bytes = bytes::Bytes::from(bytes);
        assert!(matches!(
            crate::kernel::decode_historical_generation_evidence(
                &forged_bytes,
                authority_id,
                &record,
                64 * 1024,
                4096,
            ),
            Err(crate::kernel::PublicationEvidenceError::Mismatch)
        ));
    }
    Ok(())
}
