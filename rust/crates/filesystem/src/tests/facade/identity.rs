use super::*;

#[tokio::test]
async fn named_retention_preserves_the_exact_reference_without_a_cached_proof() -> TestResult {
    let cancellation = CancellationToken::new();
    let objects = Arc::new(crate::MemoryObjectStore::default());
    let authority = Arc::new(crate::MemoryAuthorityStore::default());
    let fs = Fs::new(
        authority.clone(),
        objects.clone(),
        EmbeddedCapabilities::MEMORY,
    );
    let volume = fs.create_volume(config(), B, &cancellation).await?.value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            B,
            &cancellation,
        )
        .await?
        .value;
    let mut work = crate::AuthorityWork::new(B, &cancellation);
    let plain = checkout.generation_id().object_id();
    let body = objects
        .read(plain, config().limits.maximum_object_bytes, &mut work)
        .await?;
    let root = crate::kernel::decode_generation_root::<CanonicalDecodeError>(
        &body.bytes,
        Default::default(),
        |_| Ok(()),
    )?;
    let mut reference = plain;
    reference.storage = Some(crate::ObjectStorage {
        locator: Digest::from_bytes([181; 32]),
        root_file_id: root.root_file_id,
        origin_generation: plain.digest,
    });
    objects.put(reference, body.bytes, &mut work).await?;
    for (label, target, succeeds) in [
        ("exact", reference, true),
        (
            "alias",
            ObjectId {
                storage: Some(crate::ObjectStorage {
                    locator: Digest::from_bytes([182; 32]),
                    ..reference.storage.ok_or("missing association")?
                }),
                ..reference
            },
            false,
        ),
    ] {
        let label = label.to_owned();
        let bytes = label.capacity() as u64;
        let result = fs
            .retain_workspace_generation(
                &volume,
                GenerationId::from_root(target),
                crate::kernel::RetentionKind::Pin,
                Owned::new(label.clone(), KernelOwnedAllocation::Standalone, bytes),
                None,
                B,
                &cancellation,
            )
            .await;
        assert_eq!(result.is_ok(), succeeds);
        if succeeds {
            let id = crate::kernel::retention_authority_id(
                volume.id(),
                crate::kernel::RetentionKind::Pin,
                &label,
            );
            let maximum_payload_bytes = work.accounting().remaining()?.authority_bytes_read;
            let ((_, payload), _) = authority
                .head_payload(id, maximum_payload_bytes, &mut work)
                .await?;
            let payload = payload.ok_or("missing retention record")?;
            let retained = crate::kernel::decode_retention_created(&payload, payload.len() as u64)?;
            assert_eq!(retained.generation_root, reference);
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_identity_mutation_preserves_source_and_cancelled_candidate() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut source) = fixture(config(), &cancel).await?;
    let id = create(
        &mut source,
        "source",
        OwnedBytes::from_static(b"actual source bytes"),
        &cancel,
    )
    .await?;
    commit(&mut source, &cancel).await?;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let mut candidate = source.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancel),
        &cancel,
    )?;
    candidate
        .change_content(
            id,
            ContentChange::Resize { logical_bytes: 0 },
            ContentTimes::Preserve,
            &original,
            &cancel,
        )
        .await?;
    assert_eq!(
        read(&mut source, id, 0, 19, &cancel).await?.as_ref(),
        b"actual source bytes"
    );
    let before = candidate
        .read_file_record_by_id(id, B, &cancel)
        .await?
        .value;
    cancel.cancel();
    assert!(
        candidate
            .change_content(
                id,
                ContentChange::Resize { logical_bytes: 7 },
                ContentTimes::Preserve,
                &original,
                &cancel
            )
            .await
            .is_err()
    );
    let fresh = CancellationToken::new();
    assert_eq!(
        candidate.read_file_record_by_id(id, B, &fresh).await?.value,
        before
    );
    Ok(())
}

#[tokio::test]
async fn authority_volume_identity_fails_closed() -> TestResult {
    assert_authority_volume_identity_fails_closed().await
}
