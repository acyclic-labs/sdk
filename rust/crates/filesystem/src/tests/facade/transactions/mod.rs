use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_authored_transaction_keeps_true_ids_and_original_result_custody() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "source",
        OwnedBytes::from_static(b"transaction source"),
        &cancel,
    )
    .await?;
    commit(&mut checkout, &cancel).await?;
    let metadata = checkout
        .read_metadata(&path("source")?, B, &cancel)
        .await?
        .value;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancel),
        &cancel,
    )?;
    candidate.bind_authored_operation(OperationId::new());
    let first = path("transaction-first")?;
    let second = path("transaction-second")?;
    let input = original_authored_creates(
        &[
            (&first, OwnedBytes::from_static(b"first actual source")),
            (&second, OwnedBytes::from_static(b"second actual source")),
        ],
        metadata,
        &original,
        &cancel,
    )?;
    let mut accounting = KernelAccounting::native(&original, &cancel);
    let applying = candidate.apply_authored_transaction_with_accounting(
        crate::kernel::Owned {
            value: input.0,
            bytes: input.1.bytes(),
            allocation: KernelOwnedAllocation::NativeOriginal(input.1),
        },
        &mut accounting,
        &cancel,
    );
    // The caller owns input descriptors and its funded frame, not the compiler
    // and mutation phases. The original descriptors fit within two KiB.
    let caller_bytes = std::mem::size_of_val(&applying);
    assert!(
        caller_bytes <= 2 * 1024,
        "authored caller retains {caller_bytes} bytes"
    );
    eprintln!("authored transaction caller: {caller_bytes} bytes");
    let result = applying.await?;
    let expected = &result.value.created_file_ids;
    assert_eq!(expected.len(), 2);
    assert!(expected.iter().all(Option::is_some));
    assert_ne!(expected[0], expected[1]);
    let KernelOwnedAllocation::NativeOriginal(allocation) = &result.allocation else {
        return Err("native result lost original custody".into());
    };
    assert!(allocation.work().shares_ledger(&original));
    for (path, id) in [(&first, expected[0]), (&second, expected[1])] {
        assert_eq!(
            Some(
                candidate
                    .lookup_no_follow(path, &original, &cancel)
                    .await?
                    .record
                    .unwrap()
                    .file_id
            ),
            id
        );
    }
    let live = original.live_allocation_bytes().unwrap();
    let result_bytes = allocation.bytes();
    drop(result);
    assert_eq!(
        original.live_allocation_bytes().unwrap(),
        live - result_bytes
    );
    assert!(
        checkout
            .lookup_no_follow(&first, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    Ok(())
}

/// Under one bound operation a name renamed away is created again, step by
/// step as the native mount applies it and within one batch, and an identical
/// replay from the same base derives identical identities.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn bound_operation_recreates_a_renamed_away_name_with_replayable_ids() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, checkout) = fixture(config(), &cancel).await?;
    let operation = OperationId::new();
    let lock = AuthoredMutation::CreateFile {
        file_id: None,
        path: path("lock")?,
        bytes: OwnedBytes::from_static(b"lock"),
        metadata: FileMetadata::default(),
    };
    let rename = AuthoredMutation::Rename {
        source: path("lock")?,
        destination: path("index")?,
        replace: false,
    };
    let steps = [lock.clone(), rename, lock];
    for batched in [false, true] {
        let mut replays = Vec::new();
        for _replay in 0..2 {
            let mut candidate = checkout.private_candidate(
                &mut crate::kernel::allocation::KernelAccounting::standalone(B, &cancel),
                &cancel,
            )?;
            candidate.bind_authored_operation(operation);
            let mut created = Vec::new();
            if batched {
                created = candidate
                    .apply_authored_transaction(steps.to_vec(), B, &cancel)
                    .await?
                    .value
                    .created_file_ids;
            } else {
                for step in &steps {
                    created.extend(
                        candidate
                            .apply_authored_transaction(vec![step.clone()], B, &cancel)
                            .await?
                            .value
                            .created_file_ids,
                    );
                }
            }
            let [Some(renamed), None, Some(recreated)] = created[..] else {
                return Err(format!("unexpected creations {created:?}").into());
            };
            assert_ne!(renamed, recreated);
            for (name, id) in [("index", renamed), ("lock", recreated)] {
                let record = candidate
                    .lookup_no_follow(&path(name)?, B, &cancel)
                    .await?
                    .value
                    .record
                    .ok_or(name)?;
                assert_eq!(record.file_id, id, "{name} batched={batched}");
            }
            replays.push(created);
        }
        assert_eq!(replays[0], replays[1], "batched={batched}");
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn borrowed_authored_second_staged_write_failure_keeps_candidate_source_and_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let control = Arc::new(FaultControl::disabled());
    let objects = Arc::new(crate::memory::MemoryObjectStore::default());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        PostPutObjectStore {
            inner: Arc::clone(&objects),
            control: Arc::clone(&control),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let volume = fs
        .create_volume_with_id(VolumeId::from_bytes([122; 16]), config(), B, &cancellation)
        .await?
        .value;
    let mut checkout = open_checkout(&volume, writable_pinned(), &cancellation).await?;
    let source = path("source")?;
    let destination = path("destination")?;
    let removed = path("removed")?;
    for (requested, body) in [
        (&source, b"source body".as_slice()),
        (&destination, b"destination body".as_slice()),
        (&removed, b"removed body".as_slice()),
    ] {
        checkout
            .create(
                crate::AuthoredMutation::CreateFile {
                    file_id: None,
                    path: requested.clone(),
                    bytes: (bytes::Bytes::copy_from_slice(body)).into(),
                    metadata: crate::kernel::FileMetadata::default(),
                },
                B,
                &cancellation,
            )
            .await?;
    }
    let mut metadata = checkout
        .lookup_no_follow_with_metadata(&source, B, &cancellation)
        .await?
        .value
        .ok_or("real source missing")?
        .metadata;
    metadata.modified_ns = MetadataField::Value(99);
    let original =
        crate::NativeMaterializationWork::new(B, &cancellation).map_err(|(cause, _)| cause)?;
    let mut candidate = checkout.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancellation),
        &cancellation,
    )?;
    let input = original_borrowed_path_changes(
        &source,
        &destination,
        &removed,
        metadata,
        &original,
        &cancellation,
    )?;
    control.arm(2);
    let failure = candidate
        .apply_borrowed_authored_transaction_with_native_work(
            &input.0,
            &input.1,
            &original,
            &cancellation,
        )
        .await
        .err()
        .ok_or("second staged write unexpectedly succeeded")?;
    assert!(matches!(
        failure,
        FsError::Object(ObjectStoreError::Corrupt)
    ));
    original.receipt().unwrap().verify(B)?;
    assert!(matches!(&input.0[2], AuthoredMutation::Rename {
        source: old_source, destination: old_destination, ..
    } if old_source == &source && old_destination == &destination));
    control.disable();
    for requested in [&source, &destination, &removed] {
        let retained = candidate
            .lookup_no_follow(requested, B, &cancellation)
            .await?
            .value
            .record
            .ok_or("failed candidate lost existing record")?;
        let installed = checkout
            .lookup_no_follow(requested, B, &cancellation)
            .await?
            .value
            .record
            .ok_or("installed source record disappeared")?;
        assert_eq!(retained, installed);
    }
    Ok(())
}

mod limits;
