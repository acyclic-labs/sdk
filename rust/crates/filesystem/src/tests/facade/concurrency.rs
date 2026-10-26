use super::*;
mod batching;
mod histories;
#[cfg(not(target_arch = "wasm32"))]
mod merge_ancestry;
#[cfg(not(target_arch = "wasm32"))]
mod native_failure;
mod owners;
mod rounds;

#[tokio::test]
async fn disjoint_rebase_checks_intermediate_edits_even_when_the_head_restores_them() -> TestResult
{
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    create(&mut seed, "file", OwnedBytes::from_static(b"A"), &cancel).await?;
    commit(&mut seed, &cancel).await?;
    let mut local = open_checkout(&volume, writable_manual(), &cancel).await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    for (checkout, value) in [(&mut local, b'L'), (&mut remote, b'B')] {
        checkout
            .change_content(
                path("file")?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from(vec![value]),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
    }
    commit(&mut remote, &cancel).await?;
    remote
        .change_content(
            path("file")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"A"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    assert_eq!(
        local.base_root.file_table, remote.base_root.file_table,
        "control did not restore the same tree"
    );
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let (object, head) = volume
        .resolve_head_generation(&mut accounting, &cancel)
        .await?;
    let dependencies = local.dependencies.proof().dependencies.clone();
    let certificate = local
        .head_changes_disjoint(object, head, &dependencies, &mut accounting, &cancel)
        .await?;
    assert!(
        certificate.is_none(),
        "net delta hid a conflicting intermediate authority step"
    );
    Ok(())
}

#[tokio::test]
async fn disjoint_rebase_composes_directory_bindings_without_losing_siblings() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    create(
        &mut seed,
        "anchor",
        OwnedBytes::from_static(b"base"),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let mut local = open_checkout(&volume, writable_manual(), &cancel).await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    create(&mut local, "left", OwnedBytes::from_static(b"L"), &cancel).await?;
    create(&mut remote, "right", OwnedBytes::from_static(b"R"), &cancel).await?;
    commit(&mut remote, &cancel).await?;
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let (object, head) = volume
        .resolve_head_generation(&mut accounting, &cancel)
        .await?;
    let dependencies = local.dependencies.proof().dependencies.clone();
    let certificate = local
        .head_changes_disjoint(object, head, &dependencies, &mut accounting, &cancel)
        .await?
        .ok_or("independent names in an existing directory did not certify")?;
    certificate.retire(&mut accounting)?;
    let decision = local.rebase_head(8, B, &cancel).await?.value;
    assert!(matches!(decision, RebaseDecision::Safe { .. }));
    commit(&mut local, &cancel).await?;
    let mut final_tree = open_checkout(&volume, pinned(), &cancel).await?;
    for (name, bytes) in [
        ("anchor", b"base".as_slice()),
        ("left", b"L"),
        ("right", b"R"),
    ] {
        let read = final_tree
            .read_file_range(
                &path(name)?,
                ByteRange {
                    offset: 0,
                    length: bytes.len() as u64,
                },
                B,
                &cancel,
            )
            .await?;
        assert_eq!(read.value.bytes.as_ref(), bytes);
    }
    Ok(())
}

#[tokio::test]
async fn disjoint_rebase_adoption_fences_reads_added_during_preparation() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for value in ["local", "remote"] {
        create(
            &mut seed,
            value,
            OwnedBytes::from(vec![b'a'; 4096]),
            &cancel,
        )
        .await?;
    }
    commit(&mut seed, &cancel).await?;
    let mut local = open_checkout(&volume, writable_manual(), &cancel).await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    for (checkout, name) in [(&mut local, "local"), (&mut remote, "remote")] {
        checkout
            .change_content(
                path(name)?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"changed"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
    }
    commit(&mut remote, &cancel).await?;
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let (object, head) = local
        .volume
        .resolve_head_generation(&mut accounting, &cancel)
        .await?;
    let dependencies = local.dependencies.proof().dependencies.clone();
    let certificate = local
        .head_changes_disjoint(object, head, &dependencies, &mut accounting, &cancel)
        .await?
        .ok_or("independent proposal was not certified")?;
    let (root, prepared) = local
        .prepare_rebased_head(object, 8, Some(&certificate), &mut accounting, &cancel)
        .await?
        .ok_or("independent merge conflicted")?;
    // This is the actual interleaving of an observing handle: its old-base
    // observation arrives after planning and before the root's adoption lock.
    local
        .read_file_range(
            &path("remote")?,
            ByteRange {
                offset: 0,
                length: 7,
            },
            B,
            &cancel,
        )
        .await?;
    let before = checkout_state(&local);
    let classified = (
        RebaseDecision::Safe {
            generation: GenerationId::new(object.digest),
        },
        accounting.claim_owned(0, 0)?,
    );
    let result = local.adopt_rebased_head(
        crate::facade::rebase::RebaseAdoption {
            object,
            head,
            classified,
            root,
            prepared,
            settled: None,
            certificate: Some(certificate),
        },
        &mut accounting,
        &cancel,
    );
    assert!(
        matches!(
            result,
            Err(FsError::Rebase(RebaseError::DependenciesChanged))
        ),
        "late conflicting observation escaped the adoption fence"
    );
    assert_eq!(before, checkout_state(&local));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn disjoint_native_rebase_retains_original_custody_and_rejects_unfunded_work() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    for value in ["local", "remote"] {
        create(
            &mut seed,
            value,
            OwnedBytes::from(vec![b'a'; 4096]),
            &cancel,
        )
        .await?;
    }
    commit(&mut seed, &cancel).await?;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let mut local = volume
        .checkout(
            GenerationSelector::Head,
            writable_manual(),
            &original,
            &cancel,
        )
        .await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    local
        .change_content(
            path("local")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"LOCAL"),
            },
            ContentTimes::Preserve,
            &original,
            &cancel,
        )
        .await?;
    remote
        .change_content(
            path("remote")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"REMOTE"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut remote, &cancel).await?;
    let before = checkout_state(&local);
    let denied = original.native_volume_work(WorkBudget {
        items_examined: 0,
        ..B
    })?;
    assert!(local.rebase_head(8, &denied, &cancel).await.is_err());
    assert_eq!(before, checkout_state(&local));
    let (decision, allocation) = local.rebase_head(8, &original, &cancel).await?;
    assert!(matches!(decision, RebaseDecision::Safe { .. }));
    assert!(allocation.work().shares_ledger(&original));
    for (value, expected) in [
        ("local", b"LOCAL".as_slice()),
        ("remote", b"REMOTE".as_slice()),
    ] {
        let range = local
            .read_file_range(
                &path(value)?,
                ByteRange {
                    offset: 0,
                    length: expected.len() as u64,
                },
                &original,
                &cancel,
            )
            .await?;
        assert_eq!(range.bytes.as_ref(), expected);
    }
    Ok(())
}

#[tokio::test]
async fn disjoint_rebase_preserves_observed_content_without_blob_reads() -> TestResult {
    let cancel = CancellationToken::new();
    let unreadable = Arc::new(Mutex::new(None));
    let fs = Fs::new(
        crate::MemoryAuthorityStore::default(),
        CountingObjectStore {
            inner: crate::MemoryObjectStore::default(),
            tracked: Arc::default(),
            reads: Arc::default(),
            unreadable: unreadable.clone(),
        },
        EmbeddedCapabilities::MEMORY,
    );
    let volume = fs.create_volume(config(), B, &cancel).await?.value;
    let mut seed = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
        .await?
        .value;
    for value in ["observed", "local", "remote"] {
        seed.create(
            AuthoredMutation::CreateFile {
                path: path(value)?,
                bytes: OwnedBytes::from(vec![b'a'; 4096]),
                metadata: FileMetadata::default(),
            },
            B,
            &cancel,
        )
        .await?;
    }
    seed.commit(
        OperationId::new(),
        None,
        None,
        None,
        PublicationPermit::Unrestricted,
        B,
        &cancel,
        allow::<FsError>,
    )
    .await?;
    let mut local = volume
        .checkout(GenerationSelector::Head, writable_manual(), B, &cancel)
        .await?
        .value;
    let mut remote = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
        .await?
        .value;
    local
        .read_file_range(
            &path("observed")?,
            ByteRange {
                offset: 0,
                length: 4096,
            },
            B,
            &cancel,
        )
        .await?;
    for (checkout, value) in [(&mut local, "local"), (&mut remote, "remote")] {
        checkout
            .change_content(
                path(value)?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"changed"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
    }
    remote
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?;
    *unreadable.lock().map_err(|_| "fixture lock poisoned")? = Some(ObjectKind::Blob);
    // Negative control: the production full classifier must fetch the observed
    // byte range even though the intervening change touches another file.
    {
        let mut accounting = KernelAccounting::standalone(B, &cancel);
        let (candidate, _) = local
            .volume
            .resolve_head_generation(&mut accounting, &cancel)
            .await?;
        let dependencies = local.dependencies.proof().dependencies.clone();
        let replayed = local
            .probe_rebase_head(candidate, &dependencies, 8, &mut accounting, &cancel)
            .await;
        assert!(
            matches!(replayed, Err(FsError::Object(ObjectStoreError::Missing))),
            "full replay unexpectedly avoided the unavailable content"
        );
    }
    let rebased = local.rebase_head(8, B, &cancel).await?;
    assert!(matches!(rebased.value, RebaseDecision::Safe { .. }));
    *unreadable.lock().map_err(|_| "fixture lock poisoned")? = None;
    local
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<FsError>,
        )
        .await?;
    for value in ["local", "remote"] {
        assert_eq!(
            local
                .read_file_range(
                    &path(value)?,
                    ByteRange {
                        offset: 0,
                        length: 7
                    },
                    B,
                    &cancel
                )
                .await?
                .value
                .bytes
                .as_ref(),
            b"changed"
        );
    }
    Ok(())
}
