use super::*;
#[cfg(feature = "native-mount")]
fn allow_publication(_: WorkBudget) -> std::future::Ready<crate::FsResult<()>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn promotion_cancellation_does_not_install_a_partial_candidate() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from(vec![
        0x5a;
        2 * 1024 * 1024
    ])));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let head = root.head(accounting!()).await.unwrap().id();
    let mut checkout = root
        .workspace()
        .checkout_accounted(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            accounting!(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let (lookup, _) = root.lookup("/file.txt", accounting!()).await.unwrap();
    let id = root
        .stable_file_id_for_lookup("/file.txt", &lookup, accounting!())
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    source.cancel_next_range();
    assert!(
        root.stage_into_checkout(
            &mut checkout,
            None,
            "/file.txt",
            id,
            u64::MAX,
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel)
        )
        .await
        .is_err()
    );
    assert!(cancel.is_cancelled());
    assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
    assert!(
        root.authored_record(&mut checkout, "/file.txt", accounting!())
            .await
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn dirty_candidate_keeps_unpublished_files_and_shared_aliases_until_publication() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"shared"));
    source.node.link_count = Some(2);
    let root = attach(
        &fs,
        "root",
        Arc::new(source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let head = root.head(accounting!()).await.unwrap().id();
    let mut checkout = root
        .workspace()
        .checkout_accounted(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            accounting!(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let path = root.namespace_path("/other", accounting!()).unwrap().value;
    checkout
        .apply_authored_transaction(
            vec![AuthoredMutation::CreateFile {
                path: path,
                bytes: acyclic_stream::OwnedBytes::from_static(b"dirty"),
                metadata: FileMetadata::default(),
            }],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    for path in ["/a", "/b"] {
        let (lookup, _) = root.lookup(path, accounting!()).await.unwrap();
        let id = root
            .stable_file_id_for_lookup(path, &lookup, accounting!())
            .await
            .unwrap();
        assert!(
            root.stage_into_checkout(&mut checkout, None, path, id, 64, accounting!())
                .await
                .unwrap()
        );
        assert!(
            !root
                .stage_into_checkout(&mut checkout, None, path, id, 64, accounting!())
                .await
                .unwrap()
        );
    }
    let a = root
        .authored_record(&mut checkout, "/a", accounting!())
        .await
        .unwrap()
        .unwrap()
        .0;
    let b = root
        .authored_record(&mut checkout, "/b", accounting!())
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(b.link_count, 2);
    assert!(
        root.authored_record(&mut checkout, "/other", accounting!())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
    checkout
        .commit(
            crate::OperationId::new(),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            allow_publication,
        )
        .await
        .unwrap();
    assert_ne!(root.head(accounting!()).await.unwrap().id(), head);
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn mounted_listing_includes_dirty_authored_bindings() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "root",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut checkout = root
        .workspace()
        .checkout_accounted(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            accounting!(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    checkout
        .apply_authored_transaction(
            vec![AuthoredMutation::CreateFile {
                path: root
                    .namespace_path("/new.txt", accounting!())
                    .unwrap()
                    .value,
                bytes: acyclic_stream::OwnedBytes::from_static(b"dirty"),
                metadata: FileMetadata::default(),
            }],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let (source, _, _) = root
        .list_directory_in_checkout(&mut checkout, "/", None, 16, None, accounting!())
        .await
        .unwrap();
    let (authored, _, _) = root
        .list_directory_in_checkout(&mut checkout, "/", source.next, 16, None, accounting!())
        .await
        .unwrap();
    assert!(
        authored
            .entries
            .iter()
            .any(|e| e.authored && e.name.as_bytes() == b"new.txt")
    );
    assert!(
        root.workspace()
            .stat("/new.txt", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .is_err()
    );
}

#[cfg(all(
    feature = "native-mount",
    feature = "native-watch",
    not(target_arch = "wasm32")
))]
#[tokio::test]
async fn replacement_directory_hides_old_source_children() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("d")).unwrap();
    std::fs::write(directory.path().join("d/obsolete"), b"source").unwrap();
    let root = attach(
        &Fs::memory(),
        "root",
        native_source(directory.path(), crate::model::FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut checkout = root
        .workspace()
        .checkout_accounted(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            accounting!(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    checkout
        .apply_authored_transaction(
            vec![AuthoredMutation::CreateDirectory {
                path: root.namespace_path("/d", accounting!()).unwrap().value,
                metadata: FileMetadata::default(),
            }],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let (page, _, _) = root
        .list_directory_in_checkout(&mut checkout, "/d", None, 16, None, accounting!())
        .await
        .unwrap();
    assert!(page.entries.is_empty());
}

#[tokio::test]
async fn source_only_removal_does_not_change_the_authored_head() {
    let root = attach(
        &Fs::memory(),
        "root",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let head = root.head(accounting!()).await.unwrap().id();
    root.remove("/file.txt", None, accounting!()).await.unwrap();
    assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn mounted_whiteouts_follow_the_original_publication_decision() {
    let root = attach(
        &Fs::memory(),
        "root",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    for committed in [false, true] {
        let key = IdempotencyKey::new();
        root.prepare_mount_removals(&["/file.txt".to_owned()], &[], &[], key, accounting!())
            .await
            .unwrap();
        if committed {
            let mut checkout = root
                .workspace()
                .checkout_accounted(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                    accounting!(),
                    &CancellationToken::new(),
                )
                .await
                .unwrap();
            checkout
                .commit_even_if_clean(key.operation_id(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), &mut allow_publication, None)
                .await
                .unwrap();
        }
        assert_eq!(
            root.finish_mount_removals(accounting!()).await.unwrap(),
            committed
        );
        assert_eq!(
            root.lookup("/file.txt", accounting!()).await.is_ok(),
            !committed
        );
    }
}

#[cfg(all(feature = "native-mount", feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shadowed_identity_keeps_256k_of_content_across_restart() {
    let directory = tempfile::tempdir().unwrap();
    let fs_path = directory.path().join("fs");
    let state_path = directory.path().join("state");
    let content = acyclic_stream::OwnedBytes::from(vec![0x5a; 256 * 1024]);
    let cancel = CancellationToken::new();
    let key = IdempotencyKey::new();
    let record;
    {
        let fs = Fs::local(crate::LocalOptions::new(&fs_path)).await.unwrap();
        let released = fs.local_root_release_barrier().unwrap();
        let root = attach(
            &fs,
            "root",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            crate::core_state::LocalCoreStateStore::new(&state_path),
        )
        .await
        .unwrap();
        let mut checkout = root
            .workspace()
            .checkout_accounted(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                accounting!(),
                &cancel,
            )
            .await
            .unwrap();
        let path = root
            .namespace_path("/removed.bin", accounting!())
            .unwrap()
            .value;
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: path.clone(),
                    bytes: content.clone(),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .unwrap();
        let identity = checkout
            .lookup_no_follow_with_metadata(&path, WorkBudget::UNBOUNDED, &cancel)
            .await
            .unwrap()
            .value
            .unwrap();
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::Remove {
                    path: path,
                    expected_file_id: None,
                }],
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .unwrap();
        root.prepare_mount_removals(
            &[],
            &[],
            &[(identity.record, identity.metadata)],
            key,
            accounting!(),
        )
        .await
        .unwrap();
        checkout
            .commit_even_if_clean(key.operation_id(), None, None, None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &cancel, &mut allow_publication, None)
            .await
            .unwrap();
        assert!(root.finish_mount_removals(accounting!()).await.unwrap());
        record = identity.record;
        drop((checkout, root, fs));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !released.is_released() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    let fs = Fs::local(crate::LocalOptions::new(&fs_path)).await.unwrap();
    let root = LazyWorkspace::open(
        fs.open_workspace("root", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).unwrap(),
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        crate::core_state::LocalCoreStateStore::new(&state_path),
        accounting!(),
    )
    .await
    .unwrap();
    let shadow = root
        .shadow_record(
            root.state(accounting!()).await.unwrap().shadows,
            record.file_id,
            accounting!(),
        )
        .await
        .unwrap()
        .unwrap()
        .0;
    let detached = crate::facade::DetachedFile::from_record(
        root.workspace().volume.clone(),
        shadow,
        WorkBudget::UNBOUNDED,
        &cancel,
    )
    .unwrap()
    .value;
    let read = detached
        .read_range(
            crate::ByteRange {
                offset: 0,
                length: content.len() as u64,
            },
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .unwrap()
        .value;
    assert_eq!(read.bytes, content);
}
