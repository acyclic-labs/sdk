use super::*;
use crate::model::{CheckoutMode, GenerationSelector};

#[cfg(feature = "native-mount")]
fn allow_publication(_: WorkBudget) -> std::future::Ready<crate::FsResult<()>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn mounted_promotion_keeps_dirty_content_private_and_cancellation_atomic() {
    for midstream in [true, false] {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(if midstream {
            Bytes::from(vec![0x5a; 2 * 1024 * 1024])
        } else {
            Bytes::from_static(b"source")
        }));
        let root = attach(
            &fs,
            "mount",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        let head = root.head(accounting!()).await.unwrap().id();
        let cancellation = CancellationToken::new();
        let mut accounting =
            crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut checkout = root
            .workspace()
            .checkout_accounted(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &mut accounting,
                &cancellation,
            )
            .await
            .unwrap();
        let other = root
            .namespace_path("/other.txt", &mut accounting)
            .unwrap()
            .value;
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: other.clone(),
                    bytes: Bytes::from_static(b"dirty").into(),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .unwrap();
        if !midstream {
            let (page, _, _) = root
                .list_directory_in_checkout(&mut checkout, "/", None, 16, None, &mut accounting)
                .await
                .unwrap();
            assert_eq!(page.entries.len(), 1);
            assert!(!page.entries[0].authored);
            let (page, _, _) = root
                .list_directory_in_checkout(
                    &mut checkout,
                    "/",
                    page.next,
                    16,
                    None,
                    &mut accounting,
                )
                .await
                .unwrap();
            assert_eq!(page.entries.len(), 1);
            assert!(page.entries[0].authored);
            assert_eq!(page.entries[0].name.as_bytes(), b"other.txt");
            assert!(page.next.is_none());
        }
        let expected = root
            .source_file_id(&source.node.file_identity, &mut accounting)
            .unwrap();
        if midstream {
            source.cancel_next_range();
        } else {
            cancellation.cancel();
        }
        assert!(matches!(
            root.stage_into_checkout(
                &mut checkout,
                None,
                "/file.txt",
                expected,
                u64::MAX,
                &mut accounting,
            )
            .await,
            Err(LazyWorkspaceError::Cancelled)
        ));
        assert!(cancellation.is_cancelled());
        assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
        let file = root
            .namespace_path("/file.txt", accounting!())
            .unwrap()
            .value;
        assert!(
            checkout
                .lookup_no_follow(&file, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .unwrap()
                .value
                .record
                .is_none()
        );
        assert!(
            checkout
                .lookup_no_follow(&other, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .unwrap()
                .value
                .record
                .is_some()
        );
        if midstream {
            continue;
        }
        for changed in [true, false] {
            assert_eq!(
                root.stage_into_checkout(
                    &mut checkout,
                    None,
                    "/file.txt",
                    expected,
                    u64::MAX,
                    accounting!(),
                )
                .await
                .unwrap(),
                changed
            );
        }
        assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
        for path in [&other, &file] {
            assert!(
                checkout
                    .lookup_no_follow(path, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .unwrap()
                    .value
                    .record
                    .is_some()
            );
        }
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
        assert_eq!(
            root.read("/other.txt", 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_ref(),
            b"dirty"
        );
        assert_eq!(
            root.read("/file.txt", 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_ref(),
            b"source"
        );
    }
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn mounted_alias_promotions_preserve_source_hard_links_before_publication() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"shared"));
    source.node.link_count = Some(2);
    let identity = source.node.file_identity;
    let root = attach(
        &fs,
        "aliases",
        Arc::new(source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut checkout = root
        .workspace()
        .checkout_accounted(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            &mut accounting,
            &cancellation,
        )
        .await
        .unwrap();
    let expected = root.source_file_id(&identity, &mut accounting).unwrap();
    for path in ["/a", "/b"] {
        assert!(
            root.stage_into_checkout(
                &mut checkout,
                None,
                path,
                expected,
                u64::MAX,
                &mut accounting
            )
            .await
            .unwrap()
        );
    }
    for path in ["/a", "/b"] {
        let path = root.namespace_path(path, &mut accounting).unwrap().value;
        let record = checkout
            .lookup_no_follow(&path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value
            .record
            .unwrap();
        assert_eq!((record.file_id, record.link_count), (expected, 2));
    }
}

#[cfg(all(
    feature = "native-mount",
    feature = "native-watch",
    not(target_arch = "wasm32")
))]
#[tokio::test]
async fn mounted_replacement_directory_does_not_expose_obsolete_source_children() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("d")).unwrap();
    std::fs::write(directory.path().join("d/old.txt"), b"old").unwrap();
    let cancellation = CancellationToken::new();
    let source = NativeDemandSource::open(
        directory.path().into(),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .unwrap()
    .value;
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "replacement",
        Arc::new(source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut checkout = root
        .workspace()
        .checkout_accounted(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            &mut accounting,
            &cancellation,
        )
        .await
        .unwrap();
    assert_eq!(
        root.list_directory_in_checkout(&mut checkout, "/d", None, 16, None, &mut accounting)
            .await
            .unwrap()
            .0
            .entries
            .len(),
        1
    );
    checkout
        .apply_authored_transaction(
            vec![AuthoredMutation::CreateDirectory {
                path: root.namespace_path("/d", &mut accounting).unwrap().value,
                metadata: FileMetadata::default(),
            }],
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap();
    let (page, _, _) = root
        .list_directory_in_checkout(&mut checkout, "/d", None, 16, None, &mut accounting)
        .await
        .unwrap();
    assert!(page.entries.is_empty());
    assert!(page.next.is_none());
}

#[tokio::test]
async fn source_only_remove_does_not_advance_the_authored_head() {
    let fs = Fs::memory();
    let source = CountingSource::new(Bytes::from_static(b"source"));
    let identity = source.node.file_identity;
    let root = attach(
        &fs,
        "remove",
        Arc::new(source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let head = root.head(accounting!()).await.unwrap().id();
    let expected = root.source_file_id(&identity, accounting!()).unwrap();
    root.remove("/file.txt", Some(expected), accounting!())
        .await
        .unwrap();
    assert_eq!(root.head(accounting!()).await.unwrap().id(), head);
    assert!(matches!(
        root.lookup("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn mounted_whiteout_follows_the_checkout_publication_decision() {
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "whiteout",
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let paths = vec!["/file.txt".to_owned()];
    root.prepare_mount_removals(&paths, &[], &[], IdempotencyKey::new(), accounting!())
        .await
        .unwrap();
    assert!(!root.finish_mount_removals(accounting!()).await.unwrap());
    assert!(matches!(
        root.lookup("/file.txt", accounting!()).await.unwrap().0,
        LazyLookup::Source { .. }
    ));
    let committed = IdempotencyKey::new();
    root.prepare_mount_removals(&paths, &[], &[], committed, accounting!())
        .await
        .unwrap();
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut checkout = root
        .workspace()
        .checkout_accounted(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            &mut accounting,
            &cancellation,
        )
        .await
        .unwrap();
    assert!(matches!(
        checkout
            .commit_even_if_clean(
                committed.operation_id(),
                None,
                None,
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                &mut allow_publication,
                None,
            )
            .await
            .unwrap()
            .value,
        crate::LivePublicationObservation::Committed { .. }
    ));
    assert!(root.finish_mount_removals(&mut accounting).await.unwrap());
    assert!(matches!(
        root.lookup("/file.txt", &mut accounting).await,
        Err(LazyWorkspaceError::NotFound)
    ));
}

#[cfg(all(feature = "native-mount", feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shadowed_identity_keeps_its_content_across_power_loss() {
    let directory = tempfile::tempdir().unwrap();
    let fs_root = directory.path().join("fs");
    let state_root = directory.path().join("state");
    let content = Bytes::from(vec![0x5a; 256 * 1024]);
    let cancellation = CancellationToken::new();
    let removal = IdempotencyKey::new();
    let path = NamespacePath::new(
        vec![LogicalName::new(NameEncoding::Utf8, b"removed.bin".to_vec(), 255).unwrap()],
        crate::model::VolumeLimits::default(),
    )
    .unwrap();
    let shadowed;
    {
        let fs = Fs::local(crate::LocalOptions::new(&fs_root)).await.unwrap();
        let released = fs.local_root_release_barrier().unwrap();
        let root = attach(
            &fs,
            "power-loss-shadow",
            Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
            crate::core_state::LocalCoreStateStore::new(&state_root),
        )
        .await
        .unwrap();
        let mut accounting =
            crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut checkout = root
            .workspace()
            .checkout_accounted(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &mut accounting,
                &cancellation,
            )
            .await
            .unwrap();
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: path.clone(),
                    bytes: content.clone().into(),
                    metadata: FileMetadata::default(),
                }],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .unwrap();
        let identity = checkout
            .lookup_no_follow_with_metadata(&path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value
            .unwrap();
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::Remove {
                    path: path.clone(),
                    expected_file_id: None,
                }],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .unwrap();
        root.prepare_mount_removals(
            &[],
            &[],
            &[(identity.record, identity.metadata)],
            removal,
            &mut accounting,
        )
        .await
        .unwrap();
        checkout
            .commit_even_if_clean(
                removal.operation_id(),
                None,
                None,
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                &mut allow_publication,
                None,
            )
            .await
            .unwrap();
        assert!(root.finish_mount_removals(&mut accounting).await.unwrap());
        shadowed = identity.record;
        drop((checkout, root, fs));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !released.is_released() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    let fs = Fs::local(crate::LocalOptions::new(&fs_root)).await.unwrap();
    let reopened = LazyWorkspace::open(
        fs.open_workspace("power-loss-shadow", WorkBudget::UNBOUNDED, &cancellation)
            .await
            .unwrap()
            .value,
        Arc::new(CountingSource::new(Bytes::from_static(b"source"))),
        crate::core_state::LocalCoreStateStore::new(&state_root),
        accounting!(),
    )
    .await
    .unwrap();
    let state = reopened.state(accounting!()).await.unwrap();
    let shadow = reopened
        .shadow_record(state.shadows, shadowed.file_id, accounting!())
        .await
        .unwrap()
        .unwrap();
    let read = crate::DetachedFile::from_record(
        reopened.workspace.volume.clone(),
        shadow.0,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .unwrap()
    .value
    .read_range(
        crate::ByteRange {
            offset: 0,
            length: content.len() as u64,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .unwrap();
    assert_eq!(read.value.bytes.as_ref(), content.as_ref());
}
