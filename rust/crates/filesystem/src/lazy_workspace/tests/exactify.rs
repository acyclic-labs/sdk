use super::*;

#[tokio::test]
async fn authored_state_and_tombstones_mask_the_source() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");

    root.write(
        "/file.txt",
        Bytes::from_static(b"authored").into(),
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .expect("write");
    assert_eq!(
        root.read("/file.txt", 32, accounting!())
            .await
            .expect("authored read")
            .bytes
            .as_ref(),
        b"authored"
    );
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
    root.remove("/file.txt", None, accounting!())
        .await
        .expect("remove");
    assert!(matches!(
        root.stat("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn source_epoch_can_rebind_without_enumeration() {
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
    let root = attach(
        &fs,
        "root",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    root.stat("/observed.txt", accounting!())
        .await
        .expect("observe");
    let before = root
        .snapshot(accounting!())
        .await
        .expect("snapshot before rebind");
    source.invalidate();
    let rebound = root.rebind_source(accounting!()).await.expect("rebind");
    assert_eq!(rebound.source, source.reference());
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    assert_eq!(
        root.stat("/observed.txt", accounting!())
            .await
            .expect("cached")
            .logical_bytes,
        Some(6)
    );
    let after = root
        .snapshot(accounting!())
        .await
        .expect("snapshot after refresh");
    assert_ne!(after, before);
    assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn snapshot_exactification_pins_only_previously_observed_paths() {
    for observed in [false, true] {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = attach(
            &fs,
            "root",
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .expect("attach");
        if observed {
            root.stat("/file.txt", accounting!())
                .await
                .expect("observe old epoch");
        }
        let snapshot = root
            .snapshot(accounting!())
            .await
            .expect("snapshot before rebind");

        source.replace(Bytes::from_static(b"changed"));
        root.rebind_source(accounting!()).await.expect("rebind");
        let exact = root
            .exactify_snapshot(
                snapshot,
                "historical-exact",
                IdempotencyKey::new(),
                accounting!(),
            )
            .await
            .expect("exactify historical snapshot");

        assert_eq!(
            exact
                .read(
                    "/file.txt",
                    32,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new()
                )
                .await
                .expect("read exactified file")
                .value
                .as_ref(),
            if observed {
                b"source".as_slice()
            } else {
                b"changed".as_slice()
            }
        );
    }
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn native_source_rebind_refreshes_an_observed_file() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};

    let directory = tempfile::tempdir().expect("temporary source");
    let path = directory.path().join("file.txt");
    std::fs::write(&path, b"before").expect("initial file");
    let source = Arc::new(
        NativeDemandSource::open(
            (directory.path()).into(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("source")
        .value,
    );
    let fs = Fs::memory();
    let root = attach(
        &fs,
        "native-rebind",
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    assert_eq!(
        root.read("/file.txt", 64, accounting!())
            .await
            .expect("initial read")
            .bytes
            .as_ref(),
        b"before"
    );
    let before = root
        .snapshot(accounting!())
        .await
        .expect("snapshot before change");

    std::fs::write(&path, b"after").expect("external write");
    source
        .invalidate(&mut crate::demand::DemandWork::new(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))
        .expect("invalidate");
    root.rebind_source(accounting!())
        .await
        .expect("rebind source");

    assert_eq!(
        root.read("/file.txt", 64, accounting!())
            .await
            .expect("refreshed read")
            .bytes
            .as_ref(),
        b"after"
    );
    assert_ne!(
        root.snapshot(accounting!())
            .await
            .expect("snapshot after change"),
        before
    );
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn exactify_is_bounded_and_returns_only_a_complete_generation() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};

    let directory = tempfile::tempdir().expect("temporary source");
    std::fs::create_dir(directory.path().join("nested")).expect("nested directory");
    std::fs::write(directory.path().join("a.txt"), b"alpha").expect("first file");
    std::fs::write(directory.path().join("nested").join("b.txt"), b"beta").expect("second file");
    let source = Arc::new(
        NativeDemandSource::open(
            (directory.path()).into(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("source")
        .value,
    );
    let fs = Fs::memory();
    let root = attach(&fs, "exactify", source, MemoryLazyWorkspaceStore::default())
        .await
        .expect("attach");
    let mut insufficient = WorkBudget::UNBOUNDED;
    insufficient.source_entries_visited = 1;
    assert!(matches!(
        root.exactify(
            "/",
            crate::PublicationPermit::Unrestricted,
            &mut crate::kernel::KernelAccounting::standalone(
                insufficient,
                &CancellationToken::new()
            )
        )
        .await,
        Err(LazyWorkspaceError::Demand(DemandError::Work(_)))
    ));

    let exact = root
        .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
        .await
        .expect("exact capture");
    assert_eq!(
        exact
            .read(
                "/nested/b.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .expect("captured file")
            .value
            .as_ref(),
        b"beta"
    );
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn exactify_subtree_keeps_siblings_lazy() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};

    let directory = tempfile::tempdir().expect("temporary source");
    std::fs::create_dir(directory.path().join("hot")).expect("hot directory");
    std::fs::create_dir(directory.path().join("cold")).expect("cold directory");
    std::fs::write(directory.path().join("hot/file.txt"), b"hot").expect("hot file");
    std::fs::write(directory.path().join("cold/file.txt"), b"cold").expect("cold file");
    let source = Arc::new(
        NativeDemandSource::open(
            (directory.path()).into(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("source")
        .value,
    );
    let root = attach(
        &Fs::memory(),
        "subtree-exactify",
        source,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");

    let exact = root
        .exactify(
            "/hot",
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .expect("capture hot subtree");
    assert_eq!(
        exact
            .read(
                "/hot/file.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .expect("hot content")
            .value
            .as_ref(),
        b"hot"
    );
    #[cfg(feature = "native-mount")]
    {
        let staged = tempfile::tempdir().expect("empty native stage");
        exact
            .materialize(
                std::iter::once("/hot"),
                &crate::MaterializeOptions::native(staged.path()),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .expect("materialize only pinned hot subtree");
        assert_eq!(
            std::fs::read(staged.path().join("hot/file.txt")).expect("staged hot file"),
            b"hot"
        );
        assert!(!staged.path().join("cold").exists());
    }
    assert!(matches!(
        exact
            .read(
                "/cold/file.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map(|receipt| Bytes::copy_from_slice(receipt.value.as_ref()))
            .map_err(|failure| failure.error),
        Err(WorkspaceError::NotFound)
    ));
    assert!(matches!(
        root.lookup("/cold/file.txt", accounting!())
            .await
            .map(|(lookup, _)| lookup)
            .expect("cold remains lazy"),
        LazyLookup::Source { .. }
    ));
    assert!(matches!(
        root.exactify(
            "/hot/file.txt",
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await,
        Err(LazyWorkspaceError::NotDirectory)
    ));
}

#[cfg(all(windows, feature = "native-watch"))]
#[tokio::test]
async fn source_page_batch_lookup_transcodes_windows_names_to_workspace_profile() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};

    let directory = tempfile::tempdir().expect("temporary source");
    std::fs::write(directory.path().join("café.txt"), b"source").expect("source file");
    let source = Arc::new(
        NativeDemandSource::open(
            (directory.path()).into(),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("Windows source")
        .value,
    );
    let root = attach(
        &Fs::memory(),
        "windows-source-portable-authored",
        source,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach portable authored workspace");
    assert_eq!(root.workspace().profile(), FilesystemProfile::Portable);
    let exact = root
        .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
        .await
        .expect("exactify Windows source into portable authored workspace");
    assert_eq!(
        exact
            .read(
                "/café.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .expect("captured file")
            .value
            .as_ref(),
        b"source"
    );
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn exactify_preserves_hard_links_across_batch_boundaries() {
    use crate::demand::native::NativeDemandSource;
    use crate::model::{FilesystemProfile, VolumeLimits};

    let directory = tempfile::tempdir().expect("temporary source");
    let source_path = directory.path().join("a000");
    std::fs::write(&source_path, b"shared").expect("source file");
    for index in 1..520 {
        std::fs::hard_link(&source_path, directory.path().join(format!("a{index:03}")))
            .expect("source hard link");
    }
    let source = Arc::new(
        NativeDemandSource::open(
            (directory.path()).into(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .expect("source")
        .value,
    );
    let root = attach(
        &Fs::memory(),
        "hard-link-batches",
        source,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .expect("attach");
    let exact = root
        .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
        .await
        .expect("exactify all batches");
    let first = exact
        .stat("/a000", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("first stat")
        .value;
    let last = exact
        .stat("/a519", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .expect("last stat")
        .value;
    assert_eq!(first.file_id, last.file_id);
    assert_eq!(last.link_count, 520);
}
