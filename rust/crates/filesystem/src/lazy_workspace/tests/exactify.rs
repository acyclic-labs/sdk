use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn exactification_preserves_removals_after_its_cut_and_prepared_bytes() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("file.txt"), b"original").unwrap();
    let source = native_source(directory.path(), crate::FilesystemProfile::Portable).await;
    let fs = Fs::memory();
    let root = attach(&fs, source, MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    let cut = root.snapshot(accounting!()).await.unwrap();
    let state = root.state(accounting!()).await.unwrap();
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let (paths, _) = root
        .enumerate_paths("/", cut, &state, &mut accounting)
        .await
        .unwrap();
    let lookup = root.inspect("/file.txt", accounting!()).await.unwrap();
    let key = IdempotencyKey::new();
    let mut transaction = root
        .workspace()
        .begin_transaction(key, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    root.apply_resolved_promotion(
        &mut transaction,
        "/file.txt",
        &lookup.0,
        u64::MAX,
        accounting!(),
    )
    .await
    .unwrap();
    let prepared = root
        .observed_overlay(state.overlay, "/file.txt", &lookup.0, accounting!())
        .await
        .unwrap();
    root.remove("/file.txt", None, accounting!()).await.unwrap();
    let removed = root.state(accounting!()).await.unwrap();
    root.retain_observation("/file.txt", &lookup.0, accounting!())
        .await
        .unwrap();
    assert_eq!(root.state(accounting!()).await.unwrap(), removed);
    root.retire_lookup(lookup, accounting!()).unwrap();
    assert!(matches!(
        root.commit_overlay(
            &mut transaction,
            state.clone(),
            prepared,
            key,
            crate::PublicationPermit::Unrestricted,
            accounting!()
        )
        .await,
        Err(LazyWorkspaceError::Concurrent)
    ));
    assert_eq!(
        root.head(accounting!()).await.unwrap().id(),
        cut.authored_generation
    );
    let start = root.head(accounting!()).await.unwrap();
    let stale = root
        .exactify_into(
            "/",
            cut,
            paths,
            start,
            crate::PublicationPermit::Unrestricted,
            &mut accounting,
        )
        .await
        .map(|_| ());
    assert!(
        matches!(stale, Err(LazyWorkspaceError::Concurrent)),
        "{stale:?}"
    );
    assert!(matches!(
        root.inspect("/file.txt", accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    assert!(
        root.head(accounting!())
            .await
            .unwrap()
            .stat(
                "/file.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn historical_exactification_keeps_observations_and_refreshes_unresolved_paths() {
    for observed in [false, true] {
        let fs = Fs::memory();
        let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
        let root = attach(
            &fs,
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        if observed {
            root.stat("/file.txt", accounting!()).await.unwrap();
        }
        let cut = root.snapshot(accounting!()).await.unwrap();
        source.replace(Bytes::from_static(b"changed"));
        root.state(accounting!()).await.unwrap();
        let exact = root
            .exactify_snapshot(cut, "exact", IdempotencyKey::new(), accounting!())
            .await
            .unwrap();
        let bytes = exact
            .read(
                "/file.txt",
                32,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .unwrap()
            .value;
        assert_eq!(
            bytes.as_slice(),
            if observed {
                b"source".as_slice()
            } else {
                b"changed".as_slice()
            }
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn source_refresh_and_subtree_exactification_preserve_lazy_siblings() {
    use crate::model::FilesystemProfile;
    let directory = tempfile::tempdir().unwrap();
    for name in ["hot", "cold"] {
        std::fs::create_dir(directory.path().join(name)).unwrap();
        std::fs::write(
            directory.path().join(name).join("file.txt"),
            name.as_bytes(),
        )
        .unwrap();
    }
    let source = native_source(directory.path(), FilesystemProfile::Portable).await;
    let fs = Fs::memory();
    let root = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        root.read("/hot/file.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"hot"
    );
    std::fs::write(directory.path().join("hot/file.txt"), b"changed").unwrap();
    source.invalidate();
    root.state(accounting!()).await.unwrap();
    assert_eq!(
        root.read("/hot/file.txt", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"changed"
    );
    let exact = root
        .exactify(
            "/hot",
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    assert_eq!(
        exact
            .read(
                "/hot/file.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .unwrap()
            .value
            .as_slice(),
        b"changed"
    );
    assert!(
        exact
            .read(
                "/cold/file.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert!(matches!(
        root.lookup("/cold/file.txt", accounting!())
            .await
            .unwrap()
            .0,
        LazyLookup::Source { .. }
    ));
    assert!(matches!(
        root.exactify(
            "/hot/file.txt",
            crate::PublicationPermit::Unrestricted,
            accounting!()
        )
        .await,
        Err(LazyWorkspaceError::NotDirectory)
    ));
    #[cfg(not(target_arch = "wasm32"))]
    {
        let staged = tempfile::tempdir().unwrap();
        exact
            .materialize(
                std::iter::once("/hot"),
                &crate::MaterializeOptions::native(staged.path()),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(staged.path().join("hot/file.txt")).unwrap(),
            b"changed"
        );
        assert!(!staged.path().join("cold").exists());
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn directory_inspection_keeps_cursors_valid_and_promotion_keeps_children_lazy() {
    use crate::model::FilesystemProfile;
    let directory = tempfile::tempdir().unwrap();
    for name in ["a", "b"] {
        std::fs::write(directory.path().join(name), name.as_bytes()).unwrap();
    }
    std::fs::create_dir(directory.path().join("repo")).unwrap();
    std::fs::write(
        directory.path().join("repo/HEAD"),
        b"ref: refs/heads/main\n",
    )
    .unwrap();
    let fs = Fs::memory();
    let root = attach(
        &fs,
        native_source(directory.path(), FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let (first, _) = root
        .list_directory("/", None, 1, accounting!())
        .await
        .unwrap();
    root.inspect("/a", accounting!()).await.unwrap();
    let (second, _) = root
        .list_directory("/", first.next, 1, accounting!())
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 1);
    let (lookup, _) = root.lookup("/repo", accounting!()).await.unwrap();
    let id = root
        .stable_file_id_for_lookup("/repo", &lookup, accounting!())
        .await
        .unwrap();
    root.promote(
        "/repo",
        0,
        IdempotencyKey::new(),
        crate::PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let (lookup, _) = root.lookup("/repo", accounting!()).await.unwrap();
    assert_eq!(
        root.stable_file_id_for_lookup("/repo", &lookup, accounting!())
            .await
            .unwrap(),
        id
    );
    assert_eq!(
        root.read("/repo/HEAD", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"ref: refs/heads/main\n"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn exactification_enforces_the_entry_bound_before_returning_a_complete_generation() {
    use crate::model::FilesystemProfile;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("nested")).unwrap();
    std::fs::write(directory.path().join("a.txt"), b"alpha").unwrap();
    std::fs::write(directory.path().join("nested/b.txt"), b"beta").unwrap();
    let root = attach(
        &Fs::memory(),
        native_source(directory.path(), FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let budget = WorkBudget {
        source_entries_visited: 1,
        ..WorkBudget::UNBOUNDED
    };
    assert!(
        root.exactify(
            "/",
            crate::PublicationPermit::Unrestricted,
            &mut KernelAccounting::standalone(budget, &CancellationToken::new())
        )
        .await
        .is_err()
    );
    let exact = root
        .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
        .await
        .unwrap();
    assert_eq!(
        exact
            .read(
                "/nested/b.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .unwrap()
            .value
            .as_slice(),
        b"beta"
    );
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
#[tokio::test]
async fn windows_names_are_transcoded_to_the_authored_profile() {
    use crate::model::FilesystemProfile;
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("caf\u{e9}.txt"), b"source").unwrap();
    let root = attach(
        &Fs::memory(),
        native_source(directory.path(), FilesystemProfile::Windows).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let exact = root
        .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
        .await
        .unwrap();
    assert_eq!(
        exact
            .read(
                "/caf\u{e9}.txt",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .unwrap()
            .value
            .as_slice(),
        b"source"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn hard_links_survive_exactification_across_batch_boundaries() {
    use crate::model::FilesystemProfile;
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("a000");
    std::fs::write(&first, b"shared").unwrap();
    for index in 1..520 {
        std::fs::hard_link(&first, directory.path().join(format!("a{index:03}"))).unwrap();
    }
    let root = attach(
        &Fs::memory(),
        native_source(directory.path(), FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let exact = root
        .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
        .await
        .unwrap();
    let stat = async |path| {
        exact
            .stat(path, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .unwrap()
            .value
    };
    let a = stat("/a000").await;
    let b = stat("/a519").await;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(b.link_count, 520);
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn sealed_exactification_skips_saved_entries_and_reopens_on_source_invalidation() {
    let mut costs = Vec::new();
    for entries in [1, 256] {
        let directory = tempfile::tempdir().unwrap();
        for index in 0..entries {
            std::fs::write(directory.path().join(format!("file-{index:03}")), b"source").unwrap();
        }
        let source = native_source(directory.path(), crate::FilesystemProfile::Portable).await;
        let root = attach(
            &Fs::memory(),
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        let first = root
            .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
            .await
            .unwrap();
        root.write(
            "/file-000",
            acyclic_stream::OwnedBytes::from_static(b"authored"),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
        // Saved directory entries must not be returned or source entries visited.
        let cancellation = CancellationToken::new();
        let mut bounded = KernelAccounting::standalone(
            WorkBudget {
                source_entries_visited: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let repeated = root
            .exactify("/", crate::PublicationPermit::Unrestricted, &mut bounded)
            .await
            .unwrap();
        costs.push(bounded.spent());
        assert_ne!(repeated.id(), first.id());
        assert_eq!(
            repeated
                .read("/file-000", 16, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .unwrap()
                .value
                .as_slice(),
            b"authored"
        );
        std::fs::write(directory.path().join("added"), b"new source").unwrap();
        source.invalidate();
        let refreshed = root
            .exactify("/", crate::PublicationPermit::Unrestricted, accounting!())
            .await
            .unwrap();
        assert_eq!(
            refreshed
                .read("/added", 16, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .unwrap()
                .value
                .as_slice(),
            b"new source"
        );
    }
    eprintln!("sealed exactification work: {costs:?}");
    assert_eq!(costs[0].items_returned, costs[1].items_returned);
}
