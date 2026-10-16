use super::*;
use proptest::prelude::*;

// Property: a fresh read observes arbitrary same-epoch source edits, still gives
// authored files/tombstones precedence, never changes a cut, and reads each
// admitted body once without directory enumeration. Every generated case uses
// a new in-memory provider; no sleeps, native processes or filesystem setup.
#[test]
fn latest_reads_obey_view_precedence_bounds_and_snapshot_isolation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let strategy = (
        prop::collection::vec(any::<u8>(), 0..64),
        prop::collection::vec(any::<u8>(), 0..64),
        0u64..128,
        0u8..5,
        any::<bool>(),
    );
    proptest::test_runner::TestRunner::default()
        .run(&strategy, |(before, after, maximum, mode, unlink)| {
            runtime.block_on(async {
                let fs = Fs::memory();
                let changed = before != after;
                let mut source = CountingSource::new(Bytes::from(before));
                if mode >= 3 {
                    source.node.link_count = Some(2);
                }
                let source = Arc::new(source);
                let root = attach(
                    &fs,
                    Arc::clone(&source),
                    MemoryLazyWorkspaceStore::default(),
                )
                .await
                .unwrap();
                root.read("/file.txt", 64, accounting!()).await.unwrap();
                if mode == 1 || mode >= 3 {
                    root.write(
                        if mode >= 3 {
                            "/authored-alias"
                        } else {
                            "/file.txt"
                        },
                        acyclic_stream::OwnedBytes::from_static(b"authored"),
                        IdempotencyKey::new(),
                        crate::PublicationPermit::Unrestricted,
                        accounting!(),
                    )
                    .await
                    .unwrap();
                } else if mode == 2 {
                    root.hide_source_path("/file.txt", accounting!())
                        .await
                        .unwrap();
                }
                if mode == 4 {
                    // The last authored alias can be removed while the shared
                    // identity shadow still supplies every unresolved alias.
                    root.remove("/authored-alias", None, accounting!())
                        .await
                        .unwrap();
                }
                if mode >= 3 && unlink {
                    source.counts.link_count.store(1, Ordering::Relaxed);
                }
                let cut = root.snapshot(accounting!()).await.unwrap();
                // A native source can change between requests without an active
                // watch advancing its epoch. Change exactly that existing version.
                source
                    .versions
                    .lock()
                    .unwrap()
                    .insert(3, Bytes::copy_from_slice(&after));
                let lookups = source.counts.lookups.load(Ordering::Relaxed);
                let reads = source.counts.ranges.load(Ordering::Relaxed);
                let result = root.read_latest("/file.txt", maximum, accounting!()).await;
                if mode == 2 {
                    prop_assert!(matches!(result, Err(LazyWorkspaceError::NotFound)));
                } else {
                    let expected = if mode == 1 || mode >= 3 {
                        b"authored".as_slice()
                    } else {
                        after.as_slice()
                    };
                    if expected.len() as u64 > maximum {
                        prop_assert!(matches!(result, Err(LazyWorkspaceError::TooLarge)));
                    } else {
                        let result = result.unwrap();
                        prop_assert_eq!(result.bytes.as_slice(), expected);
                    }
                }
                prop_assert_eq!(root.snapshot(accounting!()).await.unwrap(), cut);
                prop_assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
                prop_assert_eq!(
                    source.counts.lookups.load(Ordering::Relaxed) - lookups,
                    usize::from(mode == 0 || mode >= 3)
                );
                prop_assert_eq!(
                    source.counts.ranges.load(Ordering::Relaxed) - reads,
                    usize::from(mode == 0 && after.len() as u64 <= maximum)
                );
                if mode == 0 && changed {
                    // Negative control: the retained lookup still rejects this
                    // edit. A fresh read must not rewrite that older observation.
                    prop_assert!(root.read("/file.txt", 64, accounting!()).await.is_err());
                }
                Ok(())
            })
        })
        .unwrap();
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn latest_read_uses_original_native_work_and_rejects_wrong_kinds() {
    for kind in [
        SourceNodeKind::RegularFile,
        SourceNodeKind::Directory,
        SourceNodeKind::SymbolicLink,
    ] {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"native"));
        source.node.kind = kind;
        let source = Arc::new(source);
        let root = attach(
            &fs,
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .unwrap_or_else(|(error, _)| panic!("{error:?}"));
        let mut ctx = KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let result = root.read_latest("/file.txt", 64, &mut ctx).await;
        if kind == SourceNodeKind::RegularFile {
            let bytes = result.unwrap().bytes;
            assert_eq!(bytes.as_slice(), b"native");
            assert_eq!(work.receipt().source_bytes_read, 6);
            assert!(bytes.retained_capacity().is_some());
        } else {
            assert!(matches!(result, Err(LazyWorkspaceError::NotRegularFile)));
            assert_eq!(work.receipt().source_bytes_read, 0);
        }
        assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 1);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
    }
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn latest_native_read_sees_edit_and_deletion_without_a_watch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("AGENTS.md");
    std::fs::write(&path, b"original").unwrap();
    let source = native_source(directory.path(), crate::model::FilesystemProfile::Portable).await;
    let fs = Fs::memory();
    let root = attach(&fs, source, MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    assert_eq!(
        root.read("/AGENTS.md", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"original"
    );
    let cut = root.snapshot(accounting!()).await.unwrap();
    std::fs::write(&path, b"replacement").unwrap();
    assert!(root.read("/AGENTS.md", 64, accounting!()).await.is_err());
    assert_eq!(
        root.read_latest("/AGENTS.md", 64, accounting!())
            .await
            .unwrap()
            .bytes
            .as_slice(),
        b"replacement"
    );
    assert_eq!(root.snapshot(accounting!()).await.unwrap(), cut);
    std::fs::remove_file(path).unwrap();
    assert!(matches!(
        root.read_latest("/AGENTS.md", 64, accounting!()).await,
        Err(LazyWorkspaceError::NotFound)
    ));
    assert_eq!(root.snapshot(accounting!()).await.unwrap(), cut);
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
#[tokio::test]
async fn latest_native_read_preserves_authored_alias_after_native_unlink() {
    for remove_authored_alias in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let a = directory.path().join("a");
        let b = directory.path().join("b");
        std::fs::write(&a, b"native original").unwrap();
        std::fs::hard_link(&a, &b).unwrap();
        let source =
            native_source(directory.path(), crate::model::FilesystemProfile::Portable).await;
        let fs = Fs::memory();
        let root = attach(&fs, source, MemoryLazyWorkspaceStore::default())
            .await
            .unwrap();
        assert_eq!(
            root.read("/b", 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_slice(),
            b"native original"
        );
        root.write(
            "/a",
            acyclic_stream::OwnedBytes::from_static(b"authored"),
            IdempotencyKey::new(),
            crate::PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
        if remove_authored_alias {
            root.remove("/a", None, accounting!()).await.unwrap();
        }
        let cut = root.snapshot(accounting!()).await.unwrap();
        std::fs::remove_file(a).unwrap();
        std::fs::write(b, b"native replacement").unwrap();
        assert_eq!(
            root.read_latest("/b", 64, accounting!())
                .await
                .unwrap()
                .bytes
                .as_slice(),
            b"authored"
        );
        assert_eq!(root.snapshot(accounting!()).await.unwrap(), cut);
    }
}
