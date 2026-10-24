use super::*;

#[cfg(windows)]
#[tokio::test]

async fn live_name_index_journal_uses_held_source_before_discovery() {
    let parent = tempfile::tempdir().unwrap();
    let selected = parent.path().join("selected");
    let moved = parent.path().join("moved");
    std::fs::create_dir(&selected).unwrap();
    let source = native_source(&selected, crate::FilesystemProfile::Windows).await;
    let identity = source.root_identity();
    let fs = Fs::memory();
    let lazy = attach(&fs, source, MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    std::fs::rename(&selected, &moved).unwrap();
    std::fs::create_dir(&selected).unwrap();
    let stop = CancellationToken::new();
    let mut work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &stop).unwrap();
    let journal = lazy.capture_indexed_native_journal(&mut work).unwrap();
    let native = crate::WindowsUsnCheckpoint::from_name_checkpoint(journal);
    assert_eq!(native.root_identity(), identity);
    // Negative control: reopening the source's saved path observes another
    // root, even though the source still owns the original directory handle.
    let replacement = crate::capture_windows_usn_checkpoint(&selected, &mut work).unwrap();
    assert_ne!(replacement.root_identity(), native.root_identity());
    assert!(work.receipt().backend_read_operations > 0);
    assert_eq!(work.receipt().source_bytes_read, 0);
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut checkpoint = walk.checkpoint().unwrap();
    checkpoint.record_journal_checkpoint(journal).unwrap();
    let resumed = NameWalk::resume(&lazy, checkpoint, GitIgnorePolicy::default())
        .await
        .unwrap();
    assert_eq!(
        resumed.checkpoint().unwrap().journal_checkpoint(),
        Some(journal)
    );
    // Capturing after discovery would be unsafe: this write belongs after
    // the baseline marker, whether or not a subsequent scan sees its name.
    std::fs::write(moved.join("after-capture"), b"unread").unwrap();
    assert_eq!(
        crate::validate_windows_usn_checkpoint(&moved, native, &mut work).unwrap(),
        crate::WindowsUsnContinuity::BaselineRequired(
            crate::WindowsUsnDiscontinuity::VolumeAdvanced
        )
    );
    while walk.next_page().await.unwrap().is_some() {}
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn name_search_journal_cursor_and_delta_publish_atomically_across_restart() {
    use crate::{NameIndex, NameIndexRefresh, NameJournalCheckpoint};
    let fs = Fs::memory();
    let source = Arc::new(CountingSource::new(Bytes::from_static(b"unread")));
    let lazy = attach(&fs, source.clone(), MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    let ignore = GitIgnorePolicy::default();
    let walk = NameWalk::lazy(&lazy, None, ignore.clone()).await.unwrap();
    let initial = publish_name_walk(lazy.workspace(), walk).await;
    let crate::NameCut::Lazy(snapshot) = initial.manifest.cut.clone() else {
        panic!("lazy cut required")
    };
    let marker = |root, journal, usn| {
        NameJournalCheckpoint::windows_usn(snapshot.source.identity, root, journal, usn).unwrap()
    };
    let baseline = marker([1; 16], 2, 10);
    let mut manifest = initial.manifest.clone();
    manifest
        .checkpoint
        .record_journal_checkpoint(baseline)
        .unwrap();
    let baseline_index = NameIndex::publish(
        lazy.workspace(),
        &initial.generation(),
        manifest,
        None,
        Some(&initial),
    )
    .await
    .unwrap()
    .unwrap();
    let restored = NameIndex::open(lazy.workspace()).await.unwrap().unwrap();
    assert_eq!(
        restored.manifest.checkpoint.journal_checkpoint(),
        Some(baseline)
    );

    let mut losing_manifest = restored.manifest.clone();
    losing_manifest
        .checkpoint
        .record_journal_checkpoint(marker([1; 16], 2, 30))
        .unwrap();
    losing_manifest.pages += 1;
    let losing_page = [crate::SearchName {
        path: "loser.txt".into(),
        regular: true,
        directory: false,
    }];
    source.replace(Bytes::from_static(b"changed"));
    let next = marker([1; 16], 2, 20);
    let winner = match baseline_index
        .refresh_native(
            &lazy,
            lazy.snapshot(accounting!()).await.unwrap(),
            crate::lazy_workspace::NativeNameDelta {
                paths: ["winner.txt".into()].into(),
                all: false,
                journal: Some(next),
            },
            &ignore,
        )
        .await
        .unwrap()
    {
        NameIndexRefresh::Published(index) => index,
        _ => panic!("journal leaf delta must publish without rediscovery"),
    };
    assert_eq!(winner.manifest.checkpoint.journal_checkpoint(), Some(next));
    // Negative control: publishing a sidecar cursor before the losing CAS
    // would leave USN 30 durable while only USN 20's names were committed.
    assert!(
        NameIndex::publish(
            lazy.workspace(),
            &restored.generation(),
            losing_manifest,
            Some(&losing_page),
            Some(&restored),
        )
        .await
        .unwrap()
        .is_none()
    );
    drop(winner);
    let reopened = NameIndex::open(lazy.workspace()).await.unwrap().unwrap();
    assert_eq!(
        reopened.manifest.checkpoint.journal_checkpoint(),
        Some(next)
    );
    let mut names = Vec::new();
    for page in 0..reopened.manifest.pages {
        names.extend(
            reopened
                .page(page)
                .await
                .unwrap()
                .into_iter()
                .map(|name| name.path),
        );
    }
    assert!(names.iter().any(|path| path == "winner.txt"));
    assert!(!names.iter().any(|path| path == "loser.txt"));
    let resumed = NameWalk::resume(&lazy, reopened.manifest.checkpoint.clone(), ignore)
        .await
        .unwrap();
    assert_eq!(
        resumed.checkpoint().unwrap().journal_checkpoint(),
        Some(next)
    );

    let mut checkpoint = reopened.manifest.checkpoint.clone();
    for invalid in [
        marker([9; 16], 2, 21),
        marker([1; 16], 9, 21),
        marker([1; 16], 2, 19),
        NameJournalCheckpoint::windows_usn([9; 16], [1; 16], 2, 21).unwrap(),
    ] {
        assert!(checkpoint.record_journal_checkpoint(invalid).is_err());
        assert_eq!(checkpoint.journal_checkpoint(), Some(next));
    }
    let encoded = serde_json::to_value(&checkpoint).unwrap();
    let mut wrong_source = encoded.clone();
    wrong_source["journal"]["source_identity"] = serde_json::to_value([9u8; 16]).unwrap();
    assert!(serde_json::from_value::<crate::NameCheckpoint>(wrong_source).is_err());
    let mut impossible = encoded;
    impossible["journal"]["next_usn"] = serde_json::json!(u64::MAX);
    assert!(serde_json::from_value::<crate::NameCheckpoint>(impossible).is_err());
    assert!(NameJournalCheckpoint::windows_usn([7; 16], [1; 16], 2, u64::MAX).is_err());
}

#[cfg(feature = "native-watch")]
#[tokio::test]
async fn name_search_directory_identities_survive_file_backed_restore_and_offline_rename() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("before")).unwrap();
    std::fs::write(root.path().join("before/file"), b"unread").unwrap();
    std::fs::write(root.path().join(".gitignore"), b"# local policy").unwrap();
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        native_source(root.path(), crate::FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    while walk.next_page().await.unwrap().is_some() {}
    let checkpoint = walk.checkpoint().unwrap();
    let identities: std::collections::BTreeMap<_, _> = checkpoint
        .source_directory_identities()
        .map(|(path, id)| (path.to_owned(), id))
        .collect();
    assert_eq!(identities.len(), 2);
    assert!(identities.contains_key(""));
    assert!(identities.contains_key("before"));
    assert!(
        !identities.contains_key(".gitignore"),
        "policy files are not directory identities"
    );
    let stop = CancellationToken::new();
    let expected = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let index = crate::NameIndex::publish(
        lazy.workspace(),
        &expected,
        crate::NameIndexManifest {
            schema: crate::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages: 0,
            pending_directories: 0,
            complete: true,
            checkpoint,
        },
        None,
        None,
    )
    .await
    .unwrap()
    .unwrap();
    drop(index);
    let restored = crate::NameIndex::open(lazy.workspace())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        restored
            .manifest
            .checkpoint
            .source_directory_identities()
            .map(|(path, id)| (path.to_owned(), id))
            .collect::<std::collections::BTreeMap<_, _>>(),
        identities
    );
    std::fs::rename(root.path().join("before"), root.path().join("after")).unwrap();
    let snapshot = lazy
        .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&stop))
        .await
        .unwrap();
    let current = lazy
        .source_metadata_at_snapshot(
            snapshot,
            "/after",
            &mut WorkBudget::UNBOUNDED.accounting(&stop),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identities["before"], current.file_identity);
    // Negative control: the old path is gone. A version or path-only
    // checkpoint cannot identify which former subtree this directory was.
    assert!(
        lazy.source_metadata_at_snapshot(
            snapshot,
            "/before",
            &mut WorkBudget::UNBOUNDED.accounting(&stop)
        )
        .await
        .unwrap()
        .is_none()
    );
    let old: serde_json::Value = serde_json::json!({"Directory":["before", current.version]});
    assert!(
        restored
            .manifest
            .checkpoint
            .clone()
            .restore_record(&serde_json::to_vec(&old).unwrap())
            .is_err()
    );
}

#[cfg(feature = "native-watch")]
#[tokio::test]
async fn name_search_native_metadata_benchmark_keeps_pages_bounded() {
    let root = tempfile::tempdir().unwrap();
    for directory in 0..4 {
        let path = root.path().join(format!("directory-{directory}"));
        std::fs::create_dir(&path).unwrap();
        for file in 0..128 {
            std::fs::write(
                path.join(format!("candidate-{file:03}.rs")),
                b"unread fixture",
            )
            .unwrap();
        }
        std::fs::write(path.join("excluded.ignored"), b"not a candidate").unwrap();
    }
    // The excluded objects exist in the real native tree.
    assert!(root.path().join("directory-0/excluded.ignored").is_file());
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        native_source(root.path(), crate::FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let started = std::time::Instant::now();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::parse("*.ignored"))
        .await
        .unwrap();
    let mut first_regular = None;
    let mut maximum_page = 0;
    let mut names = std::collections::BTreeSet::new();
    let mut files = 0;
    while let Some(page) = walk.next_page().await.unwrap() {
        maximum_page = maximum_page.max(page.len());
        for name in page {
            if name.regular {
                first_regular.get_or_insert_with(|| started.elapsed());
                files += 1;
            }
            assert!(!name.path.ends_with(".ignored"));
            assert!(
                names.insert(name.path),
                "native enumeration repeated a name"
            );
        }
    }
    assert_eq!(files, 512);
    assert_eq!(names.len(), 516);
    assert_eq!(walk.unavailable_entries(), 0);
    assert!(maximum_page <= 128);
    eprintln!(
        "volume-search benchmark component=native-name-walk files={files} directories=4 first_regular_us={} elapsed_us={} maximum_page_entries={maximum_page}",
        first_regular.unwrap().as_micros(),
        started.elapsed().as_micros(),
    );
}

#[cfg(all(unix, feature = "native-watch"))]
#[tokio::test]
async fn name_search_native_symlink_cycles_are_leaves_and_cannot_escape_the_volume() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("nested")).unwrap();
    std::fs::write(root.path().join("healthy.rs"), b"unread").unwrap();
    std::fs::write(root.path().join("nested/child.rs"), b"unread").unwrap();
    std::fs::write(outside.path().join("private.rs"), b"must not be mentioned").unwrap();
    std::os::unix::fs::symlink("self", root.path().join("self")).unwrap();
    std::os::unix::fs::symlink("..", root.path().join("nested/up")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.path().join("outside")).unwrap();
    // Negative controls establish both a real cycle and a traversable escape.
    assert!(std::fs::metadata(root.path().join("self")).is_err());
    assert!(
        std::fs::metadata(root.path().join("outside/private.rs"))
            .unwrap()
            .is_file()
    );
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        native_source(root.path(), crate::FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut pages = 0;
    let mut names = std::collections::BTreeMap::new();
    while let Some(page) = walk.next_page().await.unwrap() {
        pages += 1;
        assert!(pages < 12, "directory traversal followed a symlink cycle");
        for name in page {
            names.insert(name.path.clone(), name);
        }
    }
    assert!(names["healthy.rs"].regular);
    assert!(names["nested/child.rs"].regular);
    for link in ["self", "nested/up", "outside"] {
        assert!(!names[link].regular);
        assert!(!names[link].directory);
    }
    assert_eq!(names.len(), 6);
    assert_eq!(walk.unavailable_entries(), 0);
}

#[cfg(all(target_os = "linux", feature = "native-watch"))]
#[tokio::test]
async fn name_search_lazy_checkpoint_reopens_native_directory_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    for ordinal in 0..300 {
        std::fs::write(
            directory.path().join(format!("candidate-{ordinal:03}")),
            b"unread",
        )
        .unwrap();
    }
    let fs = Fs::memory();
    let source = native_source(directory.path(), crate::FilesystemProfile::Portable).await;
    let reference = source.reference();
    let lazy = attach(&fs, source, MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut names = std::collections::BTreeSet::new();
    while names.is_empty() {
        names.extend(
            walk.next_page()
                .await
                .unwrap()
                .unwrap()
                .into_iter()
                .map(|name| name.path),
        );
    }
    assert!(names.len() < 300);
    let bytes = serde_json::to_vec(&walk.checkpoint().unwrap()).unwrap();
    drop(walk);
    let workspace = lazy.workspace.clone();
    let store = lazy.store.clone();
    drop(lazy);
    let source = crate::demand::native::NativeDemandSource::open(
        std::borrow::Cow::Borrowed(directory.path()),
        crate::FilesystemProfile::Portable,
        crate::VolumeLimits::default(),
        Some(reference),
        None,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .unwrap()
    .value;
    let reopened = LazyWorkspace {
        workspace,
        store,
        source: Arc::new(source),
        _allocation: None,
    };
    let mut resumed = NameWalk::resume(
        &reopened,
        serde_json::from_slice(&bytes).unwrap(),
        GitIgnorePolicy::default(),
    )
    .await
    .unwrap();
    while let Some(page) = resumed.next_page().await.unwrap() {
        for name in page {
            assert!(names.insert(name.path), "replayed a checkpointed candidate");
        }
    }
    assert_eq!(names.len(), 300);
}
use crate::facade::FsWork as _;
use crate::{GitIgnorePolicy, NameWalk, PublicationPermit};

struct NameIndexReadCounter {
    inner: crate::memory::MemoryObjectStore,
    reads: Arc<AtomicUsize>,
}

impl AsyncObjectStore for NameIndexReadCounter {
    async fn put(
        &self,
        id: crate::ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }

    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_many(writes, work).await
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_hashed(object, work).await
    }

    async fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.inner.put_many_hashed(objects, work).await
    }

    async fn contains(
        &self,
        id: crate::ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        self.inner.contains(id, work).await
    }

    async fn read(
        &self,
        id: crate::ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read(id, maximum, work).await
    }

    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        self.reads.fetch_add(requests.len(), Ordering::Relaxed);
        self.inner.read_many(requests, work).await
    }
}

#[tokio::test]
async fn name_search_same_generation_follow_does_not_restore_checkpoint_files() {
    let reads = Arc::new(AtomicUsize::new(0));
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        NameIndexReadCounter {
            inner: Default::default(),
            reads: Arc::clone(&reads),
        },
        crate::EmbeddedCapabilities::MEMORY,
    );
    let lazy = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::new())),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let stop = CancellationToken::new();
    let directories: Vec<_> = (0..128).map(|ordinal| format!("/d{ordinal:03}")).collect();
    let mut transaction = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .write_batch(
            &directories.iter().map(String::as_str).collect::<Vec<_>>(),
            &[],
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    let walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let original = publish_name_walk(lazy.workspace(), walk).await;
    let head_reads = reads.load(Ordering::Relaxed);
    lazy.workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap();
    let head_reads = reads.load(Ordering::Relaxed) - head_reads;
    let before = reads.load(Ordering::Relaxed);
    let followed = crate::NameIndex::open_reusing(lazy.workspace(), Some(&original))
        .await
        .unwrap()
        .unwrap();
    let follow_reads = reads.load(Ordering::Relaxed) - before;
    assert_eq!(follow_reads, head_reads);
    assert_eq!(followed.generation().id(), original.generation().id());
    assert_eq!(
        serde_json::to_vec(&followed.page(0).await.unwrap()).unwrap(),
        serde_json::to_vec(&original.page(0).await.unwrap()).unwrap()
    );
    // Negative control: opening without the held generation reads the index
    // manifest and checkpoint files in addition to the same head observation.
    let before = reads.load(Ordering::Relaxed);
    let reopened = crate::NameIndex::open(lazy.workspace())
        .await
        .unwrap()
        .unwrap();
    let cold_reads = reads.load(Ordering::Relaxed) - before;
    assert!(cold_reads > follow_reads);
    assert_eq!(reopened.manifest.pages, followed.manifest.pages);
    lazy.workspace()
        .write_text("/ordinary", "unrelated workspace write")
        .await
        .unwrap();
    let before = reads.load(Ordering::Relaxed);
    let followed = crate::NameIndex::open_reusing(lazy.workspace(), Some(&original))
        .await
        .unwrap()
        .unwrap();
    let unrelated_reads = reads.load(Ordering::Relaxed) - before;
    assert_ne!(followed.generation().id(), original.generation().id());
    assert!(
        unrelated_reads < cold_reads,
        "warm diff read {unrelated_reads} objects, cold restore read {cold_reads}"
    );
    assert_eq!(
        serde_json::to_vec(&followed.manifest.checkpoint).unwrap(),
        serde_json::to_vec(&original.manifest.checkpoint).unwrap()
    );
    // Negative control: a manifest-only cache check misses a direct checkpoint
    // edit, even when the manifest's bytes and its binding are unchanged.
    for path in [
        "/.acyclic-sdk/name-index/checkpoint-0000000000000000.jsonl".to_owned(),
        format!(
            "/.acyclic-sdk/name-index/directories/{}.json",
            blake3::hash(b"d127/.ignore").to_hex()
        ),
    ] {
        let bytes = original
            .generation()
            .read(&path, 2 * 1024 * 1024, WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        lazy.workspace().write_text(&path, "{}\n").await.unwrap();
        assert!(
            crate::NameIndex::open_reusing(lazy.workspace(), Some(&followed))
                .await
                .is_err(),
            "changed {path} was hidden by reuse"
        );
        lazy.workspace()
            .write(&path, Bytes::copy_from_slice(&bytes))
            .await
            .unwrap();
        assert!(
            crate::NameIndex::open_reusing(lazy.workspace(), Some(&followed))
                .await
                .is_ok()
        );
    }
    lazy.write(
        "/.acyclic-sdk/name-index/manifest.json",
        Bytes::from_static(b"{}").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    assert!(
        crate::NameIndex::open_reusing(lazy.workspace(), Some(&original))
            .await
            .is_err(),
        "a changed generation must query its files rather than reuse stale state"
    );
    let mut transaction = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .rename(
            "/.acyclic-sdk",
            "/detached-sdk",
            false,
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    assert!(
        crate::NameIndex::open_reusing(lazy.workspace(), Some(&followed))
            .await
            .unwrap()
            .is_none()
    );
}

#[cfg(all(unix, feature = "native-watch"))]
#[tokio::test]
async fn name_search_directory_tokens_survive_reopen_and_detect_offline_edits() {
    let root = tempfile::tempdir().unwrap();
    for directory in ["changed", "unchanged"] {
        std::fs::create_dir(root.path().join(directory)).unwrap();
        std::fs::write(root.path().join(directory).join("original"), b"unread").unwrap();
    }
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        native_source(root.path(), crate::FilesystemProfile::Portable).await,
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let original = publish_name_walk(lazy.workspace(), walk).await;
    let reopened = crate::NameIndex::open(lazy.workspace())
        .await
        .unwrap()
        .unwrap();
    let tokens: std::collections::BTreeMap<_, _> =
        reopened.manifest.checkpoint.source_versions().collect();
    assert_eq!(tokens.len(), 10);
    assert!(
        tokens
            .iter()
            .filter(|(path, _)| !path.ends_with("ignore") && path.as_bytes() != b".git/info/exclude")
            .all(|(_, token)| token.is_some())
    );
    assert_eq!(
        serde_json::to_vec(&original.manifest.checkpoint).unwrap(),
        serde_json::to_vec(&reopened.manifest.checkpoint).unwrap()
    );
    let stop = CancellationToken::new();
    let snapshot = lazy
        .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&stop))
        .await
        .unwrap();
    for (path, token) in &tokens {
        let current = lazy
            .source_metadata_version_at_snapshot(
                snapshot,
                &format!("/{path}"),
                &mut WorkBudget::UNBOUNDED.accounting(&stop),
            )
            .await
            .unwrap();
        assert_eq!(current, *token);
    }
    // Negative control: a root-only token misses edits below an existing child.
    std::fs::write(root.path().join("changed/created-offline"), b"unread").unwrap();
    for (path, token) in tokens {
        let current = lazy
            .source_metadata_version_at_snapshot(
                snapshot,
                &format!("/{path}"),
                &mut WorkBudget::UNBOUNDED.accounting(&stop),
            )
            .await
            .unwrap();
        assert_eq!(current == token, path != "changed", "{path}");
    }
}

async fn publish_name_walk<A, O, D, S>(
    workspace: &crate::Workspace<A, O>,
    mut walk: NameWalk<'_, A, O, D, S>,
) -> crate::NameIndex<A, O>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let stop = CancellationToken::new();
    let mut expected = workspace
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let mut pages = 0;
    let mut previous = None;
    loop {
        let page = walk.next_page().await.unwrap();
        pages += u64::from(page.is_some());
        let manifest = crate::NameIndexManifest {
            schema: crate::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages,
            pending_directories: walk.pending_directories(),
            complete: page.is_none(),
            checkpoint: walk.checkpoint().unwrap(),
        };
        let index = crate::NameIndex::publish(
            workspace,
            &expected,
            manifest,
            page.as_deref(),
            previous.as_ref(),
        )
        .await
        .unwrap()
        .unwrap();
        expected = index.generation();
        if page.is_none() {
            return index;
        }
        previous = Some(index);
    }
}

#[tokio::test]
async fn name_search_authored_subtree_refresh_streams_only_new_directories_once() {
    use crate::{NameCut, NameIndexRefresh};
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "authored-subtree",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .value;
    let stop = CancellationToken::new();
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .create_dir_all("/untouched", WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap();
    transaction
        .write_text("/untouched/old.txt", "old")
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    let generation = workspace
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let ignore = GitIgnorePolicy::default();
    let walk = NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::exact(
        generation,
        None,
        ignore.clone(),
    )
    .await
    .unwrap();
    let index = publish_name_walk(&workspace, walk).await;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .create_dir_all("/created/nested", WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap();
    transaction
        .write_text("/created/fresh.txt", "new")
        .await
        .unwrap();
    transaction
        .write_text("/created/nested/deep.txt", "deep")
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    let generation = workspace
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let cut = NameCut::Exact {
        workspace: workspace.id(),
        generation: generation.id(),
    };
    let index = match index
        .refresh_authored(&workspace, cut, &ignore)
        .await
        .unwrap()
    {
        NameIndexRefresh::Published(index) => index,
        _ => panic!("new authored directories required a whole-tree walk"),
    };
    assert_eq!(
        index.manifest.pending_directories, 1,
        "nested roots must coalesce"
    );
    let mut resumed = NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::resume_exact(
        &workspace,
        index.manifest.checkpoint,
        ignore.clone(),
    )
    .await
    .unwrap();
    let mut discovered = Vec::new();
    while let Some(page) = resumed.next_page().await.unwrap() {
        discovered.extend(page.into_iter().map(|name| name.path));
    }
    assert_eq!(discovered.len(), 3);
    assert_eq!(
        discovered
            .iter()
            .filter(|path| path.as_str() == "created/nested/deep.txt")
            .count(),
        1
    );
    assert!(!discovered.iter().any(|path| path.starts_with("untouched/")));
    // Negative control: a full walk re-emits the unchanged directory's names.
    let mut full =
        NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::exact(generation, None, ignore)
            .await
            .unwrap();
    let mut old_seen = false;
    while let Some(page) = full.next_page().await.unwrap() {
        old_seen |= page.iter().any(|name| name.path == "untouched/old.txt");
    }
    assert!(old_seen);
}

async fn names_hydrate(n: usize, broken: bool) -> usize {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"candidate content"));
    source.directory_entries = n;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::parse("file-0001.txt\n"))
        .await
        .unwrap();
    let mut seen = 0;
    while let Some(page) = walk.next_page().await.unwrap() {
        for name in page {
            assert!(name.regular);
            if broken {
                walk.read(&name.path, 1024).await.unwrap();
            }
            seen += 1;
        }
    }
    assert_eq!(seen, n - usize::from(n > 1));
    source.counts.ranges.load(Ordering::Relaxed)
}

#[tokio::test]
async fn name_search_does_not_hydrate_lazy_candidates() {
    assert_eq!(names_hydrate(64, false).await, 0);
}

#[tokio::test]
async fn name_search_authored_raw_names_trigger_partial_rediscovery_instead_of_failure() {
    use crate::kernel::{LogicalName, NamespacePath};
    use crate::{NameCut, NameIndexRefresh};
    for (profile, encoding, bytes) in [
        (
            crate::FilesystemProfile::Posix,
            NameEncoding::PosixBytes,
            vec![0xff],
        ),
        (
            crate::FilesystemProfile::Windows,
            NameEncoding::WindowsUtf16Le,
            vec![0, 0xd8],
        ),
    ] {
        let fs = Fs::memory();
        let stop = CancellationToken::new();
        let config = VolumeConfig {
            profile,
            ..VolumeConfig::portable(crate::Lifecycle::Ephemeral)
        };
        let workspace = fs
            .create_workspace_with_config("raw-authored", config, WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let generation = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let walk = NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::exact(
            generation,
            None,
            GitIgnorePolicy::default(),
        )
        .await
        .unwrap();
        let index = publish_name_walk(&workspace, walk).await;
        let raw = LogicalName::new(encoding, bytes, config.limits.maximum_component_bytes).unwrap();
        // Negative control: the incremental path's previous string conversion
        // fails on both supported native encodings.
        assert!(raw.unicode_text(WorkBudget::UNBOUNDED, &stop).is_err());
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        transaction
            .checkout
            .create(
                crate::AuthoredMutation::CreateFile {
                    path: NamespacePath::new(vec![raw], config.limits).unwrap(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"raw"),
                    metadata: crate::kernel::FileMetadata::default(),
                },
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .unwrap();
        transaction
            .write_text("/healthy.txt", "healthy")
            .await
            .unwrap();
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &stop,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .unwrap();
        let generation = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        assert!(matches!(
            index
                .refresh_authored(
                    &workspace,
                    NameCut::Exact {
                        workspace: workspace.id(),
                        generation: generation.id(),
                    },
                    &GitIgnorePolicy::default()
                )
                .await
                .unwrap(),
            NameIndexRefresh::Ineligible
        ));
        let mut walk = NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::exact(
            generation,
            None,
            GitIgnorePolicy::default(),
        )
        .await
        .unwrap();
        let mut names = Vec::new();
        while let Some(page) = walk.next_page().await.unwrap() {
            names.extend(page.into_iter().map(|name| name.path));
        }
        assert_eq!(names, ["healthy.txt"]);
        assert_eq!(walk.unavailable_entries(), 1);
    }
}

#[tokio::test]
async fn name_search_unrepresentable_names_are_explicitly_partial_without_losing_other_names() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::new());
    source.directory_entries = 2;
    source.non_unicode_entry = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut seen = Vec::new();
    while let Some(page) = walk.next_page().await.unwrap() {
        seen.extend(page.into_iter().map(|name| name.path));
    }
    assert_eq!(seen, ["file-0001.txt"]);
    assert_eq!(walk.unavailable_entries(), 1);
    let checkpoint = walk.checkpoint().unwrap();
    let restored = NameWalk::resume(&lazy, checkpoint, GitIgnorePolicy::default())
        .await
        .unwrap();
    assert_eq!(restored.unavailable_entries(), 1);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    // Negative control: the string projection cannot represent the raw name.
    let (page, _, _) = lazy
        .list_directory_at_snapshot(
            lazy.snapshot(accounting!()).await.unwrap(),
            "/",
            None,
            128,
            accounting!(),
        )
        .await
        .unwrap();
    assert!(page.entries.iter().any(|entry| {
        entry
            .name
            .unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .is_err()
    }));
}

#[tokio::test]
async fn name_search_unreadable_subtree_does_not_abort_healthy_subtrees() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::new());
    source.directory_entries = 0;
    source.unreadable_directory = Some("blocked".into());
    let lazy = attach(&fs, Arc::new(source), MemoryLazyWorkspaceStore::default())
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let mut transaction = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .write_batch(
            &["/blocked", "/healthy"],
            &[(
                "/healthy/visible.txt",
                acyclic_stream::OwnedBytes::from_static(b"visible"),
            )],
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut seen = Vec::new();
    while let Some(page) = walk.next_page().await.unwrap() {
        seen.extend(page.into_iter().map(|name| name.path));
    }
    assert!(seen.iter().any(|name| name == "healthy/visible.txt"));
    assert_eq!(walk.unavailable_entries(), 1);
    assert_eq!(walk.checkpoint().unwrap().unavailable_entries(), 1);
    // Negative control: the underlying directory API still returns the denial.
    assert!(
        matches!(lazy.list_directory_at_snapshot(lazy.snapshot(accounting!()).await.unwrap(),
        "/blocked", None, 128, accounting!()).await,
        Err(crate::LazyWorkspaceError::Demand(DemandError::Io(error)))
            if error.kind() == std::io::ErrorKind::PermissionDenied)
    );
}

#[tokio::test]
async fn name_search_hydration_counter_detects_content_reads() {
    assert!(names_hydrate(64, true).await > 0);
}

#[tokio::test]
async fn name_search_placeholder_policies_are_partial_without_hydration() {
    for metadata in [
        SourceMetadata {
            windows_attributes: Some(0x0000_1000), // OFFLINE
            ..SourceMetadata::default()
        },
        SourceMetadata {
            windows_attributes: Some(0x0004_0000), // RECALL_ON_OPEN
            ..SourceMetadata::default()
        },
        SourceMetadata {
            windows_attributes: Some(0x0040_0000), // RECALL_ON_DATA_ACCESS
            ..SourceMetadata::default()
        },
        SourceMetadata {
            posix_flags: Some(0x4000_0000), // Darwin SF_DATALESS
            ..SourceMetadata::default()
        },
    ] {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"secret.txt\n"));
        source.node.metadata = metadata;
        source.policy_files = true;
        source.directory_entries = 0;
        let source = Arc::new(source);
        let lazy = attach(
            &fs,
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        let stop = CancellationToken::new();
        let mut transaction = lazy
            .workspace()
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        transaction
            .write_batch(
                &["/nested", "/healthy"],
                &[
                    (
                        "/nested/secret.txt",
                        acyclic_stream::OwnedBytes::from_static(b"hidden"),
                    ),
                    (
                        "/healthy/visible.txt",
                        acyclic_stream::OwnedBytes::from_static(b"visible"),
                    ),
                    (
                        "/healthy/.gitignore",
                        acyclic_stream::OwnedBytes::from_static(b""),
                    ),
                    (
                        "/healthy/.ignore",
                        acyclic_stream::OwnedBytes::from_static(b""),
                    ),
                ],
                WorkBudget::UNBOUNDED,
                &stop,
            )
            .await
            .unwrap();
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &stop,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .unwrap();
        let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
            .await
            .unwrap();
        // The owner-side root policy loader uses the same guarded read.
        assert!(matches!(
            walk.read_policy(".gitignore", 1024).await,
            Err(crate::SearchError::PolicyUnavailable(_))
        ));
        let mut seen = Vec::new();
        while let Some(page) = walk.next_page().await.unwrap() {
            seen.extend(page.into_iter().map(|name| name.path));
        }
        assert!(seen.iter().any(|path| path == "healthy/visible.txt"));
        assert!(!seen.iter().any(|path| path == "nested/secret.txt"));
        assert!(walk.unavailable_entries() > 0);
        assert_eq!(source.counts.opens.load(Ordering::Relaxed), 0);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        // Negative control: an explicit content read really reaches this
        // provider; a zero counter is not an inert or empty source.
        assert_eq!(
            &*walk.read(".gitignore", 1024).await.unwrap(),
            b"secret.txt\n"
        );
        assert!(source.counts.ranges.load(Ordering::Relaxed) > 0);
        // Range reads and held-file opens are separate provider entry points.
        // Exercise the latter explicitly so its zero-open control is live too.
        let (
            LazyLookup::Source {
                source: pinned,
                node,
            },
            _,
        ) = lazy.inspect("/.gitignore", accounting!()).await.unwrap()
        else {
            panic!("source policy required");
        };
        let held = lazy
            .open_source_file("/.gitignore", pinned, node, accounting!())
            .await
            .unwrap();
        assert!(source.counts.opens.load(Ordering::Relaxed) > 0);
        drop(held);
    }
}

#[tokio::test]
async fn name_search_checks_mount_boundaries_before_nested_policy_content_reads() {
    async fn read_count(cross_mounts: bool) -> usize {
        let fs = Fs::memory();
        let mut source = CountingSource::new(Bytes::from_static(b"policy\n"));
        source.policy_files = true;
        source.cross_directory_mounts = cross_mounts;
        let source = Arc::new(source);
        let lazy = attach(
            &fs,
            Arc::clone(&source),
            MemoryLazyWorkspaceStore::default(),
        )
        .await
        .unwrap();
        let stop = CancellationToken::new();
        let mut transaction = lazy
            .workspace()
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        transaction
            .write_batch(&["/foreign"], &[], WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap();
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &stop,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .unwrap();
        let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
            .await
            .unwrap();
        let policy = walk.read_policy("foreign/.gitignore", 1024).await;
        if cross_mounts {
            assert!(policy.is_ok());
        } else {
            assert!(matches!(
                policy,
                Err(crate::SearchError::PolicyUnavailable(_))
            ));
            assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
        }
        while walk.next_page().await.unwrap().is_some() {}
        source.counts.ranges.load(Ordering::Relaxed)
    }
    assert_eq!(read_count(false).await, 0);
    // Negative control: allowing entry reads the provider's nested policies.
    assert!(read_count(true).await > 0);
}

#[tokio::test]
async fn name_search_million_generated_entries_are_paged_without_retaining_the_tree() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"never hydrate"));
    source.directory_entries = 1_000_000;
    source.paged = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut total = 0;
    let mut largest = 0;
    let started = std::time::Instant::now();
    while let Some(page) = walk.next_page().await.unwrap() {
        total += page.len();
        largest = largest.max(page.len());
        assert!(
            walk.pending_directories() <= 1,
            "flat generated source retained a namespace frontier"
        );
    }
    assert_eq!(total, 1_000_000);
    assert_eq!(largest, 128);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    eprintln!(
        "volume-search benchmark component=name-walk entries={total} elapsed_us={} maximum_page_entries={largest} hydration_calls={}",
        started.elapsed().as_micros(),
        source.counts.ranges.load(Ordering::Relaxed),
    );
}

#[tokio::test]
async fn name_search_explicit_symlink_is_never_a_regular_candidate() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"/loop"));
    source.node.kind = SourceNodeKind::SymbolicLink;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, Some("loop"), GitIgnorePolicy::parse(""))
        .await
        .unwrap();
    let page = loop {
        let page = walk.next_page().await.unwrap().unwrap();
        if !page.is_empty() {
            break page;
        }
    };
    assert_eq!(page.len(), 1);
    assert!(!page[0].regular);
    assert!(!page[0].directory);
    assert!(walk.next_page().await.unwrap().is_none());
    assert!(matches!(
        walk.read("loop", 1024).await,
        Err(crate::SearchError::Lazy(
            crate::LazyWorkspaceError::NotRegularFile
        ))
    ));
    assert_eq!(source.counts.links.load(Ordering::Relaxed), 0);
    // Negative control: a generic link reader reaches the provider, which
    // this fixture deliberately rejects. Search must reject before that call.
    assert!(
        lazy.read_link(
            "/loop",
            1024,
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new())
        )
        .await
        .is_err()
    );
    assert_eq!(source.counts.links.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn name_search_index_pages_survive_reopen_without_relisting_or_hydrating() {
    use crate::{NameIndex, NameIndexManifest};
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"unread contents"));
    source.directory_entries = 513;
    source.paged = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::parse(""))
        .await
        .unwrap();
    let page = loop {
        let page = walk.next_page().await.unwrap().unwrap();
        if !page.is_empty() {
            break page;
        }
    };
    assert_eq!(page.len(), 128);
    let expected = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    let manifest = NameIndexManifest {
        schema: crate::NameIndexManifest::SCHEMA,
        cut: walk.cut(),
        names_cut: walk.cut(),
        observed_generation: expected.id(),
        pages: 1,
        pending_directories: walk.pending_directories(),
        complete: false,
        checkpoint: walk.checkpoint().unwrap(),
    };
    NameIndex::publish(lazy.workspace(), &expected, manifest, Some(&page), None)
        .await
        .unwrap()
        .unwrap();
    drop(walk);
    let before = source.counts.pages.load(Ordering::Relaxed);
    let index = NameIndex::open(lazy.workspace()).await.unwrap().unwrap();
    for _ in 0..4 {
        assert_eq!(index.page(0).await.unwrap().len(), 128);
    }
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), before);
    let mut resumed =
        NameWalk::resume(&lazy, index.manifest.checkpoint, GitIgnorePolicy::parse(""))
            .await
            .unwrap();
    let mut remaining = 0;
    while let Some(page) = resumed.next_page().await.unwrap() {
        remaining += page.len();
    }
    assert_eq!(remaining, 513 - 128);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn name_search_checkpoint_rejects_another_volume() {
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::new())),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let checkpoint = NameWalk::lazy(&lazy, None, GitIgnorePolicy::parse(""))
        .await
        .unwrap()
        .checkpoint()
        .unwrap();
    let (other, _) = lazy
        .fork(
            "other",
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            accounting!(),
        )
        .await
        .unwrap();
    assert_ne!(lazy.workspace().id(), other.workspace().id());
    assert!(
        NameWalk::resume(&other, checkpoint, GitIgnorePolicy::parse(""))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn name_search_nested_ignore_rules_survive_checkpoint_and_are_relative() {
    let fs = Fs::memory();
    let stop = CancellationToken::new();
    let workspace = fs
        .create_workspace("nested-ignores", WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let bytes = |text: &'static [u8]| acyclic_stream::OwnedBytes::from_static(text);
    transaction
        .write_batch(
            &["/src", "/src/deep", "/src/cache", "/else"],
            &[
                (
                    "/src/.gitignore",
                    bytes(b"!visible.txt\n/only-here.rs\ncache/\n"),
                ),
                ("/src/visible.txt", bytes(b"visible")),
                ("/src/private.txt", bytes(b"ignored")),
                ("/src/only-here.rs", bytes(b"ignored")),
                ("/src/deep/only-here.rs", bytes(b"visible")),
                ("/src/cache/entry.rs", bytes(b"ignored subtree")),
                ("/else/only-here.rs", bytes(b"visible")),
                ("/else/.ignore", bytes(b"!unignored.txt\n")),
                ("/else/unignored.txt", bytes(b"visible")),
            ],
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    for control in [false, true] {
        if control {
            workspace.remove("/src/.gitignore").await.unwrap();
        }
        let head = workspace
            .head(WorkBudget::UNBOUNDED, &stop)
            .await
            .unwrap()
            .value;
        let mut walk = NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::exact(
            head,
            None,
            GitIgnorePolicy::parse("*.txt\n"),
        )
        .await
        .unwrap();
        let mut paths = Vec::new();
        while let Some(page) = walk.next_page().await.unwrap() {
            paths.extend(
                page.into_iter()
                    .filter(|name| name.regular)
                    .map(|name| name.path),
            );
            // Exercise the real serialized frontier, including loaded policy
            // metadata, at every directory continuation.
            let checkpoint = walk.checkpoint().unwrap();
            walk =
                NameWalk::resume_exact(&workspace, checkpoint, GitIgnorePolicy::parse("*.txt\n"))
                    .await
                    .unwrap();
        }
        assert_eq!(paths.iter().any(|path| path == "src/visible.txt"), !control);
        assert_eq!(paths.iter().any(|path| path == "src/only-here.rs"), control);
        assert_eq!(
            paths.iter().any(|path| path == "src/cache/entry.rs"),
            control
        );
        assert!(!paths.iter().any(|path| path == "src/private.txt"));
        assert!(paths.iter().any(|path| path == "src/deep/only-here.rs"));
        assert!(paths.iter().any(|path| path == "else/only-here.rs"));
        assert!(paths.iter().any(|path| path == "else/unignored.txt"));
    }
}

#[tokio::test]
async fn name_search_uses_authored_metadata_before_the_unindexed_source() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"unread"));
    source.directory_entries = 1_000_000;
    source.paged = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    lazy.write(
        "/near.txt",
        Bytes::from_static(b"overlay").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let first = walk.next_page().await.unwrap().unwrap();
    assert_eq!(
        first
            .iter()
            .map(|name| name.path.as_str())
            .collect::<Vec<_>>(),
        ["near.txt"]
    );
    assert_eq!(
        source.counts.pages.load(Ordering::Relaxed),
        0,
        "authored seed waited for source listing"
    );
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
    // Negative control: merged directory enumeration starts with the unindexed
    // source. It cannot provide the known authored file immediately.
    let cut = lazy.snapshot(accounting!()).await.unwrap();
    let (page, _, _) = lazy
        .list_directory_at_snapshot(cut, "/", None, 128, accounting!())
        .await
        .unwrap();
    assert!(
        !page
            .entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"near.txt")
    );
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn name_search_query_prioritizes_pending_siblings_without_hydrating() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::new());
    source.directory_entries = 0;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let stop = CancellationToken::new();
    let mut transaction = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .write_batch(
            &["/a-cold", "/z-hot"],
            &[
                (
                    "/a-cold/cold.txt",
                    acyclic_stream::OwnedBytes::from_static(b"cold"),
                ),
                (
                    "/z-hot/hot.txt",
                    acyclic_stream::OwnedBytes::from_static(b"hot"),
                ),
            ],
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();

    for (query, first) in [(None, "a-cold/cold.txt"), (Some("HoT"), "z-hot/hot.txt")] {
        let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
            .await
            .unwrap();
        let root = walk.next_page().await.unwrap().unwrap();
        assert_eq!(root.iter().filter(|name| name.directory).count(), 2);
        // The query arrives after the siblings were queued. Restart first to
        // establish that scheduling is derived, rather than required index state.
        walk = NameWalk::resume(
            &lazy,
            walk.checkpoint().unwrap(),
            GitIgnorePolicy::default(),
        )
        .await
        .unwrap();
        walk.prefer_directory_names(query);
        let mut found = Vec::new();
        while let Some(page) = walk.next_page().await.unwrap() {
            assert!(walk.pending_directories() <= 5);
            found.extend(
                page.into_iter()
                    .filter(|name| name.regular)
                    .map(|name| name.path),
            );
        }
        // Negative control: ordinary DFS reaches the alphabetically first
        // directory; the active query must change discovery order.
        assert_eq!(found.first().unwrap(), first);
        assert_eq!(found.len(), 2);
    }
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn name_search_authored_directory_frontier_depends_on_depth_not_tree_breadth() {
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::new());
    source.directory_entries = 0;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let stop = CancellationToken::new();
    let directories: Vec<_> = (0..300).map(|i| format!("/dir-{i:04}")).collect();
    let paths: Vec<_> = directories.iter().map(String::as_str).collect();
    let mut transaction = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .write_batch(&paths, &[], WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let mut seen = 0;
    let mut largest = 0;
    while let Some(page) = walk.next_page().await.unwrap() {
        seen += page.iter().filter(|name| name.directory).count();
        largest = largest.max(walk.pending_directories());
        assert!(walk.pending_directories() <= 258);
        // Restoring a mixed authored/native frontier must preserve its DFS
        // order, rather than separating all authored work from native work.
        let checkpoint = walk.checkpoint().unwrap();
        walk = NameWalk::resume(&lazy, checkpoint, GitIgnorePolicy::default())
            .await
            .unwrap();
    }
    assert_eq!(seen, 300);
    assert!(source.counts.pages.load(Ordering::Relaxed) > 0);
    // Negative control: draining a separate authored queue first retains one
    // native continuation per authored directory, exceeding this bound.
    assert!(directories.len() > largest);
}

#[tokio::test]
async fn name_search_index_publication_cannot_rebase_over_a_workspace_write() {
    use crate::{NameIndex, NameIndexManifest};
    let fs = Fs::memory();
    let lazy = attach(
        &fs,
        Arc::new(CountingSource::new(Bytes::new())),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let mut walk = NameWalk::lazy(&lazy, None, GitIgnorePolicy::default())
        .await
        .unwrap();
    let page = walk.next_page().await.unwrap().unwrap();
    let expected = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    let manifest = NameIndexManifest {
        schema: crate::NameIndexManifest::SCHEMA,
        cut: walk.cut(),
        names_cut: walk.cut(),
        observed_generation: expected.id(),
        pages: 1,
        pending_directories: walk.pending_directories(),
        complete: false,
        checkpoint: walk.checkpoint().unwrap(),
    };
    lazy.write(
        "/concurrent.txt",
        Bytes::from_static(b"written").into(),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        accounting!(),
    )
    .await
    .unwrap();
    let current = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    assert_ne!(expected.id(), current.id());
    assert!(
        NameIndex::publish(
            lazy.workspace(),
            &expected,
            manifest.clone(),
            Some(&page),
            None
        )
        .await
        .unwrap()
        .is_none()
    );
    // Negative control: the same index bytes are valid under the current head.
    assert!(
        NameIndex::publish(lazy.workspace(), &current, manifest, Some(&page), None)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn name_search_authored_leaf_refresh_does_not_relist_unchanged_directories() {
    use crate::{NameCut, NameIndex, NameIndexManifest, NameIndexRefresh};
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::from_static(b"never read"));
    source.directory_entries = 513;
    source.paged = true;
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let ignore = GitIgnorePolicy::parse("*.tmp\n");
    let mut walk = NameWalk::lazy(&lazy, None, ignore.clone()).await.unwrap();
    let stop = CancellationToken::new();
    let mut expected = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let mut pages = 0;
    let mut index = loop {
        let page = walk.next_page().await.unwrap();
        pages += u64::from(page.is_some());
        let manifest = NameIndexManifest {
            schema: crate::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages,
            pending_directories: walk.pending_directories(),
            complete: page.is_none(),
            checkpoint: walk.checkpoint().unwrap(),
        };
        let index =
            NameIndex::publish(lazy.workspace(), &expected, manifest, page.as_deref(), None)
                .await
                .unwrap()
                .unwrap();
        expected = index.generation();
        if page.is_none() {
            break index;
        }
    };
    for path in ["/near.txt", "/ignored.tmp"] {
        lazy.workspace().write_text(path, "authored").await.unwrap();
    }
    let before = source.counts.pages.load(Ordering::Relaxed);
    let reads = source.counts.ranges.load(Ordering::Relaxed);
    let cut = NameCut::Lazy(lazy.snapshot(accounting!()).await.unwrap());
    index = match index
        .refresh_authored(lazy.workspace(), cut, &ignore)
        .await
        .unwrap()
    {
        NameIndexRefresh::Published(index) => index,
        _ => panic!("authored leaf delta required namespace discovery"),
    };
    let mut names = Vec::new();
    for page in 0..index.manifest.pages {
        names.extend(
            index
                .page(page)
                .await
                .unwrap()
                .into_iter()
                .map(|name| name.path),
        );
    }
    assert_eq!(names.len(), 514);
    assert!(names.iter().any(|path| path == "near.txt"));
    assert!(!names.iter().any(|path| path == "ignored.tmp"));
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), before);
    assert_eq!(source.counts.ranges.load(Ordering::Relaxed), reads);
    let pages = index.manifest.pages;
    let names_cut = index.manifest.names_cut.clone();
    for revision in 0..3 {
        lazy.workspace()
            .write_text("/near.txt", &format!("changed-{revision}"))
            .await
            .unwrap();
        let cut = NameCut::Lazy(lazy.snapshot(accounting!()).await.unwrap());
        index = match index
            .refresh_authored(lazy.workspace(), cut, &ignore)
            .await
            .unwrap()
        {
            NameIndexRefresh::Published(index) => index,
            _ => panic!("content-only edit required namespace discovery"),
        };
        assert_eq!(
            index.manifest.pages, pages,
            "content edits appended duplicate pages"
        );
        assert_eq!(index.manifest.names_cut, names_cut);
        // Negative control: using the content cut would reset every follower.
        assert_ne!(index.manifest.cut, index.manifest.names_cut);
    }
    for added in 0..8 {
        lazy.workspace()
            .write_text(&format!("/added-{added}.txt"), "new file")
            .await
            .unwrap();
        let cut = NameCut::Lazy(lazy.snapshot(accounting!()).await.unwrap());
        index = match index
            .refresh_authored(lazy.workspace(), cut, &ignore)
            .await
            .unwrap()
        {
            NameIndexRefresh::Published(index) => index,
            _ => panic!("new leaf required namespace discovery"),
        };
        assert_eq!(
            index.manifest.pages, pages,
            "one new file appended an almost empty page"
        );
    }
    // Negative control: append-only publication would add eight pages here.
    assert_ne!(index.manifest.pages, pages + 8);
    #[cfg(feature = "native-mount")]
    {
        let old_cut = index.manifest.cut.clone();
        let old_names = index.manifest.names_cut.clone();
        source.replace(Bytes::from_static(b"native content changed"));
        index = match index
            .refresh_native(
                &lazy,
                lazy.snapshot(accounting!()).await.unwrap(),
                crate::lazy_workspace::NativeNameDelta::default(),
                &ignore,
            )
            .await
            .unwrap()
        {
            NameIndexRefresh::Published(index) => index,
            _ => panic!("content-only native event required namespace discovery"),
        };
        assert_ne!(index.manifest.cut, old_cut);
        assert_eq!(index.manifest.names_cut, old_names);
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), before);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), reads);
        source.replace(Bytes::from_static(b"native metadata changed"));
        let snapshot = lazy.snapshot(accounting!()).await.unwrap();
        let before = source.counts.pages.load(Ordering::Relaxed);
        // Negative control: the authored-only refresh cannot account for the
        // new native epoch; it would require discovery.
        assert!(matches!(
            index
                .refresh_authored(lazy.workspace(), NameCut::Lazy(snapshot), &ignore)
                .await
                .unwrap(),
            NameIndexRefresh::Ineligible
        ));
        index = match index
            .refresh_native(
                &lazy,
                snapshot,
                crate::lazy_workspace::NativeNameDelta {
                    paths: std::collections::BTreeSet::from(["host-new.txt".into()]),
                    all: false,
                    journal: None,
                },
                &ignore,
            )
            .await
            .unwrap()
        {
            NameIndexRefresh::Published(index) => index,
            _ => panic!("native leaf required directory enumeration"),
        };
        assert_eq!(source.counts.pages.load(Ordering::Relaxed), before);
        assert_eq!(source.counts.ranges.load(Ordering::Relaxed), reads);
        let mut found = false;
        for page in 0..index.manifest.pages {
            found |= index
                .page(page)
                .await
                .unwrap()
                .iter()
                .any(|name| name.path == "host-new.txt");
        }
        assert!(found);
    }
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), before);
    // Negative control: rebuilding the same namespace re-lists native pages.
    let mut rebuilt = NameWalk::lazy(&lazy, None, ignore.clone()).await.unwrap();
    while rebuilt.next_page().await.unwrap().is_some() {}
    assert!(source.counts.pages.load(Ordering::Relaxed) > before);
    lazy.workspace().remove("/near.txt").await.unwrap();
    let cut = NameCut::Lazy(lazy.snapshot(accounting!()).await.unwrap());
    assert!(
        matches!(
            index
                .refresh_authored(lazy.workspace(), cut, &ignore)
                .await
                .unwrap(),
            NameIndexRefresh::Ineligible
        ),
        "a native fallback needs source-aware discovery"
    );
    lazy.workspace()
        .write_text("/near.txt", "restored")
        .await
        .unwrap();
    // A policy edit cannot reuse a leaf-only interpretation of the old tree.
    lazy.workspace()
        .write_text("/.ignore", "near.txt\n")
        .await
        .unwrap();
    let cut = NameCut::Lazy(lazy.snapshot(accounting!()).await.unwrap());
    assert!(matches!(
        index
            .refresh_authored(lazy.workspace(), cut, &ignore)
            .await
            .unwrap(),
        NameIndexRefresh::Ineligible
    ));
}

#[tokio::test]
async fn name_search_leaf_refresh_rejects_a_superseded_generation() {
    use crate::{NameCut, NameIndex, NameIndexManifest, NameIndexRefresh};
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "leaf-conflict",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .value;
    let expected = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    let mut walk = NameWalk::<_, _, CountingSource, MemoryLazyWorkspaceStore>::exact(
        expected.clone(),
        None,
        GitIgnorePolicy::default(),
    )
    .await
    .unwrap();
    while walk.next_page().await.unwrap().is_some() {}
    let manifest = NameIndexManifest {
        schema: crate::NameIndexManifest::SCHEMA,
        cut: walk.cut(),
        names_cut: walk.cut(),
        observed_generation: expected.id(),
        pages: 0,
        pending_directories: 0,
        complete: true,
        checkpoint: walk.checkpoint().unwrap(),
    };
    let index = NameIndex::publish(&workspace, &expected, manifest, None, None)
        .await
        .unwrap()
        .unwrap();
    workspace.write_text("/first.txt", "first").await.unwrap();
    let observed = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    let cut = NameCut::Exact {
        workspace: workspace.id(),
        generation: observed.id(),
    };
    workspace.write_text("/second.txt", "second").await.unwrap();
    assert!(matches!(
        index
            .refresh_authored(&workspace, cut, &GitIgnorePolicy::default())
            .await
            .unwrap(),
        NameIndexRefresh::Conflict
    ));
    // Negative control: the same delta publishes when based on the current head.
    let current = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .unwrap()
        .value;
    let cut = NameCut::Exact {
        workspace: workspace.id(),
        generation: current.id(),
    };
    let refreshed = match index
        .refresh_authored(&workspace, cut, &GitIgnorePolicy::default())
        .await
        .unwrap()
    {
        NameIndexRefresh::Published(index) => index,
        _ => panic!("current leaf delta was not published"),
    };
    assert_eq!(refreshed.page(0).await.unwrap().len(), 2);
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn name_search_native_directory_refresh_never_relists_its_unchanged_sibling() {
    use crate::{NameIndex, NameIndexManifest, NameIndexRefresh};
    let fs = Fs::memory();
    let mut source = CountingSource::new(Bytes::new());
    source.directory_names = ["changed".into(), "untouched".into()].into();
    let source = Arc::new(source);
    let lazy = attach(
        &fs,
        Arc::clone(&source),
        MemoryLazyWorkspaceStore::default(),
    )
    .await
    .unwrap();
    let stop = CancellationToken::new();
    let mut transaction = lazy
        .workspace()
        .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    transaction
        .write_batch(
            &["/changed", "/untouched"],
            &[],
            WorkBudget::UNBOUNDED,
            &stop,
        )
        .await
        .unwrap();
    transaction
        .commit(
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &stop,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await
        .unwrap();
    let ignore = GitIgnorePolicy::default();
    let mut walk = NameWalk::lazy(&lazy, None, ignore.clone()).await.unwrap();
    let mut expected = lazy
        .workspace()
        .head(WorkBudget::UNBOUNDED, &stop)
        .await
        .unwrap()
        .value;
    let mut pages = 0;
    let index = loop {
        let page = walk.next_page().await.unwrap();
        pages += u64::from(page.is_some());
        let manifest = NameIndexManifest {
            schema: crate::NameIndexManifest::SCHEMA,
            cut: walk.cut(),
            names_cut: walk.cut(),
            observed_generation: expected.id(),
            pages,
            pending_directories: walk.pending_directories(),
            complete: page.is_none(),
            checkpoint: walk.checkpoint().unwrap(),
        };
        let index =
            NameIndex::publish(lazy.workspace(), &expected, manifest, page.as_deref(), None)
                .await
                .unwrap()
                .unwrap();
        expected = index.generation();
        if page.is_none() {
            break index;
        }
    };
    let before = source.counts.pages.load(Ordering::Relaxed);
    source.replace(Bytes::new());
    let index = match index
        .refresh_native(
            &lazy,
            lazy.snapshot(accounting!()).await.unwrap(),
            crate::lazy_workspace::NativeNameDelta {
                paths: ["changed".into()].into(),
                all: false,
                journal: None,
            },
            &ignore,
        )
        .await
        .unwrap()
    {
        NameIndexRefresh::Published(index) => index,
        _ => panic!("a known directory delta required whole-tree discovery"),
    };
    assert!(!index.manifest.complete);
    assert!(index.manifest.pending_directories > 0);
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), before);
    let mut unaffected = Vec::new();
    for page in 0..index.manifest.pages {
        unaffected.extend(
            index
                .page(page)
                .await
                .unwrap()
                .into_iter()
                .map(|name| name.path),
        );
    }
    assert!(unaffected.iter().any(|path| path == "untouched/file.txt"));
    assert!(!unaffected.iter().any(|path| path == "changed/file.txt"));
    let mut resumed = NameWalk::resume(&lazy, index.manifest.checkpoint, ignore.clone())
        .await
        .unwrap();
    let mut changed = Vec::new();
    while let Some(page) = resumed.next_page().await.unwrap() {
        changed.extend(page.into_iter().map(|name| name.path));
    }
    assert!(changed.iter().any(|path| path == "changed/file.txt"));
    assert!(!changed.iter().any(|path| path.starts_with("untouched/")));
    assert_eq!(source.counts.pages.load(Ordering::Relaxed), before + 1);
    // Negative control: restarting at the root re-lists unchanged siblings.
    let mut full = NameWalk::lazy(&lazy, None, ignore).await.unwrap();
    while full.next_page().await.unwrap().is_some() {}
    assert!(source.counts.pages.load(Ordering::Relaxed) > before + 2);
}
