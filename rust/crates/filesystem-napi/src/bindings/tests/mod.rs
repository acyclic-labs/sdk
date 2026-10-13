use super::*;
use acyclic_fs::WorkBudget;

fn test_config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Durable,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

fn cache(maximum_entries: u32, maximum_bytes: BigInt) -> NativeObjectCacheOptions {
    NativeObjectCacheOptions {
        maximum_entries: NapiU32(maximum_entries),
        maximum_bytes,
        maximum_in_flight: NapiU32(2),
        maximum_waiters_per_object: NapiU32(2),
    }
}

async fn open(root: &tempfile::TempDir) -> Result<NativeFs> {
    NativeFs::open(
        root.path().to_string_lossy().into_owned(),
        cache(8, bigint(1 << 20)),
    )
    .await
}

#[test]
fn native_work_json_preserves_full_u64_values() {
    let work = acyclic_fs::WorkCounters {
        authority_records_read: 9_007_199_254_740_993,
        bytes_copied: u64::MAX,
        ..Default::default()
    };
    let value: serde_json::Value =
        serde_json::from_str(&work_json(&work).expect("work counters serialize")).expect("JSON");
    let canonical = serde_json::to_value(acyclic_fs::WorkCounters::default()).expect("JSON");
    let keys = |value: &serde_json::Value| {
        let mut keys = value
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_unstable();
        keys
    };
    assert_eq!(keys(&value), keys(&canonical));
    assert!(
        value
            .as_object()
            .expect("object")
            .values()
            .all(serde_json::Value::is_string)
    );
    assert_eq!(value["authorityRecordsRead"], "9007199254740993");
    assert_eq!(value["bytesCopied"], u64::MAX.to_string());
}

#[test]
fn string_paths_and_directory_cursors_use_native_profile() -> Result<()> {
    let mut config = test_config();
    config.profile = FilesystemProfile::Windows;
    let path = native_path("/\u{e9}.txt", config)?;
    let cursor = native_name("\u{e9}.txt", config)?;
    assert_eq!(path.components(), &[cursor]);
    assert_eq!(
        path.components().first().map(LogicalName::encoding),
        Some(NameEncoding::WindowsUtf16Le)
    );
    Ok(())
}

#[test]
fn capability_identity_matches_build_target() {
    let facts = native_capabilities();
    assert_eq!(facts.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(facts.platform, std::env::consts::OS);
    assert_eq!(facts.architecture, std::env::consts::ARCH);
    assert!(facts.local && facts.native_watch);
}

#[tokio::test]
async fn object_cache_bounds_are_exact() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = open(&root).await?;
    assert_eq!(fs.object_cache_stats()?.resident_entries.get_u64().1, 0);
    fs.clear_object_cache()?;
    let path = || root.path().to_string_lossy().into_owned();
    assert!(
        NativeFs::open(path(), cache(0, bigint(1024)))
            .await
            .is_err()
    );
    let lossy = BigInt {
        sign_bit: false,
        words: vec![0, 1],
    };
    assert!(NativeFs::open(path(), cache(8, lossy)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn workspace_transaction_statuses_cross_the_boundary()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let workspace = open(&root)
        .await?
        .create_workspace("main".to_owned())
        .await?;
    let written = workspace
        .write("/value.bin".to_owned(), Buffer::from(vec![0, 255]))
        .await?;
    assert_eq!(written.status, "committed");
    assert_eq!(
        workspace
            .read("/value.bin".to_owned(), bigint(16))
            .await?
            .as_ref(),
        &[0, 255]
    );
    let winner = workspace
        .begin_transaction(Some(Buffer::from(vec![1; 16])))
        .await?;
    let loser = workspace
        .begin_transaction(Some(Buffer::from(vec![2; 16])))
        .await?;
    winner
        .write("/race".to_owned(), Buffer::from(vec![1]))
        .await?;
    loser
        .write("/race".to_owned(), Buffer::from(vec![2]))
        .await?;
    assert_eq!(winner.commit().await?.status, "committed");
    assert_eq!(loser.commit().await?.status, "conflict");
    let rebased = loser.rebase(NapiU32(16)).await?;
    assert_eq!(rebased.status, "conflicted");
    assert!(!rebased.conflicts.is_empty());
    Ok(())
}

#[tokio::test]
async fn attached_workspace_rescans_and_seals_one_source()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("value.bin"), [1_u8, 2, 3])?;
    let options = NativeSourceOptions {
        mode: "tracking".to_owned(),
        maximum_paths: NapiU32(128),
        maximum_extent_spans: NapiU32(128),
        maximum_queued_changes: NapiU32(128),
        excluded_paths: None,
    };
    let source_path = source.path().to_string_lossy().into_owned();
    let workspace = open(&root)
        .await?
        .attach_directory("attached".to_owned(), source_path, options)
        .await?;
    assert_eq!(workspace.source_state().await.status, "clean");
    std::fs::write(source.path().join("value.bin"), [4_u8, 5])?;
    assert_eq!(workspace.rescan_source().await?.status, "clean");
    let sealed = workspace.seal().await?;
    assert_eq!(
        sealed
            .read("/value.bin".to_owned(), bigint(8))
            .await?
            .as_ref(),
        &[4, 5]
    );
    assert_eq!(workspace.source_state().await.status, "sealed");
    Ok(())
}

type Shared = SharedCheckout<LocalAuthorityBackend, LocalObjectBackend>;

async fn checkout(fs: &LocalFs) -> std::result::Result<Shared, Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let volume = fs
        .create_volume(test_config(), WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mode = CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::PrivateOverlay,
    };
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            mode,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    Ok(SharedCheckout::new(checkout))
}

#[tokio::test]
async fn publication_fence_blocks_mutation_and_reconcile_until_resolved()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source_root = tempfile::tempdir()?;
    let fs = LocalFs::local(LocalOptions::new(root.path())).await?;
    let shared = Arc::new(checkout(&fs).await?);
    let native = NativeCheckout {
        inner: Arc::clone(&shared),
        config: test_config(),
        cancellation: CancellationToken::new(),
        acquisition_work: acyclic_fs::WorkCounters::default(),
    };
    let unresolved = OperationId::new();
    Box::pin(shared.lock())
        .await
        .retain_operation_id(unresolved)?;
    let blocked = native
        .create_file("/blocked".to_owned(), Buffer::from(b"x".to_vec()))
        .await;
    assert!(blocked.is_err());
    assert!(!Box::pin(shared.lock()).await.has_pending_mutations());

    let watch_options = NativeWatchOptions {
        limits: test_config().limits,
        maximum_queued_changes: 64,
        recursive: true,
    };
    let watch =
        FsNativeWatch::open_with_profile(source_root.path(), test_config().profile, watch_options)?;
    let watcher = NativeWatcher {
        inner: std::sync::Mutex::new(watch),
        pending: std::sync::Mutex::new(None),
        pending_reconcile: std::sync::Mutex::new(None),
        operation: tokio::sync::Mutex::new(()),
        checkout: Arc::clone(&shared),
        source_root: source_root.path().to_path_buf(),
        cancellation: CancellationToken::new(),
    };
    assert!(
        Box::pin(watcher.reconcile(NapiU32(64), NapiU32(64)))
            .await
            .is_err()
    );
    Box::pin(shared.lock())
        .await
        .clear_retained_operation(unresolved);
    Box::pin(watcher.reconcile(NapiU32(64), NapiU32(64))).await?;
    Ok(())
}

#[tokio::test]
async fn materialization_writes_checkout_bytes()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let destination = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let fs = LocalFs::local(LocalOptions::new(root.path())).await?;
    let volume = NativeVolume {
        inner: fs
            .create_volume(test_config(), WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value,
        cancellation,
        acquisition_work: Default::default(),
    };
    let checkout = volume
        .checkout(NativeCheckoutOptions {
            access: "read-write".to_owned(),
            consistency: "pinned".to_owned(),
            mutation_mode: "private-cow".to_owned(),
        })
        .await?;
    checkout
        .create_file("/value.bin".to_owned(), Buffer::from(vec![0, 1, 255]))
        .await?;
    checkout
        .materialize(NativeMaterializeOptions {
            destination: destination.path().to_string_lossy().into_owned(),
            maximum_directory_entries: NapiU32(64),
            maximum_extent_spans: NapiU32(64),
            transfer_bytes: bigint(4096),
        })
        .await?;
    assert_eq!(
        std::fs::read(destination.path().join("value.bin"))?,
        [0, 1, 255]
    );
    Ok(())
}
