#![cfg_attr(not(all(windows, feature = "native-mount")), allow(missing_docs))]
#![cfg(all(windows, feature = "native-mount"))]

//! Local NTFS qualification for pinned, no-driver SDK working sets.

use std::sync::Arc;
use std::time::Instant;

use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::kernel::{FileMetadata, MetadataField};
use acyclic_fs::model::{
    CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle, VolumeConfig, VolumeLimits,
};
use acyclic_fs::{
    CancellationToken, CaptureOptions, Fs, IdempotencyKey, LazyWorkspace, LocalOptions,
    MaterializeOptions, MemoryLazyWorkspaceStore, MountPath, PublicationPermit, TransactionCommit,
    WorkBudget, capture_paths, capture_root_identity, host_path_to_namespace,
};
use bytes::Bytes;
use serde_json::json;

fn component(value: &str) -> Vec<u8> {
    value.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

fn percentile(samples: &mut [u128], numerator: usize, denominator: usize) -> u128 {
    samples.sort_unstable();
    let rank = samples
        .len()
        .saturating_mul(numerator)
        .div_ceil(denominator)
        .saturating_sub(1);
    samples
        .get(rank.min(samples.len().saturating_sub(1)))
        .copied()
        .unwrap_or(0)
}

/// Every Windows source stat reads the file record, which counts every name
/// exactly, so each name of one file reports the same link count, and the
/// names are identified exactly by the file identity they share.
#[tokio::test]
async fn ntfs_demand_identifies_hard_links_by_file_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("single"), b"single")?;
    std::fs::write(source.path().join("linked"), b"linked")?;
    std::fs::hard_link(source.path().join("linked"), source.path().join("alias"))?;
    let demand = NativeDemandSource::open(
        source.path(),
        FilesystemProfile::Windows,
        VolumeLimits::default(),
    )
    .await?;
    let mut identities = Vec::new();
    for (name, links) in [("single", 1), ("linked", 2), ("alias", 2)] {
        let path = host_path_to_namespace(
            std::path::Path::new(name),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
        )?;
        let node = demand
            .lookup(demand.reference(), &path, &CancellationToken::new())
            .await?
            .value
            .ok_or("source node was absent")?;
        assert_eq!(node.link_count, Some(links), "{name}");
        identities.push(node.file_identity);
    }
    let [single, linked, alias] = identities.as_slice() else {
        return Err("three lookups were expected".into());
    };
    assert_eq!(linked, alias);
    assert_ne!(single, linked);
    Ok(())
}

#[tokio::test]
async fn ntfs_exactification_preserves_source_hard_links() -> Result<(), Box<dyn std::error::Error>>
{
    let source = tempfile::tempdir()?;
    let hot = source.path().join("hot");
    std::fs::create_dir(&hot)?;
    std::fs::write(hot.join("file"), b"shared")?;
    std::fs::hard_link(hot.join("file"), hot.join("alias"))?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "ntfs-source-links",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let exact = tokio::spawn(async move {
        lazy.exactify_subtree("/hot", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
    })
    .await??;
    let file = exact.value.stat("/hot/file").await?;
    let alias = exact.value.stat("/hot/alias").await?;
    assert_eq!(file.file_id, alias.file_id);
    assert_eq!(file.link_count, 2);
    let view = tempfile::tempdir()?;
    exact
        .value
        .materialize_path(
            "/hot",
            &MaterializeOptions::native(view.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    let view_dir = cap_std::fs::Dir::open_ambient_dir(view.path(), cap_std::ambient_authority())?;
    let file = view_dir.metadata("hot/file")?;
    let alias = view_dir.metadata("hot/alias")?;
    assert_eq!(
        cap_primitives::fs::_WindowsByHandle::file_index(&file),
        cap_primitives::fs::_WindowsByHandle::file_index(&alias),
    );
    assert_eq!(
        cap_primitives::fs::_WindowsByHandle::number_of_links(&file),
        Some(2),
    );
    Ok(())
}

#[tokio::test]
async fn materialized_ntfs_view_preserves_basic_metadata_and_hard_links()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::fs::MetadataExt as _;

    const FIXED_NS: i64 = 1_700_000_000_000_000_000;
    const ARCHIVE: u32 = 0x20;
    const DIRECTORY: u32 = 0x10;
    const HIDDEN: u32 = 0x2;
    const WINDOWS_EPOCH_TICKS: u64 = 116_444_736_000_000_000;
    let ticks = |nanoseconds: i64| -> Result<u64, std::num::TryFromIntError> {
        Ok(WINDOWS_EPOCH_TICKS + u64::try_from(nanoseconds)? / 100)
    };
    let metadata = |attributes| FileMetadata {
        windows_attributes: MetadataField::Value(attributes),
        created_ns: MetadataField::Value(FIXED_NS),
        modified_ns: MetadataField::Value(FIXED_NS + 100),
        accessed_ns: MetadataField::Value(FIXED_NS + 200),
        changed_ns: MetadataField::Value(FIXED_NS + 300),
        ..FileMetadata::default()
    };
    let fs = Fs::memory();
    let workspace = fs.create_workspace("ntfs-metadata-view").await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_directory("/tree").await?;
    transaction
        .write("/tree/file", Bytes::from_static(b"payload"))
        .await?;
    transaction.hard_link("/tree/file", "/tree/alias").await?;
    transaction
        .create_symbolic_link("/tree/link", Bytes::from(component("file")))
        .await?;
    transaction
        .set_metadata("/tree/file", metadata(ARCHIVE | HIDDEN))
        .await?;
    transaction
        .set_metadata("/tree", metadata(DIRECTORY | HIDDEN))
        .await?;
    transaction
        .set_metadata(
            "/tree/link",
            FileMetadata {
                created_ns: MetadataField::Value(FIXED_NS),
                modified_ns: MetadataField::Value(FIXED_NS + 100),
                accessed_ns: MetadataField::Value(FIXED_NS + 200),
                changed_ns: MetadataField::Value(FIXED_NS + 300),
                ..FileMetadata::default()
            },
        )
        .await?;
    let TransactionCommit::Committed(generation) = transaction.commit().await? else {
        return Err("metadata fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    generation
        .materialize_path(
            "/tree",
            &MaterializeOptions::native(destination.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    for (relative, attributes) in [
        ("tree", DIRECTORY | HIDDEN),
        ("tree/file", ARCHIVE | HIDDEN),
        ("tree/alias", ARCHIVE | HIDDEN),
    ] {
        let observed = std::fs::symlink_metadata(destination.path().join(relative))?;
        assert_eq!(observed.file_attributes(), attributes);
        assert_eq!(observed.creation_time(), ticks(FIXED_NS)?);
        assert_eq!(observed.last_write_time(), ticks(FIXED_NS + 100)?);
        assert_eq!(observed.last_access_time(), ticks(FIXED_NS + 200)?);
    }
    let link = std::fs::symlink_metadata(destination.path().join("tree/link"))?;
    assert!(link.file_type().is_symlink());
    assert_eq!(link.creation_time(), ticks(FIXED_NS)?);
    assert_eq!(link.last_write_time(), ticks(FIXED_NS + 100)?);
    assert_eq!(link.last_access_time(), ticks(FIXED_NS + 200)?);
    std::fs::write(destination.path().join("tree/file"), b"changed")?;
    assert_eq!(
        std::fs::read(destination.path().join("tree/alias"))?,
        b"changed"
    );
    Ok(())
}

#[tokio::test]
async fn captured_ntfs_link_retains_restorable_metadata() -> Result<(), Box<dyn std::error::Error>>
{
    use std::os::windows::fs::MetadataExt as _;
    use std::os::windows::fs::OpenOptionsExt as _;
    use std::time::{Duration, UNIX_EPOCH};

    let host = tempfile::tempdir()?;
    std::fs::write(host.path().join("target"), b"target")?;
    let link = host.path().join("link");
    std::os::windows::fs::symlink_file("target", &link)?;
    const FILE_WRITE_ATTRIBUTES: u32 = 0x100;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    let historic_time = UNIX_EPOCH - Duration::from_secs(86_400);
    std::fs::OpenOptions::new()
        .access_mode(FILE_WRITE_ATTRIBUTES)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(&link)?
        .set_times(std::fs::FileTimes::new().set_modified(historic_time))?;
    let expected_attributes = std::fs::symlink_metadata(&link)?.file_attributes();
    assert_eq!(std::fs::symlink_metadata(&link)?.modified()?, historic_time);

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let workspace = fs
        .create_workspace_with_config(
            "ntfs-link-capture",
            VolumeConfig::native(Lifecycle::Durable),
        )
        .await?;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
        )
        .await?;
    let path = host_path_to_namespace(
        std::path::Path::new("link"),
        FilesystemProfile::Windows,
        VolumeLimits::default(),
    )?;
    capture_paths(
        &mut checkout,
        std::slice::from_ref(&path),
        &CaptureOptions {
            source_root: host.path().to_path_buf(),
            expected_root_identity: capture_root_identity(host.path())?,
            maximum_paths: 1,
            maximum_extent_spans: 8,
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let captured = checkout
        .read_metadata(&path, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    assert_eq!(
        captured.windows_attributes,
        MetadataField::Value(expected_attributes)
    );
    assert_eq!(
        captured.modified_ns,
        MetadataField::Value(-86_400_000_000_000)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "local-only 1k-path NTFS preparation and capture qualification"]
#[allow(
    clippy::too_many_lines,
    reason = "one ignored benchmark measures every phase of one working-set lifecycle"
)]
async fn report_ntfs_working_set_costs() -> Result<(), Box<dyn std::error::Error>> {
    const DIRECTORIES: usize = 100;
    const FILES_PER_DIRECTORY: usize = 10;
    const CHANGES: usize = 100;
    const ACTIVATIONS: usize = 101;

    let source = tempfile::tempdir()?;
    let hot = source.path().join("hot");
    std::fs::create_dir(&hot)?;
    for directory in 0..DIRECTORIES {
        let target = hot.join(format!("d{directory:03}"));
        std::fs::create_dir(&target)?;
        for file in 0..FILES_PER_DIRECTORY {
            std::fs::write(
                target.join(format!("f{file:03}.txt")),
                format!("payload-{directory:03}-{file:03}\n"),
            )?;
        }
    }

    // A regular-file-only native copy is a lower bound, not an SDK-semantic
    // substitute: it does not capture source lineage, conflicts, or recovery.
    let native_view = tempfile::tempdir()?;
    let started = Instant::now();
    let native_hot = native_view.path().join("hot");
    std::fs::create_dir(&native_hot)?;
    for directory in 0..DIRECTORIES {
        let name = format!("d{directory:03}");
        let from = hot.join(&name);
        let to = native_hot.join(&name);
        std::fs::create_dir(&to)?;
        for file in 0..FILES_PER_DIRECTORY {
            let name = format!("f{file:03}.txt");
            std::fs::copy(from.join(&name), to.join(&name))?;
        }
    }
    let native_copy_us = started.elapsed().as_micros();

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let started = Instant::now();
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "windows-ntfs-costs",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let attach_us = started.elapsed().as_micros();

    let view = tempfile::tempdir()?;
    let started = Instant::now();
    let working_set = lazy
        .prepare_native_working_set(
            "/hot",
            &MaterializeOptions::native(view.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await?;
    let preparation_us = started.elapsed().as_micros();

    let warm_view = tempfile::tempdir()?;
    let started = Instant::now();
    let warm_working_set = lazy
        .prepare_native_working_set(
            "/hot",
            &MaterializeOptions::native(warm_view.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await?;
    let warm_preparation_us = started.elapsed().as_micros();
    warm_working_set.validate_for_presentation().await?;

    let mut activation_samples = Vec::with_capacity(ACTIVATIONS);
    for _ in 0..ACTIVATIONS {
        let started = Instant::now();
        working_set.validate_for_presentation().await?;
        activation_samples.push(started.elapsed().as_micros());
    }
    let activation_p50_us = percentile(&mut activation_samples.clone(), 50, 100);
    let activation_p95_us = percentile(&mut activation_samples, 95, 100);

    let mut changed = Vec::with_capacity(CHANGES);
    for index in 0..CHANGES {
        let directory = index % DIRECTORIES;
        let file = index / DIRECTORIES;
        std::fs::write(
            view.path()
                .join("hot")
                .join(format!("d{directory:03}/f{file:03}.txt")),
            format!("changed-{index}\n"),
        )?;
        changed.push(
            MountPath::root()
                .child(component(&format!("d{directory:03}")))
                .child(component(&format!("f{file:03}.txt"))),
        );
    }
    let started = Instant::now();
    working_set.capture_host_paths(&changed).await?;
    let capture_us = started.elapsed().as_micros();
    let started = Instant::now();
    working_set.capture_host_subtree(&MountPath::root()).await?;
    let subtree_capture_us = started.elapsed().as_micros();
    let started = Instant::now();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted)
        .await?;
    let sync_us = started.elapsed().as_micros();

    // Profile the two preparation phases independently on a fresh lazy root.
    let phase_state = tempfile::tempdir()?;
    let phase_fs = Fs::local(LocalOptions::new(phase_state.path())).await?;
    let phase_demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
        )
        .await?,
    );
    let phase_lazy = LazyWorkspace::attach(
        &phase_fs,
        "windows-ntfs-phases",
        phase_demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let started = Instant::now();
    let phase_exact = tokio::spawn(async move {
        phase_lazy
            .exactify_subtree("/hot", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
    })
    .await??;
    let exactify_us = started.elapsed().as_micros();
    let phase_view = tempfile::tempdir()?;
    let started = Instant::now();
    phase_exact
        .value
        .materialize_path(
            "/hot",
            &MaterializeOptions::native(phase_view.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    // The public materializer requests durable output, including file syncs;
    // native working-set preparation uses a reconstructible view instead.
    let durable_materialize_us = started.elapsed().as_micros();

    println!(
        "{}",
        json!({
            "schema": "acyclic-windows-ntfs-working-set-cost-v4",
            "files": DIRECTORIES * FILES_PER_DIRECTORY,
            "changed_paths": CHANGES,
            "native_copy_us": native_copy_us,
            "attach_us": attach_us,
            "preparation_us": preparation_us,
            "exactify_us": exactify_us,
            "durable_materialize_us": durable_materialize_us,
            "warm_preparation_us": warm_preparation_us,
            "activation_p50_us": activation_p50_us,
            "activation_p95_us": activation_p95_us,
            "capture_us": capture_us,
            "subtree_capture_us": subtree_capture_us,
            "sync_us": sync_us,
        })
    );
    Ok(())
}

#[tokio::test]
async fn native_lazy_workspace_lists_windows_source_directories()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let store = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::write(source.path().join("dir").join("file"), b"file")?;
    let fs = Fs::local(LocalOptions::new(store.path())).await?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach_with_config(
        &fs,
        "ntfs-native-listing",
        demand,
        MemoryLazyWorkspaceStore::default(),
        VolumeConfig::native(Lifecycle::Durable),
    )
    .await?;
    for directory in ["/", "/dir"] {
        let page = lazy.list_directory(directory, None, 64).await?;
        assert_eq!(page.entries.len(), 1, "{directory}");
    }
    Ok(())
}
