//! Local NTFS qualification for pinned, no-driver SDK working sets.

#![cfg(windows)]

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

use std::sync::Arc;

use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::facade::FsWork;
use acyclic_fs::model::{
    CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle, VolumeConfig, VolumeLimits,
};
use acyclic_fs_format::metadata::{FileMetadata, MetadataField};
use acyclic_stream::OwnedBytes;
use {
    acyclic_fs::{
        CancellationToken, CaptureOptions, Fs, IdempotencyKey, LazyWorkspace, LocalOptions,
        MaterializeOptions, MemoryLazyWorkspaceStore, PublicationPermit, TransactionCommit,
        capture_paths, capture_root_identity, host_path_to_namespace,
    },
    acyclic_fs_work::performance::WorkBudget,
};

fn component(value: &str) -> Vec<u8> {
    value.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

#[tokio::test]
async fn ntfs_demand_identifies_hard_links_by_file_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("single"), b"single")?;
    std::fs::write(source.path().join("linked"), b"linked")?;
    std::fs::hard_link(source.path().join("linked"), source.path().join("alias"))?;
    let demand = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(source.path()).to_path_buf()),
        FilesystemProfile::Windows,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let mut identities = Vec::new();
    for (name, links) in [("single", 1), ("linked", 2), ("alias", 2)] {
        let path = host_path_to_namespace(
            std::path::Path::new(name),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
        )?;
        let node = demand
            .lookup(
                demand.reference(),
                &path,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
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
            std::borrow::Cow::Owned(std::path::Path::new(source.path()).to_path_buf()),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
            None,
            None,
            acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "ntfs-source-links",
        demand,
        MemoryLazyWorkspaceStore::default(),
        acyclic_fs::VolumeInitialization {
            config: acyclic_fs::VolumeConfig::portable(if (&fs).capabilities().durable {
                acyclic_fs::Lifecycle::Durable
            } else {
                acyclic_fs::Lifecycle::Ephemeral
            }),
            access_policy: None,
        },
        acyclic_fs_format::identity::OperationId::new(),
        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
    )
    .await
    .map(|(lazy, _)| lazy)?;
    let cancellation = CancellationToken::new();
    let mut work = WorkBudget::UNBOUNDED.accounting(&cancellation);
    let exact = lazy
        .exactify("/hot", PublicationPermit::Unrestricted, &mut work)
        .await?;
    let file = unbounded!(exact, stat("/hot/file"))?;
    let alias = unbounded!(exact, stat("/hot/alias"))?;
    assert_eq!(file.file_id, alias.file_id);
    assert_eq!(file.link_count, 2);
    let view = tempfile::tempdir()?;
    exact
        .materialize(
            std::iter::once("/hot"),
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
    let cancellation = CancellationToken::new();
    let workspace =
        Box::pin(fs.create_workspace("ntfs-metadata-view", WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value;
    let mut transaction = Box::pin(workspace.begin_transaction(
        IdempotencyKey::new(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    Box::pin(transaction.create_directory("/tree", WorkBudget::UNBOUNDED, &cancellation)).await?;
    Box::pin(transaction.write(
        "/tree/file",
        OwnedBytes::from_static(b"payload"),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Box::pin(transaction.hard_link(
        "/tree/file",
        "/tree/alias",
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Box::pin(transaction.create_symbolic_link(
        "/tree/link",
        OwnedBytes::from(component("file")),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Box::pin(transaction.set_metadata(
        "/tree/file",
        metadata(ARCHIVE | HIDDEN),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Box::pin(transaction.set_metadata(
        "/tree",
        metadata(DIRECTORY | HIDDEN),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Box::pin(transaction.set_metadata(
        "/tree/link",
        FileMetadata {
            created_ns: MetadataField::Value(FIXED_NS),
            modified_ns: MetadataField::Value(FIXED_NS + 100),
            accessed_ns: MetadataField::Value(FIXED_NS + 200),
            changed_ns: MetadataField::Value(FIXED_NS + 300),
            ..FileMetadata::default()
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let TransactionCommit::Committed(generation) = Box::pin(transaction.commit(
        None,
        PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &cancellation,
        |_| async {
            Ok::<_, acyclic_fs_work::performance::OperationFailure<acyclic_fs::WorkspaceError>>(
                acyclic_fs_work::performance::OperationReceipt {
                    value: (),
                    work: acyclic_fs_work::performance::WorkCounters::default(),
                },
            )
        },
    ))
    .await?
    .value
    else {
        return Err("metadata fixture did not commit".into());
    };
    let destination = tempfile::tempdir()?;
    Box::pin(generation.materialize(
        std::iter::once("/tree"),
        &MaterializeOptions::native(destination.path()),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
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
    let workspace = unbounded!(
        fs,
        create_workspace_with_config(
            "ntfs-link-capture",
            VolumeConfig::native(Lifecycle::Durable)
        )
    )?;
    let mut checkout = unbounded!(
        workspace,
        checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction()
        )
    )?;
    let path = host_path_to_namespace(
        std::path::Path::new("link"),
        FilesystemProfile::Windows,
        VolumeLimits::default(),
    )?;
    capture_paths(
        &mut checkout,
        std::slice::from_ref(&path),
        &CaptureOptions {
            materialized_view: None,
            source_namespace: None,
            source_root: host.path().to_path_buf(),
            expected_root_identity: capture_root_identity(host.path())?,
            maximum_paths: Some(1),
            maximum_extent_spans: Some(8),
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
async fn native_lazy_workspace_lists_windows_source_directories()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let store = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("dir"))?;
    std::fs::write(source.path().join("dir").join("file"), b"file")?;
    let fs = Fs::local(LocalOptions::new(store.path())).await?;
    let demand = Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Owned(std::path::Path::new(source.path()).to_path_buf()),
            FilesystemProfile::Windows,
            VolumeLimits::default(),
            None,
            None,
            acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "ntfs-native-listing",
        demand,
        MemoryLazyWorkspaceStore::default(),
        acyclic_fs::VolumeInitialization::new(VolumeConfig::native(Lifecycle::Durable), None),
        acyclic_fs_format::identity::OperationId::new(),
        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
    )
    .await
    .map(|(lazy, _)| lazy)?;
    for directory in ["/", "/dir"] {
        let page = lazy
            .list_directory(
                directory,
                None,
                64,
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .await?
            .0;
        assert_eq!(page.entries.len(), 1, "{directory}");
    }
    Ok(())
}
