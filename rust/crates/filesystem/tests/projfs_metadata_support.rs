use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    VolumeConfig,
};
use acyclic_fs::{
    CancellationToken, CheckoutMountSource, Fs, LocalOptions, MountFilesystem, MountOptions,
    MountPath, MountPublication, SharedCheckout, WorkBudget,
};
use bytes::Bytes;
use std::sync::Arc;

pub(super) type MemorySource =
    CheckoutMountSource<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;

pub(super) fn windows_name(name: &str) -> Vec<u8> {
    name.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

pub(super) fn windows_path(name: &str) -> MountPath {
    MountPath::root().child(windows_name(name))
}

pub(super) async fn windows_checkout_source()
-> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
    windows_checkout_source_with_files(0).await
}

pub(super) async fn windows_checkout_source_with_files(
    files: usize,
) -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = acyclic_fs::model::FilesystemProfile::Windows;
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = fs
        .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    if files != 0 {
        let mut mutations = Vec::with_capacity(files);
        for index in 0..files {
            let portable =
                acyclic_fs::path::PortablePath::parse(&format!("/f{index}.txt"), config.limits)?;
            mutations.push(acyclic_fs::AuthoredMutation::CreateFile {
                path: acyclic_fs::kernel::NamespacePath::from_portable_in_profile(
                    &portable,
                    config.profile,
                    config.limits,
                )?,
                bytes: Bytes::from(vec![b'x'; 4096 * (index % 2)]).into(),
                metadata: FileMetadata::default(),
            });
        }
        checkout
            .apply_authored_transaction(mutations, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
    }
    Ok(Arc::new(CheckoutMountSource::new(
        Arc::new(SharedCheckout::new(checkout)),
        config,
    )?))
}

pub(super) fn mount_source(
    source: &Arc<MemorySource>,
) -> Result<
    (
        tempfile::TempDir,
        std::path::PathBuf,
        acyclic_fs::NativeMountSession,
    ),
    Box<dyn std::error::Error>,
> {
    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    let session = acyclic_fs::mount_native(
        acyclic_fs::NativeMountRequest {
            mount_id: acyclic_fs::MountId::new(),
            volume_id: source.volume_id()?,
            destination: destination.clone(),
            writable: true,
        },
        Arc::clone(source) as Arc<dyn MountFilesystem>,
    )?;
    Ok((root, destination, session))
}

pub(super) fn setup_work()
-> Result<acyclic_fs::NativeMaterializationWork, Box<dyn std::error::Error>> {
    acyclic_fs::initialize_native_mount_services()?;
    Ok(
        acyclic_fs::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .map_err(|(cause, _)| cause)?,
    )
}

pub(super) async fn local_workspace(
    root: &std::path::Path,
    name: &str,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<
    acyclic_fs::Workspace<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    Box<dyn std::error::Error>,
> {
    Ok(Fs::local(LocalOptions::new(root.join("state")))
        .await?
        .create_workspace(name, WorkBudget::UNBOUNDED, work.cancellation())
        .await
        .map_err(|failure| failure.error)?
        .value)
}

pub(super) fn manual_mount_options() -> MountOptions {
    MountOptions::read_write().publication(MountPublication::Manual)
}
