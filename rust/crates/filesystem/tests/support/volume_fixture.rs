use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, ByteRange, CancellationToken,
    Checkout, Fs, FsError, MemoryAuthorityBackend, MemoryObjectBackend, PublicationPermit, Volume,
    VolumeConfig,
};
use acyclic_fs_format::identity::{FileId, OperationId};
use acyclic_fs_work::performance::OperationFailure;
use acyclic_stream::OwnedBytes;

use super::checkout_fixture::{path, writable_pinned};
use super::{B, allow};

pub(super) type TestVolume = Volume<MemoryAuthorityBackend, MemoryObjectBackend>;
pub(super) type TestCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) async fn fixture(
    config: VolumeConfig,
    cancel: &CancellationToken,
) -> TestResult<(TestVolume, TestCheckout)> {
    #[cfg(not(target_arch = "wasm32"))]
    acyclic_native_runtime::initialize_native_io_services()?;
    let volume = Fs::memory().create_volume(config, B, cancel).await?.value;
    let checkout = open_checkout(&volume, writable_pinned(), cancel).await?;
    Ok((volume, checkout))
}

pub(super) async fn create(
    checkout: &mut TestCheckout,
    name: &str,
    bytes: OwnedBytes,
    cancel: &CancellationToken,
) -> TestResult<FileId> {
    Ok(checkout
        .create(
            AuthoredMutation::CreateFile {
                file_id: None,
                path: path(name)?,
                bytes,
                metadata: FileMetadata::default(),
            },
            B,
            cancel,
        )
        .await?
        .value)
}

pub(super) async fn commit<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    checkout: &mut Checkout<A, O>,
    cancel: &CancellationToken,
) -> TestResult {
    checkout
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            cancel,
            allow::<FsError>,
        )
        .await?;
    Ok(())
}

pub(super) async fn read(
    checkout: &mut TestCheckout,
    id: FileId,
    offset: u64,
    length: u64,
    cancel: &CancellationToken,
) -> TestResult<OwnedBytes> {
    Ok(checkout
        .read_file_range(id, ByteRange { offset, length }, B, cancel)
        .await?
        .value
        .bytes)
}

pub(super) async fn open_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    mode: CheckoutMode,
    cancel: &CancellationToken,
) -> Result<Checkout<A, O>, OperationFailure<FsError>> {
    volume
        .checkout(GenerationSelector::Head, mode, B, cancel)
        .await
        .map(|receipt| receipt.value)
}
