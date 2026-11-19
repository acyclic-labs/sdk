use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, Fs, FsError,
    MemoryAuthorityBackend, MemoryObjectBackend, Volume, VolumeConfig,
};
use acyclic_fs_work::performance::{OperationFailure, WorkBudget};

/// A volume whose authority and objects use in-memory providers.
pub type TestVolume = Volume<MemoryAuthorityBackend, MemoryObjectBackend>;
/// A checkout whose authority and objects use in-memory providers.
pub type TestCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;
/// An error propagated from a case or fixture.
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(crate) async fn volume(
    config: VolumeConfig,
    cancel: &CancellationToken,
) -> TestResult<TestVolume> {
    Ok(Fs::memory()
        .create_volume(config, WorkBudget::UNBOUNDED, cancel)
        .await?
        .value)
}

/// Opens the volume head in the given mode and returns the checkout from its receipt.
pub async fn open_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    mode: CheckoutMode,
    cancel: &CancellationToken,
) -> Result<Checkout<A, O>, Box<OperationFailure<FsError>>> {
    volume
        .checkout(
            GenerationSelector::Head,
            mode,
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(Box::new)
}
