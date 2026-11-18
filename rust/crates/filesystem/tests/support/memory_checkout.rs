use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Checkout, Fs, FsError,
    MemoryAuthorityBackend, MemoryObjectBackend, Volume, VolumeConfig,
};
use acyclic_fs_work::performance::{OperationFailure, WorkBudget};

pub(crate) type TestVolume = Volume<MemoryAuthorityBackend, MemoryObjectBackend>;
pub(crate) type TestCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;
pub(crate) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(crate) async fn volume(
    config: VolumeConfig,
    cancel: &CancellationToken,
) -> TestResult<TestVolume> {
    Ok(Fs::memory()
        .create_volume(config, WorkBudget::UNBOUNDED, cancel)
        .await?
        .value)
}

pub(crate) async fn open_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    mode: CheckoutMode,
    cancel: &CancellationToken,
) -> Result<Checkout<A, O>, OperationFailure<FsError>> {
    volume
        .checkout(
            GenerationSelector::Head,
            mode,
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await
        .map(|receipt| receipt.value)
}
