use acyclic_fs::model::{
    CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, VolumeConfig,
};
use acyclic_fs::{
    CancellationToken, Checkout, Fs, MemoryAuthorityBackend, MemoryObjectBackend, Volume,
};
use acyclic_fs_work::performance::WorkBudget;

pub(super) type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
pub(super) type TestCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;
type TestVolume = Volume<MemoryAuthorityBackend, MemoryObjectBackend>;

pub(super) fn config() -> VolumeConfig {
    VolumeConfig::portable(Lifecycle::Ephemeral)
}

pub(super) async fn checkout() -> TestResult<TestCheckout> {
    checkout_with_config(config()).await
}

pub(super) async fn checkout_with_config(config: VolumeConfig) -> TestResult<TestCheckout> {
    open_checkout(&volume(config).await?).await
}

pub(super) async fn volume(config: VolumeConfig) -> TestResult<TestVolume> {
    Ok(Fs::memory()
        .create_volume(config, WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value)
}

pub(super) async fn open_checkout(volume: &TestVolume) -> TestResult<TestCheckout> {
    Ok(volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                consistency: ConsistencyMode::Pinned,
                ..CheckoutMode::tracking_transaction()
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value)
}
