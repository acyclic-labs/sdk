use acyclic_fs::model::{GenerationSelector, VolumeConfig};
use acyclic_fs::{
    CancellationToken, Checkout, Fs, MemoryAuthorityBackend, MemoryObjectBackend, Volume,
};
use acyclic_fs_work::performance::WorkBudget;

#[path = "checkout_defaults.rs"]
mod checkout_defaults;
pub(super) use checkout_defaults::config;
use checkout_defaults::writable_pinned;

pub(super) type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
pub(super) type TestCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;
type TestVolume = Volume<MemoryAuthorityBackend, MemoryObjectBackend>;

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
            writable_pinned(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value)
}
