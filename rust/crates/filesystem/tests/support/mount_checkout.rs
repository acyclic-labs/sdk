use acyclic_fs::{CancellationToken, VolumeConfig};

#[path = "checkout_defaults.rs"]
mod checkout_defaults;
pub(super) use checkout_defaults::config;
pub(super) use checkout_defaults::writable_pinned;

#[path = "memory_checkout.rs"]
mod memory_checkout;
pub(super) use memory_checkout::{TestCheckout, TestResult, open_checkout, volume};

pub(super) async fn checkout() -> TestResult<TestCheckout> {
    checkout_with_config(config()).await
}

pub(super) async fn checkout_with_config(config: VolumeConfig) -> TestResult<TestCheckout> {
    let cancel = CancellationToken::new();
    Ok(open_checkout(&volume(config, &cancel).await?, writable_pinned(), &cancel).await?)
}
