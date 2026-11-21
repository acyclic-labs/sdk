use acyclic_fs::{CancellationToken, VolumeConfig};

pub(super) use super::checkout_defaults::{config, writable_pinned};

pub(super) use super::memory_checkout::{TestCheckout, TestResult, open_checkout, volume};

pub(super) async fn checkout() -> TestResult<TestCheckout> {
    checkout_with_config(config()).await
}

pub(super) async fn checkout_with_config(config: VolumeConfig) -> TestResult<TestCheckout> {
    let cancel = CancellationToken::new();
    Ok(open_checkout(&volume(config, &cancel).await?, writable_pinned(), &cancel).await?)
}
