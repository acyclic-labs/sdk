use super::*;

type LocalCheckout =
    acyclic_fs::Checkout<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

pub(super) async fn reopen_volume(
    store: &Path,
    volume: VolumeId,
    cancellation: &CancellationToken,
) -> Result<acyclic_fs::LocalVolume, Failure> {
    let engine = LocalFs::local(local_options(store)?)
        .await
        .map_err(engine_err("reopen store"))?;
    engine
        .open_volume(volume, WorkCounters::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value)
        .map_err(engine_err("reopen volume"))
}

pub(super) async fn reopen_checkout(
    store: &Path,
    volume: VolumeId,
    selector: GenerationSelector,
    mode: CheckoutMode,
    cancellation: &CancellationToken,
) -> Result<LocalCheckout, Failure> {
    // Checkout owns Volume, which retains the filesystem's shared state.
    reopen_volume(store, volume, cancellation)
        .await?
        .checkout(selector, mode, WorkCounters::UNBOUNDED, cancellation)
        .await
        .map(|receipt| receipt.value)
        .map_err(engine_err("checkout"))
}
