use crate::{B, TestCheckout, TestResult, TestVolume, allow, open_checkout, writable_pinned};
use acyclic_fs::{
    CancellationToken, Fs, FsError, Head, LivePublicationObservation, PublicationPermit,
    VolumeConfig,
};
use acyclic_fs_format::identity::OperationId;
use acyclic_stream::{MemoryStream, ProviderOwner};
use std::sync::Arc;

/// The caller-owned providers shared by independent filesystem owners in publication tests.
pub struct MemoryProviders {
    /// The authority stream, retained to inspect durable records.
    pub stream: ProviderOwner<MemoryStream>,
    objects: Arc<acyclic_objects::MemoryObjects>,
}

impl MemoryProviders {
    /// Creates another filesystem owner over the same streams and objects.
    pub fn filesystem(&self) -> acyclic_fs::MemoryFs {
        Fs::from_memory_providers(
            ProviderOwner::clone(&self.stream),
            Arc::clone(&self.objects),
        )
    }
}

/// Creates a pinned checkout while retaining both providers used by its filesystem.
pub async fn fixture_with_providers(
    config: VolumeConfig,
    cancel: &CancellationToken,
) -> TestResult<(MemoryProviders, TestVolume, TestCheckout)> {
    #[cfg(not(target_arch = "wasm32"))]
    acyclic_native_runtime::initialize_native_io_services()?;
    let providers = MemoryProviders {
        stream: ProviderOwner::new(MemoryStream::default()),
        objects: Arc::new(acyclic_objects::MemoryObjects::memory()),
    };
    let fs = providers.filesystem();
    let volume = fs.create_volume(config, B, cancel).await?.value;
    let checkout = open_checkout(&volume, writable_pinned(), cancel).await?;
    Ok((providers, volume, checkout))
}

/// Commits a seed checkout and returns its published authority head.
pub async fn commit_head(
    checkout: &mut TestCheckout,
    cancel: &CancellationToken,
) -> TestResult<Head> {
    let outcome = checkout
        .commit(
            OperationId::new(),
            acyclic_fs::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
            B,
            cancel,
            allow::<FsError>,
        )
        .await?;
    match outcome.value {
        LivePublicationObservation::Committed { head, .. } => Ok(head),
        outcome => Err(format!("seed publication did not commit: {outcome:?}").into()),
    }
}
