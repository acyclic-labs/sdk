//! content fork protocol.
use super::*;
/// Stream history proof scoped to the already-open parent aggregate.
pub struct StreamHistoryForkVerifier {
    pub(in crate::fork) provider: ProviderRef,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::fork) _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl StreamHistoryForkVerifier {
    /// Registers the exact Stream provider using the caller's optional ledger.
    pub fn new(provider: &ProviderRef, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        let mut owned = CodecQuota::observed(work)?;
        provider.validate(owned.work())?;
        if provider.family() != "stream" {
            return Err(boundary_error(owned.work(), crate::wire::ErrorCode::Invalid,
                "history verifier requires a Stream provider"));
        }
        let provider = crate::core::copy_provider(provider, &mut owned)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _native_metadata = {
            let bytes = owned.retained();
            owned.work().as_deref_mut().filter(|io| io.original_native_work().is_some())
                .map(|io| io.take_native_custody(bytes)).transpose()?
        };
        owned.transfer();
        Ok(Self { provider,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_metadata })
    }

    /// Shares the same funded provider identity; Arc's private backing is opaque.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn into_native_shared(
        self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>> {
        admit_native_verifier_share(self._native_metadata.as_ref(), work, std::mem::size_of::<Self>())?;
        Ok(std::sync::Arc::new(self))
    }
}

impl ForkSeedVerifier for StreamHistoryForkVerifier {
    fn provider(&self) -> &ProviderRef { &self.provider }
    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            seed.validate(work)?;
            for capture in &seed.resources {
                for revision in [&capture.source, &capture.revision] {
                    if revision.provider() == &self.provider
                        && !matches!(revision, ResourceRevision::History(_))
                    {
                        return Err(boundary_error(work, crate::wire::ErrorCode::Unsupported,
                            "Stream history verifier cannot prove this resource"));
                    }
                }
            }
            Ok(())
        })
    }
}
