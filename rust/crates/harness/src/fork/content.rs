//! content fork protocol.
use super::*;
/// Fork barrier for immutable file-only providers such as Objects. Mutable
pub struct ContentForkVerifier {
    pub(in crate::fork) provider: ProviderRef,
    pub(in crate::fork) content: std::sync::Arc<dyn ContentResidencyVerifier>,
}

impl ContentForkVerifier {
    /// Binds one exact provider identity to its authenticated content reader.
    pub fn new(
        provider: ProviderRef,
        content: std::sync::Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        provider.validate(&mut None)?;
        Ok(Self { provider, content })
    }
}

impl ForkSeedVerifier for ContentForkVerifier {
    fn provider(&self) -> &ProviderRef { &self.provider }
    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            seed.validate(work)?;
            if seed.child_private_volume.provider() == &self.provider
                || seed.resources.iter().any(|capture| {
                    capture.source.provider() == &self.provider
                        || capture.revision.provider() == &self.provider
                })
            {
                return Err(boundary_error(work, crate::wire::ErrorCode::Unsupported,
                    "content-only provider cannot attest mutable fork resources"));
            }
            let mut owned = CodecQuota::observed(work)?;
            let mut checked = BTreeSet::new();
            for file in seed.inherited_context.iter()
                .chain(seed.reference_grants.iter().map(|grant| &grant.file))
                .chain(seed.attachment_manifests.iter())
            {
                if file.volume().provider() != &self.provider {
                    continue;
                }
                let capability = owned.file_digest(file)?;
                if checked.contains(&capability) {
                    continue;
                }
                owned.slots::<[u8; 32]>(1)?;
                checked.insert(capability);
                match owned.work().as_deref_mut() {
                    Some(observer) => crate::store::history::verify_content_with_work(
                        observer, self.content.as_ref(), file).await?,
                    None => self.content.verify(file, file.descriptor().byte_length(), None)?.await?,
                }
            }
            Ok(())
        })
    }
    fn read_manifest<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        admit_fork_future(work, |work| async move {
            if manifest.volume().provider() != &self.provider {
                return Err(boundary_error(work, crate::wire::ErrorCode::Unauthorized,
                    "fork manifest belongs to another provider"));
            }
            match work.as_deref_mut() {
                Some(observer) => crate::store::history::read_content_with_work(
                    observer, self.content.as_ref(), manifest).await,
                None => self.content.read(manifest, manifest.descriptor().byte_length(), None)?.await.map(|bytes| (bytes, 0)),
            }
        })
    }
    fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn HistoryIo>) -> Result<HistoryContentFuture<'a, ()>> {
        admit_fork_future(work, |work| async move {
            if file.volume().provider() != &self.provider {
                return Err(boundary_error(work, crate::wire::ErrorCode::Unauthorized,
                    "fork file belongs to another provider"));
            }
            match work.as_deref_mut() {
                Some(observer) => crate::store::history::verify_content_with_work(
                    observer, self.content.as_ref(), file).await,
                None => self.content.verify(file, file.descriptor().byte_length(), None)?.await,
            }
        })
    }
}

/// Stream history proof scoped to the parent aggregate already opened by the admission path. `ForkSeed::validate` checks its exact path and revision.
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
