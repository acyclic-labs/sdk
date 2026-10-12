//! verification fork protocol.
use super::*;
/// Exhaustive provider dispatcher.
pub struct CompositeForkVerifier {
    pub(in crate::fork) providers: Vec<(ProviderRef, std::sync::Arc<dyn ForkSeedVerifier>)>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::fork) _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::fork) fn admit_verifier_allocation(
    inline_bytes: usize,
    io: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let Some(work) = io.as_deref().and_then(HistoryIo::original_native_work) else {
        return Ok(());
    };
    work.admit_retained(inline_bytes as u64)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: None,
        })?;
    work.charge(acyclic_fs::WorkCounters {
        allocation_operations: 1,
        ..acyclic_fs::WorkCounters::default()
    })
    .map_err(|cause| Error::NativeWork {
        cause,
        custody: None,
    })
}

impl CompositeForkVerifier {
    /// Registers exact provider contracts using the caller's optional ledger.
    pub fn new(
        verifiers: &[std::sync::Arc<dyn ForkSeedVerifier>],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<std::sync::Arc<Self>> {
        let mut owned = CodecQuota::observed(work)?;
        let mut providers = crate::core::reserve_copy_vector::<(
            ProviderRef,
            std::sync::Arc<dyn ForkSeedVerifier>,
        )>(verifiers.len(), &mut owned)?;
        for verifier in verifiers {
            owned.checkpoint()?;
            verifier.provider().validate(owned.work())?;
            for (prior, _) in &providers {
                owned.checkpoint()?;
                if prior == verifier.provider() {
                    return Err(owned.invalid("fork provider verifier is registered twice"));
                }
            }
            providers.push((
                crate::core::copy_provider(verifier.provider(), &mut owned)?,
                std::sync::Arc::clone(verifier),
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _native_metadata = {
            let bytes = owned.retained();
            owned
                .work()
                .as_deref_mut()
                .filter(|io| io.original_native_work().is_some())
                .map(|io| io.take_native_custody(bytes))
                .transpose()?
        };
        owned.transfer();
        let verifier = Self {
            providers,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_metadata,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        admit_verifier_allocation(std::mem::size_of::<Self>(), owned.work())?;
        Ok(std::sync::Arc::new(verifier))
    }

    /// Fences the child-private writer, then verifies all selected providers.
    pub(crate) async fn activate(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<ObservedForkPublicationGuard> {
        seed.validate(work)?;
        let private = self.provider(
            seed.child_private_volume.provider(),
            &mut CodecQuota::observed(work)?,
        )?;
        let guard = acquire_fork_fence(private.as_ref(), seed, work).await?;
        if let Err(error) = self.verify(seed, work).await {
            let released = release_fork_guard_with_work(guard.guard.as_ref(), work).await;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let preserve_primary = work
                .as_ref()
                .is_some_and(|work| work.original_native_work().is_some());
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let preserve_primary = false;
            if released.is_err() && !preserve_primary {
                return Err(Error::Indeterminate(seed.operation_id));
            }
            return Err(error);
        }
        Ok(guard)
    }

    /// Releases a recovered fence after Stream proves publication terminal.
    pub async fn release_private_fence(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate(work)?;
        let private = self.provider(
            seed.child_private_volume.provider(),
            &mut CodecQuota::observed(work)?,
        )?;
        match acquire_fork_fence(private.as_ref(), seed, work).await {
            Ok(guard) => release_fork_guard_with_work(guard.guard.as_ref(), work).await,
            // A different operation can acquire the gate after this fork's terminal release.
            Err(error) if error.code() == crate::wire::ErrorCode::Conflict => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Verifies every source, child, private, inherited, and attested provider.
    pub async fn verify(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        seed.validate(work)?;
        let mut owned = CodecQuota::observed(work)?;
        let providers =
            || {
                std::iter::once(seed.child_private_volume.provider())
                    .chain(
                        seed.inherited_context
                            .iter()
                            .map(|file| file.volume().provider()),
                    )
                    .chain(
                        seed.reference_grants
                            .iter()
                            .map(|grant| grant.file.volume().provider()),
                    )
                    .chain(
                        seed.attachment_manifests
                            .iter()
                            .map(|file| file.volume().provider()),
                    )
                    .chain(seed.resources.iter().flat_map(|capture| {
                        [capture.source.provider(), capture.revision.provider()]
                    }))
                    .chain(seed.boundary.iter().map(|boundary| &boundary.provider))
            };
        for provider in providers() {
            self.provider(provider, &mut owned)?;
        }
        for (identity, verifier) in &self.providers {
            let mut required = false;
            for provider in providers() {
                owned.checkpoint()?;
                required |= provider == identity;
            }
            if !required {
                continue;
            }
            let future = verifier.verify(seed, owned.work())?;
            let bytes = frame_bytes(&future);
            let result = future.await;
            release_frame(result, bytes, owned.work())?;
            if let Some(boundary) = &seed.boundary
                && &boundary.provider == identity
            {
                let future = verifier.verify_boundary(boundary, owned.work())?;
                let bytes = frame_bytes(&future);
                let result = future.await;
                release_frame(result, bytes, owned.work())?;
            }
        }
        self.verify_references(seed, &mut owned).await
    }

    pub(in crate::fork) fn provider<'a>(
        &'a self,
        provider: &ProviderRef,
        owned: &mut CodecQuota<'_, '_>,
    ) -> Result<&'a std::sync::Arc<dyn ForkSeedVerifier>> {
        for (identity, verifier) in &self.providers {
            owned.checkpoint()?;
            if identity == provider {
                return Ok(verifier);
            }
        }
        Err(boundary_error(
            owned.work(),
            crate::wire::ErrorCode::Unsupported,
            "fork resource has no exact provider verifier",
        ))
    }

    // Verify each exact file once across direct refs and complete manifests.
    async fn verify_references(
        &self,
        seed: &ForkSeed,
        owned: &mut CodecQuota<'_, '_>,
    ) -> Result<()> {
        let mut verified_files = BTreeSet::new();
        let mut reference_bytes = BTreeMap::new();
        for file in seed
            .inherited_context
            .iter()
            .chain(seed.reference_grants.iter().map(|grant| &grant.file))
        {
            self.verify_file(seed, file, &mut verified_files, &mut reference_bytes, owned)
                .await?;
        }
        let mut manifest_members = 0_usize;
        for manifest in &seed.attachment_manifests {
            self.verify_file(
                seed,
                manifest,
                &mut verified_files,
                &mut reference_bytes,
                owned,
            )
            .await?;
            let verifier = self.provider(manifest.volume().provider(), owned)?;
            let (bytes, retained) =
                read_fork_manifest(verifier.as_ref(), seed, manifest, owned.work()).await?;
            if let Err(error) = owned.adopt(retained) {
                drop(bytes);
                if let Some(observer) = owned.work().as_deref_mut() {
                    let _ = observer.release_owned(retained);
                }
                return Err(error);
            }
            let items = decode_fork_manifest(manifest, bytes.as_ref(), owned);
            drop(bytes);
            owned.release(retained)?;
            let items = items?;
            manifest_members = manifest_members
                .checked_add(items.len())
                .ok_or_else(|| Error::Invalid("fork manifest membership count overflow".into()))?;
            if manifest_members as u64 > MAX_FORK_REFERENCES as u64 {
                return Err(Error::Invalid(
                    "fork manifests exceed aggregate member limit".into(),
                ));
            }
            Self::verify_manifest_grants(seed, manifest, &items, owned)?;
            for item in items {
                self.verify_file(
                    seed,
                    &item.file,
                    &mut verified_files,
                    &mut reference_bytes,
                    owned,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn verify_file<'a>(
        &'a self,
        seed: &ForkSeed,
        file: &FileRef,
        verified_files: &mut BTreeSet<[u8; 32]>,
        reference_bytes: &mut BTreeMap<&'a ProviderRef, u64>,
        owned: &mut CodecQuota<'_, '_>,
    ) -> Result<()> {
        let capability = owned.file_digest(file)?;
        if verified_files.contains(&capability) {
            return Ok(());
        }
        owned.slots::<[u8; 32]>(1)?;
        verified_files.insert(capability);
        let verifier = self.provider(file.volume().provider(), owned)?;
        reserve_reference_bytes(reference_bytes, verifier.as_ref(), file, owned)?;
        let future = verifier.verify_file(seed, file, owned.work())?;
        let frame = frame_bytes(&future);
        let result = future.await;
        release_frame(result, frame, owned.work())
    }

    pub(in crate::fork) fn verify_manifest_grants(
        seed: &ForkSeed,
        manifest: &FileRef,
        items: &[crate::conversation::Attachment],
        owned: &mut CodecQuota<'_, '_>,
    ) -> Result<()> {
        for grant in &seed.reference_grants {
            owned.checkpoint()?;
            if grant.attachment_manifest.as_ref() == Some(manifest) && grant.file != *manifest {
                let mut member = false;
                for item in items {
                    owned.checkpoint()?;
                    member |= item.file == grant.file;
                }
                if !member {
                    return Err(owned.invalid("fork reference is not in its published manifest"));
                }
                if !seed.allows_manifest(manifest, grant.reader, owned)? {
                    return Err(owned.invalid("fork omits a manifest attachment read grant"));
                }
            }
        }
        Ok(())
    }
}
